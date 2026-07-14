package riverpro

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/divyam234/riverpro/riverworkflow"
	"github.com/riverqueue/river"
)

type testArgs struct{ KindValue string }

func (a testArgs) Kind() string { return a.KindValue }

type ephemeralTestArgs struct{}

func (ephemeralTestArgs) Kind() string                 { return "ephemeral-test" }
func (ephemeralTestArgs) EphemeralOpts() EphemeralOpts { return EphemeralOpts{} }

var _ JobArgsWithEphemeralOpts = ephemeralTestArgs{}

func TestWorkflowPrepareMetadataAndPending(t *testing.T) {
	w := NewWorkflow(&WorkflowOpts{ID: "wf-id", Name: "wf-name"})
	w.Add("root", testArgs{KindValue: "root-kind"}, nil, nil)
	w.Add("child", testArgs{KindValue: "child-kind"}, nil, &WorkflowTaskOpts{Deps: []string{"root"}})

	res, err := w.Prepare(context.Background())
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if len(res.Jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(res.Jobs))
	}
	if res.Jobs[0].InsertOpts.Pending {
		t.Fatal("root task should not be pending")
	}
	if !res.Jobs[1].InsertOpts.Pending {
		t.Fatal("dependent task should be pending")
	}

	var meta map[string]any
	if err := json.Unmarshal(res.Jobs[1].InsertOpts.Metadata, &meta); err != nil {
		t.Fatalf("metadata JSON: %v", err)
	}
	if meta[riverworkflow.MetadataKeyWorkflowID] != "wf-id" || meta[riverworkflow.MetadataKeyWorkflowName] != "wf-name" || meta[riverworkflow.MetadataKeyWorkflowTask] != "child" {
		t.Fatalf("unexpected metadata: %#v", meta)
	}
	deps := meta[riverworkflow.MetadataKeyWorkflowDeps].([]any)
	if len(deps) != 1 || deps[0] != "root" {
		t.Fatalf("unexpected deps metadata: %#v", meta[riverworkflow.MetadataKeyWorkflowDeps])
	}
}

func TestWorkflowPrepareValidation(t *testing.T) {
	t.Run("duplicate", func(t *testing.T) {
		w := NewWorkflow(&WorkflowOpts{ID: "wf-dup"})
		w.Add("same", testArgs{KindValue: "a"}, nil, nil)
		w.Add("same", testArgs{KindValue: "b"}, nil, nil)
		_, err := w.Prepare(context.Background())
		var duplicate *DuplicateTaskError
		if !errors.As(err, &duplicate) {
			t.Fatalf("expected DuplicateTaskError, got %T %v", err, err)
		}
	})

	t.Run("missing dependency", func(t *testing.T) {
		w := NewWorkflow(&WorkflowOpts{ID: "wf-missing"})
		w.Add("child", testArgs{KindValue: "child"}, nil, &WorkflowTaskOpts{Deps: []string{"root"}})
		_, err := w.Prepare(context.Background())
		var missing *MissingDependencyError
		if !errors.As(err, &missing) {
			t.Fatalf("expected MissingDependencyError, got %T %v", err, err)
		}
	})

	t.Run("cycle", func(t *testing.T) {
		w := NewWorkflow(&WorkflowOpts{ID: "wf-cycle"})
		w.Add("a", testArgs{KindValue: "a"}, nil, &WorkflowTaskOpts{Deps: []string{"b"}})
		w.Add("b", testArgs{KindValue: "b"}, nil, &WorkflowTaskOpts{Deps: []string{"a"}})
		_, err := w.Prepare(context.Background())
		var cycle *DependencyCycleError
		if !errors.As(err, &cycle) {
			t.Fatalf("expected DependencyCycleError, got %T %v", err, err)
		}
	})
}

func TestConcurrencyConfigPublicAPI(t *testing.T) {
	cfg := ConcurrencyConfig{
		GlobalLimit: 2,
		LocalLimit:  1,
		Partition:   PartitionConfig{ByKind: true, ByArgs: []string{"customer_id"}},
	}
	if cfg.GlobalLimit != 2 || cfg.LocalLimit != 1 || !cfg.Partition.ByKind || len(cfg.Partition.ByArgs) != 1 {
		t.Fatalf("unexpected concurrency config: %#v", cfg)
	}
}

func TestConfigWithDefaultsWorkflowAwareRetention(t *testing.T) {
	cfg := (&Config{WorkflowAwareRetention: true}).WithDefaults()
	if !cfg.WorkflowAwareRetention {
		t.Fatal("WorkflowAwareRetention should be preserved through WithDefaults")
	}
	if cfg.WorkflowCancelledRetentionPeriod == 0 {
		t.Fatal("WorkflowCancelledRetentionPeriod should have a default")
	}
	if cfg.WorkflowClosedRetentionPeriod == 0 {
		t.Fatal("WorkflowClosedRetentionPeriod should have a default")
	}

	cfg = (&Config{}).WithDefaults()
	if cfg.WorkflowAwareRetention {
		t.Fatal("WorkflowAwareRetention should default to false")
	}
}

func TestConfigWithDefaultsProducerRetention(t *testing.T) {
	cfg := (&Config{ProducerRetentionEnabled: true, ProducerStaleRetentionPeriod: 5 * time.Minute, ProducerRetentionInterval: time.Minute}).WithDefaults()
	if !cfg.ProducerRetentionEnabled {
		t.Fatal("ProducerRetentionEnabled should be preserved through WithDefaults")
	}
	if cfg.ProducerStaleRetentionPeriod != 5*time.Minute {
		t.Fatalf("ProducerStaleRetentionPeriod should be preserved, got %s", cfg.ProducerStaleRetentionPeriod)
	}
	if cfg.ProducerRetentionInterval != time.Minute {
		t.Fatalf("ProducerRetentionInterval should be preserved, got %s", cfg.ProducerRetentionInterval)
	}

	cfg = (&Config{}).WithDefaults()
	if cfg.ProducerRetentionEnabled {
		t.Fatal("ProducerRetentionEnabled should default to false")
	}
	if cfg.ProducerStaleRetentionPeriod != 30*time.Minute {
		t.Fatalf("ProducerStaleRetentionPeriod should default to 30m, got %s", cfg.ProducerStaleRetentionPeriod)
	}
	if cfg.ProducerRetentionInterval != 5*time.Minute {
		t.Fatalf("ProducerRetentionInterval should default to 5m, got %s", cfg.ProducerRetentionInterval)
	}
}

func TestWorkflowSignalsPublicAPI(t *testing.T) {
	ctx := context.Background()
	workflow := NewWorkflow(&WorkflowOpts{ID: "wf-signals-api"})
	signals := workflow.Signals()
	if signals == nil {
		t.Fatal("expected workflow signals handle")
	}

	_, err := signals.Emit(ctx, "manual_review", map[string]any{"approved": true}, &WorkflowSignalEmitOpts{IdempotencyKey: "request-id"})
	if err == nil {
		t.Fatal("expected Emit to require a configured Pro driver")
	}
	_, err = signals.List(ctx, &WorkflowSignalListParams{Key: "manual_review"})
	if err == nil {
		t.Fatal("expected List to require a configured Pro driver")
	}
	_, err = signals.ListForTask(ctx, "approve_order", &WorkflowSignalListForTaskParams{Key: "manual_review"})
	if err == nil {
		t.Fatal("expected ListForTask to require a configured Pro driver")
	}
	_, err = signals.LatestForTask(ctx, "approve_order", "manual_review", &WorkflowSignalLatestForTaskOpts{})
	if err == nil {
		t.Fatal("expected LatestForTask to require a configured Pro driver")
	}
}

func TestWorkflowWaitDiagnosticsPublicAPI(t *testing.T) {
	workflow := NewWorkflow(&WorkflowOpts{ID: "wf-diagnostics-api"})
	_, err := workflow.WaitDiagnostics(context.Background(), "approve_order", &WorkflowWaitDiagnosticsOpts{SignalScanLimit: 10})
	if err == nil {
		t.Fatal("expected WaitDiagnostics to require a configured workflow task")
	}
}

func TestClientContextHelpersAndNilSafety(t *testing.T) {
	ctx := context.Background()
	if got, err := ClientFromContextSafely[any](ctx); err == nil || got != nil {
		t.Fatalf("expected missing client error, got client=%#v err=%v", got, err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("ClientFromContext should panic when no client is in context")
			}
		}()
		_ = ClientFromContext[any](ctx)
	}()

	var nilClient *Client[any]
	if nilClient.ProExecutor() != nil {
		t.Fatal("nil client should not expose a pro executor")
	}
	if nilClient.Schema() != "" {
		t.Fatalf("nil client schema should be empty, got %q", nilClient.Schema())
	}
	if nilClient.Queues() != nil {
		t.Fatal("nil client queues should be nil")
	}
	if err := nilClient.Start(ctx); err == nil {
		t.Fatal("nil client Start should return an error")
	}
}

func TestClientContextRoundTripAndSmallHelpers(t *testing.T) {
	client := &Client[any]{config: &Config{Config: river.Config{Schema: "schema-a"}}}
	ctx := ContextWithClient(context.Background(), client)
	got, err := ClientFromContextSafely[any](ctx)
	if err != nil {
		t.Fatalf("ClientFromContextSafely: %v", err)
	}
	if got != client || ClientFromContext[any](ctx) != client {
		t.Fatal("client context round trip returned the wrong client")
	}
	if client.Schema() != "schema-a" {
		t.Fatalf("unexpected schema: %q", client.Schema())
	}

	names := ReindexerIndexNamesDefault()
	want := []string{"river_job_kind", "river_job_queue", "river_job_state_and_finalized_at", "river_job_workflow"}
	if len(names) != len(want) {
		t.Fatalf("unexpected reindexer names: %#v", names)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("unexpected reindexer names: %#v", names)
		}
	}
	names[0] = "mutated"
	if ReindexerIndexNamesDefault()[0] != "river_job_kind" {
		t.Fatal("ReindexerIndexNamesDefault should return a fresh slice")
	}

	missingOutput := &TaskHasNoOutputError{JobID: 12, TaskName: "task-a", WorkflowID: "wf-a"}
	if !errors.Is(missingOutput, &TaskHasNoOutputError{}) {
		t.Fatal("TaskHasNoOutputError should support errors.Is")
	}
	if missingOutput.Error() != `riverpro: workflow task "task-a" has no output` {
		t.Fatalf("unexpected TaskHasNoOutputError message: %q", missingOutput.Error())
	}
}
