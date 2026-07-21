package riverpro

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/divyam234/riverpro/riverworkflow"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/require"
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

type periodicTestArgs struct {
	Value string `json:"value"`
}

func (periodicTestArgs) Kind() string { return "periodic_test" }
func (periodicTestArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: "periodic", Priority: 3, MaxAttempts: 7, Tags: []string{"typed"}}
}

func TestBuildPeriodicJobUpsertParamsUsesTypedArgsAndInsertDefaults(t *testing.T) {
	params, err := buildPeriodicJobUpsertParams(&PeriodicJobUpsertOpts{
		ID:      "typed-periodic",
		JobArgs: periodicTestArgs{Value: "hello"},
		Schedule: &PeriodicJobSchedule{
			CronExpression: "0 */2 * * *",
		},
	}, &Config{Config: river.Config{Schema: "custom"}})
	if err != nil {
		t.Fatalf("buildPeriodicJobUpsertParams: %v", err)
	}
	if params.Kind != "periodic_test" || string(params.Args) != `{"value":"hello"}` {
		t.Fatalf("unexpected encoded args: kind=%q args=%s", params.Kind, params.Args)
	}
	if params.Queue != "periodic" || params.Priority != 3 || params.MaxAttempts != 7 {
		t.Fatalf("unexpected insert defaults: %#v", params)
	}
	if len(params.Tags) != 1 || params.Tags[0] != "typed" {
		t.Fatalf("unexpected tags: %#v", params.Tags)
	}
	if params.NextRunAt.IsZero() {
		t.Fatal("cron upsert should calculate an initial next run")
	}
	if params.ResetNextRunAt {
		t.Fatal("implicit cron next run should not force replacement on an existing row")
	}
	if params.Schema != "custom" {
		t.Fatalf("unexpected schema: %q", params.Schema)
	}
}

func TestBuildPeriodicJobUpsertParamsExplicitNextRunResetsCursor(t *testing.T) {
	nextRunAt := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	params, err := buildPeriodicJobUpsertParams(&PeriodicJobUpsertOpts{
		ID:      "explicit-next-run",
		JobArgs: periodicTestArgs{},
		Schedule: &PeriodicJobSchedule{
			CronExpression: "0 */2 * * *",
			NextRunAt:      nextRunAt,
		},
	}, nil)
	if err != nil {
		t.Fatalf("buildPeriodicJobUpsertParams: %v", err)
	}
	if !params.ResetNextRunAt || !params.NextRunAt.Equal(nextRunAt) {
		t.Fatalf("explicit next run not preserved: %#v", params)
	}
}

func TestBuildPeriodicJobUpdateParamsPatchesOnlySelectedFields(t *testing.T) {
	params, err := buildPeriodicJobUpdateParams("periodic-update", &PeriodicJobUpdateOpts{
		JobArgs: periodicTestArgs{Value: "updated"},
	}, nil)
	if err != nil {
		t.Fatalf("buildPeriodicJobUpdateParams: %v", err)
	}
	if !params.SetArgs || params.Kind != "periodic_test" || string(params.Args) != `{"value":"updated"}` {
		t.Fatalf("unexpected args patch: %#v", params)
	}
	if params.SetQueue || params.SetPriority || params.SetMaxAttempts || params.SetTags || params.SetSchedule {
		t.Fatalf("unexpected fields selected: %#v", params)
	}

	if _, err := buildPeriodicJobUpdateParams("periodic-update", &PeriodicJobUpdateOpts{}, nil); err == nil {
		t.Fatal("empty periodic update should fail validation")
	}
}

func TestNormalizeProQueues(t *testing.T) {
	t.Parallel()

	config := &Config{Config: river.Config{Workers: river.NewWorkers()}, ProQueues: map[string]QueueConfig{
		"limited": {
			MaxWorkers:        7,
			FetchCooldown:     time.Second,
			FetchPollInterval: 2 * time.Second,
			Concurrency:       ConcurrencyConfig{GlobalLimit: 3, LocalLimit: 2},
		},
	}}
	clone := cloneConfig(config)
	require.NoError(t, normalizeProQueues(clone))
	require.Nil(t, config.Queues, "normalization must not mutate the caller's config")
	require.Equal(t, river.QueueConfig{MaxWorkers: 7, FetchCooldown: time.Second, FetchPollInterval: 2 * time.Second}, clone.Queues["limited"])
}

func TestNormalizeProQueuesValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config *Config
		match  string
	}{
		{name: "empty queue", config: &Config{ProQueues: map[string]QueueConfig{"": {}}}, match: "empty queue name"},
		{name: "negative global", config: &Config{ProQueues: map[string]QueueConfig{"q": {Concurrency: ConcurrencyConfig{GlobalLimit: -1}}}}, match: "GlobalLimit"},
		{name: "negative local", config: &Config{ProQueues: map[string]QueueConfig{"q": {Concurrency: ConcurrencyConfig{LocalLimit: -1}}}}, match: "LocalLimit"},
		{name: "empty arg", config: &Config{ProQueues: map[string]QueueConfig{"q": {Concurrency: ConcurrencyConfig{Partition: PartitionConfig{ByArgs: []string{""}}}}}}, match: "must not be empty"},
		{name: "duplicate arg", config: &Config{ProQueues: map[string]QueueConfig{"q": {Concurrency: ConcurrencyConfig{Partition: PartitionConfig{ByArgs: []string{"tenant", "tenant"}}}}}}, match: "duplicated"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorContains(t, normalizeProQueues(tt.config), tt.match)
		})
	}
}

func TestProProducerStatePartitionCounts(t *testing.T) {
	t.Parallel()

	state := &proProducerState{partition: PartitionConfig{ByKind: true, ByArgs: []string{"tenant"}}, running: map[string]int32{}}
	jobs := []*rivertype.JobRow{
		{ID: 1, Queue: "default", Kind: "email", EncodedArgs: []byte(`{"other": 1, "tenant": "a"}`)},
		{ID: 2, Queue: "default", Kind: "email", EncodedArgs: []byte(`{"tenant": "a"}`)},
		{ID: 3, Queue: "default", Kind: "email", EncodedArgs: []byte(`{"tenant": "b"}`)},
	}
	state.add(jobs)
	keys, counts := state.snapshot()
	require.Equal(t, []string{`kind=email|args={"tenant": "a"}`, `kind=email|args={"tenant": "b"}`}, keys)
	require.Equal(t, []int32{2, 1}, counts)

	state.JobFinish(jobs[0])
	keys, counts = state.snapshot()
	require.Equal(t, []string{`kind=email|args={"tenant": "a"}`, `kind=email|args={"tenant": "b"}`}, keys)
	require.Equal(t, []int32{1, 1}, counts)
}

func TestSequenceShouldContinue(t *testing.T) {
	t.Parallel()

	require.True(t, sequenceShouldContinue(rivertype.JobStateCompleted, nil))
	require.False(t, sequenceShouldContinue(rivertype.JobStateCancelled, nil))
	require.False(t, sequenceShouldContinue(rivertype.JobStateDiscarded, nil))
	require.True(t, sequenceShouldContinue(rivertype.JobStateCancelled, map[string]any{metadataKeySequenceContinueOnCancelled: true}))
	require.True(t, sequenceShouldContinue(rivertype.JobStateDiscarded, map[string]any{metadataKeySequenceContinueOnDiscarded: true}))
	require.False(t, sequenceShouldContinue(rivertype.JobStateRetryable, map[string]any{metadataKeySequenceContinueOnCancelled: true, metadataKeySequenceContinueOnDiscarded: true}))
}

func TestSequenceKeyOptions(t *testing.T) {
	t.Parallel()

	type taggedArgs struct {
		CustomerID string `json:"customer_id" river:"sequence"`
		TraceID    string `json:"trace_id"`
	}
	args := taggedArgs{CustomerID: "customer-1", TraceID: "trace-1"}
	encoded := []byte(`{"customer_id":"customer-1","trace_id":"trace-1"}`)

	defaultKey := sequenceKey("queue-a", "kind-a", encoded, SequenceOpts{}, args)
	require.Equal(t, stableKey("kind=kind-a"), defaultKey)

	byArgsKey := sequenceKey("queue-a", "kind-a", encoded, SequenceOpts{ByArgs: true}, args)
	require.Equal(t, stableKey("kind=kind-a", `args={"customer_id":"customer-1"}`), byArgsKey)

	crossKindKey := sequenceKey("queue-a", "kind-a", encoded, SequenceOpts{ByArgs: true, ExcludeKind: true}, args)
	require.Equal(t, stableKey(`args={"customer_id":"customer-1"}`), crossKindKey)

	byQueueKey := sequenceKey("queue-a", "kind-a", encoded, SequenceOpts{ByQueue: true}, args)
	require.Equal(t, stableKey("queue=queue-a", "kind=kind-a"), byQueueKey)
}

func TestRandomWorkflowIDIsCanonicalULID(t *testing.T) {
	t.Parallel()

	id := randomWorkflowID()
	require.Len(t, id, 26)
	require.Regexp(t, `^[0-7][0-9A-HJKMNP-TV-Z]{25}$`, id)
}
