package riverpro

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/divyam234/riverpro/riverworkflow"
)

type testArgs struct{ KindValue string }

func (a testArgs) Kind() string { return a.KindValue }

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
