package driver

import (
	"context"
	"io/fs"
	"testing"
	"time"

	"github.com/riverqueue/river/riverdriver"
)

func TestPublicExecutorInterfaces(t *testing.T) {
	var _ ProExecutor = (*Executor)(nil)
	var _ ProExecutorTx = (*ExecutorTx)(nil)
	var _ riverdriver.Executor = (*Executor)(nil)
	var _ riverdriver.ExecutorTx = (*ExecutorTx)(nil)
}

func TestMigrationLinesAndFS(t *testing.T) {
	w := NewWrapper[any](nil)
	lines := w.GetMigrationLines()
	for _, want := range []string{MigrationLinePro} {
		found := false
		for _, got := range lines {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing migration line %q in %#v", want, lines)
		}
	}
	for _, removed := range []string{"sequence", "workflow"} {
		for _, got := range lines {
			if got == removed {
				t.Fatalf("deprecated migration line %q should not be exposed in %#v", removed, lines)
			}
		}
		if fsys := w.GetMigrationFS(removed); fsys != nil {
			t.Fatalf("deprecated migration line %q should not have migration FS", removed)
		}
	}

	migrationFS := w.GetMigrationFS(MigrationLinePro)
	if migrationFS == nil {
		t.Fatal("expected migration FS for pro line")
	}
	for _, name := range []string{"001_create_river_pro_schema.up.sql", "001_create_river_pro_schema.down.sql"} {
		data, err := fs.ReadFile(migrationFS, "migration/pro/"+name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if len(data) == 0 {
			t.Fatalf("migration %s is empty", name)
		}
	}
}

func TestCompatibilityPeriodicAndSignalStore(t *testing.T) {
	ctx := context.Background()
	exec := &Executor{}
	schema := "test_schema_compat_store"
	now := time.Now().UTC()

	if _, err := exec.PeriodicJobInsert(ctx, &PeriodicJobInsertParams{ID: "periodic-a", NextRunAt: now, Schema: schema, UpdatedAt: &now}); err != nil {
		t.Fatalf("PeriodicJobInsert: %v", err)
	}
	periodic, err := exec.PeriodicJobGetByID(ctx, &PeriodicJobGetByIDParams{ID: "periodic-a", Schema: schema})
	if err != nil {
		t.Fatalf("PeriodicJobGetByID: %v", err)
	}
	if periodic.ID != "periodic-a" || !periodic.NextRunAt.Equal(now) {
		t.Fatalf("unexpected periodic job: %#v", periodic)
	}

	first, err := exec.WorkflowSignalInsert(ctx, &WorkflowSignalInsertParams{IdempotencyKey: "idem", Key: "ready", Payload: []byte(`{"ok":true}`), Schema: schema, WorkflowID: "workflow-a"})
	if err != nil {
		t.Fatalf("WorkflowSignalInsert first: %v", err)
	}
	dup, err := exec.WorkflowSignalInsert(ctx, &WorkflowSignalInsertParams{IdempotencyKey: "idem", Key: "ready", Payload: []byte(`{"ok":true}`), Schema: schema, WorkflowID: "workflow-a"})
	if err != nil {
		t.Fatalf("WorkflowSignalInsert duplicate: %v", err)
	}
	if !dup.SkippedAsDuplicate || dup.ID != first.ID {
		t.Fatalf("expected idempotent duplicate, first=%#v dup=%#v", first, dup)
	}
	listed, err := exec.WorkflowSignalList(ctx, &WorkflowSignalListParams{LimitCount: 10, Schema: schema, WorkflowID: "workflow-a"})
	if err != nil {
		t.Fatalf("WorkflowSignalList: %v", err)
	}
	if len(listed) != 1 || listed[0].Key != "ready" {
		t.Fatalf("unexpected signals: %#v", listed)
	}
}
