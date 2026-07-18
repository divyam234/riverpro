package driver

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

func TestPublicExecutorInterfaces(t *testing.T) {
	var _ ProExecutor = (*Executor)(nil)
	var _ ProExecutorTx = (*ExecutorTx)(nil)
	var _ riverdriver.Executor = (*Executor)(nil)
	var _ riverdriver.ExecutorTx = (*ExecutorTx)(nil)
}

func TestPeriodicJobChangeTopic(t *testing.T) {
	if got, want := PeriodicJobChangeTopic("app"), "app."+PeriodicJobChangeTopicSuffix; got != want {
		t.Fatalf("short topic = %q, want %q", got, want)
	}

	longSchema := "riverpro_2026_07_18t20_22_48_schema_01"
	got := PeriodicJobChangeTopic(longSchema)
	if len(got) > 63 {
		t.Fatalf("long topic has %d bytes, want at most 63: %q", len(got), got)
	}
	if got != PeriodicJobChangeTopic(longSchema) {
		t.Fatalf("long topic is not deterministic: %q", got)
	}
	if got == PeriodicJobChangeTopic(longSchema+"_other") {
		t.Fatalf("different schemas produced the same topic: %q", got)
	}
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
	for _, name := range []string{
		"001_create_river_pro_schema.up.sql",
		"001_create_river_pro_schema.down.sql",
		"002_add_periodic_admin_and_spec.up.sql",
		"002_add_periodic_admin_and_spec.down.sql",
	} {
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

	if _, err := exec.PeriodicJobUpsert(ctx, &PeriodicJobUpsertParams{ID: "periodic-a", NextRunAt: now, Schema: schema, UpdatedAt: &now}); err != nil {
		t.Fatalf("PeriodicJobUpsert: %v", err)
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

func TestWrapperNilAndPluginHelpers(t *testing.T) {
	var nilWrapper *Wrapper[any]
	if nilWrapper.HasPool() {
		t.Fatal("nil wrapper should not report a pool")
	}
	if nilWrapper.PluginPilot() != nil {
		t.Fatal("nil wrapper should not expose a plugin pilot")
	}
	if got := nilWrapper.GetProExecutor(); got == nil {
		t.Fatal("nil wrapper should return a non-nil compatibility pro executor")
	}
	if got := nilWrapper.GetExecutor(); got == nil {
		t.Fatal("nil wrapper should return a non-nil compatibility executor")
	}
	if got := nilWrapper.UnwrapProExecutor(nil); got == nil {
		t.Fatal("nil wrapper should return a non-nil compatibility tx executor")
	}
	if got := nilWrapper.UnwrapExecutor(nil); got == nil {
		t.Fatal("nil wrapper should return a non-nil compatibility river tx executor")
	}
	if got := nilWrapper.UnwrapTx(nil); got != nil {
		t.Fatalf("nil wrapper should return zero tx, got %#v", got)
	}
	if got := nilWrapper.GetMigrationLines(); len(got) != 1 || got[0] != MigrationLinePro {
		t.Fatalf("nil wrapper migration lines = %#v", got)
	}
	if got := nilWrapper.GetMigrationDefaultLines(); len(got) != 1 || got[0] != MigrationLinePro {
		t.Fatalf("nil wrapper default migration lines = %#v", got)
	}
	if nilWrapper.GetMigrationFS("main") != nil {
		t.Fatal("nil wrapper should not expose base migration FS")
	}
	if got := nilWrapper.GetMigrationTruncateTables(MigrationLinePro, 0); len(got) == 0 {
		t.Fatal("nil wrapper should expose pro truncate tables")
	}

	w := NewWrapper[any](nil)
	w.PluginInit(nil)
	w.ProConfigInit(nil)
	if w.PluginPilot() != nil {
		t.Fatal("nil pilot should round-trip as nil")
	}
}

func TestDriverPublicErrorCompatibility(t *testing.T) {
	if !errors.Is(&DeadlockError{}, ErrDeadlock) {
		t.Fatal("DeadlockError should match ErrDeadlock")
	}
	if !errors.Is(&StatementTimeoutError{}, ErrStatementTimeout) {
		t.Fatal("StatementTimeoutError should match ErrStatementTimeout")
	}
	if !errors.Is(&UniqueViolationError{}, &UniqueViolationError{}) {
		t.Fatal("UniqueViolationError should match ErrUniqueViolation")
	}
	if got := MigrationLineProTruncateTables(MigrationLinePro, 0); len(got) == 0 {
		t.Fatal("expected pro truncate tables")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected unknown migration line to panic")
			}
		}()
		_ = MigrationLineProTruncateTables("main", 0)
	}()
}

func TestCompatibilityPeriodicJobMutationSemantics(t *testing.T) {
	ctx := context.Background()
	exec := &Executor{}
	schema := "test_schema_periodic_mutations"
	cron := "0 */2 * * *"
	initialNextRun := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)

	inserted, err := exec.PeriodicJobInsert(ctx, &PeriodicJobInsertParams{
		ID: "periodic", Kind: "kind-a", Args: []byte(`{"value":"a"}`),
		Queue: "queue-a", Priority: 2, MaxAttempts: 3,
		CronExpression: &cron, CronTimezone: "UTC", NextRunAt: initialNextRun, Schema: schema,
	})
	if err != nil {
		t.Fatalf("PeriodicJobInsert: %v", err)
	}
	if _, err := exec.PeriodicJobInsert(ctx, &PeriodicJobInsertParams{
		ID: "periodic", Kind: "kind-a", CronExpression: &cron, NextRunAt: initialNextRun, Schema: schema,
	}); !errors.Is(err, ErrPeriodicJobAlreadyExists) {
		t.Fatalf("duplicate insert error = %v, want ErrPeriodicJobAlreadyExists", err)
	}

	candidateNextRun := initialNextRun.Add(2 * time.Hour)
	upserted, err := exec.PeriodicJobUpsert(ctx, &PeriodicJobUpsertParams{
		ID: "periodic", Kind: "kind-b", Args: []byte(`{"value":"b"}`),
		Queue: "queue-b", Priority: 3, MaxAttempts: 4,
		CronExpression: &cron, CronTimezone: "UTC", NextRunAt: candidateNextRun, Schema: schema,
	})
	if err != nil {
		t.Fatalf("PeriodicJobUpsert: %v", err)
	}
	if !upserted.NextRunAt.Equal(initialNextRun) {
		t.Fatalf("unchanged schedule reset next run: got %v want %v", upserted.NextRunAt, initialNextRun)
	}
	if upserted.Kind != "kind-b" || upserted.Queue != "queue-b" {
		t.Fatalf("upsert did not reconcile definition: %#v", upserted)
	}

	reset, err := exec.PeriodicJobUpsert(ctx, &PeriodicJobUpsertParams{
		ID: "periodic", Kind: "kind-b", Args: []byte(`{"value":"b"}`),
		Queue: "queue-b", Priority: 3, MaxAttempts: 4,
		CronExpression: &cron, CronTimezone: "UTC", NextRunAt: candidateNextRun,
		ResetNextRunAt: true, Schema: schema,
	})
	if err != nil {
		t.Fatalf("PeriodicJobUpsert reset: %v", err)
	}
	if !reset.NextRunAt.Equal(candidateNextRun) {
		t.Fatalf("explicit reset next run = %v, want %v", reset.NextRunAt, candidateNextRun)
	}

	updated, err := exec.PeriodicJobUpdate(ctx, &PeriodicJobUpdateParams{
		ID: "periodic", Schema: schema, SetArgs: true, Kind: "kind-c", Args: []byte(`{"value":"c"}`),
	})
	if err != nil {
		t.Fatalf("PeriodicJobUpdate: %v", err)
	}
	if updated.Kind != "kind-c" || !updated.NextRunAt.Equal(candidateNextRun) {
		t.Fatalf("args update changed runtime state: %#v", updated)
	}

	paused, err := exec.PeriodicJobPause(ctx, &PeriodicJobPauseParams{ID: "periodic", Schema: schema})
	if err != nil {
		t.Fatalf("PeriodicJobPause: %v", err)
	}
	if paused.PausedAt == nil || !paused.NextRunAt.Equal(candidateNextRun) {
		t.Fatalf("pause changed next run or did not pause: %#v", paused)
	}
	resumed, err := exec.PeriodicJobResume(ctx, &PeriodicJobResumeParams{ID: "periodic", Schema: schema})
	if err != nil {
		t.Fatalf("PeriodicJobResume: %v", err)
	}
	if resumed.PausedAt != nil || !resumed.NextRunAt.Equal(candidateNextRun) {
		t.Fatalf("resume changed next run or did not resume: %#v", resumed)
	}

	if _, err := exec.PeriodicJobUpdate(ctx, &PeriodicJobUpdateParams{ID: "missing", Schema: schema, SetQueue: true, Queue: "x"}); !errors.Is(err, rivertype.ErrNotFound) {
		t.Fatalf("missing update error = %v, want rivertype.ErrNotFound", err)
	}
	_ = inserted
}
