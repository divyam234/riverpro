package riverprodrivertest

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/divyam234/riverpro/driver"
	"github.com/divyam234/riverpro/riverworkflow"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/require"
)

var safeIdentRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func RequireProDriver[TTx any](tb testing.TB, d driver.ProDriver[TTx]) driver.ProDriver[TTx] {
	tb.Helper()
	if d == nil {
		tb.Fatal("nil pro driver")
	}
	return d
}

// Benchmark exercises hot Pro executor paths enough for driver implementations
// to hook into package-level benchmarks. It is intentionally conservative so it
// can be reused by drivers that run against ephemeral databases.
func Benchmark[TTx any](ctx context.Context, b *testing.B,
	driverWithPool func(ctx context.Context, b *testing.B) (driver.ProDriver[TTx], string),
	executorWithTx func(ctx context.Context, b *testing.B) driver.ProExecutor,
) {
	b.Helper()
	if driverWithPool != nil {
		d, _ := driverWithPool(ctx, b)
		RequireProDriver(b, d)
	}
	if executorWithTx == nil {
		return
	}
	b.Run("TimeNow", func(b *testing.B) {
		exec := executorWithTx(ctx, b)
		for i := 0; i < b.N; i++ {
			if _, err := exec.TimeNow(ctx, &driver.TimeNowParams{}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Exercise fully exercises the documented Pro driver surface using a real
// database supplied through TEST_DATABASE_URL. It mirrors River OSS's
// riverdrivertest.Exercise pattern: driver packages pass constructors for an
// isolated migrated schema and a rolled-back test transaction, and this package
// performs behavioral checks shared by all Pro drivers.
func Exercise[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (driver.ProDriver[TTx], string),
	executorWithTx func(ctx context.Context, t *testing.T) (driver.ProExecutor, driver.ProDriver[TTx]),
) {
	t.Helper()

	exerciseMigration(ctx, t, driverWithSchema)
	exercisePeriodicJobs(ctx, t, executorWithTx)
	exerciseProducers(ctx, t, executorWithTx)
	exerciseSequences(ctx, t, executorWithTx)
	exerciseDeadLetter(ctx, t, executorWithTx)
	exerciseWorkflowPersistence(ctx, t, executorWithTx)
	exerciseWorkflowRuntimeQueries(ctx, t, executorWithTx)
	exerciseWorkflowSignals(ctx, t, executorWithTx)
	exerciseWorkflowTimers(ctx, t, executorWithTx)
	exerciseWorkflowWorklists(ctx, t, executorWithTx)
	exerciseConcurrencyLimits(ctx, t, executorWithTx)
	exerciseDocumentedExecutorAPI(ctx, t, executorWithTx)
}

// ExerciseMigrations verifies that a driver exposes the Pro migration lines and
// can apply them through River's normal migration/test-schema infrastructure.
func ExerciseMigrations[TTx any](ctx context.Context, t *testing.T,
	makeDriver func(ctx context.Context, t *testing.T) (riverdriver.Driver[TTx], *pgxpool.Pool),
) {
	t.Helper()
	if makeDriver == nil {
		t.Fatal("nil makeDriver")
	}
	d, pool := makeDriver(ctx, t)
	if d == nil {
		t.Fatal("nil driver")
	}
	if pool == nil {
		t.Fatal("nil pgx pool")
	}
	found := false
	for _, line := range d.GetMigrationLines() {
		if line == driver.MigrationLinePro {
			found = true
			break
		}
	}
	require.True(t, found, "driver missing migration line %q", driver.MigrationLinePro)
	require.NotNil(t, d.GetMigrationFS(driver.MigrationLinePro))
}

func exerciseMigration[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (driver.ProDriver[TTx], string),
) {
	t.Helper()
	t.Run("Migration", func(t *testing.T) {
		t.Parallel()
		d, schema := driverWithSchema(ctx, t, nil)
		RequireProDriver(t, d)

		require.ElementsMatch(t, []string{riverdriver.MigrationLineMain, driver.MigrationLinePro}, d.GetMigrationDefaultLines())
		require.NotNil(t, d.GetMigrationFS(driver.MigrationLinePro), "migration FS for %s", driver.MigrationLinePro)
		require.NotEmpty(t, d.GetMigrationTruncateTables(driver.MigrationLinePro, 0), "truncate tables for %s", driver.MigrationLinePro)
		for _, deprecatedLine := range []string{"sequence", "workflow"} {
			require.Nil(t, d.GetMigrationFS(deprecatedLine), "deprecated migration line %s must not be exposed", deprecatedLine)
			require.NotContains(t, d.GetMigrationLines(), deprecatedLine)
		}

		exec := d.GetProExecutor()
		for _, table := range []string{
			"river_job_dead_letter",
			"river_periodic_job",
			"river_producer",
			"river_job_sequence",
			"river_workflow",
			"river_workflow_attempt",
			"river_workflow_attempt_task",
			"river_workflow_signal",
			"river_workflow_timer",
			"river_workflow_worklist",
		} {
			requireTableExists(ctx, t, exec, schema, table)
		}
	})
}

func exercisePeriodicJobs[TTx any](ctx context.Context, t *testing.T,
	executorWithTx func(ctx context.Context, t *testing.T) (driver.ProExecutor, driver.ProDriver[TTx]),
) {
	t.Helper()
	t.Run("PeriodicJobs", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		now := time.Now().UTC().Truncate(time.Microsecond)
		stale := now.Add(-2 * time.Hour)

		inserted, err := exec.PeriodicJobInsert(ctx, &driver.PeriodicJobInsertParams{ID: "periodic-a", Kind: "kind-a", NextRunAt: now.Add(time.Hour), Schema: schema, UpdatedAt: &stale})
		require.NoError(t, err)
		require.Equal(t, "periodic-a", inserted.ID)
		require.Equal(t, "kind-a", inserted.Kind)
		requireRowCount(ctx, t, exec, schema, "river_periodic_job", 1)

		got, err := exec.PeriodicJobGetByID(ctx, &driver.PeriodicJobGetByIDParams{ID: "periodic-a", Schema: schema})
		require.NoError(t, err)
		require.Equal(t, inserted.ID, got.ID)
		require.Equal(t, "kind-a", got.Kind)

		upserted, err := exec.PeriodicJobUpsertMany(ctx, &driver.PeriodicJobUpsertManyParams{Schema: schema, Jobs: []*driver.PeriodicJobUpsertParams{
			{ID: "periodic-a", Kind: "kind-a", NextRunAt: now.Add(2 * time.Hour), UpdatedAt: now},
			{ID: "periodic-b", Kind: "kind-b", NextRunAt: now.Add(3 * time.Hour), UpdatedAt: stale},
		}})
		require.NoError(t, err)
		require.Len(t, upserted, 2)
		requireRowCount(ctx, t, exec, schema, "river_periodic_job", 2)

		all, err := exec.PeriodicJobGetAll(ctx, &driver.PeriodicJobGetAllParams{Schema: schema, Max: 10})
		require.NoError(t, err)
		require.Equal(t, []string{"periodic-a", "periodic-b"}, periodicIDs(all))

		reaped, err := exec.PeriodicJobKeepAliveAndReap(ctx, &driver.PeriodicJobKeepAliveAndReapParams{ID: []string{"periodic-a"}, Now: &now, Schema: schema, StaleUpdatedAtHorizon: now.Add(-time.Hour)})
		require.NoError(t, err)
		require.Equal(t, []string{"periodic-b"}, periodicIDs(reaped))
		requireRowCount(ctx, t, exec, schema, "river_periodic_job", 1)
	})

	t.Run("PeriodicJobsFullSpec", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		now := time.Now().UTC().Truncate(time.Microsecond)
		cron := "*/5 * * * *"

		inserted, err := exec.PeriodicJobInsert(ctx, &driver.PeriodicJobInsertParams{
			ID:             "periodic-full",
			Kind:           "kind-full",
			NextRunAt:      now,
			Schema:         schema,
			UpdatedAt:      &now,
			Args:           []byte(`{"hello":"world"}`),
			Queue:          "queue-a",
			Priority:       3,
			MaxAttempts:    7,
			Tags:           []string{"alpha", "beta"},
			CronExpression: &cron,
			CronTimezone:   "UTC",
		})
		require.NoError(t, err)
		require.Equal(t, "periodic-full", inserted.ID)
		require.Equal(t, "kind-full", inserted.Kind)
		require.JSONEq(t, `{"hello":"world"}`, string(inserted.Args))
		require.Equal(t, "queue-a", inserted.Queue)
		require.Equal(t, 3, inserted.Priority)
		require.Equal(t, 7, inserted.MaxAttempts)
		require.Equal(t, []string{"alpha", "beta"}, inserted.Tags)
		require.NotNil(t, inserted.CronExpression)
		require.Equal(t, "*/5 * * * *", *inserted.CronExpression)
		require.Equal(t, "UTC", inserted.CronTimezone)
		require.JSONEq(t, `{"hello":"world"}`, string(inserted.Args))

		got, err := exec.PeriodicJobGetByID(ctx, &driver.PeriodicJobGetByIDParams{ID: "periodic-full", Schema: schema})
		require.NoError(t, err)
		require.Equal(t, inserted.ID, got.ID)
		require.JSONEq(t, `{"hello":"world"}`, string(got.Args))
		require.Equal(t, []string{"alpha", "beta"}, got.Tags)
	})

	t.Run("PeriodicJobsDelete", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		now := time.Now().UTC().Truncate(time.Microsecond)

		_, err := exec.PeriodicJobInsert(ctx, &driver.PeriodicJobInsertParams{ID: "periodic-del", Kind: "k", NextRunAt: now, Schema: schema, UpdatedAt: &now})
		require.NoError(t, err)

		deleted, err := exec.PeriodicJobDelete(ctx, &driver.PeriodicJobDeleteParams{ID: "periodic-del", Schema: schema})
		require.NoError(t, err)
		require.Equal(t, "periodic-del", deleted.ID)
		requireRowCount(ctx, t, exec, schema, "river_periodic_job", 0)

		_, err = exec.PeriodicJobDelete(ctx, &driver.PeriodicJobDeleteParams{ID: "periodic-del", Schema: schema})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("PeriodicJobsPauseResume", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		now := time.Now().UTC().Truncate(time.Microsecond)
		pausedAt := now

		_, err := exec.PeriodicJobInsert(ctx, &driver.PeriodicJobInsertParams{ID: "periodic-pr", Kind: "k", NextRunAt: now, Schema: schema, UpdatedAt: &now})
		require.NoError(t, err)

		paused, err := exec.PeriodicJobPause(ctx, &driver.PeriodicJobPauseParams{ID: "periodic-pr", PausedAt: &pausedAt, Schema: schema})
		require.NoError(t, err)
		require.NotNil(t, paused.PausedAt)
		require.True(t, pausedAt.Equal(*paused.PausedAt), "expected paused_at %v got %v", pausedAt, *paused.PausedAt)

		// Pausing again leaves the existing paused_at unchanged.
		pausedAgain, err := exec.PeriodicJobPause(ctx, &driver.PeriodicJobPauseParams{ID: "periodic-pr", PausedAt: &pausedAt, Schema: schema})
		require.NoError(t, err)
		require.NotNil(t, pausedAgain.PausedAt)

		resumed, err := exec.PeriodicJobResume(ctx, &driver.PeriodicJobResumeParams{ID: "periodic-pr", Schema: schema})
		require.NoError(t, err)
		require.Nil(t, resumed.PausedAt)
	})

	t.Run("PeriodicJobsEnqueueDue", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		now := time.Now().UTC().Truncate(time.Microsecond)
		cronExpr := "*/5 * * * *"

		// One-shot, due now.
		_, err := exec.PeriodicJobInsert(ctx, &driver.PeriodicJobInsertParams{ID: "p-once", Kind: "k", NextRunAt: now.Add(-time.Hour), Schema: schema, UpdatedAt: &now, Args: []byte(`{"i":1}`)})
		require.NoError(t, err)
		// Cron, due now.
		_, err = exec.PeriodicJobInsert(ctx, &driver.PeriodicJobInsertParams{ID: "p-cron", Kind: "k", NextRunAt: now.Add(-time.Hour), Schema: schema, UpdatedAt: &now, Args: []byte(`{"i":2}`), CronExpression: &cronExpr, CronTimezone: "UTC"})
		require.NoError(t, err)
		// Future, should NOT be enqueued.
		_, err = exec.PeriodicJobInsert(ctx, &driver.PeriodicJobInsertParams{ID: "p-future", Kind: "k", NextRunAt: now.Add(time.Hour), Schema: schema, UpdatedAt: &now})
		require.NoError(t, err)
		// Paused, should NOT be enqueued.
		_, err = exec.PeriodicJobInsert(ctx, &driver.PeriodicJobInsertParams{ID: "p-paused", Kind: "k", NextRunAt: now.Add(-time.Hour), Schema: schema, UpdatedAt: &now})
		require.NoError(t, err)
		_, err = exec.PeriodicJobPause(ctx, &driver.PeriodicJobPauseParams{ID: "p-paused", PausedAt: &now, Schema: schema})
		require.NoError(t, err)

		// Caller-supplied next tick for the cron row.
		nextTick := now.Add(5 * time.Minute)
		res, err := exec.PeriodicJobEnqueueDue(ctx, &driver.PeriodicJobEnqueueDueParams{
			Max:       100,
			NextRunAt: map[string]time.Time{"p-cron": nextTick},
			Schema:    schema,
		})
		require.NoError(t, err)
		require.Len(t, res.Inserted, 2)
		require.Equal(t, []string{"p-once"}, res.Deleted)

		// One-shot row gone; cron row updated to nextTick; future/paused untouched.
		_, err = exec.PeriodicJobGetByID(ctx, &driver.PeriodicJobGetByIDParams{ID: "p-once", Schema: schema})
		require.ErrorIs(t, err, rivertype.ErrNotFound)

		cronRow, err := exec.PeriodicJobGetByID(ctx, &driver.PeriodicJobGetByIDParams{ID: "p-cron", Schema: schema})
		require.NoError(t, err)
		require.True(t, nextTick.Equal(cronRow.NextRunAt), "expected %v got %v", nextTick, cronRow.NextRunAt)

		futureRow, err := exec.PeriodicJobGetByID(ctx, &driver.PeriodicJobGetByIDParams{ID: "p-future", Schema: schema})
		require.NoError(t, err)
		require.True(t, now.Add(time.Hour).Equal(futureRow.NextRunAt), "expected %v got %v", now.Add(time.Hour), futureRow.NextRunAt)
	})
}

func exerciseProducers[TTx any](ctx context.Context, t *testing.T,
	executorWithTx func(ctx context.Context, t *testing.T) (driver.ProExecutor, driver.ProDriver[TTx]),
) {
	t.Helper()
	t.Run("Producers", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		now := time.Now().UTC().Truncate(time.Microsecond)
		pausedAt := now.Add(time.Minute)

		producer, err := exec.ProducerInsertOrUpdate(ctx, &driver.ProducerInsertOrUpdateParams{ClientID: "client-a", CreatedAt: &now, MaxWorkers: 7, Metadata: []byte(`{"role":"primary"}`), QueueName: "default", Schema: schema, UpdatedAt: &now})
		require.NoError(t, err)
		require.NotZero(t, producer.ID)
		requireRowCount(ctx, t, exec, schema, "river_producer", 1)

		got, err := exec.ProducerGetByID(ctx, &driver.ProducerGetByIDParams{ID: producer.ID, Schema: schema})
		require.NoError(t, err)
		require.Equal(t, producer.ClientID, got.ClientID)

		updated, err := exec.ProducerUpdate(ctx, &driver.ProducerUpdateParams{ID: producer.ID, MaxWorkers: 3, MaxWorkersDoUpdate: true, Metadata: []byte(`{"role":"secondary"}`), MetadataDoUpdate: true, PausedAt: &pausedAt, PausedAtDoUpdate: true, Schema: schema, UpdatedAt: &now})
		require.NoError(t, err)
		require.EqualValues(t, 3, updated.MaxWorkers)
		require.Equal(t, &pausedAt, updated.PausedAt)

		listed, err := exec.ProducerListByQueue(ctx, &driver.ProducerListByQueueParams{QueueName: "default", Schema: schema})
		require.NoError(t, err)
		require.Len(t, listed, 1)
		require.Equal(t, producer.ID, listed[0].Producer.ID)

		alive, err := exec.ProducerKeepAlive(ctx, &driver.ProducerKeepAliveParams{ID: producer.ID, QueueName: "default", Schema: schema, StaleUpdatedAtHorizon: now.Add(-time.Hour)})
		require.NoError(t, err)
		require.Equal(t, producer.ID, alive.ID)

		require.NoError(t, exec.ProducerDelete(ctx, &driver.ProducerDeleteParams{ID: producer.ID, Schema: schema}))
		requireRowCount(ctx, t, exec, schema, "river_producer", 0)
	})

	t.Run("ProducerDeleteStale", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		now := time.Now().UTC().Truncate(time.Microsecond)
		stale := now.Add(-2 * time.Hour)

		fresh, err := exec.ProducerInsertOrUpdate(ctx, &driver.ProducerInsertOrUpdateParams{ClientID: "client-fresh", CreatedAt: &now, MaxWorkers: 4, QueueName: "q1", Schema: schema, UpdatedAt: &now})
		require.NoError(t, err)
		_, err = exec.ProducerInsertOrUpdate(ctx, &driver.ProducerInsertOrUpdateParams{ClientID: "client-stale-q1", CreatedAt: &stale, MaxWorkers: 4, QueueName: "q1", Schema: schema, UpdatedAt: &stale})
		require.NoError(t, err)
		_, err = exec.ProducerInsertOrUpdate(ctx, &driver.ProducerInsertOrUpdateParams{ClientID: "client-stale-q2", CreatedAt: &stale, MaxWorkers: 4, QueueName: "q2", Schema: schema, UpdatedAt: &stale})
		require.NoError(t, err)
		requireRowCount(ctx, t, exec, schema, "river_producer", 3)

		// Default horizon reaps the two stale rows, keeps the fresh one.
		deleted, err := exec.ProducerDeleteStale(ctx, &driver.ProducerDeleteStaleParams{Schema: schema, StaleUpdatedAtHorizon: now.Add(-time.Hour), Max: 100})
		require.NoError(t, err)
		require.Equal(t, 2, deleted)
		requireRowCount(ctx, t, exec, schema, "river_producer", 1)

		// Reap across queues scoped to a single queue.
		_, err = exec.ProducerInsertOrUpdate(ctx, &driver.ProducerInsertOrUpdateParams{ClientID: "client-stale-q1b", CreatedAt: &stale, MaxWorkers: 4, QueueName: "q1", Schema: schema, UpdatedAt: &stale})
		require.NoError(t, err)
		_, err = exec.ProducerInsertOrUpdate(ctx, &driver.ProducerInsertOrUpdateParams{ClientID: "client-stale-q2b", CreatedAt: &stale, MaxWorkers: 4, QueueName: "q2", Schema: schema, UpdatedAt: &stale})
		require.NoError(t, err)
		requireRowCount(ctx, t, exec, schema, "river_producer", 3)
		deleted, err = exec.ProducerDeleteStale(ctx, &driver.ProducerDeleteStaleParams{Schema: schema, StaleUpdatedAtHorizon: now.Add(-time.Hour), Max: 100, QueueName: "q1"})
		require.NoError(t, err)
		require.Equal(t, 1, deleted)
		requireRowCount(ctx, t, exec, schema, "river_producer", 2)

		// Fresh row survives any reap.
		survivor, err := exec.ProducerGetByID(ctx, &driver.ProducerGetByIDParams{ID: fresh.ID, Schema: schema})
		require.NoError(t, err)
		require.Equal(t, fresh.ClientID, survivor.ClientID)

		// Max bounds the batch.
		_, err = exec.ProducerInsertOrUpdate(ctx, &driver.ProducerInsertOrUpdateParams{ClientID: "client-stale-q3a", CreatedAt: &stale, MaxWorkers: 4, QueueName: "q3", Schema: schema, UpdatedAt: &stale})
		require.NoError(t, err)
		_, err = exec.ProducerInsertOrUpdate(ctx, &driver.ProducerInsertOrUpdateParams{ClientID: "client-stale-q3b", CreatedAt: &stale, MaxWorkers: 4, QueueName: "q3", Schema: schema, UpdatedAt: &stale})
		require.NoError(t, err)
		requireRowCount(ctx, t, exec, schema, "river_producer", 4)
		deleted, err = exec.ProducerDeleteStale(ctx, &driver.ProducerDeleteStaleParams{Schema: schema, StaleUpdatedAtHorizon: now.Add(-time.Hour), Max: 1})
		require.NoError(t, err)
		require.Equal(t, 1, deleted)
		requireRowCount(ctx, t, exec, schema, "river_producer", 3)
	})
}

func exerciseSequences[TTx any](ctx context.Context, t *testing.T,
	executorWithTx func(ctx context.Context, t *testing.T) (driver.ProExecutor, driver.ProDriver[TTx]),
) {
	t.Helper()
	t.Run("Sequences", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)

		n, err := exec.SequenceAppendMany(ctx, &driver.SequenceAppendManyParams{Schema: schema, SeqKeys: []string{"seq-a", "seq-b", "seq-a", ""}})
		require.NoError(t, err)
		require.Equal(t, 2, n)
		requireRowCount(ctx, t, exec, schema, "river_job_sequence", 2)

		seqs, err := exec.SequenceList(ctx, &driver.SequenceListParams{Schema: schema, MaxCount: 10})
		require.NoError(t, err)
		require.Equal(t, []string{"seq-a", "seq-b"}, sequenceKeys(seqs))

		promoted, err := exec.SequencePromote(ctx, &driver.SequencePromoteParams{Keys: []string{"seq-a", "missing"}, Schema: schema})
		require.NoError(t, err)
		require.Equal(t, []string{"seq-a"}, promoted.PromotedKeys)
		require.Equal(t, []string{"missing"}, promoted.SkippedKeys)
	})
}

func exerciseDeadLetter[TTx any](ctx context.Context, t *testing.T,
	executorWithTx func(ctx context.Context, t *testing.T) (driver.ProExecutor, driver.ProDriver[TTx]),
) {
	t.Helper()
	t.Run("DeadLetter", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		now := time.Now().UTC().Truncate(time.Microsecond)

		job := insertJob(ctx, t, exec, schema, "dead-kind", rivertype.JobStateDiscarded, []byte(`{}`), []byte(`{}`), &now)
		require.NoError(t, exec.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states, dead_lettered_at)
			SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states, now()
			FROM %s WHERE id = $1
		`, qname(schema, "river_job_dead_letter"), qname(schema, "river_job")), job.ID))
		requireRowCount(ctx, t, exec, schema, "river_job_dead_letter", 1)

		got, err := exec.JobDeadLetterGetByID(ctx, &driver.JobDeadLetterGetByIDParams{ID: job.ID, Schema: schema})
		require.NoError(t, err)
		require.Equal(t, job.ID, got.ID)

		all, err := exec.JobDeadLetterGetAll(ctx, &driver.JobDeadLetterGetAllParams{Schema: schema})
		require.NoError(t, err)
		require.Len(t, all, 1)

		moved, err := exec.JobDeadLetterMoveByID(ctx, &driver.JobDeadLetterMoveByIDParams{ID: job.ID, Schema: schema})
		require.NoError(t, err)
		require.Equal(t, job.ID, moved.ID)
		require.Equal(t, rivertype.JobStateAvailable, moved.State)
		requireRowCount(ctx, t, exec, schema, "river_job_dead_letter", 0)
	})
}

func exerciseWorkflowPersistence[TTx any](ctx context.Context, t *testing.T,
	executorWithTx func(ctx context.Context, t *testing.T) (driver.ProExecutor, driver.ProDriver[TTx]),
) {
	t.Helper()
	t.Run("WorkflowPersistence", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		now := time.Now().UTC().Truncate(time.Microsecond)

		require.NoError(t, exec.WorkflowInsertMany(ctx, &driver.WorkflowInsertManyParams{IDs: []string{"wf-a", "wf-b"}, Names: []string{"alpha", "beta"}, Schema: schema}))
		requireRowCount(ctx, t, exec, schema, "river_workflow", 2)

		wf, err := exec.WorkflowGetByID(ctx, &driver.WorkflowGetByIDParams{Schema: schema, WorkflowID: "wf-a"})
		require.NoError(t, err)
		require.Equal(t, "wf-a", wf.ID)
		require.NotNil(t, wf.Name)
		require.Equal(t, "alpha", *wf.Name)

		attempt, err := exec.WorkflowAttemptInsert(ctx, &driver.WorkflowAttemptInsertParams{Attempt: 1, ResetHistory: true, RetryMode: "all", Schema: schema, TriggeredBy: []byte(`{"by":"test"}`), WorkflowID: "wf-a"})
		require.NoError(t, err)
		require.Equal(t, 1, attempt.Attempt)
		requireRowCount(ctx, t, exec, schema, "river_workflow_attempt", 1)

		job := insertWorkflowJob(ctx, t, exec, schema, "wf-a", "root", nil, rivertype.JobStateAvailable, nil)
		task, err := exec.WorkflowAttemptTaskInsert(ctx, &driver.WorkflowAttemptTaskInsertParams{Attempt: 1, AttemptCount: 2, JobID: job.ID, Metadata: []byte(`{"ok":true}`), Schema: schema, State: string(rivertype.JobStateAvailable), Task: "root", WorkflowID: "wf-a"})
		require.NoError(t, err)
		require.Equal(t, "root", task.Task)
		requireRowCount(ctx, t, exec, schema, "river_workflow_attempt_task", 1)

		attempts, err := exec.WorkflowAttemptListByWorkflowID(ctx, &driver.WorkflowAttemptListByWorkflowIDParams{Schema: schema, WorkflowID: "wf-a"})
		require.NoError(t, err)
		require.Len(t, attempts, 1)

		tasks, err := exec.WorkflowAttemptTaskListByWorkflowID(ctx, &driver.WorkflowAttemptTaskListByWorkflowIDParams{Attempt: 1, Schema: schema, WorkflowID: "wf-a"})
		require.NoError(t, err)
		require.Len(t, tasks, 1)

		finalized, err := exec.WorkflowFinalizeIfCompleteMany(ctx, &driver.WorkflowFinalizeIfCompleteManyParams{Now: now, Schema: schema, WorkflowIDs: []string{"wf-b"}})
		require.NoError(t, err)
		require.Equal(t, []string{"wf-b"}, finalized)
		inactive, err := exec.WorkflowListInactive(ctx, &driver.WorkflowListParams{Schema: schema, PaginationLimit: 10})
		require.NoError(t, err)
		require.Contains(t, workflowListIDs(inactive), "wf-b")
	})
}

func exerciseWorkflowRuntimeQueries[TTx any](ctx context.Context, t *testing.T,
	executorWithTx func(ctx context.Context, t *testing.T) (driver.ProExecutor, driver.ProDriver[TTx]),
) {
	t.Helper()
	t.Run("WorkflowRuntimeQueries", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		require.NoError(t, exec.WorkflowInsertMany(ctx, &driver.WorkflowInsertManyParams{IDs: []string{"wf-runtime"}, Names: []string{"runtime"}, Schema: schema}))

		root := insertWorkflowJob(ctx, t, exec, schema, "wf-runtime", "root", nil, rivertype.JobStateCompleted, ptrTime(time.Now().UTC()))
		child := insertWorkflowJob(ctx, t, exec, schema, "wf-runtime", "child", []string{"root"}, rivertype.JobStatePending, nil)
		_ = insertWorkflowJob(ctx, t, exec, schema, "wf-runtime", "grandchild", []string{"child"}, rivertype.JobStatePending, nil)

		ready, err := exec.WorkflowReadyTaskIDsByWorkflowIDs(ctx, &driver.WorkflowReadyTaskIDsByWorkflowIDsParams{LimitCount: 10, Schema: schema, WorkflowIDs: []string{"wf-runtime"}})
		require.NoError(t, err)
		require.Equal(t, []int64{child.ID}, readyJobIDs(ready))

		staged, err := exec.WorkflowStageJobsByIDMany(ctx, &driver.WorkflowStageJobsByIDManyParams{JobIDs: []int64{child.ID}, Schema: schema, WorkflowStagedAt: time.Now().UTC()})
		require.NoError(t, err)
		require.Len(t, staged, 1)
		require.Equal(t, rivertype.JobStateAvailable, staged[0].State)

		withDeps, err := exec.WorkflowLoadTaskWithDeps(ctx, &driver.WorkflowLoadTaskWithDepsParams{Schema: schema, Task: "child", WorkflowID: "wf-runtime"})
		require.NoError(t, err)
		require.Equal(t, child.ID, withDeps.Job.ID)
		require.Equal(t, []string{"root"}, withDeps.Deps)

		deps, err := exec.WorkflowLoadDepTasksAndIDs(ctx, &driver.WorkflowLoadDepTasksAndIDsParams{Recursive: true, Schema: schema, Task: "grandchild", WorkflowID: "wf-runtime"})
		require.NoError(t, err)
		require.Equal(t, map[string]*int64{"child": &child.ID, "root": &root.ID}, deps)

		names, err := exec.WorkflowLoadTaskNamesByWorkflowID(ctx, &driver.WorkflowLoadTaskNamesByWorkflowIDParams{Schema: schema, WorkflowID: "wf-runtime"})
		require.NoError(t, err)
		require.Equal(t, []string{"child", "grandchild", "root"}, names)

		jobs, err := exec.WorkflowJobList(ctx, &driver.WorkflowJobListParams{Schema: schema, WorkflowID: "wf-runtime", PaginationLimit: 10})
		require.NoError(t, err)
		require.Len(t, jobs, 3)

		cancelled, err := exec.WorkflowCancel(ctx, &driver.WorkflowCancelParams{CancelAttemptedAt: time.Now().UTC(), Schema: schema, WorkflowID: "wf-runtime"})
		require.NoError(t, err)
		require.NotEmpty(t, cancelled)
		wf, err := exec.WorkflowGetByID(ctx, &driver.WorkflowGetByIDParams{Schema: schema, WorkflowID: "wf-runtime"})
		require.NoError(t, err)
		require.Equal(t, "cancelled", wf.State)
	})
}

func exerciseWorkflowSignals[TTx any](ctx context.Context, t *testing.T,
	executorWithTx func(ctx context.Context, t *testing.T) (driver.ProExecutor, driver.ProDriver[TTx]),
) {
	t.Helper()
	t.Run("WorkflowSignals", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		require.NoError(t, exec.WorkflowInsertMany(ctx, &driver.WorkflowInsertManyParams{IDs: []string{"wf-signal"}, Names: []string{"signal"}, Schema: schema}))

		first, err := exec.WorkflowSignalInsert(ctx, &driver.WorkflowSignalInsertParams{IdempotencyKey: "idem", Key: "ready", Payload: []byte(`{"n":1}`), Schema: schema, Source: []byte(`{"source":"test"}`), WorkflowID: "wf-signal"})
		require.NoError(t, err)
		require.False(t, first.SkippedAsDuplicate)
		requireRowCount(ctx, t, exec, schema, "river_workflow_signal", 1)

		dup, err := exec.WorkflowSignalInsert(ctx, &driver.WorkflowSignalInsertParams{IdempotencyKey: "idem", Key: "ready", Payload: []byte(`{"n":1}`), Schema: schema, Source: []byte(`{"source":"test"}`), WorkflowID: "wf-signal"})
		require.NoError(t, err)
		require.True(t, dup.SkippedAsDuplicate)
		require.Equal(t, first.ID, dup.ID)
		requireRowCount(ctx, t, exec, schema, "river_workflow_signal", 1)

		_, err = exec.WorkflowSignalInsert(ctx, &driver.WorkflowSignalInsertParams{Key: "other", Payload: []byte(`{"n":2}`), Schema: schema, WorkflowID: "wf-signal"})
		require.NoError(t, err)

		listed, err := exec.WorkflowSignalList(ctx, &driver.WorkflowSignalListParams{LimitCount: 10, Schema: schema, WorkflowID: "wf-signal"})
		require.NoError(t, err)
		require.Equal(t, []string{"ready", "other"}, signalKeys(listed))

		key := "ready"
		byKey, err := exec.WorkflowSignalList(ctx, &driver.WorkflowSignalListParams{Key: &key, LimitCount: 10, Schema: schema, WorkflowID: "wf-signal"})
		require.NoError(t, err)
		require.Equal(t, []string{"ready"}, signalKeys(byKey))

		stats, err := exec.WorkflowSignalStatsByWorkflowIDs(ctx, &driver.WorkflowSignalStatsByWorkflowIDsParams{Keys: []string{"ready", "missing"}, Schema: schema, WorkflowIDs: []string{"wf-signal"}})
		require.NoError(t, err)
		require.Len(t, stats, 1)
		require.Equal(t, "ready", stats[0].Key)
		require.Equal(t, int64(1), stats[0].SignalCount)
	})
}

func exerciseWorkflowTimers[TTx any](ctx context.Context, t *testing.T,
	executorWithTx func(ctx context.Context, t *testing.T) (driver.ProExecutor, driver.ProDriver[TTx]),
) {
	t.Helper()
	t.Run("WorkflowTimers", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		require.NoError(t, exec.WorkflowInsertMany(ctx, &driver.WorkflowInsertManyParams{IDs: []string{"wf-timer-a", "wf-timer-b"}, Names: []string{"timer-a", "timer-b"}, Schema: schema}))
		now := time.Now().UTC().Truncate(time.Microsecond)

		require.NoError(t, exec.WorkflowTimerUpsertMany(ctx, &driver.WorkflowTimerUpsertManyParams{Schema: schema, WorkflowIDs: []string{"wf-timer-a", "wf-timer-b"}, NextFireAts: []time.Time{now.Add(-time.Minute), now.Add(time.Minute)}}))
		requireRowCount(ctx, t, exec, schema, "river_workflow_timer", 2)

		timer, err := exec.WorkflowTimerGetByWorkflowID(ctx, &driver.WorkflowTimerGetByWorkflowIDParams{Schema: schema, WorkflowID: "wf-timer-a"})
		require.NoError(t, err)
		require.Equal(t, "wf-timer-a", timer.WorkflowID)

		due, err := exec.WorkflowTimerConsumeDue(ctx, &driver.WorkflowTimerConsumeDueParams{LimitCount: 10, AsOf: now, Schema: schema})
		require.NoError(t, err)
		require.Equal(t, []string{"wf-timer-a"}, timerWorkflowIDs(due))
		requireRowCount(ctx, t, exec, schema, "river_workflow_timer", 1)

		next, err := exec.WorkflowTimerNextFireAtByWorkflowIDs(ctx, &driver.WorkflowTimerNextFireAtByWorkflowIDsParams{Schema: schema, WorkflowIDs: []string{"wf-timer-b"}})
		require.NoError(t, err)
		require.Len(t, next, 1)

		require.NoError(t, exec.WorkflowTimerDeleteByWorkflowIDs(ctx, &driver.WorkflowTimerDeleteByWorkflowIDsParams{Schema: schema, WorkflowIDs: []string{"wf-timer-b"}}))
		requireRowCount(ctx, t, exec, schema, "river_workflow_timer", 0)
	})
}

func exerciseWorkflowWorklists[TTx any](ctx context.Context, t *testing.T,
	executorWithTx func(ctx context.Context, t *testing.T) (driver.ProExecutor, driver.ProDriver[TTx]),
) {
	t.Helper()
	t.Run("WorkflowWorklists", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		require.NoError(t, exec.WorkflowInsertMany(ctx, &driver.WorkflowInsertManyParams{IDs: []string{"wf-worklist"}, Names: []string{"worklist"}, Schema: schema}))

		require.NoError(t, exec.WorkflowWorklistInsertMany(ctx, &driver.WorkflowWorklistInsertManyParams{Reason: 1, Schema: schema, WorkflowIDs: []string{"wf-worklist"}}))
		requireRowCount(ctx, t, exec, schema, "river_workflow_worklist", 1)

		ids, err := exec.WorkflowWorklistListIDs(ctx, &driver.WorkflowWorklistListParams{LimitCount: 10, Schema: schema})
		require.NoError(t, err)
		require.Len(t, ids, 1)

		items, err := exec.WorkflowWorklistList(ctx, &driver.WorkflowWorklistListParams{LimitCount: 10, Schema: schema})
		require.NoError(t, err)
		require.Len(t, items, 1)

		deleted, err := exec.WorkflowWorklistDeleteByWorkflowIDsReturningReasons(ctx, &driver.WorkflowWorklistDeleteByWorkflowIDsReturningReasonsParams{Schema: schema, WorkflowIDs: []string{"wf-worklist"}})
		require.NoError(t, err)
		require.Equal(t, []int16{1}, worklistReasons(deleted))
		requireRowCount(ctx, t, exec, schema, "river_workflow_worklist", 0)
	})
}

func exerciseConcurrencyLimits[TTx any](ctx context.Context, t *testing.T,
	executorWithTx func(ctx context.Context, t *testing.T) (driver.ProExecutor, driver.ProDriver[TTx]),
) {
	t.Helper()

	fetch := func(ctx context.Context, t *testing.T, exec driver.ProExecutor, schema, client string, params *driver.JobGetAvailableLimitedParams) []*rivertype.JobRow {
		t.Helper()
		if params.JobGetAvailableParams == nil {
			params.JobGetAvailableParams = &riverdriver.JobGetAvailableParams{}
		}
		now := time.Now().UTC().Add(time.Second)
		params.JobGetAvailableParams.ClientID = client
		params.JobGetAvailableParams.MaxAttemptedBy = 8
		if params.JobGetAvailableParams.MaxToLock == 0 {
			params.JobGetAvailableParams.MaxToLock = 10
		}
		params.JobGetAvailableParams.Now = &now
		params.JobGetAvailableParams.Queue = "default"
		params.JobGetAvailableParams.Schema = schema
		jobs, err := exec.JobGetAvailableLimited(ctx, params)
		require.NoError(t, err)
		return jobs
	}

	t.Run("ConcurrencyGlobalUnpartitionedCountsAlreadyRunning", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		_ = insertJob(ctx, t, exec, schema, "global-running-a", rivertype.JobStateRunning, []byte(`{}`), []byte(`{}`), nil)
		_ = insertJob(ctx, t, exec, schema, "global-running-b", rivertype.JobStateRunning, []byte(`{}`), []byte(`{}`), nil)
		available := insertJob(ctx, t, exec, schema, "global-available", rivertype.JobStateAvailable, []byte(`{}`), []byte(`{}`), nil)

		jobs := fetch(ctx, t, exec, schema, "client-global", &driver.JobGetAvailableLimitedParams{GlobalLimit: 2})
		require.Empty(t, jobs, "global limit should include already-running jobs")

		jobs = fetch(ctx, t, exec, schema, "client-global", &driver.JobGetAvailableLimitedParams{GlobalLimit: 3})
		require.Len(t, jobs, 1)
		require.Equal(t, available.ID, jobs[0].ID)
		require.Equal(t, rivertype.JobStateRunning, jobs[0].State)
		require.Equal(t, 1, jobs[0].Attempt)
		require.Equal(t, []string{"client-global"}, jobs[0].AttemptedBy)
	})

	t.Run("ConcurrencyGlobalPartitionedByKind", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		_ = insertJob(ctx, t, exec, schema, "kind-a", rivertype.JobStateRunning, []byte(`{}`), []byte(`{}`), nil)
		blocked := insertJob(ctx, t, exec, schema, "kind-a", rivertype.JobStateAvailable, []byte(`{}`), []byte(`{}`), nil)
		allowed := insertJob(ctx, t, exec, schema, "kind-b", rivertype.JobStateAvailable, []byte(`{}`), []byte(`{}`), nil)

		jobs := fetch(ctx, t, exec, schema, "client-kind", &driver.JobGetAvailableLimitedParams{GlobalLimit: 1, PartitionByKind: true})
		require.Len(t, jobs, 1)
		require.Equal(t, allowed.ID, jobs[0].ID)
		require.NotEqual(t, blocked.ID, jobs[0].ID)
	})

	t.Run("ConcurrencyGlobalPartitionedBySelectedArgs", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		_ = insertJob(ctx, t, exec, schema, "args-kind", rivertype.JobStateRunning, []byte(`{}`), []byte(`{"customer_id":1,"other":"running"}`), nil)
		blocked := insertJob(ctx, t, exec, schema, "args-kind", rivertype.JobStateAvailable, []byte(`{}`), []byte(`{"customer_id":1,"other":"available"}`), nil)
		allowed := insertJob(ctx, t, exec, schema, "args-kind", rivertype.JobStateAvailable, []byte(`{}`), []byte(`{"customer_id":2,"other":"available"}`), nil)

		jobs := fetch(ctx, t, exec, schema, "client-args", &driver.JobGetAvailableLimitedParams{GlobalLimit: 1, PartitionByArgs: []string{"customer_id"}})
		require.Len(t, jobs, 1)
		require.Equal(t, allowed.ID, jobs[0].ID)
		require.NotEqual(t, blocked.ID, jobs[0].ID)
	})

	t.Run("ConcurrencyGlobalPartitionedByAllArgsWhenEmptySlice", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		_ = insertJob(ctx, t, exec, schema, "all-args-kind", rivertype.JobStateRunning, []byte(`{}`), []byte(`{"customer_id":1,"other":"same"}`), nil)
		blocked := insertJob(ctx, t, exec, schema, "all-args-kind", rivertype.JobStateAvailable, []byte(`{}`), []byte(`{"customer_id":1,"other":"same"}`), nil)
		allowed := insertJob(ctx, t, exec, schema, "all-args-kind", rivertype.JobStateAvailable, []byte(`{}`), []byte(`{"customer_id":1,"other":"different"}`), nil)

		jobs := fetch(ctx, t, exec, schema, "client-all-args", &driver.JobGetAvailableLimitedParams{GlobalLimit: 1, PartitionByArgs: []string{}})
		require.Len(t, jobs, 1)
		require.Equal(t, allowed.ID, jobs[0].ID)
		require.NotEqual(t, blocked.ID, jobs[0].ID)
	})

	t.Run("ConcurrencyLocalLimitUsesCurrentClientRunningJobs", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		running := insertJob(ctx, t, exec, schema, "local-kind", rivertype.JobStateRunning, []byte(`{}`), []byte(`{}`), nil)
		require.NoError(t, exec.Exec(ctx, fmt.Sprintf(`UPDATE %s SET attempted_by = ARRAY['client-local']::text[] WHERE id = $1`, qname(schema, "river_job")), running.ID))
		available := insertJob(ctx, t, exec, schema, "local-kind", rivertype.JobStateAvailable, []byte(`{}`), []byte(`{}`), nil)

		jobs := fetch(ctx, t, exec, schema, "client-local", &driver.JobGetAvailableLimitedParams{LocalLimit: 1})
		require.Empty(t, jobs, "same client should be at local capacity")

		jobs = fetch(ctx, t, exec, schema, "client-other", &driver.JobGetAvailableLimitedParams{LocalLimit: 1})
		require.Len(t, jobs, 1)
		require.Equal(t, available.ID, jobs[0].ID)
	})

	t.Run("ConcurrencyLocalLimitUsesProvidedProducerCounts", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		available := insertJob(ctx, t, exec, schema, "provided-kind", rivertype.JobStateAvailable, []byte(`{}`), []byte(`{}`), nil)

		jobs := fetch(ctx, t, exec, schema, "client-provided", &driver.JobGetAvailableLimitedParams{LocalLimit: 1, CurrentProducerPartitionKeys: []string{"queue=default"}, CurrentProducerPartitionRunningCounts: []int32{1}})
		require.Empty(t, jobs)

		jobs = fetch(ctx, t, exec, schema, "client-provided", &driver.JobGetAvailableLimitedParams{LocalLimit: 2, CurrentProducerPartitionKeys: []string{"queue=default"}, CurrentProducerPartitionRunningCounts: []int32{1}})
		require.Len(t, jobs, 1)
		require.Equal(t, available.ID, jobs[0].ID)
	})

	t.Run("ConcurrencyAvailablePartitionKeysFilter", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		kindA := insertJob(ctx, t, exec, schema, "filter-a", rivertype.JobStateAvailable, []byte(`{}`), []byte(`{}`), nil)
		_ = insertJob(ctx, t, exec, schema, "filter-b", rivertype.JobStateAvailable, []byte(`{}`), []byte(`{}`), nil)

		jobs := fetch(ctx, t, exec, schema, "client-filter", &driver.JobGetAvailableLimitedParams{GlobalLimit: 5, PartitionByKind: true, AvailablePartitionKeys: []string{"kind=filter-a"}})
		require.Len(t, jobs, 1)
		require.Equal(t, kindA.ID, jobs[0].ID)
	})

	t.Run("ConcurrencyLengthMismatchReturnsError", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		now := time.Now().UTC().Add(time.Second)
		_, err := exec.JobGetAvailableLimited(ctx, &driver.JobGetAvailableLimitedParams{
			JobGetAvailableParams:                 &riverdriver.JobGetAvailableParams{ClientID: "client-mismatch", MaxAttemptedBy: 1, MaxToLock: 1, Now: &now, Queue: "default", Schema: schema},
			LocalLimit:                            1,
			CurrentProducerPartitionKeys:          []string{"queue=default"},
			CurrentProducerPartitionRunningCounts: nil,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "length mismatch")
	})
}

func exerciseDocumentedExecutorAPI[TTx any](ctx context.Context, t *testing.T,
	executorWithTx func(ctx context.Context, t *testing.T) (driver.ProExecutor, driver.ProDriver[TTx]),
) {
	t.Helper()
	t.Run("DocumentedExecutorAPI", func(t *testing.T) {
		t.Parallel()
		exec, schema := execSchema(ctx, t, executorWithTx)
		now := time.Now().UTC().Truncate(time.Microsecond)

		proTx, err := exec.BeginPro(ctx)
		require.NoError(t, err)
		require.NoError(t, proTx.Rollback(ctx))

		// Use a random lock key so the call doesn't collide with another
		// driver package's DocumentedExecutorAPI run hitting the same
		// database in parallel (PostgreSQL advisory locks are global).
		locked, err := exec.PGTryAdvisoryXactLock(ctx, rand.Int63())
		require.NoError(t, err)
		require.True(t, locked)

		queueMeta, err := exec.QueueGetMetadataForInsert(ctx, &driver.QueueGetMetadataForInsertParams{Names: []string{"default", "missing"}, Schema: schema})
		require.NoError(t, err)
		require.NotNil(t, queueMeta)

		available := insertJob(ctx, t, exec, schema, "api-kind-a", rivertype.JobStateAvailable, []byte(`{}`), []byte(`{"n":1}`), nil)
		_ = insertJob(ctx, t, exec, schema, "api-kind-b", rivertype.JobStateAvailable, []byte(`{}`), []byte(`{"n":2}`), nil)
		partitions, err := exec.JobGetAvailablePartitionKeys(ctx, &driver.JobGetAvailablePartitionKeysParams{Queue: "default", Schema: schema})
		require.NoError(t, err)
		require.NotEmpty(t, partitions)

		fetchNow := time.Now().UTC().Add(time.Second)
		limited, err := exec.JobGetAvailableLimited(ctx, &driver.JobGetAvailableLimitedParams{JobGetAvailableParams: &riverdriver.JobGetAvailableParams{ClientID: "api-client", MaxAttemptedBy: 1, MaxToLock: 1, Now: &fetchNow, Queue: "default", Schema: schema}, LocalLimit: 1, GlobalLimit: 1})
		require.NoError(t, err)
		require.Len(t, limited, 1)

		forBatch, err := exec.JobGetAvailableForBatch(ctx, &driver.JobGetAvailableForBatchParams{AttemptedBy: "batch-client", BatchID: "batch-id", BatchKey: "batch-key", BatchLeaderJobID: available.ID, Kind: "api-kind-a", Max: 1, Queue: "default", Schema: schema})
		require.NoError(t, err)
		require.NotNil(t, forBatch)

		deletable := insertJob(ctx, t, exec, schema, "delete-by-id", rivertype.JobStateAvailable, []byte(`{}`), []byte(`{}`), nil)
		deletedMany, err := exec.JobDeleteByIDMany(ctx, &driver.JobDeleteByIDManyParams{ID: []int64{deletable.ID}, Schema: schema})
		require.NoError(t, err)
		require.Len(t, deletedMany, 1)

		oldDone := now.Add(-2 * time.Hour)
		_ = insertJob(ctx, t, exec, schema, "delete-before", rivertype.JobStateCompleted, []byte(`{}`), []byte(`{}`), &oldDone)
		deletedBefore, err := exec.JobDeleteNonWorkflowBefore(ctx, &driver.JobDeleteNonWorkflowBeforeParams{CompletedDoDelete: true, CompletedFinalizedAtHorizon: now.Add(-time.Hour), Max: 10, Schema: schema})
		require.NoError(t, err)
		require.GreaterOrEqual(t, deletedBefore, 1)

		deadOne := insertJob(ctx, t, exec, schema, "dead-delete", rivertype.JobStateDiscarded, []byte(`{}`), []byte(`{}`), &oldDone)
		require.NoError(t, exec.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states, dead_lettered_at)
			SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states, now()
			FROM %s WHERE id = $1
		`, qname(schema, "river_job_dead_letter"), qname(schema, "river_job")), deadOne.ID))
		deletedDead, err := exec.JobDeadLetterDeleteByID(ctx, &driver.JobDeadLetterDeleteByIDParams{ID: deadOne.ID, Schema: schema})
		require.NoError(t, err)
		require.Equal(t, deadOne.ID, deletedDead.ID)

		deadTwo := insertJob(ctx, t, exec, schema, "dead-move-discarded", rivertype.JobStateDiscarded, []byte(`{}`), []byte(`{}`), &oldDone)
		require.NoError(t, exec.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states, dead_lettered_at)
			SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states, now()
			FROM %s WHERE id = $1
		`, qname(schema, "river_job_dead_letter"), qname(schema, "river_job")), deadTwo.ID))
		movedDiscarded, err := exec.JobDeadLetterMoveDiscarded(ctx, &driver.JobDeadLetterMoveDiscardedParams{DiscardedFinalizedAtHorizon: now.Add(-time.Hour), Max: 10, Schema: schema})
		require.NoError(t, err)
		require.NotEmpty(t, movedDiscarded)

		_, err = exec.SequenceAppendMany(ctx, &driver.SequenceAppendManyParams{Schema: schema, SeqKeys: []string{"api-seq-a", "api-seq-b"}})
		require.NoError(t, err)
		fromTable, err := exec.SequencePromoteFromTable(ctx, &driver.SequencePromoteFromTableParams{GracePeriod: time.Millisecond, Max: 10, Now: &now, Schema: schema})
		require.NoError(t, err)
		require.NotNil(t, fromTable)
		stalled, err := exec.SequenceScanAndPromoteStalled(ctx, &driver.SequenceScanAndPromoteStalledParams{GracePeriod: time.Millisecond, Max: 10, Now: &now, Schema: schema})
		require.NoError(t, err)
		require.NotNil(t, stalled)

		require.NoError(t, exec.WorkflowInsertMany(ctx, &driver.WorkflowInsertManyParams{IDs: []string{"wf-api", "wf-api-final"}, Names: []string{"api", "api-final"}, Schema: schema}))
		root := insertWorkflowJob(ctx, t, exec, schema, "wf-api", "root", nil, rivertype.JobStateCompleted, &now)
		waitMeta := []byte(`{"workflow_wait":{"inputs":{}}}`)
		waitTask := insertWorkflowJob(ctx, t, exec, schema, "wf-api", "wait", []string{"root"}, rivertype.JobStatePending, nil)
		_, err = exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{ID: waitTask.ID, MetadataDoMerge: true, Metadata: waitMeta, Schema: schema})
		require.NoError(t, err)

		countIncomplete, err := exec.WorkflowCountIncompleteJobs(ctx, &driver.WorkflowCountIncompleteJobsParams{Schema: schema, WorkflowID: "wf-api"})
		require.NoError(t, err)
		require.GreaterOrEqual(t, countIncomplete, int64(1))

		byTask, err := exec.WorkflowJobGetByTaskName(ctx, &driver.WorkflowJobGetByTaskNameParams{Schema: schema, TaskName: "root", WorkflowID: "wf-api"})
		require.NoError(t, err)
		require.Equal(t, root.ID, byTask.ID)

		active, err := exec.WorkflowListActive(ctx, &driver.WorkflowListParams{Schema: schema, PaginationLimit: 10})
		require.NoError(t, err)
		require.Contains(t, workflowListIDs(active), "wf-api")
		all, err := exec.WorkflowListAll(ctx, &driver.WorkflowListParams{Schema: schema, PaginationLimit: 10})
		require.NoError(t, err)
		require.Contains(t, workflowListIDs(all), "wf-api")
		byIDs, err := exec.WorkflowListByIDs(ctx, &driver.WorkflowListByIDsParams{Schema: schema, WorkflowIDs: []string{"wf-api", "missing"}})
		require.NoError(t, err)
		require.Equal(t, []string{"wf-api"}, byIDs)
		waitEval, err := exec.WorkflowListByIDsForWaitEval(ctx, &driver.WorkflowListByIDsForWaitEvalParams{Schema: schema, WorkflowIDs: []string{"wf-api"}})
		require.NoError(t, err)
		require.Len(t, waitEval, 1)
		lockedWFs, err := exec.WorkflowLockByIDsSkipLocked(ctx, &driver.WorkflowLockByIDsSkipLockedParams{LimitCount: 10, Schema: schema, WorkflowIDs: []string{"wf-api"}})
		require.NoError(t, err)
		require.Contains(t, lockedWFs, "wf-api")

		loadedJobs, err := exec.WorkflowLoadJobsWithDeps(ctx, &driver.WorkflowLoadJobsWithDepsParams{JobIds: []int64{root.ID, waitTask.ID}, Schema: schema})
		require.NoError(t, err)
		require.Len(t, loadedJobs, 2)
		loadedTasks, err := exec.WorkflowLoadTasksByNames(ctx, &driver.WorkflowLoadTasksByNamesParams{Schema: schema, TaskNames: []string{"root", "wait"}, WorkflowID: "wf-api"})
		require.NoError(t, err)
		require.Len(t, loadedTasks, 2)

		hasWait, err := exec.WorkflowHasWaitTasksMany(ctx, &driver.WorkflowHasWaitTasksManyParams{Schema: schema, WorkflowIDs: []string{"wf-api"}})
		require.NoError(t, err)
		require.Contains(t, hasWait, "wf-api")
		activatable, err := exec.WorkflowWaitActivatableTaskIDsByWorkflowIDs(ctx, &driver.WorkflowWaitActivatableTaskIDsByWorkflowIDsParams{LimitCount: 10, Schema: schema, WorkflowIDs: []string{"wf-api"}})
		require.NoError(t, err)
		require.NotNil(t, activatable)
		activeWaits, err := exec.WorkflowWaitActiveTaskListByWorkflowIDs(ctx, &driver.WorkflowWaitActiveTaskListByWorkflowIDsParams{LimitCount: 10, Schema: schema, WorkflowIDs: []string{"wf-api"}})
		require.NoError(t, err)
		require.NotNil(t, activeWaits)
		outputs, err := exec.WorkflowWaitDepOutputListByWorkflowTaskPairs(ctx, &driver.WorkflowWaitDepOutputListByWorkflowTaskPairsParams{Schema: schema, Tasks: []string{"root"}, WorkflowIDs: []string{"wf-api"}})
		require.NoError(t, err)
		require.Len(t, outputs, 1)
		require.NoError(t, exec.WorkflowWaitEvalCursorUpdateByWorkflowIDMany(ctx, &driver.WorkflowWaitEvalCursorUpdateByWorkflowIDManyParams{CursorJobIDs: []int64{root.ID}, Schema: schema, WorkflowIDs: []string{"wf-api"}}))
		require.NoError(t, exec.WorkflowWaitUpdateMetadataByJobIDMany(ctx, &driver.WorkflowWaitUpdateMetadataByJobIDManyParams{JobIDs: []int64{waitTask.ID}, Schema: schema, WaitStates: [][]byte{[]byte(`{"workflow_wait":{"phase":"waiting"}}`)}}))
		activatedIDs, err := exec.WorkflowWaitActivateByJobIDMany(ctx, &driver.WorkflowWaitActivateByJobIDManyParams{JobIDs: []int64{waitTask.ID}, Now: now, Schema: schema})
		require.NoError(t, err)
		require.Contains(t, activatedIDs, waitTask.ID)

		_, err = exec.WorkflowSignalInsert(ctx, &driver.WorkflowSignalInsertParams{Key: "api-key", Payload: []byte(`{"ok":true}`), Schema: schema, WorkflowID: "wf-api"})
		require.NoError(t, err)
		byEvidence, err := exec.WorkflowSignalListByEvidence(ctx, &driver.WorkflowSignalListByEvidenceParams{Attempt: 1, Keys: []string{"api-key"}, LimitCount: 10, Schema: schema, WorkflowID: "wf-api"})
		require.NoError(t, err)
		require.NotNil(t, byEvidence)
		byKeys, err := exec.WorkflowSignalListByKeys(ctx, &driver.WorkflowSignalListByKeysParams{Keys: []string{"api-key"}, LimitCount: 10, Schema: schema, WorkflowID: "wf-api"})
		require.NoError(t, err)
		require.NotEmpty(t, byKeys)
		byWorkflowIDs, err := exec.WorkflowSignalListByWorkflowIDs(ctx, &driver.WorkflowSignalListByWorkflowIDsParams{Keys: []string{"api-key"}, Schema: schema, WorkflowIDs: []string{"wf-api"}})
		require.NoError(t, err)
		require.NotEmpty(t, byWorkflowIDs)

		check, err := exec.WorkflowRetryLockAndCheckRunning(ctx, &driver.WorkflowRetryLockAndCheckRunningParams{Schema: schema, WorkflowID: "wf-api"})
		require.NoError(t, err)
		require.True(t, check.WorkflowIsActive)
		retried, err := exec.WorkflowRetry(ctx, &driver.WorkflowRetryParams{Mode: driver.WorkflowRetryModeAll, Now: now, ResetHistory: true, Schema: schema, WorkflowID: "wf-api"})
		require.NoError(t, err)
		require.NotNil(t, retried)
		unfinalized, err := exec.WorkflowUnfinalizeIfActiveJobsMany(ctx, &driver.WorkflowUnfinalizeIfActiveJobsManyParams{Now: now, Schema: schema, WorkflowIDs: []string{"wf-api"}})
		require.NoError(t, err)
		require.Contains(t, unfinalized, "wf-api")

		failedDeps, err := exec.WorkflowCancelWithFailedDepsMany(ctx, &driver.WorkflowCancelWithFailedDepsManyParams{Schema: schema, WorkflowDepsFailedAt: now, WorkflowIDs: []string{"wf-api"}})
		require.NoError(t, err)
		require.GreaterOrEqual(t, failedDeps, int64(0))
		deletedDeps, err := exec.WorkflowCancelWithDeletedDepsMany(ctx, &driver.WorkflowCancelWithDeletedDepsManyParams{Schema: schema, WorkflowDepsFailedAt: now, WorkflowIDs: []string{"wf-api"}})
		require.NoError(t, err)
		require.GreaterOrEqual(t, deletedDeps, int64(0))

		_, err = exec.WorkflowFinalizeIfCompleteMany(ctx, &driver.WorkflowFinalizeIfCompleteManyParams{Now: now, Schema: schema, WorkflowIDs: []string{"wf-api-final"}})
		require.NoError(t, err)
		finalIDs, err := exec.WorkflowCleanupListFinalizedIDs(ctx, &driver.WorkflowCleanupListFinalizedIDsParams{FinalizedBefore: now.Add(time.Second), LimitCount: 10, Schema: schema, State: "completed"})
		require.NoError(t, err)
		require.Contains(t, finalIDs, "wf-api-final")
		candidates, err := exec.WorkflowGetFinalizationCandidates(ctx, &driver.WorkflowGetFinalizationCandidatesParams{LimitCount: 10, Schema: schema})
		require.NoError(t, err)
		require.NotNil(t, candidates)
		legacyIDs, err := exec.WorkflowGetLegacyBackfillIDs(ctx, &driver.WorkflowGetLegacyBackfillIDsParams{LimitCount: 10, Schema: schema})
		require.NoError(t, err)
		require.NotNil(t, legacyIDs)
		inited, err := exec.WorkflowInitFromJobs(ctx, &driver.WorkflowInitFromJobsParams{Schema: schema, WorkflowIDs: []string{"wf-api"}})
		require.NoError(t, err)
		require.NotNil(t, inited)

		require.NoError(t, exec.WorkflowCleanupDeleteAttemptTasksByWorkflowIDs(ctx, &driver.WorkflowCleanupDeleteByWorkflowIDsParams{Schema: schema, WorkflowIDs: []string{"wf-api"}}))
		require.NoError(t, exec.WorkflowCleanupDeleteAttemptsByWorkflowIDs(ctx, &driver.WorkflowCleanupDeleteByWorkflowIDsParams{Schema: schema, WorkflowIDs: []string{"wf-api"}}))
		require.NoError(t, exec.WorkflowCleanupDeleteDeadLetterJobsByWorkflowIDs(ctx, &driver.WorkflowCleanupDeleteByWorkflowIDsParams{Schema: schema, WorkflowIDs: []string{"wf-api"}}))
		require.NoError(t, exec.WorkflowCleanupDeleteJobsByWorkflowIDs(ctx, &driver.WorkflowCleanupDeleteByWorkflowIDsParams{Schema: schema, WorkflowIDs: []string{"wf-api"}}))
		require.NoError(t, exec.WorkflowCleanupDeleteSignalsByWorkflowIDs(ctx, &driver.WorkflowCleanupDeleteByWorkflowIDsParams{Schema: schema, WorkflowIDs: []string{"wf-api"}}))
		require.NoError(t, exec.WorkflowCleanupDeleteTimersByWorkflowIDs(ctx, &driver.WorkflowCleanupDeleteByWorkflowIDsParams{Schema: schema, WorkflowIDs: []string{"wf-api"}}))
		require.NoError(t, exec.WorkflowCleanupDeleteWorklistByWorkflowIDs(ctx, &driver.WorkflowCleanupDeleteByWorkflowIDsParams{Schema: schema, WorkflowIDs: []string{"wf-api"}}))
		require.NoError(t, exec.WorkflowCleanupDeleteWorkflowsByWorkflowIDs(ctx, &driver.WorkflowCleanupDeleteWorkflowsByWorkflowIDsParams{Schema: schema, State: "active", WorkflowIDs: []string{"wf-api"}}))
	})
}

func execSchema[TTx any](ctx context.Context, t *testing.T, executorWithTx func(context.Context, *testing.T) (driver.ProExecutor, driver.ProDriver[TTx])) (driver.ProExecutor, string) {
	t.Helper()
	exec, d := executorWithTx(ctx, t)
	RequireProDriver(t, d)
	var schema string
	require.NoError(t, exec.QueryRow(ctx, `select current_schema()`).Scan(&schema))
	require.NotEmpty(t, schema)
	return exec, schema
}

func requireTableExists(ctx context.Context, t *testing.T, exec driver.ProExecutor, schema, table string) {
	t.Helper()
	var exists bool
	require.NoError(t, exec.QueryRow(ctx, `select exists (select 1 from information_schema.tables where table_schema = $1 and table_name = $2)`, schema, table).Scan(&exists))
	require.True(t, exists, "expected %s.%s to exist", schema, table)
}

func requireRowCount(ctx context.Context, t *testing.T, exec driver.ProExecutor, schema, table string, want int) {
	t.Helper()
	var got int
	require.NoError(t, exec.QueryRow(ctx, fmt.Sprintf(`select count(*) from %s`, qname(schema, table))).Scan(&got))
	require.Equal(t, want, got, "%s.%s row count", schema, table)
}

func qname(schema, table string) string {
	if !safeIdentRE.MatchString(schema) || !safeIdentRE.MatchString(table) {
		panic("unsafe SQL identifier")
	}
	return fmt.Sprintf(`"%s"."%s"`, schema, table)
}

func insertJob(ctx context.Context, t *testing.T, exec driver.ProExecutor, schema, kind string, state rivertype.JobState, metadata, args []byte, finalizedAt *time.Time) *rivertype.JobRow {
	t.Helper()
	if len(metadata) == 0 {
		metadata = []byte(`{}`)
	}
	if len(args) == 0 {
		args = []byte(`{}`)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	job, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{CreatedAt: &now, EncodedArgs: args, FinalizedAt: finalizedAt, Kind: kind, MaxAttempts: 3, Metadata: metadata, Priority: 1, Queue: "default", ScheduledAt: &now, Schema: schema, State: state})
	require.NoError(t, err)
	return job
}

func insertWorkflowJob(ctx context.Context, t *testing.T, exec driver.ProExecutor, schema, workflowID, task string, deps []string, state rivertype.JobState, finalizedAt *time.Time) *rivertype.JobRow {
	t.Helper()
	meta := map[string]any{
		riverworkflow.MetadataKeyWorkflowID:   workflowID,
		riverworkflow.MetadataKeyWorkflowTask: task,
	}
	if deps != nil {
		meta[riverworkflow.MetadataKeyWorkflowDeps] = deps
	}
	metadata, err := json.Marshal(meta)
	require.NoError(t, err)
	return insertJob(ctx, t, exec, schema, "wf-"+task, state, metadata, []byte(`{}`), finalizedAt)
}

func ptrTime(t time.Time) *time.Time { return &t }

func periodicIDs(jobs []*driver.PeriodicJob) []string {
	ids := make([]string, 0, len(jobs))
	for _, j := range jobs {
		ids = append(ids, j.ID)
	}
	sort.Strings(ids)
	return ids
}

func sequenceKeys(seqs []*driver.Sequence) []string {
	keys := make([]string, 0, len(seqs))
	for _, s := range seqs {
		keys = append(keys, s.Key)
	}
	sort.Strings(keys)
	return keys
}

func workflowListIDs(items []*driver.WorkflowListItem) []string {
	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.ID)
	}
	sort.Strings(ids)
	return ids
}

func readyJobIDs(rows []*driver.WorkflowReadyTaskIDsByWorkflowIDsRow) []int64 {
	ids := make([]int64, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func signalKeys(signals []*driver.WorkflowSignal) []string {
	keys := make([]string, 0, len(signals))
	for _, s := range signals {
		keys = append(keys, s.Key)
	}
	return keys
}

func timerWorkflowIDs(timers []*driver.WorkflowTimer) []string {
	ids := make([]string, 0, len(timers))
	for _, timer := range timers {
		ids = append(ids, timer.WorkflowID)
	}
	sort.Strings(ids)
	return ids
}

func worklistReasons(rows []*driver.WorkflowWorklistDeleteByWorkflowIDsReturningReasonsRow) []int16 {
	reasons := make([]int16, 0, len(rows))
	for _, row := range rows {
		reasons = append(reasons, row.Reason)
	}
	sort.Slice(reasons, func(i, j int) bool { return reasons[i] < reasons[j] })
	return reasons
}
