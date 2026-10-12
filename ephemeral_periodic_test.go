package riverpro

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/require"
)

func (matrixEphemeralArgs) KindAliases() []string { return []string{"matrix_ephemeral_old"} }

func TestEphemeralDurablePeriodicCompletion(t *testing.T) {
	for _, tc := range []struct {
		name, kind                    string
		queueEphemeral, fail, deleted bool
	}{
		{name: "kind", kind: "matrix_ephemeral", deleted: true},
		{name: "alias", kind: "matrix_ephemeral_old", deleted: true},
		{name: "queue", kind: "matrix_retention", queueEphemeral: true, deleted: true},
		{name: "ordinary", kind: "matrix_retention"},
		{name: "failed_kind", kind: "matrix_ephemeral", fail: true},
		{name: "failed_queue", kind: "matrix_retention", queueEphemeral: true, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			workers := river.NewWorkers()
			workErr := func() error {
				if tc.fail {
					return errors.New("intentional failure")
				}
				return nil
			}
			river.AddWorker(workers, river.WorkFunc(func(context.Context, *river.Job[matrixEphemeralArgs]) error { return workErr() }))
			river.AddWorker(workers, river.WorkFunc(func(context.Context, *river.Job[matrixRetentionArgs]) error { return workErr() }))
			client, drv, schema := newMatrixClient(t, ctx, &Config{
				Config:              river.Config{Workers: workers},
				DurablePeriodicJobs: DurablePeriodicJobsConfig{Enabled: true},
				ProQueues:           map[string]QueueConfig{river.QueueDefault: {MaxWorkers: 1, Ephemeral: QueueEphemeralConfig{Enabled: tc.queueEphemeral}}},
			})
			_, err := client.PeriodicJobInsert(ctx, &PeriodicJobInsertOpts{
				ID: "ephemeral-test", Kind: tc.kind, Args: []byte(`{}`), MaxAttempts: 1,
				Schedule: &PeriodicJobSchedule{CronExpression: "@every 1h", NextRunAt: time.Now().Add(-time.Minute)},
			})
			require.NoError(t, err)
			require.NoError(t, client.periodicEnqueueOnce(ctx))
			var id int64
			require.NoError(t, drv.GetExecutor().QueryRow(ctx, `SELECT id FROM `+schema+`.river_job WHERE kind = $1`, tc.kind).Scan(&id))
			row, err := drv.GetExecutor().JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: id, Schema: schema})
			require.NoError(t, err)
			require.False(t, metadataBoolTest(t, row.Metadata, metadataKeyEphemeral), "periodic insertion must exercise the missing metadata path")
			require.NoError(t, client.Start(ctx))
			t.Cleanup(func() { require.NoError(t, client.Stop(context.Background())) })
			waitMatrix(t, func() bool {
				row, err := drv.GetExecutor().JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: id, Schema: schema})
				if tc.deleted {
					return errors.Is(err, rivertype.ErrNotFound)
				}
				if err != nil {
					return false
				}
				if tc.fail {
					return row.State == rivertype.JobStateDiscarded
				}
				return row.State == rivertype.JobStateCompleted
			})
		})
	}
}
