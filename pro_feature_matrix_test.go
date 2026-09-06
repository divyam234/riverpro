package riverpro

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/require"

	prodriver "github.com/divyam234/riverpro/driver"
	"github.com/divyam234/riverpro/driver/riverpropgxv5"
)

type matrixBatchArgs struct {
	CustomerID int64  `json:"customer_id" river:"batch"`
	TraceID    string `json:"trace_id"`
}

func (matrixBatchArgs) Kind() string         { return "matrix_batch" }
func (matrixBatchArgs) BatchOpts() BatchOpts { return BatchOpts{ByArgs: true} }

type matrixSeqArgs struct {
	AccountID string `json:"account_id" river:"sequence"`
}

func (matrixSeqArgs) Kind() string               { return "matrix_sequence" }
func (matrixSeqArgs) SequenceOpts() SequenceOpts { return SequenceOpts{ByArgs: true, ByQueue: true} }

type matrixEphemeralArgs struct{}

func (matrixEphemeralArgs) Kind() string                 { return "matrix_ephemeral" }
func (matrixEphemeralArgs) EphemeralOpts() EphemeralOpts { return EphemeralOpts{} }

type matrixResumeArgs struct{}

func (matrixResumeArgs) Kind() string { return "matrix_resume" }

type matrixRetentionArgs struct{ Label string }

func (matrixRetentionArgs) Kind() string { return "matrix_retention" }

type matrixBatchWorker struct{ count int }

func (w *matrixBatchWorker) WorkMany(ctx context.Context, jobs []*river.Job[matrixBatchArgs]) error {
	w.count += len(jobs)
	return nil
}

func TestProFeatureMatrix_BatchSequenceEphemeralInsertMetadata(t *testing.T) {
	ctx := context.Background()
	client, drv, schema := newMatrixClient(t, ctx, &Config{})
	_ = client

	batchRes, err := client.Insert(ctx, matrixBatchArgs{CustomerID: 7, TraceID: "a"}, nil)
	require.NoError(t, err)
	seqRes, err := client.InsertMany(ctx, []river.InsertManyParams{{Args: matrixSeqArgs{AccountID: "acct-a"}}, {Args: matrixSeqArgs{AccountID: "acct-a"}}})
	require.NoError(t, err)
	ephRes, err := client.Insert(ctx, matrixEphemeralArgs{}, nil)
	require.NoError(t, err)

	batchJob, err := drv.GetExecutor().JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: batchRes.Job.ID, Schema: schema})
	require.NoError(t, err)
	require.NotEmpty(t, metadataStringTest(t, batchJob.Metadata, metadataKeyBatchKey))

	firstSeq, err := drv.GetExecutor().JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: seqRes[0].Job.ID, Schema: schema})
	require.NoError(t, err)
	secondSeq, err := drv.GetExecutor().JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: seqRes[1].Job.ID, Schema: schema})
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateAvailable, firstSeq.State)
	require.Equal(t, rivertype.JobStatePending, secondSeq.State)
	require.NotEmpty(t, metadataStringTest(t, secondSeq.Metadata, metadataKeySequenceKey))

	ephJob, err := drv.GetExecutor().JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: ephRes.Job.ID, Schema: schema})
	require.NoError(t, err)
	require.True(t, metadataBoolTest(t, ephJob.Metadata, metadataKeyEphemeral))
}

func TestProFeatureMatrix_BatchingFetchesOnlySameBatchKey(t *testing.T) {
	ctx := context.Background()
	client, drv, schema := newMatrixClient(t, ctx, &Config{})
	_, err := client.InsertMany(ctx, []river.InsertManyParams{
		{Args: matrixBatchArgs{CustomerID: 1, TraceID: "a"}},
		{Args: matrixBatchArgs{CustomerID: 1, TraceID: "b"}},
		{Args: matrixBatchArgs{CustomerID: 2, TraceID: "c"}},
	})
	require.NoError(t, err)
	now := time.Now()
	leader, err := drv.GetExecutor().JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{ClientID: "leader", MaxAttemptedBy: 5, MaxToLock: 1, Now: &now, Queue: river.QueueDefault, Schema: schema})
	require.NoError(t, err)
	require.Len(t, leader, 1)
	batchKey := metadataStringTest(t, leader[0].Metadata, metadataKeyBatchKey)
	peers, err := drv.GetProExecutor().JobGetAvailableForBatch(ctx, &prodriver.JobGetAvailableForBatchParams{AttemptedBy: "leader", BatchKey: batchKey, BatchLeaderJobID: leader[0].ID, Kind: leader[0].Kind, Max: 10, Queue: river.QueueDefault, Schema: schema})
	require.NoError(t, err)
	require.Len(t, peers, 1)
	require.Equal(t, batchKey, metadataStringTest(t, peers[0].Metadata, metadataKeyBatchKey))
}

func TestProFeatureMatrix_QueueResumeFetchesAvailableJobs(t *testing.T) {
	ctx := context.Background()
	started := make(chan struct{}, 1)
	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(context.Context, *river.Job[matrixResumeArgs]) error {
		started <- struct{}{}
		return nil
	}))
	pool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	drv := riverpropgxv5.New(pool)
	schema := riverdbtest.TestSchema(ctx, t, drv, nil)
	client, err := NewClient(drv, &Config{
		Config: river.Config{
			Schema:            schema,
			Workers:           workers,
			Queues:            map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 1}},
			FetchCooldown:     20 * time.Millisecond,
			FetchPollInterval: 5 * time.Second,
			TestOnly:          true,
		},
		ProQueues: map[string]QueueConfig{
			river.QueueDefault: {Concurrency: ConcurrencyConfig{GlobalLimit: 1}, MaxWorkers: 1},
		},
	})
	require.NoError(t, err)
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { require.NoError(t, client.Stop(context.Background())) })
	require.NoError(t, client.QueuePause(ctx, river.QueueDefault, nil))
	inserted, err := client.Insert(ctx, matrixResumeArgs{}, nil)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	job, err := drv.GetExecutor().JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: inserted.Job.ID, Schema: schema})
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateAvailable, job.State)
	require.NoError(t, client.QueueResume(ctx, river.QueueDefault, nil))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		job, getErr := drv.GetExecutor().JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: inserted.Job.ID, Schema: schema})
		require.NoError(t, getErr)
		t.Fatalf("resumed queue did not run available job; state=%s", job.State)
	}
}

func TestProFeatureMatrix_EphemeralCompletionDeletesJob(t *testing.T) {
	ctx := context.Background()
	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(ctx context.Context, job *river.Job[matrixEphemeralArgs]) error { return nil }))
	client, drv, schema := newMatrixClient(t, ctx, &Config{Config: river.Config{Workers: workers}})
	inserted, err := client.Insert(ctx, matrixEphemeralArgs{}, nil)
	require.NoError(t, err)
	ch, cancel := client.Subscribe(river.EventKindJobCompleted)
	defer cancel()
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { require.NoError(t, client.Stop(context.Background())) })
	riversharedtest.WaitOrTimeoutN(t, ch, 1)
	waitMatrix(t, func() bool {
		_, err := drv.GetExecutor().JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: inserted.Job.ID, Schema: schema})
		return errors.Is(err, rivertype.ErrNotFound)
	})
}

func TestProFeatureMatrix_DeadLetterAndRetention(t *testing.T) {
	ctx := context.Background()
	client, drv, schema := newMatrixClient(t, ctx, &Config{
		Config:     river.Config{DiscardedJobRetentionPeriod: time.Hour},
		DeadLetter: DeadLetterConfig{Enabled: true},
		ProQueues:  map[string]QueueConfig{"short_retention": {CompletedJobRetentionPeriod: time.Millisecond, MaxWorkers: 1}},
	})
	exec := drv.GetExecutor()
	now := time.Now().Add(-time.Hour)
	dead, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{CreatedAt: &now, EncodedArgs: []byte(`{}`), Kind: "dead", MaxAttempts: 1, Priority: 1, Metadata: []byte(`{}`), Queue: river.QueueDefault, ScheduledAt: &now, Schema: schema, State: rivertype.JobStateRunning})
	require.NoError(t, err)
	pilot := client.Client.Pilot()
	_, err = pilot.JobSetStateIfRunningMany(ctx, exec, setStateManyForTest(schema, riverdriver.JobSetStateDiscarded(dead.ID, now, []byte(`{"error":"boom"}`), nil)))
	require.NoError(t, err)
	_, err = drv.GetProExecutor().JobDeadLetterGetByID(ctx, &prodriver.JobDeadLetterGetByIDParams{ID: dead.ID, Schema: schema})
	require.ErrorIs(t, err, rivertype.ErrNotFound)
	stored, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: dead.ID, Schema: schema})
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateDiscarded, stored.State)

	client.deadLetterRetentionPeriod = 0
	require.NoError(t, client.moveDiscardedToDeadLetterOnce(ctx))
	_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: dead.ID, Schema: schema})
	require.ErrorIs(t, err, rivertype.ErrNotFound)
	deadLettered, err := drv.GetProExecutor().JobDeadLetterGetByID(ctx, &prodriver.JobDeadLetterGetByIDParams{ID: dead.ID, Schema: schema})
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateDiscarded, deadLettered.State)

	old := time.Now().Add(-time.Hour)
	keep, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{CreatedAt: &old, EncodedArgs: []byte(`{}`), FinalizedAt: &old, Kind: "retention_keep", MaxAttempts: 1, Priority: 1, Metadata: []byte(`{}`), Queue: river.QueueDefault, ScheduledAt: &old, Schema: schema, State: rivertype.JobStateCompleted})
	require.NoError(t, err)
	remove, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{CreatedAt: &old, EncodedArgs: []byte(`{}`), FinalizedAt: &old, Kind: "retention_remove", MaxAttempts: 1, Priority: 1, Metadata: []byte(`{}`), Queue: "short_retention", ScheduledAt: &old, Schema: schema, State: rivertype.JobStateCompleted})
	require.NoError(t, err)
	require.NoError(t, client.cleanProQueuesOnce(ctx))
	_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: keep.ID, Schema: schema})
	require.NoError(t, err)
	_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: remove.ID, Schema: schema})
	require.ErrorIs(t, err, rivertype.ErrNotFound)
}

func TestProFeatureMatrix_SequencePromotionAndConcurrencyRuntime(t *testing.T) {
	ctx := context.Background()
	client, drv, schema := newMatrixClient(t, ctx, &Config{ProQueues: map[string]QueueConfig{river.QueueDefault: {Concurrency: ConcurrencyConfig{GlobalLimit: 1, Partition: PartitionConfig{ByKind: true}}, MaxWorkers: 10}}})
	res, err := client.InsertMany(ctx, []river.InsertManyParams{{Args: matrixSeqArgs{AccountID: "acct-z"}}, {Args: matrixSeqArgs{AccountID: "acct-z"}}})
	require.NoError(t, err)
	now := time.Now()
	first, err := drv.GetProExecutor().JobGetAvailableLimited(ctx, &prodriver.JobGetAvailableLimitedParams{GlobalLimit: 1, PartitionByKind: true, JobGetAvailableParams: &riverdriver.JobGetAvailableParams{ClientID: "c1", MaxAttemptedBy: 5, MaxToLock: 10, Now: &now, Queue: river.QueueDefault, Schema: schema}})
	require.NoError(t, err)
	require.Len(t, first, 1)
	second, err := drv.GetProExecutor().JobGetAvailableLimited(ctx, &prodriver.JobGetAvailableLimitedParams{GlobalLimit: 1, PartitionByKind: true, JobGetAvailableParams: &riverdriver.JobGetAvailableParams{ClientID: "c2", MaxAttemptedBy: 5, MaxToLock: 10, Now: &now, Queue: river.QueueDefault, Schema: schema}})
	require.NoError(t, err)
	require.Empty(t, second)
	seqKey := metadataStringTest(t, first[0].Metadata, metadataKeySequenceKey)
	_, err = drv.GetExecutor().JobSetStateIfRunningMany(ctx, setStateManyForTest(schema, riverdriver.JobSetStateCompleted(first[0].ID, now, nil)))
	require.NoError(t, err)
	_, err = drv.GetProExecutor().SequencePromote(ctx, &prodriver.SequencePromoteParams{Keys: []string{seqKey}, Now: &now, Schema: schema})
	require.NoError(t, err)
	promoted, err := drv.GetExecutor().JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: res[1].Job.ID, Schema: schema})
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateAvailable, promoted.State)
}

func setStateManyForTest(schema string, params ...*riverdriver.JobSetStateIfRunningParams) *riverdriver.JobSetStateIfRunningManyParams {
	batch := &riverdriver.JobSetStateIfRunningManyParams{Schema: schema}
	for _, param := range params {
		batch.ID = append(batch.ID, param.ID)
		batch.Attempt = append(batch.Attempt, param.Attempt)
		batch.ErrData = append(batch.ErrData, param.ErrData)
		batch.FinalizedAt = append(batch.FinalizedAt, param.FinalizedAt)
		batch.MetadataDoMerge = append(batch.MetadataDoMerge, param.MetadataDoMerge)
		batch.MetadataUpdates = append(batch.MetadataUpdates, param.MetadataUpdates)
		batch.ScheduledAt = append(batch.ScheduledAt, param.ScheduledAt)
		batch.State = append(batch.State, param.State)
	}
	return batch
}

func newMatrixClient(t *testing.T, ctx context.Context, config *Config) (*Client[pgx.Tx], prodriver.ProDriver[pgx.Tx], string) {
	t.Helper()
	pool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	drv := riverpropgxv5.New(pool)
	schema := riverdbtest.TestSchema(ctx, t, drv, nil)
	if config == nil {
		config = &Config{}
	}
	config.Schema = schema
	config.TestOnly = true
	config.PollOnly = true
	if config.Workers != nil {
		config.Queues = map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 10}}
		config.FetchCooldown = 20 * time.Millisecond
		config.FetchPollInterval = 20 * time.Millisecond
	}
	client, err := NewClient(drv, config)
	require.NoError(t, err)
	return client, drv, schema
}

func metadataStringTest(t *testing.T, data []byte, key string) string {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))
	v, _ := m[key].(string)
	return v
}

func metadataBoolTest(t *testing.T, data []byte, key string) bool {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))
	v, _ := m[key].(bool)
	return v
}

func waitMatrix(t *testing.T, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func TestProFeatureMatrix_GracefulShutdownDeletesProducer(t *testing.T) {
	ctx := context.Background()
	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(ctx context.Context, job *river.Job[matrixNoopArgs]) error { return nil }))
	client, drv, schema := newMatrixClient(t, ctx, &Config{
		Config: river.Config{Workers: workers, Queues: map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 1}}},
	})

	require.NoError(t, client.Start(ctx))
	clientID := client.Client.ID()

	waitMatrix(t, func() bool {
		rows, err := drv.GetProExecutor().ProducerListByQueue(ctx, &prodriver.ProducerListByQueueParams{QueueName: river.QueueDefault, Schema: schema})
		if err != nil || len(rows) == 0 {
			return false
		}
		return rows[0].Producer != nil && rows[0].Producer.ClientID == clientID
	})

	producers, err := client.ProducerList(ctx, &ProducerListOpts{Queue: river.QueueDefault})
	require.NoError(t, err)
	require.Len(t, producers, 1)
	require.Equal(t, clientID, producers[0].ClientID)
	require.Equal(t, river.QueueDefault, producers[0].Queue)
	require.Equal(t, 10, producers[0].MaxWorkers)

	rows, err := drv.GetProExecutor().ProducerListByQueue(ctx, &prodriver.ProducerListByQueueParams{QueueName: river.QueueDefault, Schema: schema})
	require.NoError(t, err)
	require.Len(t, rows, 1, "producer row should exist while client is running")

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, client.Stop(stopCtx))

	waitMatrix(t, func() bool {
		rows, err := drv.GetProExecutor().ProducerListByQueue(ctx, &prodriver.ProducerListByQueueParams{QueueName: river.QueueDefault, Schema: schema})
		if err != nil {
			return false
		}
		return len(rows) == 0
	})
}

func TestProFeatureMatrix_CrashedClientLeavesStaleProducerAndReaperCleansIt(t *testing.T) {
	ctx := context.Background()
	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(ctx context.Context, job *river.Job[matrixNoopArgs]) error { return nil }))
	client, drv, schema := newMatrixClient(t, ctx, &Config{
		ProducerRetentionEnabled:     true,
		ProducerRetentionInterval:    50 * time.Millisecond,
		ProducerStaleRetentionPeriod: time.Second,
		Config:                       river.Config{Workers: workers, Queues: map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 1}}},
	})

	stale := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	_, err := drv.GetProExecutor().ProducerInsertOrUpdate(ctx, &prodriver.ProducerInsertOrUpdateParams{
		ClientID: "crashed-client", CreatedAt: &stale, MaxWorkers: 1, QueueName: river.QueueDefault, Schema: schema, UpdatedAt: &stale,
	})
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	waitMatrix(t, func() bool {
		rows, err := drv.GetProExecutor().ProducerListByQueue(ctx, &prodriver.ProducerListByQueueParams{QueueName: river.QueueDefault, Schema: schema})
		if err != nil {
			return false
		}
		return len(rows) == 0
	})
}

type matrixNoopArgs struct{}

func (matrixNoopArgs) Kind() string { return "matrix_noop" }

type matrixPeriodicArgs struct {
	Note string `json:"note"`
}

func (matrixPeriodicArgs) Kind() string { return "matrix_periodic" }

type matrixPeriodicWorker struct {
	river.WorkerDefaults[matrixPeriodicArgs]
	called chan struct{}
}

func (w *matrixPeriodicWorker) Work(ctx context.Context, job *river.Job[matrixPeriodicArgs]) error {
	if w.called != nil {
		select {
		case w.called <- struct{}{}:
		default:
		}
	}
	return nil
}

func TestProFeatureMatrix_DurablePeriodicJobs_OneShotFires(t *testing.T) {
	ctx := context.Background()
	workers := river.NewWorkers()
	called := make(chan struct{}, 8)
	river.AddWorker[matrixPeriodicArgs](workers, &matrixPeriodicWorker{called: called})
	client, _, schema := newMatrixClient(t, ctx, &Config{
		DurablePeriodicJobs: DurablePeriodicJobsConfig{
			Enabled:      true,
			PollInterval: 50 * time.Millisecond,
		},
		Config: river.Config{Workers: workers, Queues: map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 1}}},
	})

	_, err := client.PeriodicJobUpsert(ctx, &PeriodicJobUpsertOpts{
		ID:   "matrix-once",
		Kind: "matrix_periodic",
		Args: []byte(`{"note":"hello"}`),
		Schedule: &PeriodicJobSchedule{
			NextRunAt: time.Now().Add(-time.Second),
		},
	})
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Stop(stopCtx)
	})

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("periodic job did not fire within 5s")
	}

	// One-shot row should be gone now.
	_, err = client.PeriodicJobGet(ctx, "matrix-once")
	require.ErrorIs(t, err, rivertype.ErrNotFound)
	_ = schema
}

func TestProFeatureMatrix_DurablePeriodicJobs_CronFires(t *testing.T) {
	ctx := context.Background()
	workers := river.NewWorkers()
	called := make(chan struct{}, 16)
	river.AddWorker[matrixPeriodicArgs](workers, &matrixPeriodicWorker{called: called})
	client, _, _ := newMatrixClient(t, ctx, &Config{
		DurablePeriodicJobs: DurablePeriodicJobsConfig{
			Enabled:      true,
			PollInterval: 50 * time.Millisecond,
		},
		Config: river.Config{Workers: workers, Queues: map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 1}}},
	})

	_, err := client.PeriodicJobUpsert(ctx, &PeriodicJobUpsertOpts{
		ID:      "matrix-cron",
		JobArgs: matrixPeriodicArgs{},
		Schedule: &PeriodicJobSchedule{
			CronExpression: "* * * * *",
			CronTimezone:   "UTC",
			NextRunAt:      time.Now().Add(-time.Second),
		},
	})
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Stop(stopCtx)
	})

	// First fire.
	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("cron job did not fire within 5s")
	}

	// Row should still exist (cron keeps it).
	got, err := client.PeriodicJobGet(ctx, "matrix-cron")
	require.NoError(t, err)
	require.NotNil(t, got.CronExpression)
	require.Equal(t, "* * * * *", *got.CronExpression)
}

func TestProFeatureMatrix_DurablePeriodicJobs_PauseResume(t *testing.T) {
	ctx := context.Background()
	workers := river.NewWorkers()
	called := make(chan struct{}, 8)
	river.AddWorker[matrixPeriodicArgs](workers, &matrixPeriodicWorker{called: called})
	client, _, _ := newMatrixClient(t, ctx, &Config{
		DurablePeriodicJobs: DurablePeriodicJobsConfig{
			Enabled:      true,
			PollInterval: 50 * time.Millisecond,
		},
		Config: river.Config{Workers: workers, Queues: map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 1}}},
	})

	schedule := &PeriodicJobSchedule{
		CronExpression: "* * * * *",
		NextRunAt:      time.Now().Add(-time.Second),
	}

	_, err := client.PeriodicJobUpsert(ctx, &PeriodicJobUpsertOpts{
		ID:       "matrix-pr",
		JobArgs:  matrixPeriodicArgs{},
		Schedule: schedule,
	})
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Stop(stopCtx)
	})

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("job did not fire before pause")
	}

	// Pause it without resubmitting the job definition.
	paused, err := client.PeriodicJobPause(ctx, "matrix-pr")
	require.NoError(t, err)
	require.NotNil(t, paused.PausedAt)

	// Drain any pending calls.
	for {
		select {
		case <-called:
		case <-time.After(100 * time.Millisecond):
			goto afterPause
		}
	}
afterPause:
	// No calls for 500ms while paused.
	select {
	case <-called:
		t.Fatal("paused job fired")
	case <-time.After(500 * time.Millisecond):
	}

	// Resume without changing the stored next run.
	resumed, err := client.PeriodicJobResume(ctx, "matrix-pr")
	require.NoError(t, err)
	require.Nil(t, resumed.PausedAt)
}

func TestProFeatureMatrix_DurablePeriodicJobs_ListIncludesPaused(t *testing.T) {
	ctx := context.Background()
	client, _, _ := newMatrixClient(t, ctx, &Config{
		DurablePeriodicJobs: DurablePeriodicJobsConfig{Enabled: true},
	})

	_, err := client.PeriodicJobInsert(ctx, &PeriodicJobInsertOpts{
		ID:      "matrix-list-paused",
		JobArgs: matrixPeriodicArgs{},
		Schedule: &PeriodicJobSchedule{
			CronExpression: "0 * * * *",
		},
		Paused: true,
	})
	require.NoError(t, err)

	listed, err := client.PeriodicJobList(ctx, nil)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, "matrix-list-paused", listed[0].ID)
	require.NotNil(t, listed[0].PausedAt)
}

func TestProFeatureMatrix_DurablePeriodicJobs_ListTxIncludesPaused(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	drv := riverpropgxv5.New(pool)
	schema := riverdbtest.TestSchema(ctx, t, drv, nil)
	client, err := NewClient(drv, &Config{
		Config: river.Config{
			Schema:   schema,
			TestOnly: true,
			PollOnly: true,
		},
		DurablePeriodicJobs: DurablePeriodicJobsConfig{Enabled: true},
	})
	require.NoError(t, err)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })

	_, err = client.PeriodicJobInsertTx(ctx, tx, &PeriodicJobInsertOpts{
		ID:      "matrix-list-tx-paused",
		JobArgs: matrixPeriodicArgs{},
		Schedule: &PeriodicJobSchedule{
			CronExpression: "0 * * * *",
		},
	})
	require.NoError(t, err)

	_, err = client.PeriodicJobPauseTx(ctx, tx, "matrix-list-tx-paused")
	require.NoError(t, err)

	listed, err := client.PeriodicJobListTx(ctx, tx, nil)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, "matrix-list-tx-paused", listed[0].ID)
	require.NotNil(t, listed[0].PausedAt)
}

func TestProFeatureMatrix_DurablePeriodicJobs_UpdateSchedule(t *testing.T) {
	ctx := context.Background()
	workers := river.NewWorkers()
	river.AddWorker[matrixPeriodicArgs](workers, &matrixPeriodicWorker{called: make(chan struct{}, 8)})
	client, _, _ := newMatrixClient(t, ctx, &Config{
		DurablePeriodicJobs: DurablePeriodicJobsConfig{
			Enabled:      true,
			PollInterval: 50 * time.Millisecond,
		},
		Config: river.Config{Workers: workers, Queues: map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 1}}},
	})

	initialNextRun := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	inserted, err := client.PeriodicJobInsert(ctx, &PeriodicJobInsertOpts{
		ID:      "matrix-update",
		JobArgs: matrixPeriodicArgs{Note: "initial"},
		Schedule: &PeriodicJobSchedule{
			NextRunAt: initialNextRun,
		},
	})
	require.NoError(t, err)
	require.True(t, initialNextRun.Equal(inserted.NextRunAt))

	_, err = client.PeriodicJobInsert(ctx, &PeriodicJobInsertOpts{
		ID:      "matrix-update",
		JobArgs: matrixPeriodicArgs{},
		Schedule: &PeriodicJobSchedule{
			NextRunAt: initialNextRun,
		},
	})
	require.ErrorIs(t, err, ErrPeriodicJobAlreadyExists)

	argsUpdated, err := client.PeriodicJobUpdate(ctx, "matrix-update", &PeriodicJobUpdateOpts{
		JobArgs: matrixPeriodicArgs{Note: "updated"},
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"note":"updated"}`, string(argsUpdated.Args))
	require.True(t, initialNextRun.Equal(argsUpdated.NextRunAt), "args update changed next_run_at")

	cron := "0 0 * * *"
	scheduleUpdated, err := client.PeriodicJobUpdate(ctx, "matrix-update", &PeriodicJobUpdateOpts{
		Schedule: &PeriodicJobSchedule{
			CronExpression: cron,
			CronTimezone:   "UTC",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, scheduleUpdated.CronExpression)
	require.Equal(t, cron, *scheduleUpdated.CronExpression)
	require.True(t, scheduleUpdated.NextRunAt.After(time.Now()), "schedule update did not calculate a future next run")
}

func TestProFeatureMatrix_DurablePeriodicJobs_DisabledGating(t *testing.T) {
	ctx := context.Background()
	workers := river.NewWorkers()
	river.AddWorker[matrixPeriodicArgs](workers, &matrixPeriodicWorker{called: make(chan struct{}, 1)})
	// No DurablePeriodicJobs enabled.
	client, _, _ := newMatrixClient(t, ctx, &Config{
		Config: river.Config{Workers: workers, Queues: map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 1}}},
	})

	_, err := client.PeriodicJobUpsert(ctx, &PeriodicJobUpsertOpts{
		ID:   "matrix-disabled",
		Kind: "matrix_periodic",
		Schedule: &PeriodicJobSchedule{
			NextRunAt: time.Now().Add(-time.Second),
		},
	})
	require.ErrorIs(t, err, prodriver.ErrNotSupported)

	_, err = client.PeriodicJobList(ctx, nil)
	require.ErrorIs(t, err, prodriver.ErrNotSupported)
}

// TestProFeatureMatrix_DurablePeriodicJobs_ListenNotifyWakesEnqueuer
// verifies that adding a due periodic job wakes the enqueuer loop
// via LISTEN/NOTIFY — not via the 1s polling tick. With PollOnly=false
// the worker should fire well under 1s after the Add call.
func TestProFeatureMatrix_DurablePeriodicJobs_ListenNotifyWakesEnqueuer(t *testing.T) {
	ctx := context.Background()
	workers := river.NewWorkers()
	called := make(chan time.Time, 8)
	river.AddWorker[matrixPeriodicArgs](workers, &matrixPeriodicArgsWorker{when: called})
	// PollInterval is set to a deliberately long 5s. If the loop falls
	// back to polling (or if the LISTEN/NOTIFY path is broken) the test
	// will hang. Successful LISTEN wakeup is well under 1s.
	client, _, _ := newMatrixClient(t, ctx, &Config{
		DurablePeriodicJobs: DurablePeriodicJobsConfig{
			Enabled:      true,
			PollInterval: 5 * time.Second,
		},
		Config: river.Config{Workers: workers, Queues: map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 1}}},
	})

	// Start the client BEFORE adding the row so the listener is open
	// and the subsequent PeriodicJobUpsert NOTIFY is delivered to it.
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Stop(stopCtx)
	})

	addedAt := time.Now()
	_, err := client.PeriodicJobUpsert(ctx, &PeriodicJobUpsertOpts{
		ID:   "matrix-listen",
		Kind: "matrix_periodic",
		Schedule: &PeriodicJobSchedule{
			NextRunAt: addedAt.Add(-time.Second),
		},
	})
	require.NoError(t, err)

	select {
	case <-called:
		// The wakeup latency is from Add to worker. With LISTEN/NOTIFY
		// this should be a few hundred ms at most. With 5s polling it
		// would always exceed 1s.
		elapsed := time.Since(addedAt)
		if elapsed > 2*time.Second {
			t.Fatalf("expected LISTEN-driven wakeup under 2s; took %s (likely polling fallback)", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker was not called within 3s; enqueuer was not woken")
	}

	// One-shot row should be gone.
	_, err = client.PeriodicJobGet(ctx, "matrix-listen")
	require.ErrorIs(t, err, rivertype.ErrNotFound)
}

// matrixPeriodicArgsWorker is a worker variant for the LISTEN test that
// records the wall-clock time at which the job was invoked.
type matrixPeriodicArgsWorker struct {
	river.WorkerDefaults[matrixPeriodicArgs]
	when chan time.Time
}

func (w *matrixPeriodicArgsWorker) Work(ctx context.Context, job *river.Job[matrixPeriodicArgs]) error {
	select {
	case w.when <- time.Now():
	default:
	}
	return nil
}

var _ riverpilot.Pilot = (*proPilot[pgx.Tx])(nil)
