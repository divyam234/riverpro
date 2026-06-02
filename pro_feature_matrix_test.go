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
	require.NoError(t, err)

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
		Config:                      river.Config{Workers: workers, Queues: map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 1}}},
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

var _ riverpilot.Pilot = (*proPilot[pgx.Tx])(nil)
