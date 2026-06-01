package examples

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/require"

	"riverqueue.com/riverpro"
	prodriver "riverqueue.com/riverpro/driver"
	"riverqueue.com/riverpro/driver/riverpropgxv5"
	"riverqueue.com/riverpro/riverbatch"
	"riverqueue.com/riverpro/riverworkflow"
)

const documentedExampleCount = 17

type docsNoOpArgs struct{ Label string }

func (docsNoOpArgs) Kind() string { return "docs_noop" }

type docsNotifyCustomerArgs struct{ OrderID string }

func (docsNotifyCustomerArgs) Kind() string { return "docs_notify_customer" }

type docsPeriodicArgs struct{}

func (docsPeriodicArgs) Kind() string { return "docs_periodic" }

type docsEphemeralArgs struct{}

func (docsEphemeralArgs) Kind() string                          { return "docs_ephemeral" }
func (docsEphemeralArgs) EphemeralOpts() riverpro.EphemeralOpts { return riverpro.EphemeralOpts{} }

type docsBatchArgs struct {
	CustomerID int64 `json:"customer_id"`
	InstanceID int64 `json:"instance_id"`
}

func (docsBatchArgs) Kind() string { return "docs_bulk_terminate_instance" }
func (a docsBatchArgs) BatchOpts() riverpro.BatchOpts {
	return riverpro.BatchOpts{ByArgs: a.CustomerID != 0}
}

type docsBatchWorker struct{ seen atomic.Int64 }

func (w *docsBatchWorker) WorkMany(_ context.Context, jobs []*river.Job[docsBatchArgs]) error {
	w.seen.Add(int64(len(jobs)))
	return nil
}

type docsConcurrentArgs struct {
	AccountID string `json:"account_id"`
}

func (docsConcurrentArgs) Kind() string { return "docs_concurrent_limited" }

type docsShipOrderArgs struct{ OrderID string }

func (docsShipOrderArgs) Kind() string { return "docs_ship_order" }

type docsFraudScoreArgs struct{ OrderID string }

func (docsFraudScoreArgs) Kind() string { return "docs_score_fraud" }

type docsOrderApprovalArgs struct{ OrderID string }

func (docsOrderApprovalArgs) Kind() string { return "docs_order_approval" }

type docsRiskHoldArgs struct{ OrderID string }

func (docsRiskHoldArgs) Kind() string { return "docs_risk_hold" }

type docsManualReviewArgs struct{ OrderID string }

func (docsManualReviewArgs) Kind() string { return "docs_manual_review" }

type docsOverrideDecisionArgs struct{ OrderID string }

func (docsOverrideDecisionArgs) Kind() string { return "docs_override_decision" }

func TestProMainDocsExamples(t *testing.T) {
	examples := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"BatchWorker", testExampleBatchWorker},
		{"ClientSetup", testExampleClientSetup},
		{"DurablePeriodicJob", testExampleDurablePeriodicJob},
		{"EphemeralJob", testExampleEphemeralJob},
		{"EphemeralQueue", testExampleEphemeralQueue},
		{"GlobalConcurrencyLimiting", testExampleGlobalConcurrencyLimiting},
		{"PerQueueRetention", testExamplePerQueueRetention},
		{"WorkflowDependencyOutput", testExampleWorkflowDependencyOutput},
		{"WorkflowTaskSignalData", testExampleWorkflowTaskSignalData},
		{"WorkflowWaitMixedTermsAndRawCEL", testExampleWorkflowWaitMixedTermsAndRawCEL},
		{"WorkflowWaitRawCEL", testExampleWorkflowWaitRawCEL},
		{"WorkflowWaitResult_timeoutVsSignal", testExampleWorkflowWaitResultTimeoutVsSignal},
		{"WorkflowWaitSignalQuorum", testExampleWorkflowWaitSignalQuorum},
		{"WorkflowWaitTimerFallback", testExampleWorkflowWaitTimerFallback},
		{"ManualReview", testExampleManualReview},
		{"WorkflowAuditPagination", testExampleWorkflowAuditPagination},
		{"LatestEvidenceSignal", testExampleLatestEvidenceSignal},
	}
	require.Len(t, examples, documentedExampleCount)
	for _, ex := range examples {
		t.Run(ex.name, ex.run)
	}
}

func testExampleBatchWorker(t *testing.T) {
	ctx := context.Background()
	worker := &docsBatchWorker{}
	job := &river.Job[docsBatchArgs]{JobRow: &rivertype.JobRow{ID: 1}, Args: docsBatchArgs{CustomerID: 7, InstanceID: 42}}
	require.NoError(t, riverbatch.Work[docsBatchArgs, pgx.Tx](ctx, worker, job, &riverbatch.WorkerOpts{MaxCount: 10, MaxDelay: time.Second}))
	require.EqualValues(t, 1, worker.seen.Load())

	wrapped, err := riverbatch.WorkFuncSafely[docsBatchArgs, pgx.Tx](func(_ context.Context, jobs []*river.Job[docsBatchArgs]) error {
		require.Len(t, jobs, 1)
		return nil
	}, nil)
	require.NoError(t, err)
	require.NoError(t, wrapped.Work(ctx, job))

	multi := riverbatch.NewMultiError()
	multi.AddByID(99, errors.New("instance busy"))
	require.Error(t, multi.Err())
	require.Contains(t, multi.Error(), "instance busy")
}

func testExampleClientSetup(t *testing.T) {
	ctx := context.Background()
	pool := newExamplePool(t)
	workers := river.NewWorkers()
	var worked atomic.Int64
	river.AddWorker(workers, river.WorkFunc(func(ctx context.Context, job *river.Job[docsNotifyCustomerArgs]) error {
		worked.Add(1)
		return river.RecordOutput(ctx, map[string]string{"status": "sent", "order_id": job.Args.OrderID})
	}))

	client := newExampleClient(t, ctx, pool, &riverpro.Config{Config: river.Config{Workers: workers}})
	workflow := client.NewWorkflow(&riverpro.WorkflowOpts{Name: "workflow_wiring"})
	workflow.Add("notify_customer", docsNotifyCustomerArgs{OrderID: "ord_123"}, nil, nil)
	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)

	subscribeChan, subscribeCancel := client.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { require.NoError(t, client.Stop(context.Background())) })
	riversharedtest.WaitOrTimeoutN(t, subscribeChan, 1)
	require.EqualValues(t, 1, worked.Load())
}

func testExampleDurablePeriodicJob(t *testing.T) {
	ctx := context.Background()
	pool := newExamplePool(t)
	workers := river.NewWorkers()
	var worked atomic.Int64
	river.AddWorker(workers, river.WorkFunc(func(ctx context.Context, job *river.Job[docsPeriodicArgs]) error {
		worked.Add(1)
		return nil
	}))
	client := newExampleClient(t, ctx, pool, &riverpro.Config{
		Config: river.Config{
			PeriodicJobs: []*river.PeriodicJob{river.NewPeriodicJob(river.PeriodicInterval(time.Hour), func() (river.JobArgs, *river.InsertOpts) {
				return docsPeriodicArgs{}, nil
			}, &river.PeriodicJobOpts{ID: "docs_periodic", RunOnStart: true})},
			Workers: workers,
		},
		DurablePeriodicJobs: riverpro.DurablePeriodicJobsConfig{Enabled: true},
	})

	subscribeChan, subscribeCancel := client.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { require.NoError(t, client.Stop(context.Background())) })
	riversharedtest.WaitOrTimeoutN(t, subscribeChan, 1)
	require.EqualValues(t, 1, worked.Load())
}

func testExampleEphemeralJob(t *testing.T) {
	ctx := context.Background()
	pool := newExamplePool(t)
	workers := river.NewWorkers()
	var worked atomic.Int64
	river.AddWorker(workers, river.WorkFunc(func(ctx context.Context, job *river.Job[docsEphemeralArgs]) error {
		worked.Add(1)
		return nil
	}))
	client := newExampleClient(t, ctx, pool, &riverpro.Config{Config: river.Config{Workers: workers}})
	_, err := client.Insert(ctx, docsEphemeralArgs{}, nil)
	require.NoError(t, err)
	subscribeChan, subscribeCancel := client.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { require.NoError(t, client.Stop(context.Background())) })
	riversharedtest.WaitOrTimeoutN(t, subscribeChan, 1)
	require.EqualValues(t, 1, worked.Load())
}

func testExampleEphemeralQueue(t *testing.T) {
	ctx := context.Background()
	pool := newExamplePool(t)
	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(ctx context.Context, job *river.Job[docsNoOpArgs]) error { return nil }))
	client := newExampleClient(t, ctx, pool, &riverpro.Config{
		Config: river.Config{Workers: workers},
		ProQueues: map[string]riverpro.QueueConfig{
			"my_ephemeral_queue": {Ephemeral: riverpro.QueueEphemeralConfig{Enabled: true}, MaxWorkers: 10},
		},
	})
	inserted, err := client.Insert(ctx, docsNoOpArgs{Label: "ephemeral_queue"}, &river.InsertOpts{Queue: "my_ephemeral_queue"})
	require.NoError(t, err)
	require.Equal(t, "my_ephemeral_queue", inserted.Job.Queue)
}

func testExampleGlobalConcurrencyLimiting(t *testing.T) {
	ctx := context.Background()
	pool := newExamplePool(t)
	driver := riverpropgxv5.New(pool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)
	client, err := riverpro.NewClient(driver, &riverpro.Config{
		Config: river.Config{Schema: schema, TestOnly: true},
		ProQueues: map[string]riverpro.QueueConfig{river.QueueDefault: {
			Concurrency: riverpro.ConcurrencyConfig{GlobalLimit: 1, Partition: riverpro.PartitionConfig{ByKind: true}},
			MaxWorkers:  10,
		}},
	})
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, []river.InsertManyParams{{Args: docsConcurrentArgs{AccountID: "a"}}, {Args: docsConcurrentArgs{AccountID: "b"}}})
	require.NoError(t, err)

	now := time.Now().UTC()
	jobs, err := driver.GetProExecutor().JobGetAvailableLimited(ctx, &prodriver.JobGetAvailableLimitedParams{
		GlobalLimit:     1,
		PartitionByKind: true,
		JobGetAvailableParams: &riverdriver.JobGetAvailableParams{
			ClientID: "docs-example-client", MaxAttemptedBy: 1, MaxToLock: 10, Now: &now, Queue: river.QueueDefault, Schema: schema,
		},
	})
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	jobs, err = driver.GetProExecutor().JobGetAvailableLimited(ctx, &prodriver.JobGetAvailableLimitedParams{
		GlobalLimit:     1,
		PartitionByKind: true,
		JobGetAvailableParams: &riverdriver.JobGetAvailableParams{
			ClientID: "docs-example-client-2", MaxAttemptedBy: 1, MaxToLock: 10, Now: &now, Queue: river.QueueDefault, Schema: schema,
		},
	})
	require.NoError(t, err)
	require.Empty(t, jobs, "global concurrency limit must include the already-running job")
}

func testExamplePerQueueRetention(t *testing.T) {
	ctx := context.Background()
	pool := newExamplePool(t)
	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(ctx context.Context, job *river.Job[docsNoOpArgs]) error { return nil }))
	client := newExampleClient(t, ctx, pool, &riverpro.Config{
		Config: river.Config{Workers: workers, Queues: map[string]river.QueueConfig{}},
		ProQueues: map[string]riverpro.QueueConfig{river.QueueDefault: {
			CancelledJobRetentionPeriod: 24 * time.Hour,
			CompletedJobRetentionPeriod: 24 * time.Hour,
			DiscardedJobRetentionPeriod: 24 * time.Hour,
			MaxWorkers:                  5,
		}},
	})
	require.NotNil(t, client)
}

func testExampleWorkflowDependencyOutput(t *testing.T) {
	ctx := context.Background()
	pool := newExamplePool(t)
	client := newExampleClient(t, ctx, pool, &riverpro.Config{})
	workflow := client.NewWorkflow(&riverpro.WorkflowOpts{ID: "wf_dependency_output", Name: "Ship order"})
	fraudTask := workflow.Add("score_fraud", docsFraudScoreArgs{OrderID: "ord_123"}, &river.InsertOpts{Metadata: mustJSON(t, map[string]any{rivertype.MetadataKeyOutput: map[string]any{"score": 0.08}})}, nil)
	workflow.Add("ship_order", docsShipOrderArgs{OrderID: "ord_123"}, nil, &riverpro.WorkflowTaskOpts{Deps: []string{fraudTask.Name}})
	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)
	loaded, err := workflow.LoadAll(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"score_fraud", "ship_order"}, loaded.Names())
	var output struct {
		Score float64 `json:"score"`
	}
	require.NoError(t, workflow.LoadOutput(ctx, "score_fraud", &output))
	require.Equal(t, 0.08, output.Score)
	deps, err := workflow.LoadDeps(ctx, "ship_order", &riverpro.WorkflowLoadDepsOpts{Recursive: true})
	require.NoError(t, err)
	require.Equal(t, []string{"score_fraud"}, deps.Names())
}

func testExampleWorkflowTaskSignalData(t *testing.T) {
	ctx := context.Background()
	workflow := newPreparedWorkflow(t, ctx, "wf_task_signal_data", "approve_order", docsOrderApprovalArgs{OrderID: "ord_123"}, &riverpro.WorkflowTaskOpts{Wait: &riverworkflow.WaitSpec{Inputs: riverworkflow.WaitInputs{Signals: []string{"approval"}}}})
	_, err := workflow.Signal(ctx, "approval", map[string]any{"approved": true, "reviewer": "alice"}, nil)
	require.NoError(t, err)
	signal, err := workflow.SignalGetLatestForTask(ctx, "approve_order", "approval", nil)
	require.NoError(t, err)
	var payload struct {
		Approved bool   `json:"approved"`
		Reviewer string `json:"reviewer"`
	}
	require.NoError(t, json.Unmarshal(signal.Payload, &payload))
	require.True(t, payload.Approved)
	require.Equal(t, "alice", payload.Reviewer)
}

func testExampleWorkflowWaitMixedTermsAndRawCEL(t *testing.T) {
	ctx := context.Background()
	wait := &riverworkflow.WaitSpec{
		Expr: "manager_approval || fallback_timer",
		Terms: []riverworkflow.WaitTermSpec{
			riverworkflow.WaitTermSignal("manager_approval", "manager_approval", "payload.approved == true"),
			riverworkflow.WaitTermTimer(riverworkflow.TimerAfterWorkflowCreated("fallback_timer", time.Hour)),
		},
	}
	workflow := newPreparedWorkflow(t, ctx, "wf_wait_mixed", "resolve_mixed_risk_hold", docsRiskHoldArgs{OrderID: "ord_123"}, &riverpro.WorkflowTaskOpts{Wait: wait})
	task, err := workflow.LoadTask(ctx, "resolve_mixed_risk_hold")
	require.NoError(t, err)
	require.NotNil(t, task.Wait)
	require.Equal(t, "manager_approval", task.Wait.SignalInput("manager_approval").Key)
	diagnostics, err := workflow.TaskWaitDiagnostics(ctx, "resolve_mixed_risk_hold", nil)
	require.NoError(t, err)
	require.Equal(t, riverworkflow.WaitPhaseNotStarted, diagnostics.Phase)
}

func testExampleWorkflowWaitRawCEL(t *testing.T) {
	ctx := context.Background()
	wait := &riverworkflow.WaitSpec{Expr: `signals.approval.IncludedCount > 0`, Inputs: riverworkflow.WaitInputs{Signals: []string{"approval"}}}
	workflow := newPreparedWorkflow(t, ctx, "wf_wait_raw_cel", "approve_order_raw_cel", docsOrderApprovalArgs{OrderID: "ord_123"}, &riverpro.WorkflowTaskOpts{Wait: wait})
	_, err := workflow.Signal(ctx, "approval", map[string]any{"approved": true}, nil)
	require.NoError(t, err)
	list, err := workflow.SignalListForTask(ctx, "approve_order_raw_cel", &riverpro.WorkflowSignalListForTaskParams{Key: "approval", Limit: 10})
	require.NoError(t, err)
	require.Len(t, list.Signals, 1)
}

func testExampleWorkflowWaitResultTimeoutVsSignal(t *testing.T) {
	ctx := context.Background()
	wait := &riverworkflow.WaitSpec{
		Expr: "review_decision || timeout",
		Terms: []riverworkflow.WaitTermSpec{
			riverworkflow.WaitTermSignal("review_decision", "review_decision", "payload.decision == 'ship'"),
			riverworkflow.WaitTermTimer(riverworkflow.TimerAfterWaitStarted("timeout", 10*time.Minute)),
		},
	}
	workflow := newPreparedWorkflow(t, ctx, "wf_timeout_vs_signal", "decide_shipment", docsRiskHoldArgs{OrderID: "ord_123"}, &riverpro.WorkflowTaskOpts{Wait: wait})
	_, err := workflow.Signal(ctx, "review_decision", map[string]any{"decision": "ship"}, nil)
	require.NoError(t, err)
	latest, err := workflow.SignalGetLatestForTask(ctx, "decide_shipment", "review_decision", nil)
	require.NoError(t, err)
	require.Equal(t, "review_decision", latest.Key)
}

func testExampleWorkflowWaitSignalQuorum(t *testing.T) {
	ctx := context.Background()
	wait := &riverworkflow.WaitSpec{Terms: []riverworkflow.WaitTermSpec{riverworkflow.WaitTermSignal("approvals", "approval", "payload.approved == true").Count(2)}}
	workflow := newPreparedWorkflow(t, ctx, "wf_signal_quorum", "approve_order_quorum", docsOrderApprovalArgs{OrderID: "ord_123"}, &riverpro.WorkflowTaskOpts{Wait: wait})
	_, err := workflow.Signal(ctx, "approval", map[string]any{"reviewer": "alice", "approved": true}, nil)
	require.NoError(t, err)
	_, err = workflow.Signal(ctx, "approval", map[string]any{"reviewer": "bob", "approved": true}, nil)
	require.NoError(t, err)
	list, err := workflow.SignalListForTask(ctx, "approve_order_quorum", &riverpro.WorkflowSignalListForTaskParams{Key: "approval", Limit: 10})
	require.NoError(t, err)
	require.Len(t, list.Signals, 2)
}

func testExampleWorkflowWaitTimerFallback(t *testing.T) {
	ctx := context.Background()
	wait := &riverworkflow.WaitSpec{
		Expr: "approval || fallback_timer",
		Terms: []riverworkflow.WaitTermSpec{
			riverworkflow.WaitTermSignal("approval", "approval", "payload.approved == true"),
			riverworkflow.WaitTermTimer(riverworkflow.TimerAfterWaitStarted("fallback_timer", time.Minute)),
		},
	}
	workflow := newPreparedWorkflow(t, ctx, "wf_timer_fallback", "resolve_risk_hold", docsRiskHoldArgs{OrderID: "ord_123"}, &riverpro.WorkflowTaskOpts{Wait: wait})
	task, err := workflow.LoadTask(ctx, "resolve_risk_hold")
	require.NoError(t, err)
	require.NotNil(t, task.Wait.TimerInput("fallback_timer"))
}

func testExampleManualReview(t *testing.T) {
	ctx := context.Background()
	wait := &riverworkflow.WaitSpec{Inputs: riverworkflow.WaitInputs{Signals: []string{"manual_review"}}}
	workflow := newPreparedWorkflow(t, ctx, "wf_manual_review", "manual_review", docsManualReviewArgs{OrderID: "ord_123"}, &riverpro.WorkflowTaskOpts{Wait: wait})
	_, err := workflow.Signal(ctx, "manual_review", map[string]any{"approved": true, "reviewer": "alice"}, nil)
	require.NoError(t, err)
	latest, err := workflow.SignalGetLatestForTask(ctx, "manual_review", "manual_review", nil)
	require.NoError(t, err)
	require.Equal(t, "manual_review", latest.Key)
}

func testExampleWorkflowAuditPagination(t *testing.T) {
	ctx := context.Background()
	pool := newExamplePool(t)
	client := newExampleClient(t, ctx, pool, &riverpro.Config{})
	workflow := client.NewWorkflow(&riverpro.WorkflowOpts{ID: "wf_audit_timeline"})
	workflow.Add("audit_anchor", docsNoOpArgs{Label: "audit"}, nil, nil)
	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)
	for _, transition := range []map[string]any{
		{"actor": "system", "from": "queued", "to": "risk_scored"},
		{"actor": "rules-engine", "from": "risk_scored", "to": "manual_review"},
		{"actor": "reviewer:alice", "from": "manual_review", "to": "approved"},
	} {
		_, err := workflow.Signal(ctx, "state_transition", transition, nil)
		require.NoError(t, err)
	}
	var cursor int64
	var seen []string
	for {
		page, err := workflow.SignalList(ctx, &riverpro.WorkflowSignalListParams{CursorID: cursor, Key: "state_transition", Limit: 500})
		require.NoError(t, err)
		for _, signal := range page.Signals {
			var transition struct{ From, To string }
			require.NoError(t, json.Unmarshal(signal.Payload, &transition))
			seen = append(seen, fmt.Sprintf("%s->%s", transition.From, transition.To))
		}
		if !page.HasMore {
			break
		}
		cursor = *page.NextCursorID
	}
	require.Equal(t, []string{"queued->risk_scored", "risk_scored->manual_review", "manual_review->approved"}, seen)
}

func testExampleLatestEvidenceSignal(t *testing.T) {
	ctx := context.Background()
	wait := &riverworkflow.WaitSpec{Inputs: riverworkflow.WaitInputs{Signals: []string{"override_decision"}}}
	workflow := newPreparedWorkflow(t, ctx, "wf_latest_evidence", "resolve_risk_hold", docsOverrideDecisionArgs{OrderID: "ord_123"}, &riverpro.WorkflowTaskOpts{Wait: wait})
	_, err := workflow.Signal(ctx, "override_decision", map[string]any{"approved": false, "reason": "need more evidence"}, nil)
	require.NoError(t, err)
	_, err = workflow.Signal(ctx, "override_decision", map[string]any{"approved": true, "reason": "customer verified"}, nil)
	require.NoError(t, err)
	latest, err := workflow.SignalGetLatestForTask(ctx, "resolve_risk_hold", "override_decision", nil)
	require.NoError(t, err)
	var payload struct {
		Approved bool   `json:"approved"`
		Reason   string `json:"reason"`
	}
	require.NoError(t, json.Unmarshal(latest.Payload, &payload))
	require.True(t, payload.Approved)
	require.Equal(t, "customer verified", payload.Reason)
}

func newExamplePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), riversharedtest.TestDatabaseURL())
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func newExampleClient(t *testing.T, ctx context.Context, pool *pgxpool.Pool, config *riverpro.Config) *riverpro.Client[pgx.Tx] {
	t.Helper()
	if config == nil {
		config = &riverpro.Config{}
	}
	driver := riverpropgxv5.New(pool)
	config.Schema = riverdbtest.TestSchema(ctx, t, driver, nil)
	config.TestOnly = true
	config.PollOnly = true
	if config.Workers != nil {
		config.FetchCooldown = 20 * time.Millisecond
		config.FetchPollInterval = 20 * time.Millisecond
		config.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn, ReplaceAttr: slogutil.NoLevelTime}))
		if config.Queues == nil {
			config.Queues = map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 10}}
		}
	}
	client, err := riverpro.NewClient(driver, config)
	require.NoError(t, err)
	return client
}

func newPreparedWorkflow(t *testing.T, ctx context.Context, workflowID, taskName string, args river.JobArgs, opts *riverpro.WorkflowTaskOpts) *riverpro.WorkflowT[pgx.Tx] {
	t.Helper()
	pool := newExamplePool(t)
	client := newExampleClient(t, ctx, pool, &riverpro.Config{})
	workflow := client.NewWorkflow(&riverpro.WorkflowOpts{ID: workflowID})
	workflow.Add(taskName, args, nil, opts)
	prepared, err := workflow.Prepare(ctx)
	require.NoError(t, err)
	_, err = client.InsertMany(ctx, prepared.Jobs)
	require.NoError(t, err)
	return workflow
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	return data
}

func waitUntil(t *testing.T, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
