package riverpro

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	prodriver "github.com/divyam234/riverpro/driver"
	"github.com/divyam234/riverpro/riverworkflow"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

type BatchOpts struct{ ByArgs bool }
type JobArgsWithBatchOpts interface {
	river.JobArgs
	BatchOpts() BatchOpts
}

type SequenceOpts struct {
	ByArgs              bool
	ByQueue             bool
	ContinueOnCancelled bool
	ContinueOnDiscarded bool
	ExcludeKind         bool
}
type JobArgsWithSequenceOpts interface {
	river.JobArgs
	SequenceOpts() SequenceOpts
}

type EphemeralOpts struct{}
type JobArgsWithEphemeralOpts interface {
	river.JobArgs
	EphemeralOpts() EphemeralOpts
}

type Config struct {
	river.Config
	DeadLetter                       DeadLetterConfig
	DurablePeriodicJobs              DurablePeriodicJobsConfig
	PartitionKeyCacheTTL             time.Duration
	ProQueues                        map[string]QueueConfig
	SequenceSchedulerInterval        time.Duration
	WorkflowCancelledRetentionPeriod time.Duration
	WorkflowClosedRetentionPeriod    time.Duration
	WorkflowEvaluatorBatchSize       int
	WorkflowRescuerInterval          time.Duration
	WorkflowTimerPollerInterval      time.Duration
}

func (c *Config) WithDefaults() *Config {
	if c == nil {
		c = &Config{}
	}
	if c.PartitionKeyCacheTTL == 0 {
		c.PartitionKeyCacheTTL = time.Second
	}
	if c.SequenceSchedulerInterval == 0 {
		c.SequenceSchedulerInterval = time.Second
	}
	if c.WorkflowCancelledRetentionPeriod == 0 {
		c.WorkflowCancelledRetentionPeriod = 24 * time.Hour
	}
	if c.WorkflowClosedRetentionPeriod == 0 {
		c.WorkflowClosedRetentionPeriod = 24 * time.Hour
	}
	if c.WorkflowEvaluatorBatchSize == 0 {
		c.WorkflowEvaluatorBatchSize = 100
	}
	if c.WorkflowRescuerInterval == 0 {
		c.WorkflowRescuerInterval = time.Minute
	}
	if c.WorkflowTimerPollerInterval == 0 {
		c.WorkflowTimerPollerInterval = time.Second
	}
	if c.DurablePeriodicJobs.NextRunAtRatchetFunc == nil {
		c.DurablePeriodicJobs.NextRunAtRatchetFunc = func(nextRunAt, now time.Time) time.Time { return nextRunAt }
	}
	if c.DurablePeriodicJobs.StaleThreshold == 0 {
		c.DurablePeriodicJobs.StaleThreshold = 24 * time.Hour
	}
	if c.DurablePeriodicJobs.StartStaggerSpread == 0 {
		c.DurablePeriodicJobs.StartStaggerSpread = time.Minute
	}
	if c.DurablePeriodicJobs.StartStaggerThreshold == 0 {
		c.DurablePeriodicJobs.StartStaggerThreshold = 500
	}
	return c
}

type DeadLetterConfig struct{ Enabled bool }
type DurablePeriodicJobsConfig struct {
	Enabled               bool
	NextRunAtRatchetFunc  func(nextRunAt, now time.Time) time.Time
	StaleThreshold        time.Duration
	StartStaggerSpread    time.Duration
	StartStaggerThreshold int
}
type ConcurrencyConfig struct {
	GlobalLimit int
	LocalLimit  int
	Partition   PartitionConfig
}
type PartitionConfig struct {
	ByArgs []string
	ByKind bool
}
type QueueEphemeralConfig struct{ Enabled bool }
type QueueConfig struct {
	Concurrency                 ConcurrencyConfig
	CancelledJobRetentionPeriod time.Duration
	CompletedJobRetentionPeriod time.Duration
	DiscardedJobRetentionPeriod time.Duration
	JobCleanerTimeout           time.Duration
	Ephemeral                   QueueEphemeralConfig
	FetchCooldown               time.Duration
	FetchPollInterval           time.Duration
	MaxWorkers                  int
}

type HookJobDeadLetterMove interface {
	rivertype.Hook
	JobDeadLetterMove(ctx context.Context, job *rivertype.JobRow) error
}
type HookJobDeadLetterMoveFunc func(ctx context.Context, job *rivertype.JobRow) error

func (f HookJobDeadLetterMoveFunc) IsHook() bool { return true }
func (f HookJobDeadLetterMoveFunc) JobDeadLetterMove(ctx context.Context, job *rivertype.JobRow) error {
	return f(ctx, job)
}

type Client[TTx any] struct {
	*river.Client[TTx]
	proDriver prodriver.ProDriver[TTx]
	config    *Config
	queues    *QueueBundle
}

func NewClient[TTx any](driver prodriver.ProDriver[TTx], config *Config) (*Client[TTx], error) {
	if driver == nil {
		return nil, errors.New("riverpro: nil driver")
	}
	config = config.WithDefaults()
	pilot := newProPilot(driver, config)
	driver.ProConfigInit(pilot)
	config.Hooks = append(config.Hooks, &workflowRuntimeHook[TTx]{proDriver: driver, schema: config.Schema, interval: config.WorkflowEvaluatorBatchSize})
	c, err := river.NewClient(driver, &config.Config)
	if err != nil {
		return nil, err
	}
	return &Client[TTx]{Client: c, proDriver: driver, config: config, queues: &QueueBundle{QueueBundle: c.Queues(), proQueues: config.ProQueues}}, nil
}

type clientContextKey struct{}

func ContextWithClient[TTx any](ctx context.Context, client *Client[TTx]) context.Context {
	return context.WithValue(ctx, clientContextKey{}, client)
}
func ClientFromContext[TTx any](ctx context.Context) *Client[TTx] {
	c, err := ClientFromContextSafely[TTx](ctx)
	if err != nil {
		panic(err)
	}
	return c
}
func ClientFromContextSafely[TTx any](ctx context.Context) (*Client[TTx], error) {
	c, ok := ctx.Value(clientContextKey{}).(*Client[TTx])
	if !ok || c == nil {
		return nil, errors.New("riverpro: client not found in context")
	}
	return c, nil
}

func ReindexerIndexNamesDefault() []string {
	return []string{"river_job_kind", "river_job_queue", "river_job_state_and_finalized_at", "river_job_workflow"}
}

func (c *Client[TTx]) ProExecutor() prodriver.ProExecutor {
	if c == nil || c.proDriver == nil {
		return nil
	}
	return c.proDriver.GetProExecutor()
}
func (c *Client[TTx]) Schema() string {
	if c == nil || c.config == nil {
		return ""
	}
	return c.config.Schema
}

func (c *Client[TTx]) Queues() *QueueBundle {
	if c == nil {
		return nil
	}
	return c.queues
}

func (c *Client[TTx]) Start(ctx context.Context) error {
	if c == nil || c.Client == nil {
		return errors.New("riverpro: nil client")
	}
	if err := c.Client.Start(ctx); err != nil {
		return err
	}
	go c.workflowEvaluatorLoop(ctx)
	go c.queueRetentionCleanerLoop(ctx)
	return nil
}

func (c *Client[TTx]) queueRetentionCleanerLoop(ctx context.Context) {
	if c == nil || c.proDriver == nil || c.config == nil || len(c.config.ProQueues) == 0 {
		return
	}
	interval := time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = c.cleanProQueuesOnce(ctx)
		}
	}
}

func (c *Client[TTx]) cleanProQueuesOnce(ctx context.Context) error {
	if c == nil || c.proDriver == nil || c.config == nil {
		return nil
	}
	exec := c.proDriver.GetProExecutor()
	now := time.Now()
	for queue, qc := range c.config.ProQueues {
		params := &prodriver.JobDeleteNonWorkflowBeforeParams{Max: 10_000, QueuesIncluded: []string{queue}, Schema: c.config.Schema}
		if qc.CancelledJobRetentionPeriod > 0 {
			params.CancelledDoDelete = true
			params.CancelledFinalizedAtHorizon = now.Add(-qc.CancelledJobRetentionPeriod)
		}
		if qc.CompletedJobRetentionPeriod > 0 {
			params.CompletedDoDelete = true
			params.CompletedFinalizedAtHorizon = now.Add(-qc.CompletedJobRetentionPeriod)
		}
		if qc.DiscardedJobRetentionPeriod > 0 {
			params.DiscardedDoDelete = true
			params.DiscardedFinalizedAtHorizon = now.Add(-qc.DiscardedJobRetentionPeriod)
		}
		if params.CancelledDoDelete || params.CompletedDoDelete || params.DiscardedDoDelete {
			if _, err := exec.JobDeleteNonWorkflowBefore(ctx, params); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Client[TTx]) workflowEvaluatorLoop(ctx context.Context) {
	interval := time.Second
	if c != nil && c.config != nil && c.config.WorkflowTimerPollerInterval > 0 {
		interval = c.config.WorkflowTimerPollerInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = c.workflowEvaluateReady(ctx)
		}
	}
}

func (c *Client[TTx]) workflowEvaluateReady(ctx context.Context) error {
	if c == nil || c.proDriver == nil {
		return nil
	}
	exec := c.proDriver.GetProExecutor()
	items, err := exec.WorkflowListActive(ctx, &prodriver.WorkflowListParams{PaginationLimit: c.config.WorkflowEvaluatorBatchSize, Schema: c.config.Schema})
	if err != nil {
		return err
	}
	for _, item := range items {
		if item == nil || item.ID == "" {
			continue
		}
		ready, err := exec.WorkflowReadyTaskIDsByWorkflowIDs(ctx, &prodriver.WorkflowReadyTaskIDsByWorkflowIDsParams{LimitCount: c.config.WorkflowEvaluatorBatchSize, Schema: c.config.Schema, WorkflowIDs: []string{item.ID}})
		if err != nil {
			return err
		}
		ids := make([]int64, 0, len(ready))
		for _, row := range ready {
			ids = append(ids, row.ID)
		}
		if len(ids) > 0 {
			if _, err := exec.WorkflowStageJobsByIDMany(ctx, &prodriver.WorkflowStageJobsByIDManyParams{JobIDs: ids, Schema: c.config.Schema, WorkflowStagedAt: time.Now()}); err != nil {
				return err
			}
		}
		_, _ = exec.WorkflowFinalizeIfCompleteMany(ctx, &prodriver.WorkflowFinalizeIfCompleteManyParams{Now: time.Now(), Schema: c.config.Schema, WorkflowIDs: []string{item.ID}})
	}
	return nil
}

type workflowRuntimeHook[TTx any] struct {
	river.HookDefaults
	proDriver prodriver.ProDriver[TTx]
	schema    string
	interval  int
}

func (h *workflowRuntimeHook[TTx]) WorkEnd(ctx context.Context, job *rivertype.JobRow, workErr error) error {
	if h == nil || h.proDriver == nil || job == nil || riverworkflow.IDFromJobRow(job) == "" {
		return workErr
	}
	// WorkEnd runs before River's completion update is committed, so do a tiny
	// asynchronous evaluation to observe the completed state after River persists it.
	workflowID := riverworkflow.IDFromJobRow(job)
	go func() {
		t := time.NewTimer(25 * time.Millisecond)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		exec := h.proDriver.GetProExecutor()
		ready, err := exec.WorkflowReadyTaskIDsByWorkflowIDs(context.Background(), &prodriver.WorkflowReadyTaskIDsByWorkflowIDsParams{LimitCount: 100, Schema: h.schema, WorkflowIDs: []string{workflowID}})
		if err != nil {
			return
		}
		ids := make([]int64, 0, len(ready))
		for _, row := range ready {
			ids = append(ids, row.ID)
		}
		if len(ids) > 0 {
			_, _ = exec.WorkflowStageJobsByIDMany(context.Background(), &prodriver.WorkflowStageJobsByIDManyParams{JobIDs: ids, Schema: h.schema, WorkflowStagedAt: time.Now()})
		}
		_, _ = exec.WorkflowFinalizeIfCompleteMany(context.Background(), &prodriver.WorkflowFinalizeIfCompleteManyParams{Now: time.Now(), Schema: h.schema, WorkflowIDs: []string{workflowID}})
	}()
	return workErr
}

type QueueBundle struct {
	*river.QueueBundle
	proQueues map[string]QueueConfig
}

func (qb *QueueBundle) AddPro(name string, config QueueConfig) error {
	if qb == nil {
		return errors.New("riverpro: nil QueueBundle")
	}
	if qb.proQueues == nil {
		qb.proQueues = map[string]QueueConfig{}
	}
	if name == "" {
		return errors.New("riverpro: queue name is empty")
	}
	qb.proQueues[name] = config
	if qb.QueueBundle == nil {
		return nil
	}
	return qb.QueueBundle.Add(name, river.QueueConfig{FetchCooldown: config.FetchCooldown, FetchPollInterval: config.FetchPollInterval, MaxWorkers: config.MaxWorkers})
}

func (c *Client[TTx]) InsertMany(ctx context.Context, params []river.InsertManyParams) ([]*rivertype.JobInsertResult, error) {
	return c.Client.InsertMany(ctx, params)
}
func (c *Client[TTx]) InsertManyTx(ctx context.Context, tx TTx, params []river.InsertManyParams) ([]*rivertype.JobInsertResult, error) {
	return c.Client.InsertManyTx(ctx, tx, params)
}
func (c *Client[TTx]) JobDeadLetterGet(ctx context.Context, jobID int64) (*rivertype.JobRow, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	return c.proDriver.GetProExecutor().JobDeadLetterGetByID(ctx, &prodriver.JobDeadLetterGetByIDParams{ID: jobID, Schema: c.config.Schema})
}
func (c *Client[TTx]) JobDeadLetterGetTx(ctx context.Context, tx TTx, jobID int64) (*rivertype.JobRow, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	return c.proDriver.UnwrapProExecutor(tx).JobDeadLetterGetByID(ctx, &prodriver.JobDeadLetterGetByIDParams{ID: jobID, Schema: c.config.Schema})
}
func (c *Client[TTx]) JobDeadLetterRetry(ctx context.Context, jobID int64) (*rivertype.JobInsertResult, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	row, err := c.proDriver.GetProExecutor().JobDeadLetterMoveByID(ctx, &prodriver.JobDeadLetterMoveByIDParams{ID: jobID, Schema: c.config.Schema})
	if err != nil {
		return nil, err
	}
	return &rivertype.JobInsertResult{Job: row}, nil
}
func (c *Client[TTx]) JobDeadLetterRetryTx(ctx context.Context, tx TTx, jobID int64) (*rivertype.JobInsertResult, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	row, err := c.proDriver.UnwrapProExecutor(tx).JobDeadLetterMoveByID(ctx, &prodriver.JobDeadLetterMoveByIDParams{ID: jobID, Schema: c.config.Schema})
	if err != nil {
		return nil, err
	}
	return &rivertype.JobInsertResult{Job: row}, nil
}
func (c *Client[TTx]) WorkflowCancel(ctx context.Context, workflowID string) (*WorkflowCancelResult, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	jobs, err := c.proDriver.GetProExecutor().WorkflowCancel(ctx, &prodriver.WorkflowCancelParams{CancelAttemptedAt: time.Now(), Schema: c.config.Schema, WorkflowID: workflowID})
	if err != nil {
		return nil, err
	}
	return &WorkflowCancelResult{CancelledJobs: jobs}, nil
}
func (c *Client[TTx]) WorkflowCancelTx(ctx context.Context, tx TTx, workflowID string) (*WorkflowCancelResult, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	jobs, err := c.proDriver.UnwrapProExecutor(tx).WorkflowCancel(ctx, &prodriver.WorkflowCancelParams{CancelAttemptedAt: time.Now(), Schema: c.config.Schema, WorkflowID: workflowID})
	if err != nil {
		return nil, err
	}
	return &WorkflowCancelResult{CancelledJobs: jobs}, nil
}
func (c *Client[TTx]) WorkflowFromExisting(job *rivertype.JobRow, opts *WorkflowOpts) (*WorkflowT[TTx], error) {
	return WorkflowFromExistingT[TTx](c, job, opts)
}
func (c *Client[TTx]) WorkflowFromExistingID(ctx context.Context, workflowID string, opts *WorkflowOpts) (*WorkflowT[TTx], error) {
	if workflowID == "" {
		return nil, errors.New("riverpro: workflow ID is empty")
	}
	opts = cloneWorkflowOpts(opts)
	opts.ID = workflowID
	return c.NewWorkflow(opts), nil
}
func (c *Client[TTx]) NewWorkflow(opts *WorkflowOpts) *WorkflowT[TTx] { return newWorkflowT(c, opts) }
func (c *Client[TTx]) WorkflowPrepare(ctx context.Context, workflow *Workflow) (*WorkflowPrepareResult, error) {
	return workflow.Prepare(ctx)
}
func (c *Client[TTx]) WorkflowPrepareTx(ctx context.Context, tx TTx, workflow *Workflow) (*WorkflowPrepareResult, error) {
	return workflow.Prepare(ctx)
}

type WorkflowOpts struct {
	ID                  string
	IgnoreCancelledDeps bool
	IgnoreDeletedDeps   bool
	IgnoreDiscardedDeps bool
	Name                string
}
type WorkflowTaskOpts struct {
	Deps                []string
	IgnoreCancelledDeps *bool
	IgnoreDeletedDeps   *bool
	IgnoreDiscardedDeps *bool
	Wait                *riverworkflow.WaitSpec
}
type WorkflowTask struct{ Name string }
type WorkflowPrepareResult struct{ Jobs []river.InsertManyParams }
type WorkflowCancelResult struct{ CancelledJobs []*rivertype.JobRow }
type WorkflowLoadAllOpts struct {
	PaginationLimit  int
	PaginationOffset int
}
type WorkflowLoadDepsOpts struct{ Recursive bool }
type WorkflowRetryMode string

const (
	WorkflowRetryModeAll                 WorkflowRetryMode = "all"
	WorkflowRetryModeFailedOnly          WorkflowRetryMode = "failed_only"
	WorkflowRetryModeFailedAndDownstream WorkflowRetryMode = "failed_and_downstream"
)

type WorkflowRetryOpts struct {
	Mode         WorkflowRetryMode
	ResetHistory bool
}
type WorkflowRetryResult struct{ Jobs []*rivertype.JobRow }
type WorkflowSignalGetLatestForTaskOpts struct {
	Attempt                *int
	IncludeAfterResolution bool
}
type WorkflowSignalListForTaskParams struct {
	Attempt                *int
	CursorID               int64
	Desc                   bool
	IncludeAfterResolution bool
	Key                    string
	Limit                  int
}
type WorkflowSignalListParams struct {
	Attempt  *int
	CursorID int64
	Desc     bool
	Key      string
	Limit    int
}
type WorkflowSignalListResult struct {
	HasMore      bool
	NextCursorID *int64
	Signals      []riverworkflow.Signal
}
type WorkflowSignalOpts struct {
	Attempt        *int
	IdempotencyKey string
	Source         any
}
type WorkflowSignalResult struct {
	Attempt            int
	CreatedAt          time.Time
	ID                 int64
	IdempotencyKey     string
	Key                string
	SkippedAsDuplicate bool
	WorkflowID         string
}
type WorkflowTaskPendingReason string

const (
	WorkflowTaskPendingReasonDependencies        WorkflowTaskPendingReason = "dependencies"
	WorkflowTaskPendingReasonDependenciesAndWait WorkflowTaskPendingReason = "dependencies_and_wait"
	WorkflowTaskPendingReasonNone                WorkflowTaskPendingReason = "none"
	WorkflowTaskPendingReasonWait                WorkflowTaskPendingReason = "wait"
)

type WorkflowTaskWaitDiagnosticsOpts struct{ SignalScanLimit int }

type WorkflowRetryStillActiveError struct{ WorkflowID string }

func (e *WorkflowRetryStillActiveError) Error() string {
	return fmt.Sprintf("riverpro: workflow %q is still active", e.WorkflowID)
}
func (e *WorkflowRetryStillActiveError) Is(target error) bool {
	_, ok := target.(*WorkflowRetryStillActiveError)
	return ok
}

type DuplicateTaskError struct{ TaskName string }

func (e *DuplicateTaskError) Error() string {
	return fmt.Sprintf("riverpro: duplicate workflow task %q", e.TaskName)
}
func (e *DuplicateTaskError) Is(target error) bool { _, ok := target.(*DuplicateTaskError); return ok }

type MissingDependencyError struct{ ParentTaskName, TaskName string }

func (e *MissingDependencyError) Error() string {
	return fmt.Sprintf("riverpro: workflow task %q depends on unknown task %q", e.ParentTaskName, e.TaskName)
}
func (e *MissingDependencyError) Is(target error) bool {
	_, ok := target.(*MissingDependencyError)
	return ok
}

type DependencyCycleError struct{ DepStack []string }

func (e *DependencyCycleError) Error() string {
	return "riverpro: workflow dependency cycle: " + strings.Join(e.DepStack, " -> ")
}
func (e *DependencyCycleError) Is(target error) bool {
	_, ok := target.(*DependencyCycleError)
	return ok
}

type TaskHasNoOutputError struct {
	JobID      int64
	TaskName   string
	WorkflowID string
}

func (e *TaskHasNoOutputError) Error() string {
	return fmt.Sprintf("riverpro: workflow task %q has no output", e.TaskName)
}
func (e *TaskHasNoOutputError) Is(target error) bool {
	_, ok := target.(*TaskHasNoOutputError)
	return ok
}

type WorkflowTaskWaitDecodeError struct {
	Err                  error
	JobID                int64
	TaskName, WorkflowID string
}

func (e *WorkflowTaskWaitDecodeError) Error() string {
	return fmt.Sprintf("riverpro: could not decode wait for task %q workflow %q: %v", e.TaskName, e.WorkflowID, e.Err)
}
func (e *WorkflowTaskWaitDecodeError) Is(target error) bool {
	_, ok := target.(*WorkflowTaskWaitDecodeError)
	return ok
}
func (e *WorkflowTaskWaitDecodeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type workflowTaskDef struct {
	name       string
	args       river.JobArgs
	insertOpts *river.InsertOpts
	opts       *WorkflowTaskOpts
}
type WorkflowT[TTx any] struct {
	client   *Client[TTx]
	opts     *WorkflowOpts
	tasks    []workflowTaskDef
	existing map[string]*rivertype.JobRow
}
type Workflow = WorkflowT[any]

func NewWorkflow(opts *WorkflowOpts) *Workflow { return newWorkflowT[any](nil, opts) }
func WorkflowFromExisting(job *rivertype.JobRow, opts *WorkflowOpts) (*Workflow, error) {
	return WorkflowFromExistingT[any](nil, job, opts)
}
func WorkflowFromExistingT[TTx any](client *Client[TTx], job *rivertype.JobRow, opts *WorkflowOpts) (*WorkflowT[TTx], error) {
	if job == nil {
		return nil, errors.New("riverpro: nil job")
	}
	id := riverworkflow.IDFromJobRow(job)
	if id == "" {
		return nil, errors.New("riverpro: job is not part of a workflow")
	}
	opts = cloneWorkflowOpts(opts)
	opts.ID = id
	opts.Name = riverworkflow.NameFromJobRow(job)
	w := newWorkflowT(client, opts)
	w.existing[riverworkflow.TaskFromJobRow(job)] = job
	return w, nil
}
func newWorkflowT[TTx any](client *Client[TTx], opts *WorkflowOpts) *WorkflowT[TTx] {
	opts = cloneWorkflowOpts(opts)
	if opts.ID == "" {
		opts.ID = randomWorkflowID()
	}
	return &WorkflowT[TTx]{client: client, opts: opts, existing: map[string]*rivertype.JobRow{}}
}
func cloneWorkflowOpts(opts *WorkflowOpts) *WorkflowOpts {
	if opts == nil {
		return &WorkflowOpts{}
	}
	c := *opts
	return &c
}
func randomWorkflowID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	return deterministicWorkflowID(time.Now().String())
}
func deterministicWorkflowID(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

func (w *WorkflowT[TTx]) ID() string {
	if w == nil || w.opts == nil {
		return ""
	}
	return w.opts.ID
}
func (w *WorkflowT[TTx]) Name() string {
	if w == nil || w.opts == nil {
		return ""
	}
	return w.opts.Name
}
func (w *WorkflowT[TTx]) Add(taskName string, args river.JobArgs, insertOpts *river.InsertOpts, opts *WorkflowTaskOpts) WorkflowTask {
	task, err := w.AddSafely(taskName, args, insertOpts, opts)
	if err != nil {
		panic(err)
	}
	return task
}
func (w *WorkflowT[TTx]) AddSafely(taskName string, args river.JobArgs, insertOpts *river.InsertOpts, opts *WorkflowTaskOpts) (WorkflowTask, error) {
	if w == nil {
		return WorkflowTask{}, errors.New("riverpro: nil workflow")
	}
	if args == nil {
		return WorkflowTask{}, errors.New("riverpro: workflow task args is nil")
	}
	if taskName == "" {
		taskName = args.Kind()
	}
	if _, ok := args.(JobArgsWithEphemeralOpts); ok {
		return WorkflowTask{}, errors.New("riverpro: ephemeral jobs cannot be workflow tasks")
	}
	w.tasks = append(w.tasks, workflowTaskDef{name: taskName, args: args, insertOpts: insertOpts, opts: cloneTaskOpts(opts)})
	return WorkflowTask{Name: taskName}, nil
}
func cloneTaskOpts(opts *WorkflowTaskOpts) *WorkflowTaskOpts {
	if opts == nil {
		return nil
	}
	c := *opts
	c.Deps = append([]string(nil), opts.Deps...)
	return &c
}
func (w *WorkflowT[TTx]) Prepare(ctx context.Context) (*WorkflowPrepareResult, error) {
	var zero TTx
	return w.PrepareTx(ctx, zero)
}
func (w *WorkflowT[TTx]) PrepareTx(ctx context.Context, tx TTx) (*WorkflowPrepareResult, error) {
	_ = ctx
	_ = tx
	if w == nil {
		return nil, errors.New("riverpro: nil workflow")
	}
	if len(w.tasks) == 0 {
		return nil, errors.New("riverpro: workflow has no tasks")
	}
	if err := w.validate(); err != nil {
		return nil, err
	}
	if w.client != nil && w.client.proDriver != nil {
		var exec prodriver.ProExecutor = w.client.proDriver.GetProExecutor()
		if !isZeroValue(tx) {
			exec = w.client.proDriver.UnwrapProExecutor(tx)
		}
		_ = exec.WorkflowInsertMany(ctx, &prodriver.WorkflowInsertManyParams{IDs: []string{w.ID()}, Names: []string{w.Name()}, Schema: w.client.config.Schema})
	}
	jobs := make([]river.InsertManyParams, 0, len(w.tasks))
	for _, t := range w.tasks {
		io := cloneInsertOpts(t.insertOpts)
		meta := mergeMetadata(io.Metadata)
		meta[riverworkflow.MetadataKeyWorkflowID] = w.ID()
		meta[riverworkflow.MetadataKeyWorkflowTask] = t.name
		if w.Name() != "" {
			meta[riverworkflow.MetadataKeyWorkflowName] = w.Name()
		}
		if t.opts != nil {
			if len(t.opts.Deps) > 0 {
				meta[riverworkflow.MetadataKeyWorkflowDeps] = append([]string(nil), t.opts.Deps...)
				io.Pending = true
			}
			if t.opts.IgnoreCancelledDeps != nil {
				meta["workflow_ignore_cancelled_deps"] = *t.opts.IgnoreCancelledDeps
			} else if w.opts.IgnoreCancelledDeps {
				meta["workflow_ignore_cancelled_deps"] = true
			}
			if t.opts.IgnoreDeletedDeps != nil {
				meta["workflow_ignore_deleted_deps"] = *t.opts.IgnoreDeletedDeps
			} else if w.opts.IgnoreDeletedDeps {
				meta["workflow_ignore_deleted_deps"] = true
			}
			if t.opts.IgnoreDiscardedDeps != nil {
				meta["workflow_ignore_discarded_deps"] = *t.opts.IgnoreDiscardedDeps
			} else if w.opts.IgnoreDiscardedDeps {
				meta["workflow_ignore_discarded_deps"] = true
			}
			if t.opts.Wait != nil {
				if err := t.opts.Wait.Validate(t.opts.Deps); err != nil {
					return nil, &riverworkflow.WaitValidationError{Err: err, TaskName: t.name, WaitExpr: t.opts.Wait.Expr, WorkflowID: w.ID()}
				}
				meta[riverworkflow.MetadataKeyWorkflowWait] = t.opts.Wait
				io.Pending = true
			}
		}
		b, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		io.Metadata = b
		jobs = append(jobs, river.InsertManyParams{Args: t.args, InsertOpts: &io})
	}
	return &WorkflowPrepareResult{Jobs: jobs}, nil
}
func mergeMetadata(data []byte) map[string]any {
	m := map[string]any{}
	if len(data) > 0 {
		_ = json.Unmarshal(data, &m)
	}
	return m
}
func cloneInsertOpts(o *river.InsertOpts) river.InsertOpts {
	if o == nil {
		return river.InsertOpts{}
	}
	c := *o
	c.Tags = append([]string(nil), o.Tags...)
	c.Metadata = append([]byte(nil), o.Metadata...)
	return c
}
func (w *WorkflowT[TTx]) validate() error {
	seen := map[string]bool{}
	for _, t := range w.tasks {
		if seen[t.name] {
			return &DuplicateTaskError{TaskName: t.name}
		}
		seen[t.name] = true
	}
	for name := range w.existing {
		seen[name] = true
	}
	deps := map[string][]string{}
	for _, t := range w.tasks {
		if t.opts != nil {
			for _, d := range t.opts.Deps {
				if !seen[d] {
					return &MissingDependencyError{ParentTaskName: t.name, TaskName: d}
				}
				deps[t.name] = append(deps[t.name], d)
			}
		}
	}
	if cycle := dependencyCycle(deps); len(cycle) > 0 {
		return &DependencyCycleError{DepStack: cycle}
	}
	return nil
}
func dependencyCycle(deps map[string][]string) []string {
	temp := map[string]bool{}
	perm := map[string]bool{}
	stack := []string{}
	var visit func(string) []string
	visit = func(n string) []string {
		if perm[n] {
			return nil
		}
		if temp[n] {
			for i, s := range stack {
				if s == n {
					return append(append([]string(nil), stack[i:]...), n)
				}
			}
			return []string{n, n}
		}
		temp[n] = true
		stack = append(stack, n)
		for _, d := range deps[n] {
			if cyc := visit(d); len(cyc) > 0 {
				return cyc
			}
		}
		stack = stack[:len(stack)-1]
		temp[n] = false
		perm[n] = true
		return nil
	}
	names := make([]string, 0, len(deps))
	for n := range deps {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if cyc := visit(n); len(cyc) > 0 {
			return cyc
		}
	}
	return nil
}

func (w *WorkflowT[TTx]) LoadAll(ctx context.Context, opts *WorkflowLoadAllOpts) (*WorkflowTasks, error) {
	var zero TTx
	return w.LoadAllTx(ctx, zero, opts)
}
func (w *WorkflowT[TTx]) LoadAllTx(ctx context.Context, tx TTx, opts *WorkflowLoadAllOpts) (*WorkflowTasks, error) {
	if opts != nil && (opts.PaginationLimit < 0 || opts.PaginationOffset < 0) {
		panic("pagination values must be >= 0")
	}
	if w != nil && w.client != nil && w.client.proDriver != nil {
		var exec prodriver.ProExecutor = w.client.proDriver.GetProExecutor()
		if !isZeroValue(tx) {
			exec = w.client.proDriver.UnwrapProExecutor(tx)
		}
		limit, offset := 0, 0
		if opts != nil {
			limit, offset = opts.PaginationLimit, opts.PaginationOffset
		}
		rows, err := exec.WorkflowJobList(ctx, &prodriver.WorkflowJobListParams{PaginationLimit: limit, PaginationOffset: offset, Schema: w.client.config.Schema, WorkflowID: w.ID()})
		if err == nil && len(rows) > 0 {
			return workflowTasksFromPro(rows), nil
		}
		if err != nil && !errors.Is(err, rivertype.ErrNotFound) {
			return nil, err
		}
	}
	return newWorkflowTasksFromExisting(w.existing), nil
}
func (w *WorkflowT[TTx]) LoadDeps(ctx context.Context, taskName string, opts *WorkflowLoadDepsOpts) (*WorkflowTasks, error) {
	var zero TTx
	return w.LoadDepsTx(ctx, zero, taskName, opts)
}
func (w *WorkflowT[TTx]) LoadDepsTx(ctx context.Context, tx TTx, taskName string, opts *WorkflowLoadDepsOpts) (*WorkflowTasks, error) {
	if w != nil && w.client != nil && w.client.proDriver != nil {
		var exec prodriver.ProExecutor = w.client.proDriver.GetProExecutor()
		if !isZeroValue(tx) {
			exec = w.client.proDriver.UnwrapProExecutor(tx)
		}
		m, err := exec.WorkflowLoadDepTasksAndIDs(ctx, &prodriver.WorkflowLoadDepTasksAndIDsParams{Recursive: opts != nil && opts.Recursive, Schema: w.client.config.Schema, Task: taskName, WorkflowID: w.ID()})
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(m))
		for name := range m {
			names = append(names, name)
		}
		rows, err := exec.WorkflowLoadTasksByNames(ctx, &prodriver.WorkflowLoadTasksByNamesParams{Schema: w.client.config.Schema, TaskNames: names, WorkflowID: w.ID()})
		if err != nil {
			return nil, err
		}
		out := &WorkflowTasks{byName: map[string]*WorkflowTaskWithJob{}}
		for _, row := range rows {
			if row == nil {
				continue
			}
			job, err := exec.WorkflowJobGetByTaskName(ctx, &prodriver.WorkflowJobGetByTaskNameParams{Schema: w.client.config.Schema, TaskName: row.Task, WorkflowID: row.WorkflowID})
			if err == nil {
				out.byName[row.Task] = workflowTaskFromJob(job)
			}
		}
		return out, nil
	}
	job := w.existing[taskName]
	if job == nil {
		return &WorkflowTasks{byName: map[string]*WorkflowTaskWithJob{}}, nil
	}
	out := &WorkflowTasks{byName: map[string]*WorkflowTaskWithJob{}}
	for _, dep := range riverworkflow.DepsFromJobRow(job) {
		if j := w.existing[dep]; j != nil {
			out.byName[dep] = workflowTaskFromJob(j)
		}
	}
	return out, nil
}
func (w *WorkflowT[TTx]) LoadDepsByJob(ctx context.Context, job *rivertype.JobRow, opts *WorkflowLoadDepsOpts) (*WorkflowTasks, error) {
	var zero TTx
	return w.LoadDepsByJobTx(ctx, zero, job, opts)
}
func (w *WorkflowT[TTx]) LoadDepsByJobTx(ctx context.Context, tx TTx, job *rivertype.JobRow, opts *WorkflowLoadDepsOpts) (*WorkflowTasks, error) {
	return w.LoadDepsTx(ctx, tx, riverworkflow.TaskFromJobRow(job), opts)
}
func (w *WorkflowT[TTx]) LoadTask(ctx context.Context, taskName string) (*WorkflowTaskWithJob, error) {
	var zero TTx
	return w.LoadTaskTx(ctx, zero, taskName)
}
func (w *WorkflowT[TTx]) LoadTaskTx(ctx context.Context, tx TTx, taskName string) (*WorkflowTaskWithJob, error) {
	if w != nil && w.client != nil && w.client.proDriver != nil {
		var exec prodriver.ProExecutor = w.client.proDriver.GetProExecutor()
		if !isZeroValue(tx) {
			exec = w.client.proDriver.UnwrapProExecutor(tx)
		}
		row, err := exec.WorkflowLoadTaskWithDeps(ctx, &prodriver.WorkflowLoadTaskWithDepsParams{Schema: w.client.config.Schema, Task: taskName, WorkflowID: w.ID()})
		if err == nil && row != nil {
			return workflowTaskFromPro(row), nil
		}
		if err != nil && !errors.Is(err, rivertype.ErrNotFound) {
			return nil, err
		}
	}
	if j := w.existing[taskName]; j != nil {
		return workflowTaskFromJob(j), nil
	}
	return nil, rivertype.ErrNotFound
}
func (w *WorkflowT[TTx]) LoadOutput(ctx context.Context, taskName string, v any) error {
	var zero TTx
	return w.LoadOutputTx(ctx, zero, taskName, v)
}
func (w *WorkflowT[TTx]) LoadOutputTx(ctx context.Context, tx TTx, taskName string, v any) error {
	t, err := w.LoadTaskTx(ctx, tx, taskName)
	if err != nil {
		return err
	}
	return t.Output(v)
}
func (w *WorkflowT[TTx]) LoadOutputByJob(ctx context.Context, job *rivertype.JobRow, v any) error {
	var zero TTx
	return w.LoadOutputByJobTx(ctx, zero, job, v)
}
func (w *WorkflowT[TTx]) LoadOutputByJobTx(ctx context.Context, tx TTx, job *rivertype.JobRow, v any) error {
	_ = tx
	if job == nil {
		return errors.New("riverpro: nil job")
	}
	return (&WorkflowTaskWithJob{Job: job, Name: riverworkflow.TaskFromJobRow(job)}).Output(v)
}
func (w *WorkflowT[TTx]) Retry(ctx context.Context, opts *WorkflowRetryOpts) (*WorkflowRetryResult, error) {
	var zero TTx
	return w.RetryTx(ctx, zero, opts)
}
func (w *WorkflowT[TTx]) RetryTx(ctx context.Context, tx TTx, opts *WorkflowRetryOpts) (*WorkflowRetryResult, error) {
	if w == nil || w.client == nil || w.client.proDriver == nil {
		return nil, errors.New("riverpro: workflow has no client")
	}
	mode := prodriver.WorkflowRetryModeAll
	reset := false
	if opts != nil {
		if opts.Mode != "" {
			mode = prodriver.WorkflowRetryMode(opts.Mode)
		}
		reset = opts.ResetHistory
	}
	var exec prodriver.ProExecutor = w.client.proDriver.GetProExecutor()
	if !isZeroValue(tx) {
		exec = w.client.proDriver.UnwrapProExecutor(tx)
	}
	rows, err := exec.WorkflowRetry(ctx, &prodriver.WorkflowRetryParams{Mode: mode, Now: time.Now(), ResetHistory: reset, Schema: w.client.config.Schema, WorkflowID: w.ID()})
	if err != nil {
		return nil, err
	}
	return &WorkflowRetryResult{Jobs: rows}, nil
}
func (w *WorkflowT[TTx]) Signal(ctx context.Context, key string, payload any, opts *WorkflowSignalOpts) (*WorkflowSignalResult, error) {
	var zero TTx
	return w.SignalTx(ctx, zero, key, payload, opts)
}
func (w *WorkflowT[TTx]) SignalTx(ctx context.Context, tx TTx, key string, payload any, opts *WorkflowSignalOpts) (*WorkflowSignalResult, error) {
	if key == "" {
		return nil, errors.New("riverpro: signal key is empty")
	}
	if w == nil || w.client == nil || w.client.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	payloadBytes, err := marshalJSONObjectOrValue(payload)
	if err != nil {
		return nil, err
	}
	source := []byte(`{}`)
	if opts != nil && opts.Source != nil {
		source, err = marshalJSONObjectOrValue(opts.Source)
		if err != nil {
			return nil, err
		}
	}
	var exec prodriver.ProExecutor = w.client.proDriver.GetProExecutor()
	if !isZeroValue(tx) {
		exec = w.client.proDriver.UnwrapProExecutor(tx)
	}
	params := &prodriver.WorkflowSignalInsertParams{Key: key, Payload: payloadBytes, Schema: w.client.config.Schema, Source: source, WorkflowID: w.ID()}
	if opts != nil {
		params.IdempotencyKey = opts.IdempotencyKey
		params.RequestedAttempt = opts.Attempt
	}
	row, err := exec.WorkflowSignalInsert(ctx, params)
	if err != nil {
		return nil, err
	}
	return &WorkflowSignalResult{Attempt: row.Attempt, CreatedAt: row.CreatedAt, ID: row.ID, IdempotencyKey: row.IdempotencyKey, Key: row.Key, SkippedAsDuplicate: row.SkippedAsDuplicate, WorkflowID: row.WorkflowID}, nil
}
func (w *WorkflowT[TTx]) SignalList(ctx context.Context, opts *WorkflowSignalListParams) (*WorkflowSignalListResult, error) {
	var zero TTx
	return w.SignalListTx(ctx, zero, opts)
}
func (w *WorkflowT[TTx]) SignalListTx(ctx context.Context, tx TTx, opts *WorkflowSignalListParams) (*WorkflowSignalListResult, error) {
	if w == nil || w.client == nil || w.client.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	var cursor *int64
	var key *string
	var limit int
	if opts != nil {
		if opts.CursorID != 0 {
			cursor = &opts.CursorID
		}
		if opts.Key != "" {
			key = &opts.Key
		}
		limit = opts.Limit
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	var exec prodriver.ProExecutor = w.client.proDriver.GetProExecutor()
	if !isZeroValue(tx) {
		exec = w.client.proDriver.UnwrapProExecutor(tx)
	}
	rows, err := exec.WorkflowSignalList(ctx, &prodriver.WorkflowSignalListParams{Attempt: optsAttempt(opts), CursorID: cursor, Desc: opts != nil && opts.Desc, Key: key, LimitCount: limit, Schema: w.client.config.Schema, WorkflowID: w.ID()})
	if err != nil {
		return nil, err
	}
	return signalsToResult(rows, limit), nil
}
func (w *WorkflowT[TTx]) SignalListForTask(ctx context.Context, taskName string, opts *WorkflowSignalListForTaskParams) (*WorkflowSignalListResult, error) {
	var zero TTx
	return w.SignalListForTaskTx(ctx, zero, taskName, opts)
}
func (w *WorkflowT[TTx]) SignalListForTaskTx(ctx context.Context, tx TTx, taskName string, opts *WorkflowSignalListForTaskParams) (*WorkflowSignalListResult, error) {
	if taskName == "" {
		return nil, errors.New("riverpro: task name is empty")
	}
	if w == nil || w.client == nil || w.client.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	var cursor *int64
	var keys []string
	limit := 100
	if opts != nil {
		if opts.CursorID != 0 {
			cursor = &opts.CursorID
		}
		if opts.Key != "" {
			keys = []string{opts.Key}
		}
		if opts.Limit > 0 {
			limit = opts.Limit
		}
	}
	if limit > 1000 {
		limit = 1000
	}
	var exec prodriver.ProExecutor = w.client.proDriver.GetProExecutor()
	if !isZeroValue(tx) {
		exec = w.client.proDriver.UnwrapProExecutor(tx)
	}
	rows, err := exec.WorkflowSignalListByKeys(ctx, &prodriver.WorkflowSignalListByKeysParams{Attempt: optsTaskAttempt(opts), CursorID: cursor, Desc: opts != nil && opts.Desc, Keys: keys, LimitCount: limit, Schema: w.client.config.Schema, WorkflowID: w.ID()})
	if err != nil {
		return nil, err
	}
	return signalsToResult(rows, limit), nil
}
func (w *WorkflowT[TTx]) SignalGetLatestForTask(ctx context.Context, taskName, key string, opts *WorkflowSignalGetLatestForTaskOpts) (riverworkflow.Signal, error) {
	var zero TTx
	return w.SignalGetLatestForTaskTx(ctx, zero, taskName, key, opts)
}
func (w *WorkflowT[TTx]) SignalGetLatestForTaskTx(ctx context.Context, tx TTx, taskName, key string, opts *WorkflowSignalGetLatestForTaskOpts) (riverworkflow.Signal, error) {
	if key == "" {
		return riverworkflow.Signal{}, errors.New("riverpro: signal key is empty")
	}
	res, err := w.SignalListForTaskTx(ctx, tx, taskName, &WorkflowSignalListForTaskParams{Attempt: optsLatestAttempt(opts), Desc: true, Key: key, Limit: 1, IncludeAfterResolution: opts != nil && opts.IncludeAfterResolution})
	if err != nil {
		return riverworkflow.Signal{}, err
	}
	if res == nil || len(res.Signals) == 0 {
		return riverworkflow.Signal{}, rivertype.ErrNotFound
	}
	return res.Signals[0], nil
}
func (w *WorkflowT[TTx]) TaskWaitDiagnostics(ctx context.Context, taskName string, opts *WorkflowTaskWaitDiagnosticsOpts) (*riverworkflow.WaitDiagnostics, error) {
	var zero TTx
	return w.TaskWaitDiagnosticsTx(ctx, zero, taskName, opts)
}
func (w *WorkflowT[TTx]) TaskWaitDiagnosticsTx(ctx context.Context, tx TTx, taskName string, opts *WorkflowTaskWaitDiagnosticsOpts) (*riverworkflow.WaitDiagnostics, error) {
	_ = ctx
	_ = tx
	if opts != nil && opts.SignalScanLimit > 100000 {
		return nil, errors.New("riverpro: SignalScanLimit must be <= 100000")
	}
	task, err := w.LoadTask(ctx, taskName)
	if err != nil {
		return nil, err
	}
	if task.Wait == nil {
		return nil, &riverworkflow.WaitTaskDeclaresNoWaitError{TaskName: taskName, WorkflowID: w.ID()}
	}
	return &riverworkflow.WaitDiagnostics{InspectedAt: time.Now().UTC(), Phase: task.Wait.Phase}, nil
}

func isZeroValue[T any](v T) bool {
	rv := reflect.ValueOf(v)
	return !rv.IsValid() || rv.IsZero()
}
func optsAttempt(opts *WorkflowSignalListParams) *int {
	if opts == nil {
		return nil
	}
	return opts.Attempt
}
func optsTaskAttempt(opts *WorkflowSignalListForTaskParams) *int {
	if opts == nil {
		return nil
	}
	return opts.Attempt
}
func optsLatestAttempt(opts *WorkflowSignalGetLatestForTaskOpts) *int {
	if opts == nil {
		return nil
	}
	return opts.Attempt
}
func marshalJSONObjectOrValue(v any) ([]byte, error) {
	if v == nil {
		return []byte(`{}`), nil
	}
	switch t := v.(type) {
	case []byte:
		if len(t) == 0 {
			return []byte(`{}`), nil
		}
		return t, nil
	case json.RawMessage:
		if len(t) == 0 {
			return []byte(`{}`), nil
		}
		return []byte(t), nil
	default:
		return json.Marshal(v)
	}
}
func signalsToResult(rows []*prodriver.WorkflowSignal, requestedLimit int) *WorkflowSignalListResult {
	res := &WorkflowSignalListResult{}
	if requestedLimit <= 0 {
		requestedLimit = 100
	}
	for i, r := range rows {
		if r == nil {
			continue
		}
		if i >= requestedLimit {
			res.HasMore = true
			id := rows[i-1].ID
			res.NextCursorID = &id
			break
		}
		res.Signals = append(res.Signals, riverworkflow.Signal{Attempt: r.Attempt, CreatedAt: r.CreatedAt, ID: r.ID, Key: r.Key, Payload: json.RawMessage(r.Payload), Source: json.RawMessage(r.Source), WorkflowID: r.WorkflowID})
	}
	if !res.HasMore && len(res.Signals) > 0 {
		id := res.Signals[len(res.Signals)-1].ID
		res.NextCursorID = &id
	}
	return res
}

type WorkflowTaskWithJob struct {
	Deps                []string
	IgnoreCancelledDeps bool
	IgnoreDeletedDeps   bool
	IgnoreDiscardedDeps bool
	Job                 *rivertype.JobRow
	Name                string
	PendingReason       WorkflowTaskPendingReason
	Wait                *riverworkflow.Wait
	WorkflowID          string
}

func workflowTaskFromJob(job *rivertype.JobRow) *WorkflowTaskWithJob {
	if job == nil {
		return nil
	}
	wait, err := riverworkflow.WaitFromMetadata(job.Metadata)
	if err != nil {
		wait = nil
	}
	return &WorkflowTaskWithJob{Deps: riverworkflow.DepsFromJobRow(job), Job: job, Name: riverworkflow.TaskFromJobRow(job), Wait: wait, WorkflowID: riverworkflow.IDFromJobRow(job)}
}
func workflowTaskFromPro(row *prodriver.WorkflowTaskWithJob) *WorkflowTaskWithJob {
	if row == nil {
		return nil
	}
	t := workflowTaskFromJob(row.Job)
	if t == nil {
		t = &WorkflowTaskWithJob{}
	}
	t.Deps = append([]string(nil), row.Deps...)
	t.IgnoreCancelledDeps = row.IgnoreCancelledDeps
	t.IgnoreDeletedDeps = row.IgnoreDeletedDeps
	t.IgnoreDiscardedDeps = row.IgnoreDiscardedDeps
	if row.Task != "" {
		t.Name = row.Task
	}
	if row.WorkflowID != "" {
		t.WorkflowID = row.WorkflowID
	}
	return t
}
func workflowTasksFromPro(rows []*prodriver.WorkflowTaskWithJob) *WorkflowTasks {
	wt := &WorkflowTasks{byName: map[string]*WorkflowTaskWithJob{}}
	for _, row := range rows {
		t := workflowTaskFromPro(row)
		if t != nil && t.Name != "" {
			wt.byName[t.Name] = t
		}
	}
	return wt
}
func (w *WorkflowTaskWithJob) Output(v any) error {
	if w == nil || w.Job == nil {
		return rivertype.ErrNotFound
	}
	if len(w.Job.Metadata) == 0 {
		return &TaskHasNoOutputError{JobID: w.Job.ID, TaskName: w.Name, WorkflowID: w.WorkflowID}
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(w.Job.Metadata, &meta); err != nil {
		return err
	}
	raw := meta[rivertype.MetadataKeyOutput]
	if len(raw) == 0 {
		return &TaskHasNoOutputError{JobID: w.Job.ID, TaskName: w.Name, WorkflowID: w.WorkflowID}
	}
	return json.Unmarshal(raw, v)
}

type WorkflowTasks struct {
	byName map[string]*WorkflowTaskWithJob
}

func newWorkflowTasksFromExisting(existing map[string]*rivertype.JobRow) *WorkflowTasks {
	wt := &WorkflowTasks{byName: map[string]*WorkflowTaskWithJob{}}
	for name, job := range existing {
		wt.byName[name] = workflowTaskFromJob(job)
	}
	return wt
}
func (w *WorkflowTasks) Count() int {
	if w == nil {
		return 0
	}
	return len(w.byName)
}
func (w *WorkflowTasks) Get(name string) *WorkflowTaskWithJob {
	if w == nil {
		return nil
	}
	return w.byName[name]
}
func (w *WorkflowTasks) Names() []string {
	if w == nil {
		return nil
	}
	names := make([]string, 0, len(w.byName))
	for n := range w.byName {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
func (w *WorkflowTasks) Output(name string, v any) error {
	t := w.Get(name)
	if t == nil {
		return rivertype.ErrNotFound
	}
	return t.Output(v)
}

var Now = time.Now
