package driver

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivertype"
)

//go:embed migration/pro/*.sql
var proMigrationFS embed.FS

// ErrNotSupported is returned by Pro methods that are gated behind a
// configuration flag and called while that flag is disabled. Clients should
// use errors.Is to detect it.
var ErrNotSupported = errors.New("riverpro driver: feature is not supported with current configuration")

// ErrPeriodicJobAlreadyExists is returned by create-only periodic job inserts
// when the requested ID is already present.
var ErrPeriodicJobAlreadyExists = errors.New("riverpro driver: periodic job already exists")

func migrationFSForLine(line string) fs.FS {
	if line != MigrationLinePro {
		return nil
	}
	return proMigrationFS
}

const (
	// MigrationLinePro is the unified migration line for all River Pro features.
	MigrationLinePro = "pro"
)

var (
	ErrDeadlock         = &DeadlockError{}
	ErrStatementTimeout = &StatementTimeoutError{}
)

func MigrationLineProTruncateTables(line string, version int) []string {
	if line != MigrationLinePro {
		panic(fmt.Sprintf("unknown pro migration line %q", line))
	}
	return []string{"river_job_dead_letter", "river_job_sequence_inbox", "river_job_sequence", "river_periodic_job", "river_producer", "river_workflow", "river_workflow_attempt", "river_workflow_attempt_task", "river_workflow_signal", "river_workflow_timer", "river_workflow_worklist"}
}

type ProDriver[TTx any] interface {
	riverdriver.Driver[TTx]
	GetProExecutor() ProExecutor
	ProConfigInit(pilot riverpilot.Pilot)
	UnwrapProExecutor(tx TTx) ProExecutorTx
}

type ProExecutor interface {
	riverdriver.Executor
	BeginPro(ctx context.Context) (ProExecutorTx, error)
	JobDeadLetterDeleteByID(ctx context.Context, params *JobDeadLetterDeleteByIDParams) (*rivertype.JobRow, error)
	JobDeadLetterGetAll(ctx context.Context, params *JobDeadLetterGetAllParams) ([]*rivertype.JobRow, error)
	JobDeadLetterGetByID(ctx context.Context, params *JobDeadLetterGetByIDParams) (*rivertype.JobRow, error)
	JobDeadLetterMoveByID(ctx context.Context, params *JobDeadLetterMoveByIDParams) (*rivertype.JobRow, error)
	JobDeadLetterMoveDiscarded(ctx context.Context, params *JobDeadLetterMoveDiscardedParams) ([]*rivertype.JobRow, error)
	JobDeleteByIDMany(ctx context.Context, params *JobDeleteByIDManyParams) ([]*rivertype.JobRow, error)
	JobDeleteNonWorkflowBefore(ctx context.Context, params *JobDeleteNonWorkflowBeforeParams) (int, error)
	JobGetAvailableForBatch(ctx context.Context, params *JobGetAvailableForBatchParams) ([]*rivertype.JobRow, error)
	JobGetAvailableLimited(ctx context.Context, params *JobGetAvailableLimitedParams) ([]*rivertype.JobRow, error)
	JobGetAvailablePartitionKeys(ctx context.Context, params *JobGetAvailablePartitionKeysParams) ([]string, error)
	JobGetStuckWithInactiveProducer(ctx context.Context, params *JobGetStuckWithInactiveProducerParams) ([]*rivertype.JobRow, error)
	JobRescueManyWithInactiveProducer(ctx context.Context, params *JobRescueManyWithInactiveProducerParams) error
	PGTryAdvisoryXactLock(ctx context.Context, key int64) (bool, error)
	PeriodicJobGetAll(ctx context.Context, params *PeriodicJobGetAllParams) ([]*PeriodicJob, error)
	PeriodicJobGetByID(ctx context.Context, params *PeriodicJobGetByIDParams) (*PeriodicJob, error)
	PeriodicJobInsert(ctx context.Context, params *PeriodicJobInsertParams) (*PeriodicJob, error)
	PeriodicJobUpsert(ctx context.Context, params *PeriodicJobUpsertParams) (*PeriodicJob, error)
	PeriodicJobUpdate(ctx context.Context, params *PeriodicJobUpdateParams) (*PeriodicJob, error)
	PeriodicJobPause(ctx context.Context, params *PeriodicJobPauseParams) (*PeriodicJob, error)
	PeriodicJobResume(ctx context.Context, params *PeriodicJobResumeParams) (*PeriodicJob, error)
	PeriodicJobKeepAliveAndReap(ctx context.Context, params *PeriodicJobKeepAliveAndReapParams) ([]*PeriodicJob, error)
	PeriodicJobUpsertMany(ctx context.Context, params *PeriodicJobUpsertManyParams) ([]*PeriodicJob, error)
	PeriodicJobDelete(ctx context.Context, params *PeriodicJobDeleteParams) (*PeriodicJob, error)
	PeriodicJobEnqueueDue(ctx context.Context, params *PeriodicJobEnqueueDueParams) (*PeriodicJobEnqueueDueResult, error)
	ProducerDelete(ctx context.Context, params *ProducerDeleteParams) error
	ProducerDeleteStale(ctx context.Context, params *ProducerDeleteStaleParams) (int, error)
	ProducerGetByID(ctx context.Context, params *ProducerGetByIDParams) (*Producer, error)
	QueueGetMetadataForInsert(ctx context.Context, params *QueueGetMetadataForInsertParams) ([]*QueueGetMetadataForInsertResult, error)
	ProducerInsertOrUpdate(ctx context.Context, params *ProducerInsertOrUpdateParams) (*Producer, error)
	ProducerKeepAlive(ctx context.Context, params *ProducerKeepAliveParams) (*Producer, error)
	ProducerListByQueue(ctx context.Context, params *ProducerListByQueueParams) ([]*ProducerListByQueueResult, error)
	ProducerUpdate(ctx context.Context, params *ProducerUpdateParams) (*Producer, error)
	SequenceAppendMany(ctx context.Context, params *SequenceAppendManyParams) (int, error)
	SequenceList(ctx context.Context, params *SequenceListParams) ([]*Sequence, error)
	SequencePromote(ctx context.Context, params *SequencePromoteParams) (*SequencePromoteResult, error)
	SequencePromoteFromTable(ctx context.Context, params *SequencePromoteFromTableParams) (*SequencePromoteFromTableResult, error)
	SequenceScanAndPromoteStalled(ctx context.Context, params *SequenceScanAndPromoteStalledParams) (*SequenceScanAndPromoteStalledResult, error)
	TimeNow(ctx context.Context, params *TimeNowParams) (time.Time, error)
	WorkflowAttemptInsert(ctx context.Context, params *WorkflowAttemptInsertParams) (*WorkflowAttempt, error)
	WorkflowAttemptListByWorkflowID(ctx context.Context, params *WorkflowAttemptListByWorkflowIDParams) ([]*WorkflowAttempt, error)
	WorkflowAttemptTaskInsert(ctx context.Context, params *WorkflowAttemptTaskInsertParams) (*WorkflowAttemptTask, error)
	WorkflowAttemptTaskListByWorkflowID(ctx context.Context, params *WorkflowAttemptTaskListByWorkflowIDParams) ([]*WorkflowAttemptTask, error)
	WorkflowCancel(ctx context.Context, params *WorkflowCancelParams) ([]*rivertype.JobRow, error)
	WorkflowCancelWithDeletedDepsMany(ctx context.Context, params *WorkflowCancelWithDeletedDepsManyParams) (int64, error)
	WorkflowCancelWithFailedDepsMany(ctx context.Context, params *WorkflowCancelWithFailedDepsManyParams) (int64, error)
	WorkflowCleanupDeleteAttemptsByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteByWorkflowIDsParams) error
	WorkflowCleanupDeleteAttemptTasksByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteByWorkflowIDsParams) error
	WorkflowCleanupDeleteDeadLetterJobsByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteByWorkflowIDsParams) error
	WorkflowCleanupDeleteJobsByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteByWorkflowIDsParams) error
	WorkflowCleanupDeleteSignalsByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteByWorkflowIDsParams) error
	WorkflowCleanupDeleteTimersByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteByWorkflowIDsParams) error
	WorkflowCleanupDeleteWorkflowsByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteWorkflowsByWorkflowIDsParams) error
	WorkflowCleanupDeleteWorklistByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteByWorkflowIDsParams) error
	WorkflowCleanupListFinalizedIDs(ctx context.Context, params *WorkflowCleanupListFinalizedIDsParams) ([]string, error)
	WorkflowCleanupListFinalizedIDsWithoutJobs(ctx context.Context, params *WorkflowCleanupListFinalizedIDsWithoutJobsParams) ([]string, error)
	WorkflowCountIncompleteJobs(ctx context.Context, params *WorkflowCountIncompleteJobsParams) (int64, error)
	WorkflowFinalizeIfCompleteMany(ctx context.Context, params *WorkflowFinalizeIfCompleteManyParams) ([]string, error)
	WorkflowGetByID(ctx context.Context, params *WorkflowGetByIDParams) (*Workflow, error)
	WorkflowGetFinalizationCandidates(ctx context.Context, params *WorkflowGetFinalizationCandidatesParams) ([]string, error)
	WorkflowGetLegacyBackfillIDs(ctx context.Context, params *WorkflowGetLegacyBackfillIDsParams) ([]string, error)
	WorkflowHasWaitTasksMany(ctx context.Context, params *WorkflowHasWaitTasksManyParams) ([]string, error)
	WorkflowInitFromJobs(ctx context.Context, params *WorkflowInitFromJobsParams) ([]string, error)
	WorkflowInsertMany(ctx context.Context, params *WorkflowInsertManyParams) error
	WorkflowJobGetByTaskName(ctx context.Context, params *WorkflowJobGetByTaskNameParams) (*rivertype.JobRow, error)
	WorkflowJobList(ctx context.Context, params *WorkflowJobListParams) ([]*WorkflowTaskWithJob, error)
	WorkflowListActive(ctx context.Context, params *WorkflowListParams) ([]*WorkflowListItem, error)
	WorkflowListAll(ctx context.Context, params *WorkflowListParams) ([]*WorkflowListItem, error)
	WorkflowListByIDs(ctx context.Context, params *WorkflowListByIDsParams) ([]string, error)
	WorkflowListByIDsForWaitEval(ctx context.Context, params *WorkflowListByIDsForWaitEvalParams) ([]*WorkflowWaitWorkflow, error)
	WorkflowListInactive(ctx context.Context, params *WorkflowListParams) ([]*WorkflowListItem, error)
	WorkflowLoadDepTasksAndIDs(ctx context.Context, params *WorkflowLoadDepTasksAndIDsParams) (map[string]*int64, error)
	WorkflowLoadJobsWithDeps(ctx context.Context, params *WorkflowLoadJobsWithDepsParams) ([]*WorkflowTaskWithJob, error)
	WorkflowLoadTaskWithDeps(ctx context.Context, params *WorkflowLoadTaskWithDepsParams) (*WorkflowTaskWithJob, error)
	WorkflowLoadTaskNamesByWorkflowID(ctx context.Context, params *WorkflowLoadTaskNamesByWorkflowIDParams) ([]string, error)
	WorkflowLoadTasksByNames(ctx context.Context, params *WorkflowLoadTasksByNamesParams) ([]*WorkflowTask, error)
	WorkflowLockByIDsSkipLocked(ctx context.Context, params *WorkflowLockByIDsSkipLockedParams) ([]string, error)
	WorkflowReadyTaskIDsByWorkflowIDs(ctx context.Context, params *WorkflowReadyTaskIDsByWorkflowIDsParams) ([]*WorkflowReadyTaskIDsByWorkflowIDsRow, error)
	WorkflowRetry(ctx context.Context, params *WorkflowRetryParams) ([]*rivertype.JobRow, error)
	WorkflowRetryLockAndCheckRunning(ctx context.Context, params *WorkflowRetryLockAndCheckRunningParams) (*WorkflowRetryLockAndCheckRunningResult, error)
	WorkflowSignalInsert(ctx context.Context, params *WorkflowSignalInsertParams) (*WorkflowSignalInsertResult, error)
	WorkflowSignalList(ctx context.Context, params *WorkflowSignalListParams) ([]*WorkflowSignal, error)
	WorkflowSignalListByEvidence(ctx context.Context, params *WorkflowSignalListByEvidenceParams) ([]*WorkflowSignal, error)
	WorkflowSignalListByKeys(ctx context.Context, params *WorkflowSignalListByKeysParams) ([]*WorkflowSignal, error)
	WorkflowSignalListByWorkflowIDs(ctx context.Context, params *WorkflowSignalListByWorkflowIDsParams) ([]*WorkflowSignal, error)
	WorkflowSignalStatsByWorkflowIDs(ctx context.Context, params *WorkflowSignalStatsByWorkflowIDsParams) ([]*WorkflowSignalStat, error)
	WorkflowStageJobsByIDMany(ctx context.Context, params *WorkflowStageJobsByIDManyParams) ([]*rivertype.JobRow, error)
	WorkflowTimerConsumeDue(ctx context.Context, params *WorkflowTimerConsumeDueParams) ([]*WorkflowTimer, error)
	WorkflowTimerDeleteByWorkflowIDs(ctx context.Context, params *WorkflowTimerDeleteByWorkflowIDsParams) error
	WorkflowTimerGetByWorkflowID(ctx context.Context, params *WorkflowTimerGetByWorkflowIDParams) (*WorkflowTimer, error)
	WorkflowTimerNextFireAtByWorkflowIDs(ctx context.Context, params *WorkflowTimerNextFireAtByWorkflowIDsParams) ([]*WorkflowTimerNextFireAtByWorkflowIDsRow, error)
	WorkflowTimerUpsertMany(ctx context.Context, params *WorkflowTimerUpsertManyParams) error
	WorkflowUnfinalizeIfActiveJobsMany(ctx context.Context, params *WorkflowUnfinalizeIfActiveJobsManyParams) ([]string, error)
	WorkflowWaitActivatableTaskIDsByWorkflowIDs(ctx context.Context, params *WorkflowWaitActivatableTaskIDsByWorkflowIDsParams) ([]*WorkflowWaitActivatableTaskIDsByWorkflowIDsRow, error)
	WorkflowWaitActivateByJobIDMany(ctx context.Context, params *WorkflowWaitActivateByJobIDManyParams) ([]int64, error)
	WorkflowWaitActiveTaskListByWorkflowIDs(ctx context.Context, params *WorkflowWaitActiveTaskListByWorkflowIDsParams) ([]*WorkflowWaitActiveTask, error)
	WorkflowWaitDepOutputListByWorkflowTaskPairs(ctx context.Context, params *WorkflowWaitDepOutputListByWorkflowTaskPairsParams) ([]*WorkflowWaitDepOutput, error)
	WorkflowWaitEvalCursorUpdateByWorkflowIDMany(ctx context.Context, params *WorkflowWaitEvalCursorUpdateByWorkflowIDManyParams) error
	WorkflowWaitUpdateMetadataByJobIDMany(ctx context.Context, params *WorkflowWaitUpdateMetadataByJobIDManyParams) error
	WorkflowWorklistDeleteByWorkflowIDsReturningReasons(ctx context.Context, params *WorkflowWorklistDeleteByWorkflowIDsReturningReasonsParams) ([]*WorkflowWorklistDeleteByWorkflowIDsReturningReasonsRow, error)
	WorkflowWorklistInsertMany(ctx context.Context, params *WorkflowWorklistInsertManyParams) error
	WorkflowWorklistListIDs(ctx context.Context, params *WorkflowWorklistListParams) ([]*WorkflowWorklistIDItem, error)
	WorkflowWorklistList(ctx context.Context, params *WorkflowWorklistListParams) ([]*WorkflowWorklistItem, error)
}

type ProExecutorTx interface {
	ProExecutor
	riverdriver.ExecutorTx
}

// DeadlockError
type DeadlockError struct {
	Err error
}

// JobDeadLetterDeleteByIDParams
type JobDeadLetterDeleteByIDParams struct {
	ID     int64
	Schema string
}

// JobDeadLetterGetAllParams
type JobDeadLetterGetAllParams struct {
	Schema string
}

// JobDeadLetterGetByIDParams
type JobDeadLetterGetByIDParams struct {
	ID     int64
	Schema string
}

// JobDeadLetterMoveByIDParams
type JobDeadLetterMoveByIDParams struct {
	ID     int64
	Schema string
}

// JobDeadLetterMoveDiscardedParams
type JobDeadLetterMoveDiscardedParams struct {
	DiscardedFinalizedAtHorizon time.Time
	Max                         int
	Schema                      string
}

// JobDeleteByIDManyParams
type JobDeleteByIDManyParams struct {
	ID     []int64
	Schema string
}

// JobDeleteNonWorkflowBeforeParams
type JobDeleteNonWorkflowBeforeParams struct {
	CancelledDoDelete           bool
	CancelledFinalizedAtHorizon time.Time
	CompletedDoDelete           bool
	CompletedFinalizedAtHorizon time.Time
	DiscardedDoDelete           bool
	DiscardedFinalizedAtHorizon time.Time
	Max                         int
	QueuesExcluded              []string
	QueuesIncluded              []string
	Schema                      string
}

// JobGetAvailableForBatchParams
type JobGetAvailableForBatchParams struct {
	AttemptedBy      string
	BatchID          string
	BatchKey         string
	BatchLeaderJobID int64
	Kind             string
	Max              int32
	Queue            string
	Schema           string
}

// JobGetAvailableLimitedParams
type JobGetAvailableLimitedParams struct {
	*riverdriver.JobGetAvailableParams

	AvailablePartitionKeys                []string
	CurrentProducerPartitionKeys          []string
	CurrentProducerPartitionRunningCounts []int32
	GlobalLimit                           int32
	LocalLimit                            int32
	PartitionByArgs                       []string
	PartitionByKind                       bool
}

// JobGetAvailablePartitionKeysParams
type JobGetAvailablePartitionKeysParams struct {
	Queue  string
	Schema string
}

// JobGetStuckWithInactiveProducerParams extends River's age-based stuck-job
// query with jobs whose latest producer heartbeat is stale or missing.
type JobGetStuckWithInactiveProducerParams struct {
	*riverdriver.JobGetStuckParams
	ProducerStaleHorizon time.Time
}

// JobRescueManyWithInactiveProducerParams guards a rescue update with the same
// age-or-inactive-producer condition used to select the jobs.
type JobRescueManyWithInactiveProducerParams struct {
	*riverdriver.JobRescueManyParams
	ProducerStaleHorizon time.Time
}

// PeriodicJob
type PeriodicJob struct {
	ID             string
	CreatedAt      time.Time
	NextRunAt      time.Time
	UpdatedAt      time.Time
	PausedAt       *time.Time
	Kind           string
	Args           []byte
	Queue          string
	Priority       int
	MaxAttempts    int
	Tags           []string
	CronExpression *string
	CronTimezone   string
}

// PeriodicJobGetAllParams
type PeriodicJobGetAllParams struct {
	IncludePaused         bool
	Max                   int
	Schema                string
	StaleUpdatedAtHorizon time.Time
}

// PeriodicJobGetByIDParams
type PeriodicJobGetByIDParams struct {
	ID     string
	Schema string
}

// PeriodicJobInsertParams contains a complete create-only periodic job definition.
type PeriodicJobInsertParams struct {
	ID             string
	NextRunAt      time.Time
	Schema         string
	UpdatedAt      *time.Time
	Kind           string
	Args           []byte
	Queue          string
	Priority       int
	MaxAttempts    int
	Tags           []string
	CronExpression *string
	CronTimezone   string
	Paused         bool
}

// PeriodicJobUpsertParams contains a complete periodic job definition.
// ResetNextRunAt forces an explicit next-run replacement even when the cron
// expression and timezone are unchanged.
type PeriodicJobUpsertParams struct {
	ID             string
	NextRunAt      time.Time
	Schema         string
	UpdatedAt      *time.Time
	Kind           string
	Args           []byte
	Queue          string
	Priority       int
	MaxAttempts    int
	Tags           []string
	CronExpression *string
	CronTimezone   string
	Paused         bool
	ResetNextRunAt bool
}

// PeriodicJobUpdateParams patches selected definition fields on an existing
// periodic job. It never changes pause state.
type PeriodicJobUpdateParams struct {
	ID     string
	Schema string

	SetArgs bool
	Kind    string
	Args    []byte

	SetQueue bool
	Queue    string

	SetPriority bool
	Priority    int

	SetMaxAttempts bool
	MaxAttempts    int

	SetTags bool
	Tags    []string

	SetSchedule    bool
	NextRunAt      time.Time
	CronExpression *string
	CronTimezone   string
}

// PeriodicJobPauseParams identifies a periodic job to pause.
type PeriodicJobPauseParams struct {
	ID     string
	Schema string
}

// PeriodicJobResumeParams identifies a periodic job to resume.
type PeriodicJobResumeParams struct {
	ID     string
	Schema string
}

// PeriodicJobKeepAliveAndReapParams
type PeriodicJobKeepAliveAndReapParams struct {
	ID                    []string
	Now                   *time.Time
	Schema                string
	StaleUpdatedAtHorizon time.Time
}

// PeriodicJobUpsertManyParams
type PeriodicJobUpsertManyParams struct {
	Jobs   []*PeriodicJobUpsertItem
	Schema string
}

// PeriodicJobUpsertItem is the per-row struct used inside
// PeriodicJobUpsertManyParams.Jobs. Mirrors upstream's pilot pattern.
type PeriodicJobUpsertItem struct {
	ID             string
	NextRunAt      time.Time
	UpdatedAt      time.Time
	Kind           string
	Args           []byte
	Queue          string
	Priority       int
	MaxAttempts    int
	Tags           []string
	CronExpression *string
	CronTimezone   string
}

// PeriodicJobDeleteParams
type PeriodicJobDeleteParams struct {
	ID     string
	Schema string
}

// PeriodicJobEnqueueDueParams
type PeriodicJobEnqueueDueParams struct {
	Max       int
	NextRunAt map[string]time.Time
	Schema    string
}

// PeriodicJobEnqueueDueResult
type PeriodicJobEnqueueDueResult struct {
	Inserted []*EnqueuedPeriodicJob
	Deleted  []string
}

// EnqueuedPeriodicJob describes one job the enqueuer loop just inserted.
type EnqueuedPeriodicJob struct {
	PeriodicJobID string
	JobID         int64
	Kind          string
	ScheduledAt   time.Time
}

// Producer
type Producer struct {
	ID         int64
	ClientID   string
	QueueName  string
	MaxWorkers int32
	Metadata   []byte
	PausedAt   *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// ProducerDeleteParams
type ProducerDeleteParams struct {
	ID     int64
	Schema string
}

// ProducerDeleteStaleParams
type ProducerDeleteStaleParams struct {
	Max                   int
	QueueName             string
	Schema                string
	StaleUpdatedAtHorizon time.Time
}

// ProducerGetByIDParams
type ProducerGetByIDParams struct {
	ID     int64
	Schema string
}

// ProducerInsertOrUpdateParams
type ProducerInsertOrUpdateParams struct {
	ID         int64
	ClientID   string
	CreatedAt  *time.Time
	MaxWorkers int32
	Metadata   []byte
	PausedAt   *time.Time
	QueueName  string
	Schema     string
	UpdatedAt  *time.Time
}

// ProducerKeepAliveParams
type ProducerKeepAliveParams struct {
	ID                    int64
	QueueName             string
	Schema                string
	StaleUpdatedAtHorizon time.Time
}

// ProducerListByQueueParams
type ProducerListByQueueParams struct {
	QueueName string
	Schema    string
}

// ProducerListByQueueResult
type ProducerListByQueueResult struct {
	Producer *Producer
	Running  int32
}

// ProducerUpdateParams
type ProducerUpdateParams struct {
	ID                 int64
	MaxWorkers         int32
	MaxWorkersDoUpdate bool
	Metadata           []byte
	MetadataDoUpdate   bool
	PausedAt           *time.Time
	PausedAtDoUpdate   bool
	Schema             string
	UpdatedAt          *time.Time
}

// QueueGetMetadataForInsertParams
type QueueGetMetadataForInsertParams struct {
	Names  []string
	Schema string
}

// QueueGetMetadataForInsertResult
type QueueGetMetadataForInsertResult struct {
	Name        string
	Concurrency []byte
}

// Sequence
type Sequence struct {
	ID        int64
	Key       string
	CreatedAt time.Time
}

// SequenceAppendManyParams
type SequenceAppendManyParams struct {
	Schema  string
	SeqKeys []string
}

// SequenceListParams
type SequenceListParams struct {
	MaxCount int
	Schema   string
}

// SequencePromoteFromTableParams
type SequencePromoteFromTableParams struct {
	GracePeriod time.Duration
	Max         int
	Now         *time.Time
	Schema      string
}

// SequencePromoteFromTableResult
type SequencePromoteFromTableResult struct {
	Continue    bool
	NumDeleted  int
	NumPromoted int
}

// SequencePromoteParams
type SequencePromoteParams struct {
	GracePeriod time.Duration
	Keys        []string
	Now         *time.Time
	Schema      string
}

// SequencePromoteResult
type SequencePromoteResult struct {
	PromotedKeys []string
	SkippedKeys  []string
}

// SequenceScanAndPromoteStalledParams
type SequenceScanAndPromoteStalledParams struct {
	GracePeriod     time.Duration
	LastSequenceKey string
	Max             int
	Now             *time.Time
	Schema          string
}

// SequenceScanAndPromoteStalledResult
type SequenceScanAndPromoteStalledResult struct {
	Continue       bool
	LastSeqKey     string
	SkippedSeqKeys []string
}

// StatementTimeoutError
type StatementTimeoutError struct {
	Err error
}

// TimeNowParams
type TimeNowParams struct {
	Now *time.Time
}

// UniqueViolationError
type UniqueViolationError struct {
	ConstraintName string
	Detail         string
	Err            error
	KeyValues      map[string]string
	SQLState       string
}

// Workflow
type Workflow struct {
	CreatedAt           time.Time
	CurrentAttempt      int
	FinalizedAt         *time.Time
	ID                  string
	Metadata            []byte
	Name                *string
	State               string
	UpdatedAt           time.Time
	WaitEvalCursorJobID *int64
}

// WorkflowAttempt
type WorkflowAttempt struct {
	Attempt      int
	CreatedAt    time.Time
	ResetHistory bool
	RetryMode    string
	TriggeredBy  []byte
	WorkflowID   string
}

// WorkflowAttemptInsertParams
type WorkflowAttemptInsertParams struct {
	Attempt      int
	ResetHistory bool
	RetryMode    string
	Schema       string
	TriggeredBy  []byte
	WorkflowID   string
}

// WorkflowAttemptListByWorkflowIDParams
type WorkflowAttemptListByWorkflowIDParams struct {
	Schema     string
	WorkflowID string
}

// WorkflowAttemptTask
type WorkflowAttemptTask struct {
	Attempt      int
	AttemptCount int
	Errors       []rivertype.AttemptError
	FinalizedAt  *time.Time
	JobID        int64
	Metadata     []byte
	State        string
	Task         string
	WorkflowID   string
}

// WorkflowAttemptTaskInsertParams
type WorkflowAttemptTaskInsertParams struct {
	Attempt      int
	AttemptCount int
	Errors       [][]byte
	FinalizedAt  *time.Time
	JobID        int64
	Metadata     []byte
	Schema       string
	State        string
	Task         string
	WorkflowID   string
}

// WorkflowAttemptTaskListByWorkflowIDParams
type WorkflowAttemptTaskListByWorkflowIDParams struct {
	Attempt    int
	Schema     string
	WorkflowID string
}

// WorkflowCancelParams
type WorkflowCancelParams struct {
	CancelAttemptedAt time.Time
	ControlTopic      string
	Schema            string
	WorkflowID        string
}

// WorkflowCancelWithDeletedDepsManyParams
type WorkflowCancelWithDeletedDepsManyParams struct {
	Schema               string
	WorkflowDepsFailedAt time.Time
	WorkflowIDs          []string
}

// WorkflowCancelWithFailedDepsManyParams
type WorkflowCancelWithFailedDepsManyParams struct {
	Schema               string
	WorkflowDepsFailedAt time.Time
	WorkflowIDs          []string
}

// WorkflowCleanupDeleteByWorkflowIDsParams
type WorkflowCleanupDeleteByWorkflowIDsParams struct {
	Schema      string
	WorkflowIDs []string
}

// WorkflowCleanupDeleteWorkflowsByWorkflowIDsParams
type WorkflowCleanupDeleteWorkflowsByWorkflowIDsParams struct {
	Schema      string
	State       string
	WorkflowIDs []string
}

// WorkflowCleanupListFinalizedIDsParams
type WorkflowCleanupListFinalizedIDsParams struct {
	FinalizedBefore time.Time
	LimitCount      int
	Schema          string
	State           string
}

// WorkflowCleanupListFinalizedIDsWithoutJobsParams
type WorkflowCleanupListFinalizedIDsWithoutJobsParams struct {
	LimitCount int
	Schema     string
	State      string
}

// WorkflowCountIncompleteJobsParams
type WorkflowCountIncompleteJobsParams struct {
	Schema          string
	SupervisorJobID int64
	WorkflowID      string
}

// WorkflowFinalizeIfCompleteManyParams
type WorkflowFinalizeIfCompleteManyParams struct {
	Now         time.Time
	Schema      string
	WorkflowIDs []string
}

// WorkflowGetByIDParams
type WorkflowGetByIDParams struct {
	Schema     string
	WorkflowID string
}

// WorkflowGetFinalizationCandidatesParams
type WorkflowGetFinalizationCandidatesParams struct {
	AfterWorkflowID string
	LimitCount      int32
	Schema          string
}

// WorkflowGetLegacyBackfillIDsParams
type WorkflowGetLegacyBackfillIDsParams struct {
	AfterWorkflowID string
	LimitCount      int32
	Schema          string
}

// WorkflowHasWaitTasksManyParams
type WorkflowHasWaitTasksManyParams struct {
	Schema      string
	WorkflowIDs []string
}

// WorkflowInitFromJobsParams
type WorkflowInitFromJobsParams struct {
	Schema      string
	WorkflowIDs []string
}

// WorkflowInsertManyParams
type WorkflowInsertManyParams struct {
	IDs    []string
	Names  []string
	Schema string
}

// WorkflowJobGetByTaskNameParams
type WorkflowJobGetByTaskNameParams struct {
	Schema     string
	TaskName   string
	WorkflowID string
}

// WorkflowJobListParams
type WorkflowJobListParams struct {
	PaginationLimit  int
	PaginationOffset int
	Schema           string
	WorkflowID       string
}

// WorkflowListByIDsForWaitEvalParams
type WorkflowListByIDsForWaitEvalParams struct {
	Schema      string
	WorkflowIDs []string
}

// WorkflowListByIDsParams
type WorkflowListByIDsParams struct {
	Schema      string
	WorkflowIDs []string
}

// WorkflowListItem
type WorkflowListItem struct {
	CountAvailable  int
	CountCancelled  int
	CountCompleted  int
	CountDiscarded  int
	CountFailedDeps int
	CountPending    int
	CountRetryable  int
	CountRunning    int
	CountScheduled  int
	CreatedAt       time.Time
	ID              string
	Name            *string
}

// WorkflowListParams
type WorkflowListParams struct {
	After           string
	Before          string
	PaginationLimit int
	Schema          string
}

// WorkflowLoadDepTasksAndIDsParams
type WorkflowLoadDepTasksAndIDsParams struct {
	Recursive  bool
	Schema     string
	Task       string
	WorkflowID string
}

// WorkflowLoadJobsWithDepsParams
type WorkflowLoadJobsWithDepsParams struct {
	JobIds []int64
	Schema string
}

// WorkflowLoadTaskNamesByWorkflowIDParams
type WorkflowLoadTaskNamesByWorkflowIDParams struct {
	Schema     string
	WorkflowID string
}

// WorkflowLoadTaskWithDepsParams
type WorkflowLoadTaskWithDepsParams struct {
	Schema     string
	Task       string
	WorkflowID string
}

// WorkflowLoadTasksByNamesParams
type WorkflowLoadTasksByNamesParams struct {
	Schema     string
	TaskNames  []string
	WorkflowID string
}

// WorkflowLockByIDsSkipLockedParams
type WorkflowLockByIDsSkipLockedParams struct {
	LimitCount  int
	Schema      string
	WorkflowIDs []string
}

// WorkflowReadyTaskIDsByWorkflowIDsParams
type WorkflowReadyTaskIDsByWorkflowIDsParams struct {
	LimitCount  int
	Schema      string
	WorkflowIDs []string
}

// WorkflowReadyTaskIDsByWorkflowIDsRow
type WorkflowReadyTaskIDsByWorkflowIDsRow struct {
	ID         int64
	TotalCount int64
	WorkflowID string
}

// WorkflowRetryLockAndCheckRunningParams
type WorkflowRetryLockAndCheckRunningParams struct {
	Schema     string
	WorkflowID string
}

// WorkflowRetryLockAndCheckRunningResult
type WorkflowRetryLockAndCheckRunningResult struct {
	WorkflowIsActive bool
}

// WorkflowRetryMode
type WorkflowRetryMode string

const (
	WorkflowRetryModeAll                 WorkflowRetryMode = "all"
	WorkflowRetryModeFailedOnly          WorkflowRetryMode = "failed_only"
	WorkflowRetryModeFailedAndDownstream WorkflowRetryMode = "failed_and_downstream"
)

// WorkflowRetryParams
type WorkflowRetryParams struct {
	Mode         WorkflowRetryMode
	Now          time.Time
	ResetHistory bool
	Schema       string
	WorkflowID   string
}

// WorkflowSignal
type WorkflowSignal struct {
	Attempt        int
	CreatedAt      time.Time
	ID             int64
	IdempotencyKey string
	Key            string
	Metadata       []byte
	Payload        []byte
	Source         []byte
	WorkflowID     string
}

// WorkflowSignalInsertParams
type WorkflowSignalInsertParams struct {
	IdempotencyKey   string
	Key              string
	Metadata         []byte
	Payload          []byte
	RequestedAttempt *int
	Schema           string
	Source           []byte
	WorkflowID       string
}

// WorkflowSignalInsertResult
type WorkflowSignalInsertResult struct {
	WorkflowSignal

	// CurrentAttempt is the workflow's observed current attempt when the query
	// found a workflow row.
	CurrentAttempt int

	// PayloadSemanticEqual indicates whether the returned row payload is
	// semantically equal (Postgres jsonb equality) to the request payload.
	PayloadSemanticEqual bool

	// SignalPresent reports whether the insert query returned a real signal row.
	SignalPresent bool

	// SkippedAsDuplicate is true when an idempotent emit reused an existing
	// signal row instead of inserting a new one.
	SkippedAsDuplicate bool
}

// WorkflowSignalListByEvidenceParams
type WorkflowSignalListByEvidenceParams struct {
	Attempt               int
	CursorID              *int64
	Desc                  bool
	Keys                  []string
	LastIncludedSignalIDs []int64
	LimitCount            int
	Schema                string
	WorkflowID            string
}

// WorkflowSignalListByKeysParams
type WorkflowSignalListByKeysParams struct {
	Attempt    *int
	CursorID   *int64
	Desc       bool
	Keys       []string
	LimitCount int
	Schema     string
	WorkflowID string
}

// WorkflowSignalListByWorkflowIDsParams
type WorkflowSignalListByWorkflowIDsParams struct {
	Attempt     int
	Keys        []string
	Schema      string
	WorkflowIDs []string
}

// WorkflowSignalListParams
type WorkflowSignalListParams struct {
	Attempt    *int
	CursorID   *int64
	Desc       bool
	Key        *string
	LimitCount int
	Schema     string
	WorkflowID string
}

// WorkflowSignalStat
type WorkflowSignalStat struct {
	Key          string
	LastSignalID int64
	SignalCount  int64
	WorkflowID   string
}

// WorkflowSignalStatsByWorkflowIDsParams
type WorkflowSignalStatsByWorkflowIDsParams struct {
	Attempt     int
	Keys        []string
	Schema      string
	WorkflowIDs []string
}

// WorkflowStageJobsByIDManyParams
type WorkflowStageJobsByIDManyParams struct {
	JobIDs           []int64
	Schema           string
	WorkflowStagedAt time.Time
}

// WorkflowTask
type WorkflowTask struct {
	Deps       []string
	ID         int64
	State      rivertype.JobState
	Task       string
	WorkflowID string
}

// WorkflowTaskWithJob
type WorkflowTaskWithJob struct {
	Deps                []string
	IgnoreCancelledDeps bool
	IgnoreDeletedDeps   bool
	IgnoreDiscardedDeps bool
	Job                 *rivertype.JobRow
	Task                string
	WorkflowID          string
}

// WorkflowTimer
type WorkflowTimer struct {
	NextFireAt time.Time
	WorkflowID string
}

// WorkflowTimerConsumeDueParams
type WorkflowTimerConsumeDueParams struct {
	AsOf       time.Time
	LimitCount int
	Schema     string
}

// WorkflowTimerDeleteByWorkflowIDsParams
type WorkflowTimerDeleteByWorkflowIDsParams struct {
	Schema      string
	WorkflowIDs []string
}

// WorkflowTimerGetByWorkflowIDParams
type WorkflowTimerGetByWorkflowIDParams struct {
	Schema     string
	WorkflowID string
}

// WorkflowTimerNextFireAtByWorkflowIDsParams
type WorkflowTimerNextFireAtByWorkflowIDsParams struct {
	Now         time.Time
	Schema      string
	WorkflowIDs []string
}

// WorkflowTimerNextFireAtByWorkflowIDsRow
type WorkflowTimerNextFireAtByWorkflowIDsRow struct {
	NextFireAt time.Time
	WorkflowID string
}

// WorkflowTimerUpsertManyParams
type WorkflowTimerUpsertManyParams struct {
	NextFireAts []time.Time
	Schema      string
	WorkflowIDs []string
}

// WorkflowUnfinalizeIfActiveJobsManyParams
type WorkflowUnfinalizeIfActiveJobsManyParams struct {
	Now         time.Time
	Schema      string
	WorkflowIDs []string
}

// WorkflowWaitActivatableTaskIDsByWorkflowIDsParams
type WorkflowWaitActivatableTaskIDsByWorkflowIDsParams struct {
	LimitCount  int
	Schema      string
	WorkflowIDs []string
}

// WorkflowWaitActivatableTaskIDsByWorkflowIDsRow
type WorkflowWaitActivatableTaskIDsByWorkflowIDsRow struct {
	ID         int64
	TotalCount int64
	WorkflowID string
}

// WorkflowWaitActivateByJobIDManyParams
type WorkflowWaitActivateByJobIDManyParams struct {
	JobIDs []int64
	Now    time.Time
	Schema string
}

// WorkflowWaitActiveTask
type WorkflowWaitActiveTask struct {
	ID         int64
	Metadata   []byte
	TotalCount int64
	WorkflowID string
}

// WorkflowWaitActiveTaskListByWorkflowIDsParams
type WorkflowWaitActiveTaskListByWorkflowIDsParams struct {
	LimitCount  int
	Schema      string
	WorkflowIDs []string
}

// WorkflowWaitDepOutput
type WorkflowWaitDepOutput struct {
	FinalizedAt *time.Time
	Output      []byte
	State       *rivertype.JobState
	Task        string
	WorkflowID  string
}

// WorkflowWaitDepOutputListByWorkflowTaskPairsParams
type WorkflowWaitDepOutputListByWorkflowTaskPairsParams struct {
	Schema      string
	Tasks       []string
	WorkflowIDs []string
}

// WorkflowWaitEvalCursorUpdateByWorkflowIDManyParams
type WorkflowWaitEvalCursorUpdateByWorkflowIDManyParams struct {
	CursorJobIDs []int64
	Schema       string
	WorkflowIDs  []string
}

// WorkflowWaitUpdateMetadataByJobIDManyParams
type WorkflowWaitUpdateMetadataByJobIDManyParams struct {
	JobIDs     []int64
	Schema     string
	WaitStates [][]byte
}

// WorkflowWaitWorkflow
type WorkflowWaitWorkflow struct {
	CreatedAt      time.Time
	CurrentAttempt int
	ID             string
	Metadata       []byte
}

// WorkflowWorklistDeleteByWorkflowIDsReturningReasonsParams
type WorkflowWorklistDeleteByWorkflowIDsReturningReasonsParams struct {
	Schema      string
	WorkflowIDs []string
}

// WorkflowWorklistDeleteByWorkflowIDsReturningReasonsRow
type WorkflowWorklistDeleteByWorkflowIDsReturningReasonsRow struct {
	Reason     int16
	WorkflowID string
}

// WorkflowWorklistIDItem
type WorkflowWorklistIDItem struct {
	ID         int64
	WorkflowID string
}

// WorkflowWorklistInsertManyParams
type WorkflowWorklistInsertManyParams struct {
	Reason      int16
	Schema      string
	WorkflowIDs []string
}

// WorkflowWorklistItem
type WorkflowWorklistItem struct {
	CreatedAt  time.Time
	ID         int64
	Reason     int16
	WorkflowID string
}

// WorkflowWorklistListParams
type WorkflowWorklistListParams struct {
	AfterID    int64
	LimitCount int
	Schema     string
}

func (e *DeadlockError) Error() string {
	if e != nil && e.Err != nil {
		return "deadlock: " + e.Err.Error()
	}
	return "deadlock"
}
func (e *DeadlockError) Is(target error) bool { _, ok := target.(*DeadlockError); return ok }
func (e *DeadlockError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *StatementTimeoutError) Error() string {
	if e != nil && e.Err != nil {
		return "statement timeout: " + e.Err.Error()
	}
	return "statement timeout"
}
func (e *StatementTimeoutError) Is(target error) bool {
	_, ok := target.(*StatementTimeoutError)
	return ok
}
func (e *StatementTimeoutError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *UniqueViolationError) Error() string {
	if e == nil {
		return "unique violation"
	}
	if e.Err != nil {
		return "unique violation: " + e.Err.Error()
	}
	if e.ConstraintName != "" {
		return "unique violation: " + e.ConstraintName
	}
	return "unique violation"
}
func (e *UniqueViolationError) Is(target error) bool {
	_, ok := target.(*UniqueViolationError)
	return ok
}
func (e *UniqueViolationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type Executor struct{ riverdriver.Executor }

type ExecutorTx struct {
	*Executor
	tx riverdriver.ExecutorTx
}

type Wrapper[TTx any] struct {
	riverdriver.Driver[TTx]
	pilot riverpilot.Pilot
}

func NewWrapper[TTx any](base riverdriver.Driver[TTx]) *Wrapper[TTx] {
	return &Wrapper[TTx]{Driver: base}
}
func (w *Wrapper[TTx]) GetProExecutor() ProExecutor {
	if w == nil || w.Driver == nil {
		return &Executor{}
	}
	return &Executor{Executor: w.Driver.GetExecutor()}
}
func (w *Wrapper[TTx]) GetExecutor() riverdriver.Executor {
	if w == nil || w.Driver == nil {
		return &Executor{}
	}
	return &Executor{Executor: w.Driver.GetExecutor()}
}
func (w *Wrapper[TTx]) ProConfigInit(pilot riverpilot.Pilot) {
	if w != nil {
		w.pilot = pilot
	}
}
func (w *Wrapper[TTx]) PluginInit(archetype *baseservice.Archetype) {
	_ = archetype
}
func (w *Wrapper[TTx]) PluginPilot() riverpilot.Pilot {
	if w == nil {
		return nil
	}
	return w.pilot
}
func (w *Wrapper[TTx]) UnwrapProExecutor(tx TTx) ProExecutorTx {
	if w == nil || w.Driver == nil {
		return &ExecutorTx{Executor: &Executor{}}
	}
	execTx := w.Driver.UnwrapExecutor(tx)
	return &ExecutorTx{Executor: &Executor{Executor: execTx}, tx: execTx}
}
func (w *Wrapper[TTx]) UnwrapExecutor(tx TTx) riverdriver.ExecutorTx {
	if w == nil || w.Driver == nil {
		return &ExecutorTx{Executor: &Executor{}}
	}
	execTx := w.Driver.UnwrapExecutor(tx)
	return &ExecutorTx{Executor: &Executor{Executor: execTx}, tx: execTx}
}
func (w *Wrapper[TTx]) UnwrapTx(execTx riverdriver.ExecutorTx) TTx {
	type unwrapTxDriver interface {
		UnwrapTx(riverdriver.ExecutorTx) TTx
	}
	if wrapped, ok := execTx.(*ExecutorTx); ok && wrapped.tx != nil {
		execTx = wrapped.tx
	}
	if w != nil && w.Driver != nil {
		if unwrap, ok := w.Driver.(unwrapTxDriver); ok {
			return unwrap.UnwrapTx(execTx)
		}
	}
	var zero TTx
	return zero
}
func (w *Wrapper[TTx]) GetMigrationLines() []string {
	if w == nil || w.Driver == nil {
		return []string{MigrationLinePro}
	}
	return append(append([]string(nil), w.Driver.GetMigrationLines()...), MigrationLinePro)
}
func (w *Wrapper[TTx]) GetMigrationDefaultLines() []string {
	if w == nil || w.Driver == nil {
		return []string{MigrationLinePro}
	}
	return append(append([]string(nil), w.Driver.GetMigrationDefaultLines()...), MigrationLinePro)
}
func (w *Wrapper[TTx]) HasPool() bool { return w != nil && w.Driver != nil && w.Driver.PoolIsSet() }

func (w *Wrapper[TTx]) GetMigrationFS(line string) fs.FS {
	switch line {
	case MigrationLinePro:
		return migrationFSForLine(line)
	case "sequence", "workflow":
		return nil
	}
	if w == nil || w.Driver == nil {
		return nil
	}
	return w.Driver.GetMigrationFS(line)
}
func (w *Wrapper[TTx]) GetMigrationTruncateTables(line string, version int) []string {
	if line == MigrationLinePro {
		return MigrationLineProTruncateTables(line, version)
	}
	if line == "sequence" || line == "workflow" {
		return nil
	}
	if w == nil || w.Driver == nil {
		return nil
	}
	return w.Driver.GetMigrationTruncateTables(line, version)
}

func (e *Executor) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	if e != nil && e.Executor != nil {
		tx, err := e.Executor.Begin(ctx)
		if err != nil {
			return nil, err
		}
		return &ExecutorTx{Executor: &Executor{Executor: tx}, tx: tx}, nil
	}
	return nil, errors.New("riverpro driver: nil executor")
}
func (e *Executor) JobInsertFastMany(ctx context.Context, params *riverdriver.JobInsertFastManyParams) ([]*riverdriver.JobInsertFastResult, error) {
	if e != nil && e.Executor != nil {
		return e.Executor.JobInsertFastMany(ctx, params)
	}
	return nil, errors.New("riverpro driver: nil executor")
}
func (e *Executor) JobInsertFastManyNoReturning(ctx context.Context, params *riverdriver.JobInsertFastManyParams) (int, error) {
	if e != nil && e.Executor != nil {
		return e.Executor.JobInsertFastManyNoReturning(ctx, params)
	}
	return 0, errors.New("riverpro driver: nil executor")
}

func (e *Executor) BeginPro(ctx context.Context) (ProExecutorTx, error) {
	if e != nil && e.Executor != nil {
		tx, err := e.Executor.Begin(ctx)
		if err != nil {
			return nil, err
		}
		return &ExecutorTx{Executor: &Executor{Executor: tx}, tx: tx}, nil
	}
	return nil, errors.New("riverpro driver: nil executor")
}
func (e *ExecutorTx) BeginPro(ctx context.Context) (ProExecutorTx, error) {
	if e != nil && e.tx != nil {
		tx, err := e.tx.Begin(ctx)
		if err != nil {
			return nil, err
		}
		return &ExecutorTx{Executor: &Executor{Executor: tx}, tx: tx}, nil
	}
	return nil, errors.New("riverpro driver: nil transaction executor")
}
func (e *ExecutorTx) Commit(ctx context.Context) error {
	if e != nil && e.tx != nil {
		return e.tx.Commit(ctx)
	}
	return errors.New("riverpro driver: nil transaction executor")
}
func (e *ExecutorTx) Rollback(ctx context.Context) error {
	if e != nil && e.tx != nil {
		return e.tx.Rollback(ctx)
	}
	return errors.New("riverpro driver: nil transaction executor")
}

// ---- Clean-room runtime compatibility state -------------------------------------------------

var compat = struct {
	sync.Mutex
	periodic     map[string]map[string]*PeriodicJob
	producers    map[string]map[int64]*Producer
	producerSeq  int64
	sequences    map[string]map[string]*Sequence
	sequenceSeq  int64
	workflows    map[string]map[string]*Workflow
	attempts     map[string]map[string][]*WorkflowAttempt
	attemptTasks map[string]map[string][]*WorkflowAttemptTask
	signals      map[string]map[string][]*WorkflowSignal
	signalSeq    int64
	timers       map[string]map[string]*WorkflowTimer
	worklists    map[string]map[string]*WorkflowWorklistItem
	worklistSeq  int64
}{
	periodic:     map[string]map[string]*PeriodicJob{},
	producers:    map[string]map[int64]*Producer{},
	sequences:    map[string]map[string]*Sequence{},
	workflows:    map[string]map[string]*Workflow{},
	attempts:     map[string]map[string][]*WorkflowAttempt{},
	attemptTasks: map[string]map[string][]*WorkflowAttemptTask{},
	signals:      map[string]map[string][]*WorkflowSignal{},
	timers:       map[string]map[string]*WorkflowTimer{},
	worklists:    map[string]map[string]*WorkflowWorklistItem{},
}

func nowUTC() time.Time { return time.Now().UTC() }

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func nullableTimePtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC()
}

// periodicJobJSONBuildObject is the SELECT/RETURNING column list expressed
// as a json_build_object() call. Keep this in sync with the PeriodicJob struct.
const periodicJobJSONBuildObject = `json_build_object(
	'ID', id,
	'CreatedAt', created_at,
	'NextRunAt', next_run_at,
	'UpdatedAt', updated_at,
	'PausedAt', paused_at,
	'Kind', kind,
	'Args', encode(convert_to(args::text, 'UTF8'), 'base64'),
	'Queue', queue,
	'Priority', priority,
	'MaxAttempts', max_attempts,
	'Tags', to_jsonb(tags),
	'CronExpression', cron_expression,
	'CronTimezone', cron_timezone
)`

// periodicJobSelectColumns is the raw column list used in subqueries.
const periodicJobSelectColumns = `id, created_at, next_run_at, updated_at, paused_at,
	kind, args, queue, priority, max_attempts, tags,
	cron_expression, cron_timezone`

// PeriodicJobChangeTopicSuffix is the base name of the LISTEN/NOTIFY topic.
const PeriodicJobChangeTopicSuffix = "river_periodic_job_change"

// PeriodicJobChangeTopic returns the schema-specific LISTEN/NOTIFY channel.
// PostgreSQL channel names are limited to 63 bytes, so oversized schema names
// are replaced with a stable hash while preserving cross-schema isolation.
func PeriodicJobChangeTopic(schema string) string {
	if schema == "" {
		return PeriodicJobChangeTopicSuffix
	}
	topic := schema + "." + PeriodicJobChangeTopicSuffix
	if len(topic) <= 63 {
		return topic
	}
	hash := sha256.Sum256([]byte(schema))
	return fmt.Sprintf("%s_%x", PeriodicJobChangeTopicSuffix, hash[:8])
}

// periodicJobQueueDefault returns the default queue if the caller passed an
// empty value. Mirrors the schema default.
func periodicJobQueueDefault(q string) string {
	if q == "" {
		return "default"
	}
	return q
}

// periodicJobCronTimezoneDefault returns the default timezone for cron
// parsing if the caller passed an empty value. Mirrors the schema default.
func periodicJobCronTimezoneDefault(tz string) string {
	if tz == "" {
		return "UTC"
	}
	return tz
}

// periodicJobUpsertArgs builds the positional argument list for a
// PeriodicJobUpsert statement. Order MUST match the column list in the
// INSERT SQL: id, next_run_at, updated_at, kind, args, queue, priority,
// max_attempts, tags, cron_expression, cron_timezone.
func periodicJobUpsertArgs(now time.Time, params *PeriodicJobUpsertParams) ([]any, string) {
	if params.Kind == "" {
		return nil, "riverpro driver: periodic job kind must be non-empty"
	}
	args := []any{
		params.ID,                             // 1: id
		params.NextRunAt,                      // 2: next_run_at
		now,                                   // 3: updated_at
		params.Kind,                           // 4: kind
		nullableJSONBytes(params.Args),        // 5: args
		periodicJobQueueDefault(params.Queue), // 6: queue (filled by caller)
		params.Priority,                       // 7: priority
		params.MaxAttempts,                    // 8: max_attempts
		nonNilTags(params.Tags),               // 9: tags
	}
	if params.CronExpression == nil {
		args = append(args, nil) // 10: cron_expression
	} else {
		args = append(args, *params.CronExpression)
	}
	// 11: cron_timezone appended by the caller
	return args, ""
}
func limitDefault(limit, def int) int {
	if limit <= 0 {
		return def
	}
	return limit
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func cloneJob(job *rivertype.JobRow) *rivertype.JobRow {
	if job == nil {
		return nil
	}
	j := *job
	j.AttemptedBy = append([]string(nil), job.AttemptedBy...)
	j.EncodedArgs = append([]byte(nil), job.EncodedArgs...)
	j.Errors = append([]rivertype.AttemptError(nil), job.Errors...)
	j.Metadata = append([]byte(nil), job.Metadata...)
	j.Tags = append([]string(nil), job.Tags...)
	j.UniqueKey = append([]byte(nil), job.UniqueKey...)
	j.UniqueStates = append([]rivertype.JobState(nil), job.UniqueStates...)
	return &j
}
func cloneJobs(in []*rivertype.JobRow) []*rivertype.JobRow {
	out := make([]*rivertype.JobRow, 0, len(in))
	for _, j := range in {
		out = append(out, cloneJob(j))
	}
	return out
}

func metaMap(metadata []byte) map[string]json.RawMessage {
	var m map[string]json.RawMessage
	if len(metadata) == 0 {
		return map[string]json.RawMessage{}
	}
	_ = json.Unmarshal(metadata, &m)
	if m == nil {
		m = map[string]json.RawMessage{}
	}
	return m
}
func metaString(metadata []byte, key string) string {
	var s string
	_ = json.Unmarshal(metaMap(metadata)[key], &s)
	return s
}
func metaStrings(metadata []byte, key string) []string {
	var out []string
	_ = json.Unmarshal(metaMap(metadata)[key], &out)
	return out
}
func metaBool(metadata []byte, key string) bool {
	var b bool
	_ = json.Unmarshal(metaMap(metadata)[key], &b)
	return b
}
func jobWorkflowID(job *rivertype.JobRow) string {
	if job == nil {
		return ""
	}
	return metaString(job.Metadata, "workflow_id")
}
func jobWorkflowName(job *rivertype.JobRow) string {
	if job == nil {
		return ""
	}
	return metaString(job.Metadata, "workflow_name")
}
func jobTask(job *rivertype.JobRow) string {
	if job == nil {
		return ""
	}
	return metaString(job.Metadata, "workflow_task")
}
func jobDeps(job *rivertype.JobRow) []string {
	if job == nil {
		return nil
	}
	return metaStrings(job.Metadata, "workflow_deps")
}
func jobWaitRaw(job *rivertype.JobRow) []byte {
	m := metaMap(job.Metadata)
	if raw := m["workflow_wait"]; len(raw) > 0 {
		return []byte(raw)
	}
	return nil
}
func jsonContainsWorkflowID(id string) []byte {
	b, _ := json.Marshal(map[string]string{"workflow_id": id})
	return b
}
func jsonContainsWorkflowTask(id, task string) []byte {
	b, _ := json.Marshal(map[string]string{"workflow_id": id, "workflow_task": task})
	return b
}

func activeState(state rivertype.JobState) bool {
	switch state {
	case rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRetryable, rivertype.JobStateRunning, rivertype.JobStateScheduled:
		return true
	default:
		return false
	}
}
func finalizedState(state rivertype.JobState) bool {
	switch state {
	case rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded:
		return true
	default:
		return false
	}
}
func completedState(state rivertype.JobState) bool { return state == rivertype.JobStateCompleted }

func (e *Executor) allWorkflowJobs(ctx context.Context, schema, workflowID string, max int) ([]*rivertype.JobRow, error) {
	if e == nil || e.Executor == nil {
		return nil, errors.New("riverpro driver: nil executor")
	}
	where := "metadata @> @metadata::jsonb"
	args := map[string]any{"metadata": jsonContainsWorkflowID(workflowID)}
	return e.Executor.JobList(ctx, &riverdriver.JobListParams{Max: int32(limitDefault(max, 10000)), NamedArgs: args, OrderByClause: "id", Schema: schema, WhereClause: where})
}
func (e *Executor) allWorkflowJobsAny(ctx context.Context, schema string, max int) ([]*rivertype.JobRow, error) {
	if e == nil || e.Executor == nil {
		return nil, errors.New("riverpro driver: nil executor")
	}
	return e.Executor.JobList(ctx, &riverdriver.JobListParams{Max: int32(limitDefault(max, 10000)), OrderByClause: "id", Schema: schema, WhereClause: "metadata ? 'workflow_id'"})
}
func (e *Executor) workflowJobByTask(ctx context.Context, schema, workflowID, task string) (*rivertype.JobRow, error) {
	jobs, err := e.Executor.JobList(ctx, &riverdriver.JobListParams{Max: 1, NamedArgs: map[string]any{"metadata": jsonContainsWorkflowTask(workflowID, task)}, OrderByClause: "id", Schema: schema, WhereClause: "metadata @> @metadata::jsonb"})
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return nil, rivertype.ErrNotFound
	}
	return jobs[0], nil
}
func (e *Executor) workflowJobsByIDs(ctx context.Context, schema string, ids []int64) ([]*rivertype.JobRow, error) {
	if len(ids) == 0 {
		return []*rivertype.JobRow{}, nil
	}
	return e.Executor.JobGetByIDMany(ctx, &riverdriver.JobGetByIDManyParams{ID: ids, Schema: schema})
}

func workflowTaskWithJob(job *rivertype.JobRow) *WorkflowTaskWithJob {
	if job == nil {
		return nil
	}
	return &WorkflowTaskWithJob{
		Deps:                jobDeps(job),
		IgnoreCancelledDeps: metaBool(job.Metadata, "workflow_ignore_cancelled_deps"),
		IgnoreDeletedDeps:   metaBool(job.Metadata, "workflow_ignore_deleted_deps"),
		IgnoreDiscardedDeps: metaBool(job.Metadata, "workflow_ignore_discarded_deps"),
		Job:                 cloneJob(job),
		Task:                jobTask(job),
		WorkflowID:          jobWorkflowID(job),
	}
}
func workflowTask(job *rivertype.JobRow) *WorkflowTask {
	if job == nil {
		return nil
	}
	return &WorkflowTask{Deps: jobDeps(job), ID: job.ID, State: job.State, Task: jobTask(job), WorkflowID: jobWorkflowID(job)}
}
func taskMap(jobs []*rivertype.JobRow) map[string]*rivertype.JobRow {
	m := make(map[string]*rivertype.JobRow, len(jobs))
	for _, j := range jobs {
		if t := jobTask(j); t != "" {
			m[t] = j
		}
	}
	return m
}
func readyWorkflowJob(job *rivertype.JobRow, byTask map[string]*rivertype.JobRow) bool {
	if job == nil || job.State != rivertype.JobStatePending {
		return false
	}
	for _, dep := range jobDeps(job) {
		dj := byTask[dep]
		if dj == nil || !completedState(dj.State) {
			return false
		}
	}
	if len(jobWaitRaw(job)) > 0 && !jobWaitResolved(job) {
		return false
	}
	return true
}
func jobWaitResolved(job *rivertype.JobRow) bool {
	if job == nil {
		return false
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(job.Metadata, &metadata); err != nil {
		return false
	}
	var wait struct {
		Phase int `json:"phase"`
	}
	if err := json.Unmarshal(metadata["workflow_wait"], &wait); err != nil {
		return false
	}
	return wait.Phase == 2
}
func workflowListItemFromJobs(id string, jobs []*rivertype.JobRow) *WorkflowListItem {
	item := &WorkflowListItem{ID: id}
	if len(jobs) > 0 {
		item.CreatedAt = jobs[0].CreatedAt
		n := jobWorkflowName(jobs[0])
		if n != "" {
			item.Name = &n
		}
	}
	for _, j := range jobs {
		switch j.State {
		case rivertype.JobStateAvailable:
			item.CountAvailable++
		case rivertype.JobStateCancelled:
			item.CountCancelled++
		case rivertype.JobStateCompleted:
			item.CountCompleted++
		case rivertype.JobStateDiscarded:
			item.CountDiscarded++
		case rivertype.JobStatePending:
			item.CountPending++
		case rivertype.JobStateRetryable:
			item.CountRetryable++
		case rivertype.JobStateRunning:
			item.CountRunning++
		case rivertype.JobStateScheduled:
			item.CountScheduled++
		}
	}
	return item
}
func groupWorkflowJobs(jobs []*rivertype.JobRow) map[string][]*rivertype.JobRow {
	out := map[string][]*rivertype.JobRow{}
	for _, j := range jobs {
		if id := jobWorkflowID(j); id != "" {
			out[id] = append(out[id], j)
		}
	}
	return out
}

// ---- Job/dead-letter/batch helpers -----------------------------------------------------------

func (e *Executor) JobDeleteByIDMany(ctx context.Context, params *JobDeleteByIDManyParams) ([]*rivertype.JobRow, error) {
	if e == nil || e.Executor == nil {
		return nil, errors.New("riverpro driver: nil executor")
	}
	if params == nil || len(params.ID) == 0 {
		return []*rivertype.JobRow{}, nil
	}
	return e.Executor.JobDeleteMany(ctx, &riverdriver.JobDeleteManyParams{Max: int32(len(params.ID)), NamedArgs: map[string]any{"ids": params.ID}, OrderByClause: "id", Schema: params.Schema, WhereClause: "id = any(@ids)"})
}
func (e *Executor) JobDeleteNonWorkflowBefore(ctx context.Context, params *JobDeleteNonWorkflowBeforeParams) (int, error) {
	if e == nil || e.Executor == nil {
		return 0, errors.New("riverpro driver: nil executor")
	}
	if params == nil {
		return 0, nil
	}
	return e.Executor.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{CancelledDoDelete: params.CancelledDoDelete, CancelledFinalizedAtHorizon: params.CancelledFinalizedAtHorizon, CompletedDoDelete: params.CompletedDoDelete, CompletedFinalizedAtHorizon: params.CompletedFinalizedAtHorizon, DiscardedDoDelete: params.DiscardedDoDelete, DiscardedFinalizedAtHorizon: params.DiscardedFinalizedAtHorizon, Max: params.Max, QueuesExcluded: params.QueuesExcluded, QueuesIncluded: params.QueuesIncluded, Schema: params.Schema})
}
func (e *Executor) JobGetAvailableLimited(ctx context.Context, params *JobGetAvailableLimitedParams) ([]*rivertype.JobRow, error) {
	if e == nil || e.Executor == nil {
		return nil, errors.New("riverpro driver: nil executor")
	}
	if params == nil || params.JobGetAvailableParams == nil || params.JobGetAvailableParams.MaxToLock <= 0 {
		return []*rivertype.JobRow{}, nil
	}
	base := params.JobGetAvailableParams
	if params.GlobalLimit <= 0 && params.LocalLimit <= 0 {
		return e.Executor.JobGetAvailable(ctx, base)
	}
	if len(params.CurrentProducerPartitionKeys) != len(params.CurrentProducerPartitionRunningCounts) {
		return nil, fmt.Errorf("riverpro driver: current producer partition key/count length mismatch: %d != %d", len(params.CurrentProducerPartitionKeys), len(params.CurrentProducerPartitionRunningCounts))
	}
	now := nowUTC()
	if base.Now != nil {
		now = *base.Now
	}
	partitionExpr := jobConcurrencyPartitionKeySQL("j", params.PartitionByKind, params.PartitionByArgs)
	runningPartitionExpr := jobConcurrencyPartitionKeySQL("r", params.PartitionByKind, params.PartitionByArgs)
	schema := base.Schema
	jobTable := qt(schema, "river_job")
	jobState := qt(schema, "river_job_state")
	queueTable := qt(schema, "river_queue")
	query := fmt.Sprintf(`
WITH available_partitions AS MATERIALIZED (
	SELECT DISTINCT partition_key
	FROM (
		SELECT %[1]s AS partition_key
		FROM %[2]s AS j
		WHERE j.state = 'available'::%[3]s
		  AND j.queue = $1
		  AND j.scheduled_at <= $2
		  AND NOT EXISTS (SELECT 1 FROM %[6]s AS q WHERE q.name = $1 AND q.paused_at IS NOT NULL)
	) available_partition_source
	WHERE (coalesce(cardinality($5::text[]), 0) = 0 OR partition_key = ANY($5::text[]))
	  AND ($6::text[] IS NOT NULL OR true)
	ORDER BY partition_key
), partition_locks AS MATERIALIZED (
	SELECT pg_advisory_xact_lock(hashtextextended($13 || ':' || partition_key, 0))
	FROM available_partitions
	ORDER BY partition_key
), available_jobs AS MATERIALIZED (
	SELECT j.id,
	       %[1]s AS partition_key,
	       row_number() OVER (PARTITION BY %[1]s ORDER BY j.priority ASC, j.scheduled_at ASC, j.id ASC) AS partition_row_num,
	       j.priority,
	       j.scheduled_at
	FROM %[2]s AS j, (SELECT count(*) FROM partition_locks) locks_acquired
	WHERE j.state = 'available'::%[3]s
	  AND j.queue = $1
	  AND j.scheduled_at <= $2
	  AND (coalesce(cardinality($5::text[]), 0) = 0 OR %[1]s = ANY($5::text[]))
), global_running_counts AS MATERIALIZED (
	SELECT %[4]s AS partition_key, count(*)::integer AS running_count
	FROM %[2]s AS r, (SELECT count(*) FROM partition_locks) locks_acquired
	WHERE r.state = 'running'::%[3]s
	  AND r.queue = $1
	GROUP BY partition_key
), db_local_running_counts AS MATERIALIZED (
	SELECT %[4]s AS partition_key, count(*)::integer AS running_count
	FROM %[2]s AS r, (SELECT count(*) FROM partition_locks) locks_acquired
	WHERE r.state = 'running'::%[3]s
	  AND r.queue = $1
	  AND array_length(r.attempted_by, 1) IS NOT NULL
	  AND r.attempted_by[array_length(r.attempted_by, 1)] = $11
	GROUP BY partition_key
), passed_local_running_counts AS MATERIALIZED (
	SELECT partition_key, sum(running_count)::integer AS running_count
	FROM unnest($3::text[], $4::integer[]) AS u(partition_key, running_count)
	GROUP BY partition_key
), local_running_counts AS MATERIALIZED (
	SELECT partition_key, sum(running_count)::integer AS running_count
	FROM (
		SELECT partition_key, running_count FROM passed_local_running_counts WHERE $12::boolean
		UNION ALL
		SELECT partition_key, running_count FROM db_local_running_counts WHERE NOT $12::boolean
	) local_count_source
	GROUP BY partition_key
), eligible_jobs AS MATERIALIZED (
	SELECT a.id, a.priority, a.scheduled_at, a.partition_key,
	       least(
	           CASE WHEN $8::integer > 0 THEN greatest($8::integer - coalesce(g.running_count, 0), 0) ELSE $9::integer END,
	           CASE WHEN $7::integer > 0 THEN greatest($7::integer - coalesce(l.running_count, 0), 0) ELSE $9::integer END
	       ) AS capacity,
	       a.partition_row_num
	FROM available_jobs AS a
	LEFT JOIN global_running_counts AS g ON g.partition_key = a.partition_key
	LEFT JOIN local_running_counts AS l ON l.partition_key = a.partition_key
), locked_jobs AS MATERIALIZED (
	SELECT j.*
	FROM eligible_jobs AS e
	JOIN %[2]s AS j ON j.id = e.id
	WHERE e.capacity > 0 AND e.partition_row_num <= e.capacity
	ORDER BY e.priority ASC, e.scheduled_at ASC, e.id ASC
	LIMIT $9::integer
	FOR UPDATE OF j SKIP LOCKED
), updated_jobs AS (
	UPDATE %[2]s AS j
	SET state = 'running'::%[3]s,
	    attempt = j.attempt + 1,
	    attempted_at = $2,
	    attempted_by = array_append(
	        CASE WHEN array_length(j.attempted_by, 1) >= $10::integer
	             THEN j.attempted_by[array_length(j.attempted_by, 1) + 2 - $10::integer:]
	             ELSE j.attempted_by
	        END,
	        $11::text
	    )
	FROM locked_jobs
	WHERE j.id = locked_jobs.id
	  AND j.state = 'available'::%[3]s
	  AND j.queue = $1
	  AND j.scheduled_at <= $2
	  AND NOT EXISTS (SELECT 1 FROM %[6]s AS q WHERE q.name = $1 AND q.paused_at IS NOT NULL)
	RETURNING j.*
)
SELECT coalesce(json_agg(%[5]s ORDER BY j.priority ASC, j.scheduled_at ASC, j.id ASC), '[]'::json)
FROM updated_jobs AS j
`, partitionExpr, jobTable, jobState, runningPartitionExpr, jobRowJSONObjectSQL("j"), queueTable)
	jobs, err := scanJSON[[]*rivertype.JobRow](ctx, e.Executor, query,
		base.Queue,
		now,
		params.CurrentProducerPartitionKeys,
		params.CurrentProducerPartitionRunningCounts,
		params.AvailablePartitionKeys,
		params.PartitionByArgs,
		int(params.LocalLimit),
		int(params.GlobalLimit),
		base.MaxToLock,
		base.MaxAttemptedBy,
		base.ClientID,
		len(params.CurrentProducerPartitionKeys) > 0,
		fmt.Sprintf("%s:%s", schema, base.Queue),
	)
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (e *Executor) JobGetStuckWithInactiveProducer(ctx context.Context, params *JobGetStuckWithInactiveProducerParams) ([]*rivertype.JobRow, error) {
	if e == nil || e.Executor == nil {
		return nil, errors.New("riverpro driver: nil executor")
	}
	if params == nil || params.JobGetStuckParams == nil || params.Max <= 0 {
		return []*rivertype.JobRow{}, nil
	}
	schema := params.Schema
	return scanJSON[[]*rivertype.JobRow](ctx, e.Executor, fmt.Sprintf(`
		SELECT coalesce(json_agg(%[1]s ORDER BY j.id), '[]'::json)
		FROM (
			SELECT j.*
			FROM %[2]s AS j
			LEFT JOIN %[3]s AS p
			  ON p.client_id = j.attempted_by[array_length(j.attempted_by, 1)]
			 AND p.queue_name = j.queue
			WHERE j.state = 'running'::%[4]s
			  AND j.id > $1
			  AND (
				j.attempted_at < $2
				OR (
					coalesce(array_length(j.attempted_by, 1), 0) > 0
					AND (p.id IS NULL OR p.updated_at < $3)
				)
			  )
			ORDER BY j.id
			LIMIT $4
		) AS j
	`, jobRowJSONObjectSQL("j"), qt(schema, "river_job"), qt(schema, "river_producer"), qt(schema, "river_job_state")), params.AfterID, params.StuckHorizon, params.ProducerStaleHorizon, params.Max)
}

func (e *Executor) JobRescueManyWithInactiveProducer(ctx context.Context, params *JobRescueManyWithInactiveProducerParams) error {
	if e == nil || e.Executor == nil {
		return errors.New("riverpro driver: nil executor")
	}
	if params == nil || params.JobRescueManyParams == nil || len(params.ID) == 0 {
		return nil
	}
	if len(params.Error) != len(params.ID) || len(params.FinalizedAt) != len(params.ID) || len(params.ScheduledAt) != len(params.ID) || len(params.State) != len(params.ID) {
		return errors.New("riverpro driver: rescue parameter lengths must match job IDs")
	}
	schema := params.Schema
	return e.Executor.Exec(ctx, fmt.Sprintf(`
		UPDATE %[1]s AS j
		SET errors = array_append(j.errors, updated.error),
		    finalized_at = updated.finalized_at,
		    scheduled_at = updated.scheduled_at,
		    metadata = j.metadata || jsonb_build_object(
		        'river:rescue_count',
		        coalesce(CASE WHEN jsonb_typeof(j.metadata->'river:rescue_count') = 'number' THEN (j.metadata->>'river:rescue_count')::int END, 0) + 1
		    ),
		    state = updated.state::%[2]s
		FROM (
			SELECT unnest($1::bigint[]) AS id,
			       unnest($2::jsonb[]) AS error,
			       unnest($3::timestamptz[]) AS finalized_at,
			       unnest($4::timestamptz[]) AS scheduled_at,
			       unnest($5::text[]) AS state
		) AS updated
		WHERE j.id = updated.id
		  AND j.state = 'running'::%[2]s
		  AND (
			j.attempted_at < $6
			OR (
				coalesce(array_length(j.attempted_by, 1), 0) > 0
				AND NOT EXISTS (
					SELECT 1 FROM %[3]s AS p
					WHERE p.client_id = j.attempted_by[array_length(j.attempted_by, 1)]
					  AND p.queue_name = j.queue
					  AND p.updated_at >= $7
				)
			)
		  )
	`, qt(schema, "river_job"), qt(schema, "river_job_state"), qt(schema, "river_producer")), params.ID, params.Error, params.FinalizedAt, params.ScheduledAt, params.State, params.StuckHorizon, params.ProducerStaleHorizon)
}

func jobConcurrencyPartitionKeySQL(alias string, byKind bool, byArgs []string) string {
	parts := make([]string, 0, 2)
	if byKind {
		parts = append(parts, fmt.Sprintf("'kind=' || %s.kind", alias))
	}
	if byArgs != nil {
		if len(byArgs) == 0 {
			parts = append(parts, fmt.Sprintf("'args=' || %s.args::text", alias))
		} else {
			parts = append(parts, fmt.Sprintf("'args=' || coalesce((SELECT jsonb_object_agg(arg_key, %s.args -> arg_key ORDER BY arg_key)::text FROM unnest($6::text[]) AS arg_key), '{}'::text)", alias))
		}
	}
	if len(parts) == 0 {
		return fmt.Sprintf("'queue=' || %s.queue", alias)
	}
	return "concat(" + strings.Join(parts, ", '|', ") + ")"
}

func jobRowJSONObjectSQL(alias string) string {
	return fmt.Sprintf(`json_build_object(
		'ID', %[1]s.id,
		'Attempt', %[1]s.attempt,
		'AttemptedAt', %[1]s.attempted_at,
		'AttemptedBy', %[1]s.attempted_by,
		'CreatedAt', %[1]s.created_at,
		'EncodedArgs', encode(convert_to(%[1]s.args::text, 'UTF8'), 'base64'),
		'Errors', %[1]s.errors,
		'FinalizedAt', %[1]s.finalized_at,
		'Kind', %[1]s.kind,
		'MaxAttempts', %[1]s.max_attempts,
		'Metadata', encode(convert_to(%[1]s.metadata::text, 'UTF8'), 'base64'),
		'Priority', %[1]s.priority,
		'Queue', %[1]s.queue,
		'ScheduledAt', %[1]s.scheduled_at,
		'State', %[1]s.state,
		'Tags', %[1]s.tags,
		'UniqueKey', encode(coalesce(%[1]s.unique_key, ''::bytea), 'base64'),
		'UniqueStates', %[1]s.unique_states
	)`, alias)
}

func (e *Executor) PGTryAdvisoryXactLock(ctx context.Context, key int64) (bool, error) {
	if e == nil || e.Executor == nil {
		return false, errors.New("riverpro driver: nil executor")
	}
	row := e.Executor.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", key)
	var ok bool
	if err := row.Scan(&ok); err != nil {
		return false, err
	}
	return ok, nil
}
func (e *Executor) TimeNow(ctx context.Context, params *TimeNowParams) (time.Time, error) {
	_ = ctx
	if params != nil && params.Now != nil {
		return *params.Now, nil
	}
	return nowUTC(), nil
}

func (e *Executor) JobDeadLetterDeleteByID(ctx context.Context, params *JobDeadLetterDeleteByIDParams) (*rivertype.JobRow, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	if !dbAvailable(e) {
		return nil, errors.New("riverpro driver: nil executor")
	}
	schema := params.Schema
	job, err := scanJSON[*rivertype.JobRow](ctx, e.Executor, fmt.Sprintf(`
		DELETE FROM %s WHERE id = $1
		RETURNING json_build_object(
			'ID', id, 'Attempt', attempt, 'AttemptedAt', attempted_at, 'AttemptedBy', attempted_by,
			'CreatedAt', created_at, 'EncodedArgs', encode(convert_to(args::text, 'UTF8'), 'base64'), 'Errors', errors,
			'FinalizedAt', finalized_at, 'Kind', kind, 'MaxAttempts', max_attempts,
			'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'Priority', priority, 'Queue', queue,
			'ScheduledAt', scheduled_at, 'State', state, 'Tags', tags,
			'UniqueKey', encode(coalesce(unique_key, ''::bytea), 'base64'), 'UniqueStates', unique_states
		)
	`, qt(schema, "river_job_dead_letter")), params.ID)
	if err != nil {
		return nil, err
	}
	return job, nil
}
func (e *Executor) JobDeadLetterGetAll(ctx context.Context, params *JobDeadLetterGetAllParams) ([]*rivertype.JobRow, error) {
	if !dbAvailable(e) {
		return nil, errors.New("riverpro driver: nil executor")
	}
	schema := ""
	max := 10000
	if params != nil {
		schema = params.Schema
	}
	return scanJSON[[]*rivertype.JobRow](ctx, e.Executor, fmt.Sprintf(`
		SELECT coalesce(json_agg(json_build_object(
			'ID', id, 'Attempt', attempt, 'AttemptedAt', attempted_at, 'AttemptedBy', attempted_by,
			'CreatedAt', created_at, 'EncodedArgs', encode(convert_to(args::text, 'UTF8'), 'base64'), 'Errors', errors,
			'FinalizedAt', finalized_at, 'Kind', kind, 'MaxAttempts', max_attempts,
			'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'Priority', priority, 'Queue', queue,
			'ScheduledAt', scheduled_at, 'State', state, 'Tags', tags,
			'UniqueKey', encode(coalesce(unique_key, ''::bytea), 'base64'), 'UniqueStates', unique_states
		) ORDER BY finalized_at DESC NULLS LAST, id DESC), '[]'::json)
		FROM (SELECT * FROM %s ORDER BY finalized_at DESC NULLS LAST, id DESC LIMIT $1) dl
	`, qt(schema, "river_job_dead_letter")), max)
}
func (e *Executor) JobDeadLetterGetByID(ctx context.Context, params *JobDeadLetterGetByIDParams) (*rivertype.JobRow, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	if !dbAvailable(e) {
		return nil, errors.New("riverpro driver: nil executor")
	}
	schema := params.Schema
	return scanJSON[*rivertype.JobRow](ctx, e.Executor, fmt.Sprintf(`
		SELECT json_build_object(
			'ID', id, 'Attempt', attempt, 'AttemptedAt', attempted_at, 'AttemptedBy', attempted_by,
			'CreatedAt', created_at, 'EncodedArgs', encode(convert_to(args::text, 'UTF8'), 'base64'), 'Errors', errors,
			'FinalizedAt', finalized_at, 'Kind', kind, 'MaxAttempts', max_attempts,
			'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'Priority', priority, 'Queue', queue,
			'ScheduledAt', scheduled_at, 'State', state, 'Tags', tags,
			'UniqueKey', encode(coalesce(unique_key, ''::bytea), 'base64'), 'UniqueStates', unique_states
		)
		FROM %s WHERE id = $1
	`, qt(schema, "river_job_dead_letter")), params.ID)
}
func (e *Executor) JobDeadLetterMoveByID(ctx context.Context, params *JobDeadLetterMoveByIDParams) (*rivertype.JobRow, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	if !dbAvailable(e) {
		return nil, errors.New("riverpro driver: nil executor")
	}
	schema := params.Schema
	now := nowUTC()
	id, err := scanJSON[int64](ctx, e.Executor, fmt.Sprintf(`
		WITH moved AS (
			DELETE FROM %s WHERE id = $1 RETURNING *
		), upserted AS (
			INSERT INTO %s (id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states)
			SELECT id, args, attempt, NULL, NULL, created_at, errors, NULL, kind, max_attempts, metadata, priority, queue, 'available'::%s, $2, tags, unique_key, unique_states
			FROM moved
			ON CONFLICT (id) DO UPDATE SET attempted_at = NULL, attempted_by = NULL, finalized_at = NULL, state = 'available'::%s, scheduled_at = $2
			RETURNING id
		)
		SELECT to_json(id) FROM upserted
	`, qt(schema, "river_job_dead_letter"), qt(schema, "river_job"), qt(schema, "river_job_state"), qt(schema, "river_job_state")), params.ID, now)
	if err != nil {
		return nil, err
	}
	return e.Executor.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: id, Schema: params.Schema})
}
func (e *Executor) JobDeadLetterMoveDiscarded(ctx context.Context, params *JobDeadLetterMoveDiscardedParams) ([]*rivertype.JobRow, error) {
	if e == nil || e.Executor == nil {
		return nil, errors.New("riverpro driver: nil executor")
	}
	if params == nil || params.Max <= 0 {
		return []*rivertype.JobRow{}, nil
	}
	schema := params.Schema
	return scanJSON[[]*rivertype.JobRow](ctx, e.Executor, fmt.Sprintf(`
		WITH to_move AS MATERIALIZED (
			SELECT id
			FROM %s
			WHERE state = 'discarded'::%s
			  AND finalized_at < $1
			ORDER BY finalized_at ASC, id ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		), moved AS (
			DELETE FROM %s AS j
			USING to_move
			WHERE j.id = to_move.id
			RETURNING j.*
		), inserted AS (
			INSERT INTO %s (id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states, dead_lettered_at)
			SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states, now()
			FROM moved
			ON CONFLICT (id) DO NOTHING
			RETURNING *
		)
		SELECT coalesce(json_agg(json_build_object(
			'ID', id, 'Attempt', attempt, 'AttemptedAt', attempted_at, 'AttemptedBy', attempted_by,
			'CreatedAt', created_at, 'EncodedArgs', encode(convert_to(args::text, 'UTF8'), 'base64'), 'Errors', errors,
			'FinalizedAt', finalized_at, 'Kind', kind, 'MaxAttempts', max_attempts,
			'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'Priority', priority, 'Queue', queue,
			'ScheduledAt', scheduled_at, 'State', state, 'Tags', tags,
			'UniqueKey', encode(coalesce(unique_key, ''::bytea), 'base64'), 'UniqueStates', unique_states
		) ORDER BY finalized_at ASC, id ASC), '[]'::json)
		FROM inserted
	`, qt(schema, "river_job"), qt(schema, "river_job_state"), qt(schema, "river_job"), qt(schema, "river_job_dead_letter")), params.DiscardedFinalizedAtHorizon, params.Max)
}
func (e *Executor) JobGetAvailableForBatch(ctx context.Context, params *JobGetAvailableForBatchParams) ([]*rivertype.JobRow, error) {
	if e == nil || e.Executor == nil {
		return nil, errors.New("riverpro driver: nil executor")
	}
	if params == nil || params.Max <= 0 {
		return []*rivertype.JobRow{}, nil
	}
	schema := params.Schema
	batchKey := params.BatchKey
	if batchKey == "" && params.BatchLeaderJobID != 0 {
		if err := e.Executor.QueryRow(ctx, fmt.Sprintf(`SELECT metadata->>'riverpro_batch_key' FROM %s WHERE id = $1`, qt(schema, "river_job")), params.BatchLeaderJobID).Scan(&batchKey); err != nil {
			return nil, fmt.Errorf("riverpro driver: load batch leader %d: %w", params.BatchLeaderJobID, err)
		}
	}
	if batchKey == "" {
		return []*rivertype.JobRow{}, nil
	}
	now := nowUTC()
	return scanJSON[[]*rivertype.JobRow](ctx, e.Executor, fmt.Sprintf(`
		WITH candidates AS MATERIALIZED (
			SELECT * FROM %s
			WHERE state = 'available'::%s
			  AND queue = $1
			  AND kind = $2
			  AND metadata->>'riverpro_batch_key' = $3
			  AND id <> $4
			  AND scheduled_at <= $5
			ORDER BY priority ASC, scheduled_at ASC, id ASC
			LIMIT $6
			FOR UPDATE SKIP LOCKED
		), updated AS (
			UPDATE %s AS j
			SET state = 'running'::%s,
			    attempt = j.attempt + 1,
			    attempted_at = $5,
			    attempted_by = array_append(j.attempted_by, $7)
			FROM candidates
			WHERE j.id = candidates.id
			RETURNING j.*
		)
		SELECT coalesce(json_agg(%s ORDER BY j.priority ASC, j.scheduled_at ASC, j.id ASC), '[]'::json) FROM updated AS j
	`, qt(schema, "river_job"), qt(schema, "river_job_state"), qt(schema, "river_job"), qt(schema, "river_job_state"), jobRowJSONObjectSQL("j")), params.Queue, params.Kind, batchKey, params.BatchLeaderJobID, now, int(params.Max), params.AttemptedBy)
}
func (e *Executor) JobGetAvailablePartitionKeys(ctx context.Context, params *JobGetAvailablePartitionKeysParams) ([]string, error) {
	if e == nil || e.Executor == nil {
		return nil, errors.New("riverpro driver: nil executor")
	}
	if params == nil {
		return []string{}, nil
	}
	jobs, err := e.Executor.JobList(ctx, &riverdriver.JobListParams{Max: 10000, NamedArgs: map[string]any{"queue": params.Queue}, OrderByClause: "kind", Schema: params.Schema, WhereClause: "state in ('available','retryable') AND queue = @queue"})
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	out := []string{}
	for _, j := range jobs {
		key := j.Queue + ":" + j.Kind
		if !seen[key] {
			seen[key] = true
			out = append(out, key)
		}
	}
	return out, nil
}

// ---- Periodic jobs/producers/sequences -------------------------------------------------------

func (e *Executor) PeriodicJobGetAll(ctx context.Context, params *PeriodicJobGetAllParams) ([]*PeriodicJob, error) {
	schema := ""
	max := 10000
	includePaused := false
	var stale time.Time
	if params != nil {
		schema = params.Schema
		max = limitDefault(params.Max, max)
		includePaused = params.IncludePaused
		stale = params.StaleUpdatedAtHorizon
	}
	if dbAvailable(e) {
		clauses := []string{"true"}
		args := []any{max}
		if !stale.IsZero() {
			clauses = append(clauses, fmt.Sprintf("updated_at >= $%d", len(args)+1))
			args = append(args, stale)
		}
		if !includePaused {
			clauses = append(clauses, "paused_at IS NULL")
		}
		where := strings.Join(clauses, " AND ")
		return scanJSON[[]*PeriodicJob](ctx, e.Executor, fmt.Sprintf(`
			SELECT coalesce(json_agg(%s ORDER BY id), '[]'::json)
			FROM (
				SELECT %s
				FROM %s
				WHERE %s
				ORDER BY id
				LIMIT $1
			) p
		`, periodicJobJSONBuildObject, periodicJobSelectColumns, qt(schema, "river_periodic_job"), where), args...)
	}
	compat.Lock()
	defer compat.Unlock()
	out := []*PeriodicJob{}
	for _, j := range compat.periodic[schema] {
		if (includePaused || j.PausedAt == nil) && (stale.IsZero() || !j.UpdatedAt.Before(stale)) {
			c := *j
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if len(out) > max {
		out = out[:max]
	}
	return out, nil
}
func (e *Executor) PeriodicJobGetByID(ctx context.Context, params *PeriodicJobGetByIDParams) (*PeriodicJob, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	if dbAvailable(e) {
		return scanJSON[*PeriodicJob](ctx, e.Executor, fmt.Sprintf(`
			SELECT %s
			FROM %s
			WHERE id = $1
		`, periodicJobJSONBuildObject, qt(params.Schema, "river_periodic_job")), params.ID)
	}
	compat.Lock()
	defer compat.Unlock()
	if p := compat.periodic[params.Schema][params.ID]; p != nil {
		c := *p
		return &c, nil
	}
	return nil, rivertype.ErrNotFound
}

// PeriodicJobInsert creates a durable periodic job and fails when its ID is
// already present.
func (e *Executor) PeriodicJobInsert(ctx context.Context, params *PeriodicJobInsertParams) (*PeriodicJob, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	schema := params.Schema
	now := nowUTC()
	if params.UpdatedAt != nil {
		now = *params.UpdatedAt
	}
	if dbAvailable(e) {
		upsertParams := &PeriodicJobUpsertParams{
			ID: params.ID, NextRunAt: params.NextRunAt, Schema: params.Schema, UpdatedAt: params.UpdatedAt,
			Kind: params.Kind, Args: params.Args, Queue: params.Queue, Priority: params.Priority,
			MaxAttempts: params.MaxAttempts, Tags: params.Tags, CronExpression: params.CronExpression,
			CronTimezone: params.CronTimezone, Paused: params.Paused,
		}
		args, errMsg := periodicJobUpsertArgs(now, upsertParams)
		if errMsg != "" {
			return nil, errors.New(errMsg)
		}
		args = append(args, periodicJobCronTimezoneDefault(params.CronTimezone))
		args = append(args, params.Paused)
		notifyTopic := periodicJobChangeTopic(schema)
		args = append(args, notifyTopic)
		row, err := scanJSON[*PeriodicJob](ctx, e.Executor, fmt.Sprintf(`
			WITH ins AS (
				INSERT INTO %s (id, next_run_at, updated_at, kind, args, queue, priority, max_attempts, tags, cron_expression, cron_timezone, paused_at)
				VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9::text[], $10, $11, CASE WHEN $12::boolean THEN now() ELSE NULL END)
				ON CONFLICT (id) DO NOTHING
				RETURNING %s
			),
			notified AS (
				SELECT pg_notify($13, '') FROM ins
			)
			SELECT %s FROM ins, notified
		`, qt(schema, "river_periodic_job"), periodicJobSelectColumns, periodicJobJSONBuildObject), args...)
		if errors.Is(err, rivertype.ErrNotFound) {
			return nil, ErrPeriodicJobAlreadyExists
		}
		return row, err
	}

	compat.Lock()
	defer compat.Unlock()
	if compat.periodic[schema] == nil {
		compat.periodic[schema] = map[string]*PeriodicJob{}
	}
	if compat.periodic[schema][params.ID] != nil {
		return nil, ErrPeriodicJobAlreadyExists
	}
	job := periodicJobFromInsertParams(params, now)
	compat.periodic[schema][params.ID] = job
	clone := *job
	return &clone, nil
}

// PeriodicJobUpsert reconciles a complete durable periodic job definition.
// next_run_at is preserved when the cron schedule is unchanged unless
// ResetNextRunAt is explicitly set.
func (e *Executor) PeriodicJobUpsert(ctx context.Context, params *PeriodicJobUpsertParams) (*PeriodicJob, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	schema := params.Schema
	now := nowUTC()
	if params.UpdatedAt != nil {
		now = *params.UpdatedAt
	}
	if dbAvailable(e) {
		args, errMsg := periodicJobUpsertArgs(now, params)
		if errMsg != "" {
			return nil, errors.New(errMsg)
		}
		args = append(args, periodicJobCronTimezoneDefault(params.CronTimezone))
		args = append(args, params.Paused)
		args = append(args, params.ResetNextRunAt)
		args = append(args, periodicJobChangeTopic(schema))
		return scanJSON[*PeriodicJob](ctx, e.Executor, fmt.Sprintf(`
			WITH ins AS (
				INSERT INTO %s AS current (id, next_run_at, updated_at, kind, args, queue, priority, max_attempts, tags, cron_expression, cron_timezone, paused_at)
				VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9::text[], $10, $11, CASE WHEN $12::boolean THEN now() ELSE NULL END)
				ON CONFLICT (id) DO UPDATE SET
					next_run_at = CASE
						WHEN $13::boolean
						  OR current.cron_expression IS DISTINCT FROM excluded.cron_expression
						  OR current.cron_timezone IS DISTINCT FROM excluded.cron_timezone
						THEN excluded.next_run_at
						ELSE current.next_run_at
					END,
					updated_at = excluded.updated_at,
					kind = excluded.kind,
					args = excluded.args,
					queue = excluded.queue,
					priority = excluded.priority,
					max_attempts = excluded.max_attempts,
					tags = excluded.tags,
					cron_expression = excluded.cron_expression,
					cron_timezone = excluded.cron_timezone,
					paused_at = CASE WHEN $12::boolean THEN COALESCE(current.paused_at, now()) ELSE NULL END
				RETURNING %s
			),
			notified AS (
				SELECT pg_notify($14, '') FROM ins
			)
			SELECT %s FROM ins, notified
		`, qt(schema, "river_periodic_job"), periodicJobSelectColumns, periodicJobJSONBuildObject), args...)
	}

	compat.Lock()
	defer compat.Unlock()
	if compat.periodic[schema] == nil {
		compat.periodic[schema] = map[string]*PeriodicJob{}
	}
	existing := compat.periodic[schema][params.ID]
	if existing == nil {
		job := periodicJobFromInsertParams(&PeriodicJobInsertParams{
			ID: params.ID, NextRunAt: params.NextRunAt, Schema: params.Schema, UpdatedAt: params.UpdatedAt,
			Kind: params.Kind, Args: params.Args, Queue: params.Queue, Priority: params.Priority,
			MaxAttempts: params.MaxAttempts, Tags: params.Tags, CronExpression: params.CronExpression,
			CronTimezone: params.CronTimezone, Paused: params.Paused,
		}, now)
		compat.periodic[schema][params.ID] = job
		clone := *job
		return &clone, nil
	}
	scheduleChanged := !equalStringPtr(existing.CronExpression, params.CronExpression) ||
		existing.CronTimezone != periodicJobCronTimezoneDefault(params.CronTimezone)
	if params.ResetNextRunAt || scheduleChanged {
		existing.NextRunAt = params.NextRunAt
	}
	existing.UpdatedAt = now
	existing.Kind = params.Kind
	existing.Args = append([]byte(nil), params.Args...)
	existing.Queue = periodicJobQueueDefault(params.Queue)
	existing.Priority = params.Priority
	existing.MaxAttempts = params.MaxAttempts
	existing.Tags = append([]string(nil), params.Tags...)
	existing.CronExpression = cloneStringPtr(params.CronExpression)
	existing.CronTimezone = periodicJobCronTimezoneDefault(params.CronTimezone)
	setPeriodicJobPaused(existing, params.Paused, now)
	clone := *existing
	return &clone, nil
}

// PeriodicJobUpdate patches selected definition fields without changing pause
// state. It returns rivertype.ErrNotFound when the ID does not exist.
func (e *Executor) PeriodicJobUpdate(ctx context.Context, params *PeriodicJobUpdateParams) (*PeriodicJob, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	if params.SetArgs && params.Kind == "" {
		return nil, errors.New("riverpro driver: periodic job kind must be non-empty")
	}
	schema := params.Schema
	if dbAvailable(e) {
		notifyTopic := periodicJobChangeTopic(schema)
		return scanJSON[*PeriodicJob](ctx, e.Executor, fmt.Sprintf(`
			WITH upd AS (
				UPDATE %s
				SET kind = CASE WHEN $2::boolean THEN $3 ELSE kind END,
				    args = CASE WHEN $2::boolean THEN $4::jsonb ELSE args END,
				    queue = CASE WHEN $5::boolean THEN $6 ELSE queue END,
				    priority = CASE WHEN $7::boolean THEN $8 ELSE priority END,
				    max_attempts = CASE WHEN $9::boolean THEN $10 ELSE max_attempts END,
				    tags = CASE WHEN $11::boolean THEN $12::text[] ELSE tags END,
				    next_run_at = CASE WHEN $13::boolean THEN $14 ELSE next_run_at END,
				    cron_expression = CASE WHEN $13::boolean THEN $15 ELSE cron_expression END,
				    cron_timezone = CASE WHEN $13::boolean THEN $16 ELSE cron_timezone END,
				    updated_at = now()
				WHERE id = $1
				RETURNING %s
			),
			notified AS (
				SELECT pg_notify($17, '') FROM upd
			)
			SELECT %s FROM upd, notified
		`, qt(schema, "river_periodic_job"), periodicJobSelectColumns, periodicJobJSONBuildObject),
			params.ID,
			params.SetArgs, params.Kind, nullableJSONBytes(params.Args),
			params.SetQueue, periodicJobQueueDefault(params.Queue),
			params.SetPriority, params.Priority,
			params.SetMaxAttempts, params.MaxAttempts,
			params.SetTags, nonNilTags(params.Tags),
			params.SetSchedule, params.NextRunAt, params.CronExpression, periodicJobCronTimezoneDefault(params.CronTimezone),
			notifyTopic,
		)
	}

	compat.Lock()
	defer compat.Unlock()
	existing := compat.periodic[schema][params.ID]
	if existing == nil {
		return nil, rivertype.ErrNotFound
	}
	if params.SetArgs {
		existing.Kind = params.Kind
		existing.Args = append([]byte(nil), params.Args...)
	}
	if params.SetQueue {
		existing.Queue = periodicJobQueueDefault(params.Queue)
	}
	if params.SetPriority {
		existing.Priority = params.Priority
	}
	if params.SetMaxAttempts {
		existing.MaxAttempts = params.MaxAttempts
	}
	if params.SetTags {
		existing.Tags = append([]string(nil), params.Tags...)
	}
	if params.SetSchedule {
		existing.NextRunAt = params.NextRunAt
		existing.CronExpression = cloneStringPtr(params.CronExpression)
		existing.CronTimezone = periodicJobCronTimezoneDefault(params.CronTimezone)
	}
	existing.UpdatedAt = nowUTC()
	clone := *existing
	return &clone, nil
}

// PeriodicJobPause pauses a durable periodic job without changing its next run.
func (e *Executor) PeriodicJobPause(ctx context.Context, params *PeriodicJobPauseParams) (*PeriodicJob, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	if dbAvailable(e) {
		return scanJSON[*PeriodicJob](ctx, e.Executor, fmt.Sprintf(`
			WITH upd AS (
				UPDATE %s
				SET paused_at = COALESCE(paused_at, now()), updated_at = now()
				WHERE id = $1
				RETURNING %s
			),
			notified AS (SELECT pg_notify($2, '') FROM upd)
			SELECT %s FROM upd, notified
		`, qt(params.Schema, "river_periodic_job"), periodicJobSelectColumns, periodicJobJSONBuildObject), params.ID, periodicJobChangeTopic(params.Schema))
	}
	compat.Lock()
	defer compat.Unlock()
	existing := compat.periodic[params.Schema][params.ID]
	if existing == nil {
		return nil, rivertype.ErrNotFound
	}
	now := nowUTC()
	setPeriodicJobPaused(existing, true, now)
	existing.UpdatedAt = now
	clone := *existing
	return &clone, nil
}

// PeriodicJobResume resumes a durable periodic job without changing its next run.
func (e *Executor) PeriodicJobResume(ctx context.Context, params *PeriodicJobResumeParams) (*PeriodicJob, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	if dbAvailable(e) {
		return scanJSON[*PeriodicJob](ctx, e.Executor, fmt.Sprintf(`
			WITH upd AS (
				UPDATE %s
				SET paused_at = NULL, updated_at = now()
				WHERE id = $1
				RETURNING %s
			),
			notified AS (SELECT pg_notify($2, '') FROM upd)
			SELECT %s FROM upd, notified
		`, qt(params.Schema, "river_periodic_job"), periodicJobSelectColumns, periodicJobJSONBuildObject), params.ID, periodicJobChangeTopic(params.Schema))
	}
	compat.Lock()
	defer compat.Unlock()
	existing := compat.periodic[params.Schema][params.ID]
	if existing == nil {
		return nil, rivertype.ErrNotFound
	}
	existing.PausedAt = nil
	existing.UpdatedAt = nowUTC()
	clone := *existing
	return &clone, nil
}

func periodicJobFromInsertParams(params *PeriodicJobInsertParams, now time.Time) *PeriodicJob {
	job := &PeriodicJob{
		ID: params.ID, CreatedAt: nowUTC(), NextRunAt: params.NextRunAt, UpdatedAt: now,
		Kind: params.Kind, Args: append([]byte(nil), params.Args...), Queue: periodicJobQueueDefault(params.Queue),
		Priority: params.Priority, MaxAttempts: params.MaxAttempts, Tags: append([]string(nil), params.Tags...),
		CronExpression: cloneStringPtr(params.CronExpression), CronTimezone: periodicJobCronTimezoneDefault(params.CronTimezone),
	}
	setPeriodicJobPaused(job, params.Paused, now)
	return job
}

func periodicJobChangeTopic(schema string) string {
	return PeriodicJobChangeTopic(schema)
}

func setPeriodicJobPaused(job *PeriodicJob, paused bool, now time.Time) {
	if paused {
		if job.PausedAt == nil {
			pausedAt := now
			job.PausedAt = &pausedAt
		}
		return
	}
	job.PausedAt = nil
}

func equalStringPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func cloneStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func (e *Executor) PeriodicJobKeepAliveAndReap(ctx context.Context, params *PeriodicJobKeepAliveAndReapParams) ([]*PeriodicJob, error) {
	if params == nil {
		return []*PeriodicJob{}, nil
	}
	schema := params.Schema
	now := nowUTC()
	if params.Now != nil {
		now = *params.Now
	}
	if dbAvailable(e) {
		if len(params.ID) > 0 {
			if err := e.Executor.Exec(ctx, fmt.Sprintf(`UPDATE %s SET updated_at = $1 WHERE id = ANY($2::text[])`, qt(schema, "river_periodic_job")), now, params.ID); err != nil {
				return nil, err
			}
		}
		return scanJSON[[]*PeriodicJob](ctx, e.Executor, fmt.Sprintf(`
			WITH deleted AS (
				DELETE FROM %s
				WHERE NOT (id = ANY($1::text[])) AND ($2::timestamptz IS NOT NULL AND updated_at < $2)
				RETURNING %s
			)
			SELECT coalesce(json_agg(%s ORDER BY id), '[]'::json)
			FROM deleted
		`, qt(schema, "river_periodic_job"), periodicJobSelectColumns, periodicJobJSONBuildObject), params.ID, params.StaleUpdatedAtHorizon)
	}
	compat.Lock()
	defer compat.Unlock()
	if compat.periodic[schema] == nil {
		compat.periodic[schema] = map[string]*PeriodicJob{}
	}
	ids := map[string]bool{}
	for _, id := range params.ID {
		ids[id] = true
		if j := compat.periodic[schema][id]; j != nil {
			j.UpdatedAt = now
		}
	}
	var reaped []*PeriodicJob
	for id, j := range compat.periodic[schema] {
		if !ids[id] && !params.StaleUpdatedAtHorizon.IsZero() && j.UpdatedAt.Before(params.StaleUpdatedAtHorizon) {
			c := *j
			reaped = append(reaped, &c)
			delete(compat.periodic[schema], id)
		}
	}
	return reaped, nil
}
func (e *Executor) PeriodicJobUpsertMany(ctx context.Context, params *PeriodicJobUpsertManyParams) ([]*PeriodicJob, error) {
	if params == nil {
		return []*PeriodicJob{}, nil
	}
	schema := params.Schema
	if dbAvailable(e) {
		out := make([]*PeriodicJob, 0, len(params.Jobs))
		for _, p := range params.Jobs {
			if p == nil {
				continue
			}
			row, err := e.PeriodicJobUpsert(ctx, &PeriodicJobUpsertParams{
				ID: p.ID, NextRunAt: p.NextRunAt, Schema: schema, UpdatedAt: &p.UpdatedAt,
				Kind: p.Kind, Args: p.Args, Queue: p.Queue, Priority: p.Priority,
				MaxAttempts: p.MaxAttempts, Tags: p.Tags, CronExpression: p.CronExpression,
				CronTimezone: p.CronTimezone, ResetNextRunAt: true,
			})
			if err != nil {
				return out, err
			}
			out = append(out, row)
		}
		return out, nil
	}
	compat.Lock()
	defer compat.Unlock()
	if compat.periodic[schema] == nil {
		compat.periodic[schema] = map[string]*PeriodicJob{}
	}
	out := make([]*PeriodicJob, 0, len(params.Jobs))
	for _, p := range params.Jobs {
		if p == nil {
			continue
		}
		existing := compat.periodic[schema][p.ID]
		if existing == nil {
			existing = &PeriodicJob{ID: p.ID, CreatedAt: nowUTC()}
			compat.periodic[schema][p.ID] = existing
		}
		existing.NextRunAt = p.NextRunAt
		existing.UpdatedAt = p.UpdatedAt
		existing.Kind = p.Kind
		existing.Args = append([]byte(nil), p.Args...)
		existing.Queue = periodicJobQueueDefault(p.Queue)
		existing.Priority = p.Priority
		existing.MaxAttempts = p.MaxAttempts
		existing.Tags = append([]string(nil), p.Tags...)
		existing.CronExpression = p.CronExpression
		existing.CronTimezone = periodicJobCronTimezoneDefault(p.CronTimezone)
		c := *existing
		out = append(out, &c)
	}
	return out, nil
}

// PeriodicJobDelete returns the deleted row, or rivertype.ErrNotFound if no
// row matched the id.
func (e *Executor) PeriodicJobDelete(ctx context.Context, params *PeriodicJobDeleteParams) (*PeriodicJob, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	if dbAvailable(e) {
		notifyTopic := periodicJobChangeTopic(params.Schema)
		return scanJSON[*PeriodicJob](ctx, e.Executor, fmt.Sprintf(`
			WITH del AS (
				DELETE FROM %s
				WHERE id = $1
				RETURNING %s
			),
			notified AS (
				SELECT pg_notify($2, '') FROM del
			)
			SELECT %s FROM del, notified
		`, qt(params.Schema, "river_periodic_job"), periodicJobSelectColumns, periodicJobJSONBuildObject), params.ID, notifyTopic)
	}
	compat.Lock()
	defer compat.Unlock()
	if compat.periodic[params.Schema] == nil {
		return nil, rivertype.ErrNotFound
	}
	existing := compat.periodic[params.Schema][params.ID]
	if existing == nil {
		return nil, rivertype.ErrNotFound
	}
	delete(compat.periodic[params.Schema], params.ID)
	c := *existing
	return &c, nil
}

// PeriodicJobEnqueueDue atomically claims due periodic jobs and inserts
// them into river_job. For one-shot rows (no cron_expression) the durable
// row is deleted; for cron rows the row is kept and NextRunAt is set to
// the value computed by the caller (so the driver stays free of a cron
// dependency).
//
// The caller (the Pro enqueuer loop) is expected to hold a leader lock so
// that this method only runs on one client at a time. The per-row
// FOR UPDATE SKIP LOCKED also keeps concurrent callers from picking up
// the same rows.
func (e *Executor) PeriodicJobEnqueueDue(ctx context.Context, params *PeriodicJobEnqueueDueParams) (*PeriodicJobEnqueueDueResult, error) {
	if params == nil {
		return &PeriodicJobEnqueueDueResult{}, nil
	}
	if !dbAvailable(e) {
		// No DB means we can't enqueue — return empty so the loop
		// short-circuits cleanly.
		return &PeriodicJobEnqueueDueResult{}, nil
	}
	schema := params.Schema
	max := limitDefault(params.Max, 100)
	if max < 1 {
		max = 1
	}

	// 1. Claim due rows. Returns the full row + lock for the rest of the
	//    statement chain.
	claimed, err := scanJSON[[]*PeriodicJob](ctx, e.Executor, fmt.Sprintf(`
		WITH claimed AS (
			SELECT %s
			FROM %s
			WHERE paused_at IS NULL
			  AND next_run_at <= now()
			ORDER BY next_run_at, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		SELECT coalesce(json_agg(%s ORDER BY next_run_at, id), '[]'::json)
		FROM claimed
	`, periodicJobSelectColumns, qt(schema, "river_periodic_job"), periodicJobJSONBuildObject), max)
	if err != nil {
		return nil, err
	}
	if len(claimed) == 0 {
		return &PeriodicJobEnqueueDueResult{}, nil
	}

	// 2. Insert one river_job per claimed row. The tag array and queue
	//    fall back to the schema defaults. max_attempts must be > 0 and
	//    priority must be in [1,4] (river_job CHECK constraints);
	//    default to 1 for either if unset.
	inserted := make([]*EnqueuedPeriodicJob, 0, len(claimed))
	for _, c := range claimed {
		maxAttempts := c.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 1
		}
		priority := c.Priority
		if priority <= 0 {
			priority = 1
		}
		args := []any{
			c.Kind,
			nullableJSONBytes(c.Args),
			periodicJobQueueDefault(c.Queue),
			priority,
			maxAttempts,
			nonNilTags(c.Tags),
			c.NextRunAt,
			"{}",
		}
		jobID, err := scanJSON[int64](ctx, e.Executor, fmt.Sprintf(`
			INSERT INTO %s (kind, args, queue, priority, max_attempts, tags, state, scheduled_at, attempt, metadata)
			VALUES ($1, $2::jsonb, $3, $4, $5, $6::text[], 'available', $7, 0, $8::jsonb)
			RETURNING id
		`, qt(schema, "river_job")), args...)
		if err != nil {
			return nil, err
		}
		inserted = append(inserted, &EnqueuedPeriodicJob{
			PeriodicJobID: c.ID,
			JobID:         jobID,
			Kind:          c.Kind,
			ScheduledAt:   c.NextRunAt,
		})
	}

	// 3. Update each row's next_run_at or delete it. The caller passes
	//    a map of id -> next_run_at for cron rows. One-shot rows are
	//    deleted atomically here.
	deletedIDs := []string{}
	for _, c := range claimed {
		if c.CronExpression == nil || *c.CronExpression == "" {
			if err := e.Executor.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, qt(schema, "river_periodic_job")), c.ID); err != nil {
				return nil, err
			}
			deletedIDs = append(deletedIDs, c.ID)
			continue
		}
		next, ok := params.NextRunAt[c.ID]
		if !ok {
			// Caller didn't supply a next tick; leave the row as-is
			// and let the next loop iteration handle it. Bump
			// updated_at so the reaper sees the row as alive.
			if err := e.Executor.Exec(ctx, fmt.Sprintf(`UPDATE %s SET updated_at = now() WHERE id = $1`, qt(schema, "river_periodic_job")), c.ID); err != nil {
				return nil, err
			}
			continue
		}
		if err := e.Executor.Exec(ctx, fmt.Sprintf(`UPDATE %s SET next_run_at = $1, updated_at = now() WHERE id = $2`, qt(schema, "river_periodic_job")), next, c.ID); err != nil {
			return nil, err
		}
	}
	return &PeriodicJobEnqueueDueResult{Inserted: inserted, Deleted: deletedIDs}, nil
}

// nullableJSONBytes returns "{}" for nil/empty args so the INSERT always
// has valid JSON.
func nullableJSONBytes(b []byte) string {
	if len(b) == 0 {
		return "{}"
	}
	return string(b)
}

// nonNilTags returns an empty slice for nil tags so the INSERT always
// has a valid text[] literal.
func nonNilTags(t []string) []string {
	if t == nil {
		return []string{}
	}
	return t
}

func (e *Executor) ProducerDelete(ctx context.Context, params *ProducerDeleteParams) error {
	if params == nil {
		return nil
	}
	if dbAvailable(e) {
		return e.Executor.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, qt(params.Schema, "river_producer")), params.ID)
	}
	compat.Lock()
	defer compat.Unlock()
	delete(compat.producers[params.Schema], params.ID)
	return nil
}
func (e *Executor) ProducerDeleteStale(ctx context.Context, params *ProducerDeleteStaleParams) (int, error) {
	if params == nil {
		return 0, nil
	}
	schema := params.Schema
	max := limitDefault(params.Max, 100)
	if dbAvailable(e) {
		out, err := scanJSON[int](ctx, e.Executor, fmt.Sprintf(`
			WITH candidates AS (
				SELECT id
				FROM %s
				WHERE updated_at < $1
				  AND ($2 = '' OR queue_name = $2)
				ORDER BY updated_at ASC, id ASC
				LIMIT $3
			), deleted AS (
				DELETE FROM %s AS p
				USING candidates
				WHERE p.id = candidates.id
				RETURNING 1
			)
			SELECT count(*)::int FROM deleted
		`, qt(schema, "river_producer"), qt(schema, "river_producer")), params.StaleUpdatedAtHorizon, params.QueueName, max)
		if err != nil {
			return 0, err
		}
		return out, nil
	}
	compat.Lock()
	defer compat.Unlock()
	if compat.producers[schema] == nil {
		return 0, nil
	}
	type row struct {
		updatedAt time.Time
		id        int64
	}
	matches := make([]row, 0, len(compat.producers[schema]))
	for id, p := range compat.producers[schema] {
		if p == nil {
			continue
		}
		if !params.StaleUpdatedAtHorizon.IsZero() && !p.UpdatedAt.Before(params.StaleUpdatedAtHorizon) {
			continue
		}
		if params.QueueName != "" && p.QueueName != params.QueueName {
			continue
		}
		matches = append(matches, row{updatedAt: p.UpdatedAt, id: id})
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].updatedAt.Equal(matches[j].updatedAt) {
			return matches[i].id < matches[j].id
		}
		return matches[i].updatedAt.Before(matches[j].updatedAt)
	})
	if len(matches) > max {
		matches = matches[:max]
	}
	for _, r := range matches {
		delete(compat.producers[schema], r.id)
	}
	return len(matches), nil
}
func (e *Executor) ProducerGetByID(ctx context.Context, params *ProducerGetByIDParams) (*Producer, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	if dbAvailable(e) {
		return scanJSON[*Producer](ctx, e.Executor, fmt.Sprintf(`
			SELECT json_build_object('ID', id, 'ClientID', client_id, 'QueueName', queue_name, 'MaxWorkers', max_workers, 'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'PausedAt', paused_at, 'CreatedAt', created_at, 'UpdatedAt', updated_at)
			FROM %s WHERE id = $1
		`, qt(params.Schema, "river_producer")), params.ID)
	}
	compat.Lock()
	defer compat.Unlock()
	if p := compat.producers[params.Schema][params.ID]; p != nil {
		c := *p
		c.Metadata = append([]byte(nil), p.Metadata...)
		return &c, nil
	}
	return nil, rivertype.ErrNotFound
}
func (e *Executor) QueueGetMetadataForInsert(ctx context.Context, params *QueueGetMetadataForInsertParams) ([]*QueueGetMetadataForInsertResult, error) {
	_ = ctx
	out := []*QueueGetMetadataForInsertResult{}
	if params != nil {
		for _, n := range params.Names {
			out = append(out, &QueueGetMetadataForInsertResult{Name: n, Concurrency: []byte(`{}`)})
		}
	}
	return out, nil
}
func (e *Executor) ProducerInsertOrUpdate(ctx context.Context, params *ProducerInsertOrUpdateParams) (*Producer, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	schema := params.Schema
	now := nowUTC()
	if params.UpdatedAt != nil {
		now = *params.UpdatedAt
	}
	created := now
	if params.CreatedAt != nil {
		created = *params.CreatedAt
	}
	if len(params.Metadata) == 0 {
		params.Metadata = []byte(`{}`)
	}
	if dbAvailable(e) {
		if params.ID != 0 {
			return scanJSON[*Producer](ctx, e.Executor, fmt.Sprintf(`
				INSERT INTO %s (id, client_id, queue_name, max_workers, metadata, paused_at, created_at, updated_at)
				VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8)
				ON CONFLICT (id) DO UPDATE SET client_id = excluded.client_id, queue_name = excluded.queue_name, max_workers = excluded.max_workers, metadata = excluded.metadata, paused_at = excluded.paused_at, updated_at = excluded.updated_at
				RETURNING json_build_object('ID', id, 'ClientID', client_id, 'QueueName', queue_name, 'MaxWorkers', max_workers, 'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'PausedAt', paused_at, 'CreatedAt', created_at, 'UpdatedAt', updated_at)
			`, qt(schema, "river_producer")), params.ID, params.ClientID, params.QueueName, params.MaxWorkers, string(params.Metadata), params.PausedAt, created, now)
		}
		return scanJSON[*Producer](ctx, e.Executor, fmt.Sprintf(`
			INSERT INTO %s (client_id, queue_name, max_workers, metadata, paused_at, created_at, updated_at)
			VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)
			ON CONFLICT (client_id, queue_name) DO UPDATE SET max_workers = excluded.max_workers, metadata = excluded.metadata, paused_at = excluded.paused_at, updated_at = excluded.updated_at
			RETURNING json_build_object('ID', id, 'ClientID', client_id, 'QueueName', queue_name, 'MaxWorkers', max_workers, 'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'PausedAt', paused_at, 'CreatedAt', created_at, 'UpdatedAt', updated_at)
		`, qt(schema, "river_producer")), params.ClientID, params.QueueName, params.MaxWorkers, string(params.Metadata), params.PausedAt, created, now)
	}
	compat.Lock()
	defer compat.Unlock()
	if compat.producers[schema] == nil {
		compat.producers[schema] = map[int64]*Producer{}
	}
	id := params.ID
	if id == 0 {
		compat.producerSeq++
		id = compat.producerSeq
	}
	p := compat.producers[schema][id]
	if p == nil {
		p = &Producer{ID: id, CreatedAt: created}
		compat.producers[schema][id] = p
	}
	p.ClientID = params.ClientID
	p.QueueName = params.QueueName
	p.MaxWorkers = params.MaxWorkers
	p.Metadata = append([]byte(nil), params.Metadata...)
	p.PausedAt = params.PausedAt
	p.UpdatedAt = now
	c := *p
	c.Metadata = append([]byte(nil), p.Metadata...)
	return &c, nil
}
func (e *Executor) ProducerKeepAlive(ctx context.Context, params *ProducerKeepAliveParams) (*Producer, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	if dbAvailable(e) {
		now := nowUTC()
		return scanJSON[*Producer](ctx, e.Executor, fmt.Sprintf(`
			UPDATE %s SET updated_at = $1 WHERE id = $2
			RETURNING json_build_object('ID', id, 'ClientID', client_id, 'QueueName', queue_name, 'MaxWorkers', max_workers, 'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'PausedAt', paused_at, 'CreatedAt', created_at, 'UpdatedAt', updated_at)
		`, qt(params.Schema, "river_producer")), now, params.ID)
	}
	return e.ProducerUpdate(ctx, &ProducerUpdateParams{ID: params.ID, Schema: params.Schema, UpdatedAt: ptrTime(nowUTC())})
}
func ptrTime(t time.Time) *time.Time { return &t }

func dbAvailable(e *Executor) bool { return e != nil && e.Executor != nil }

func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func qt(schema, table string) string {
	if schema != "" {
		return quoteIdentifier(schema) + "." + quoteIdentifier(table)
	}
	return quoteIdentifier(table)
}

func scanJSON[T any](ctx context.Context, exec riverdriver.Executor, query string, args ...any) (T, error) {
	var zero T
	var raw []byte
	if err := exec.QueryRow(ctx, query, args...).Scan(&raw); err != nil {
		return zero, interpretDBError(err)
	}
	if len(raw) == 0 {
		return zero, rivertype.ErrNotFound
	}
	if err := json.Unmarshal(raw, &zero); err != nil {
		return zero, err
	}
	normalizeScannedTimes(any(zero))
	return zero, nil
}

func normalizeTimeUTC(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return t.UTC()
}
func normalizeTimePtrUTC(t **time.Time) {
	if t != nil && *t != nil {
		u := (*t).UTC()
		*t = &u
	}
}
func normalizeScannedTimes(v any) {
	switch x := v.(type) {
	case *Producer:
		x.CreatedAt = normalizeTimeUTC(x.CreatedAt)
		x.UpdatedAt = normalizeTimeUTC(x.UpdatedAt)
		normalizeTimePtrUTC(&x.PausedAt)
	case []*ProducerListByQueueResult:
		for _, it := range x {
			if it != nil && it.Producer != nil {
				normalizeScannedTimes(it.Producer)
			}
		}
	case *Workflow:
		x.CreatedAt = normalizeTimeUTC(x.CreatedAt)
		x.UpdatedAt = normalizeTimeUTC(x.UpdatedAt)
		normalizeTimePtrUTC(&x.FinalizedAt)
	case []*WorkflowListItem:
		for _, it := range x {
			if it != nil {
				it.CreatedAt = normalizeTimeUTC(it.CreatedAt)
			}
		}
	case *WorkflowAttempt:
		x.CreatedAt = normalizeTimeUTC(x.CreatedAt)
	case []*WorkflowAttempt:
		for _, it := range x {
			normalizeScannedTimes(it)
		}
	case *WorkflowAttemptTask:
		normalizeTimePtrUTC(&x.FinalizedAt)
	case []*WorkflowAttemptTask:
		for _, it := range x {
			normalizeScannedTimes(it)
		}
	case *WorkflowSignal:
		x.CreatedAt = normalizeTimeUTC(x.CreatedAt)
	case *WorkflowSignalInsertResult:
		x.CreatedAt = normalizeTimeUTC(x.CreatedAt)
	case []*WorkflowSignal:
		for _, it := range x {
			normalizeScannedTimes(it)
		}
	case *WorkflowTimer:
		x.NextFireAt = normalizeTimeUTC(x.NextFireAt)
	case []*WorkflowTimer:
		for _, it := range x {
			normalizeScannedTimes(it)
		}
	case []*WorkflowTimerNextFireAtByWorkflowIDsRow:
		for _, it := range x {
			if it != nil {
				it.NextFireAt = normalizeTimeUTC(it.NextFireAt)
			}
		}
	case *WorkflowWorklistItem:
		x.CreatedAt = normalizeTimeUTC(x.CreatedAt)
	case []*WorkflowWorklistItem:
		for _, it := range x {
			normalizeScannedTimes(it)
		}
	}
}

func interpretDBError(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "no rows") {
		return rivertype.ErrNotFound
	}
	return err
}
func (e *Executor) ProducerListByQueue(ctx context.Context, params *ProducerListByQueueParams) ([]*ProducerListByQueueResult, error) {
	schema := ""
	q := ""
	if params != nil {
		schema = params.Schema
		q = params.QueueName
	}
	if dbAvailable(e) {
		return scanJSON[[]*ProducerListByQueueResult](ctx, e.Executor, fmt.Sprintf(`
			SELECT coalesce(json_agg(json_build_object('Producer', json_build_object('ID', id, 'ClientID', client_id, 'QueueName', queue_name, 'MaxWorkers', max_workers, 'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'PausedAt', paused_at, 'CreatedAt', created_at, 'UpdatedAt', updated_at), 'Running', 0) ORDER BY id), '[]'::json)
			FROM %s
			WHERE ($1 = '' OR queue_name = $1)
		`, qt(schema, "river_producer")), q)
	}
	compat.Lock()
	defer compat.Unlock()
	out := []*ProducerListByQueueResult{}
	for _, p := range compat.producers[schema] {
		if q == "" || p.QueueName == q {
			c := *p
			c.Metadata = append([]byte(nil), p.Metadata...)
			out = append(out, &ProducerListByQueueResult{Producer: &c})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Producer.ID < out[j].Producer.ID })
	return out, nil
}
func (e *Executor) ProducerUpdate(ctx context.Context, params *ProducerUpdateParams) (*Producer, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	if dbAvailable(e) {
		now := nowUTC()
		if params.UpdatedAt != nil {
			now = *params.UpdatedAt
		}
		return scanJSON[*Producer](ctx, e.Executor, fmt.Sprintf(`
			UPDATE %s SET
				max_workers = CASE WHEN $2 THEN $3 ELSE max_workers END,
				metadata = CASE WHEN $4 THEN $5::jsonb ELSE metadata END,
				paused_at = CASE WHEN $6 THEN $7 ELSE paused_at END,
				updated_at = $8
			WHERE id = $1
			RETURNING json_build_object('ID', id, 'ClientID', client_id, 'QueueName', queue_name, 'MaxWorkers', max_workers, 'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'PausedAt', paused_at, 'CreatedAt', created_at, 'UpdatedAt', updated_at)
		`, qt(params.Schema, "river_producer")), params.ID, params.MaxWorkersDoUpdate, params.MaxWorkers, params.MetadataDoUpdate, string(params.Metadata), params.PausedAtDoUpdate, params.PausedAt, now)
	}
	compat.Lock()
	defer compat.Unlock()
	p := compat.producers[params.Schema][params.ID]
	if p == nil {
		return nil, rivertype.ErrNotFound
	}
	if params.MaxWorkersDoUpdate {
		p.MaxWorkers = params.MaxWorkers
	}
	if params.MetadataDoUpdate {
		p.Metadata = append([]byte(nil), params.Metadata...)
	}
	if params.PausedAtDoUpdate {
		p.PausedAt = params.PausedAt
	}
	if params.UpdatedAt != nil {
		p.UpdatedAt = *params.UpdatedAt
	} else {
		p.UpdatedAt = nowUTC()
	}
	c := *p
	c.Metadata = append([]byte(nil), p.Metadata...)
	return &c, nil
}
func (e *Executor) SequenceAppendMany(ctx context.Context, params *SequenceAppendManyParams) (int, error) {
	if params == nil {
		return 0, nil
	}
	schema := params.Schema
	if dbAvailable(e) {
		return scanJSON[int](ctx, e.Executor, fmt.Sprintf(`
			WITH keys AS (
				SELECT DISTINCT key FROM unnest($1::text[]) AS key WHERE key <> ''
			), inserted AS (
				INSERT INTO %s (key)
				SELECT key FROM keys
				ON CONFLICT (key) DO NOTHING
				RETURNING id
			), inbox AS (
				INSERT INTO %s (key)
				SELECT key FROM unnest($1::text[]) AS key WHERE key <> ''
			)
			SELECT to_json(count(*)::int) FROM inserted
		`, qt(schema, "river_job_sequence"), qt(schema, "river_job_sequence_inbox")), params.SeqKeys)
	}
	compat.Lock()
	defer compat.Unlock()
	if compat.sequences[schema] == nil {
		compat.sequences[schema] = map[string]*Sequence{}
	}
	n := 0
	for _, k := range params.SeqKeys {
		if k == "" {
			continue
		}
		if compat.sequences[schema][k] == nil {
			compat.sequenceSeq++
			compat.sequences[schema][k] = &Sequence{ID: compat.sequenceSeq, Key: k, CreatedAt: nowUTC()}
			n++
		}
	}
	return n, nil
}
func (e *Executor) SequenceList(ctx context.Context, params *SequenceListParams) ([]*Sequence, error) {
	schema := ""
	max := 10000
	if params != nil {
		schema = params.Schema
		max = limitDefault(params.MaxCount, max)
	}
	if dbAvailable(e) {
		return scanJSON[[]*Sequence](ctx, e.Executor, fmt.Sprintf(`
			SELECT coalesce(json_agg(json_build_object('ID', id, 'Key', key, 'CreatedAt', created_at) ORDER BY key), '[]'::json)
			FROM (SELECT id, key, created_at FROM %s ORDER BY key LIMIT $1) s
		`, qt(schema, "river_job_sequence")), max)
	}
	compat.Lock()
	defer compat.Unlock()
	out := []*Sequence{}
	for _, s := range compat.sequences[schema] {
		c := *s
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if len(out) > max {
		out = out[:max]
	}
	return out, nil
}
func (e *Executor) SequencePromote(ctx context.Context, params *SequencePromoteParams) (*SequencePromoteResult, error) {
	res := &SequencePromoteResult{}
	if params == nil || len(params.Keys) == 0 {
		return res, nil
	}
	if dbAvailable(e) {
		schema := params.Schema
		now := nowUTC()
		if params.Now != nil {
			now = *params.Now
		}
		return scanJSON[*SequencePromoteResult](ctx, e.Executor, fmt.Sprintf(`
			WITH requested AS MATERIALIZED (
				SELECT DISTINCT key FROM unnest($1::text[]) AS key
			), present AS MATERIALIZED (
				SELECT r.key, min(inbox.id) AS inbox_id
				FROM requested AS r
				JOIN %s AS s USING (key)
				JOIN %s AS inbox USING (key)
				WHERE NOT EXISTS (
					SELECT 1 FROM %s AS active
					WHERE active.metadata->>'riverpro_sequence_key' = r.key
					  AND active.state IN ('available', 'retryable', 'running', 'scheduled')
				)
				GROUP BY r.key
			), consumed AS (
				DELETE FROM %s AS inbox USING present AS p WHERE inbox.id = p.inbox_id RETURNING inbox.key
			), next_jobs AS MATERIALIZED (
				SELECT p.key, candidate.id
				FROM consumed AS p
				LEFT JOIN LATERAL (
					SELECT j.id FROM %s AS j
					WHERE j.state = 'pending'::%s AND j.metadata->>'riverpro_sequence_key' = p.key
					ORDER BY j.id
					LIMIT 1 FOR UPDATE SKIP LOCKED
				) AS candidate ON true
			), promoted AS (
				UPDATE %s AS j
				SET state = CASE WHEN j.scheduled_at <= $2 THEN 'available'::%s ELSE 'scheduled'::%s END
				FROM next_jobs AS n WHERE j.id = n.id
				RETURNING j.id
			)
			SELECT json_build_object(
				'PromotedKeys', coalesce((SELECT json_agg(key ORDER BY key) FROM consumed), '[]'::json),
				'SkippedKeys', coalesce((SELECT json_agg(r.key ORDER BY r.key) FROM requested AS r LEFT JOIN present AS p USING (key) WHERE p.key IS NULL), '[]'::json)
			)
			FROM (SELECT count(*) FROM promoted) AS promoted_count
		`, qt(schema, "river_job_sequence"), qt(schema, "river_job_sequence_inbox"), qt(schema, "river_job"), qt(schema, "river_job_sequence_inbox"), qt(schema, "river_job"), qt(schema, "river_job_state"), qt(schema, "river_job"), qt(schema, "river_job_state"), qt(schema, "river_job_state")), params.Keys, now)
	}
	res.PromotedKeys = append(res.PromotedKeys, params.Keys...)
	return res, nil
}
func (e *Executor) SequencePromoteFromTable(ctx context.Context, params *SequencePromoteFromTableParams) (*SequencePromoteFromTableResult, error) {
	res := &SequencePromoteFromTableResult{}
	if params == nil {
		return res, nil
	}
	max := limitDefault(params.Max, 100)
	if dbAvailable(e) {
		now := nowUTC()
		if params.Now != nil {
			now = *params.Now
		}
		schema := params.Schema
		return scanJSON[*SequencePromoteFromTableResult](ctx, e.Executor, fmt.Sprintf(`
			WITH eligible_keys AS MATERIALIZED (
				SELECT DISTINCT inbox.key
				FROM %s AS inbox
				WHERE NOT EXISTS (
					SELECT 1 FROM %s AS active
					WHERE active.metadata->>'riverpro_sequence_key' = inbox.key
					  AND active.state IN ('available', 'retryable', 'running', 'scheduled')
				)
			), ranked AS MATERIALIZED (
				SELECT id, key, row_number() OVER (PARTITION BY key ORDER BY id) AS key_row
				FROM %s WHERE key IN (SELECT key FROM eligible_keys)
			), selected AS MATERIALIZED (
				SELECT id, key FROM ranked ORDER BY key_row, key, id LIMIT $1
			), consumed AS MATERIALIZED (
				DELETE FROM %s AS inbox USING selected WHERE inbox.id = selected.id RETURNING inbox.key
			), keys AS MATERIALIZED (
				SELECT DISTINCT key FROM consumed
			), next_jobs AS MATERIALIZED (
				SELECT keys.key, candidate.id
				FROM keys
				LEFT JOIN LATERAL (
					SELECT j.id FROM %s AS j
					WHERE j.state = 'pending'::%s AND j.metadata->>'riverpro_sequence_key' = keys.key
					ORDER BY j.id LIMIT 1 FOR UPDATE SKIP LOCKED
				) AS candidate ON true
			), promoted AS (
				UPDATE %s AS j
				SET state = CASE WHEN j.scheduled_at <= $2 THEN 'available'::%s ELSE 'scheduled'::%s END
				FROM next_jobs WHERE j.id = next_jobs.id RETURNING j.id
			)
			SELECT json_build_object(
				'NumDeleted', (SELECT count(*) FROM consumed),
				'NumPromoted', (SELECT count(*) FROM promoted),
				'Continue', EXISTS (SELECT 1 FROM %s)
			)
		`, qt(schema, "river_job_sequence_inbox"), qt(schema, "river_job"), qt(schema, "river_job_sequence_inbox"), qt(schema, "river_job_sequence_inbox"), qt(schema, "river_job"), qt(schema, "river_job_state"), qt(schema, "river_job"), qt(schema, "river_job_state"), qt(schema, "river_job_state"), qt(schema, "river_job_sequence_inbox")), max, now)
	}
	seqs, err := e.SequenceList(ctx, &SequenceListParams{Schema: params.Schema, MaxCount: max})
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(seqs))
	for _, seq := range seqs {
		keys = append(keys, seq.Key)
	}
	promoted, err := e.SequencePromote(ctx, &SequencePromoteParams{GracePeriod: params.GracePeriod, Keys: keys, Now: params.Now, Schema: params.Schema})
	if err != nil {
		return nil, err
	}
	res.NumPromoted = len(promoted.PromotedKeys)
	res.NumDeleted = len(promoted.PromotedKeys)
	res.Continue = len(seqs) == max
	return res, nil
}
func (e *Executor) SequenceScanAndPromoteStalled(ctx context.Context, params *SequenceScanAndPromoteStalledParams) (*SequenceScanAndPromoteStalledResult, error) {
	res := &SequenceScanAndPromoteStalledResult{}
	if params == nil {
		return res, nil
	}
	max := limitDefault(params.Max, 100)
	seqs, err := e.SequenceList(ctx, &SequenceListParams{Schema: params.Schema, MaxCount: max})
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(seqs))
	for _, seq := range seqs {
		if params.LastSequenceKey != "" && seq.Key <= params.LastSequenceKey {
			continue
		}
		keys = append(keys, seq.Key)
		res.LastSeqKey = seq.Key
	}
	promoted, err := e.SequencePromote(ctx, &SequencePromoteParams{GracePeriod: params.GracePeriod, Keys: keys, Now: params.Now, Schema: params.Schema})
	if err != nil {
		return nil, err
	}
	res.SkippedSeqKeys = append(res.SkippedSeqKeys, promoted.SkippedKeys...)
	res.Continue = len(seqs) == max
	return res, nil
}

// ---- Workflows -------------------------------------------------------------------------------

func (e *Executor) WorkflowAttemptInsert(ctx context.Context, params *WorkflowAttemptInsertParams) (*WorkflowAttempt, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	schema := params.Schema
	if dbAvailable(e) {
		triggered := string(params.TriggeredBy)
		if triggered == "" {
			triggered = "{}"
		}
		return scanJSON[*WorkflowAttempt](ctx, e.Executor, fmt.Sprintf(`
			INSERT INTO %s (workflow_id, attempt, reset_history, retry_mode, triggered_by)
			VALUES ($1, $2, $3, $4, $5::jsonb)
			ON CONFLICT (workflow_id, attempt) DO UPDATE SET reset_history = excluded.reset_history, retry_mode = excluded.retry_mode, triggered_by = excluded.triggered_by
			RETURNING json_build_object('WorkflowID', workflow_id, 'Attempt', attempt, 'ResetHistory', reset_history, 'RetryMode', retry_mode, 'TriggeredBy', encode(convert_to(triggered_by::text, 'UTF8'), 'base64'), 'CreatedAt', created_at)
		`, qt(schema, "river_workflow_attempt")), params.WorkflowID, params.Attempt, params.ResetHistory, params.RetryMode, triggered)
	}
	a := &WorkflowAttempt{Attempt: params.Attempt, CreatedAt: nowUTC(), ResetHistory: params.ResetHistory, RetryMode: params.RetryMode, TriggeredBy: append([]byte(nil), params.TriggeredBy...), WorkflowID: params.WorkflowID}
	compat.Lock()
	defer compat.Unlock()
	if compat.attempts[schema] == nil {
		compat.attempts[schema] = map[string][]*WorkflowAttempt{}
	}
	compat.attempts[schema][params.WorkflowID] = append(compat.attempts[schema][params.WorkflowID], a)
	c := *a
	c.TriggeredBy = append([]byte(nil), a.TriggeredBy...)
	return &c, nil
}
func (e *Executor) WorkflowAttemptListByWorkflowID(ctx context.Context, params *WorkflowAttemptListByWorkflowIDParams) ([]*WorkflowAttempt, error) {
	if params == nil {
		return []*WorkflowAttempt{}, nil
	}
	schema := params.Schema
	if dbAvailable(e) {
		return scanJSON[[]*WorkflowAttempt](ctx, e.Executor, fmt.Sprintf(`
			SELECT coalesce(json_agg(json_build_object('WorkflowID', workflow_id, 'Attempt', attempt, 'ResetHistory', reset_history, 'RetryMode', retry_mode, 'TriggeredBy', encode(convert_to(triggered_by::text, 'UTF8'), 'base64'), 'CreatedAt', created_at) ORDER BY attempt), '[]'::json)
			FROM %s WHERE workflow_id = $1
		`, qt(schema, "river_workflow_attempt")), params.WorkflowID)
	}
	compat.Lock()
	defer compat.Unlock()
	arr := compat.attempts[schema][params.WorkflowID]
	out := make([]*WorkflowAttempt, 0, len(arr))
	for _, a := range arr {
		c := *a
		c.TriggeredBy = append([]byte(nil), a.TriggeredBy...)
		out = append(out, &c)
	}
	return out, nil
}
func (e *Executor) WorkflowAttemptTaskInsert(ctx context.Context, params *WorkflowAttemptTaskInsertParams) (*WorkflowAttemptTask, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	schema := params.Schema
	if dbAvailable(e) {
		meta := string(params.Metadata)
		if meta == "" {
			meta = "{}"
		}
		return scanJSON[*WorkflowAttemptTask](ctx, e.Executor, fmt.Sprintf(`
			INSERT INTO %s (workflow_id, attempt, task, job_id, state, attempt_count, metadata, finalized_at)
			VALUES ($1, $2, $3, $4, $5::%s, $6, $7::jsonb, $8)
			ON CONFLICT (workflow_id, attempt, task) DO UPDATE SET job_id = excluded.job_id, state = excluded.state, attempt_count = excluded.attempt_count, metadata = excluded.metadata, finalized_at = excluded.finalized_at
			RETURNING json_build_object('WorkflowID', workflow_id, 'Attempt', attempt, 'Task', task, 'JobID', job_id, 'State', state, 'AttemptCount', attempt_count, 'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'FinalizedAt', finalized_at, 'Errors', errors)
		`, qt(schema, "river_workflow_attempt_task"), qt(schema, "river_job_state")), params.WorkflowID, params.Attempt, params.Task, params.JobID, params.State, params.AttemptCount, meta, params.FinalizedAt)
	}
	at := &WorkflowAttemptTask{Attempt: params.Attempt, AttemptCount: params.AttemptCount, FinalizedAt: params.FinalizedAt, JobID: params.JobID, Metadata: append([]byte(nil), params.Metadata...), State: params.State, Task: params.Task, WorkflowID: params.WorkflowID}
	compat.Lock()
	defer compat.Unlock()
	if compat.attemptTasks[schema] == nil {
		compat.attemptTasks[schema] = map[string][]*WorkflowAttemptTask{}
	}
	compat.attemptTasks[schema][params.WorkflowID] = append(compat.attemptTasks[schema][params.WorkflowID], at)
	c := *at
	c.Metadata = append([]byte(nil), at.Metadata...)
	return &c, nil
}
func (e *Executor) WorkflowAttemptTaskListByWorkflowID(ctx context.Context, params *WorkflowAttemptTaskListByWorkflowIDParams) ([]*WorkflowAttemptTask, error) {
	if params == nil {
		return []*WorkflowAttemptTask{}, nil
	}
	schema := params.Schema
	if dbAvailable(e) {
		return scanJSON[[]*WorkflowAttemptTask](ctx, e.Executor, fmt.Sprintf(`
			SELECT coalesce(json_agg(json_build_object('WorkflowID', workflow_id, 'Attempt', attempt, 'Task', task, 'JobID', job_id, 'State', state, 'AttemptCount', attempt_count, 'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'FinalizedAt', finalized_at, 'Errors', errors) ORDER BY task), '[]'::json)
			FROM %s WHERE workflow_id = $1 AND ($2::int = 0 OR attempt = $2)
		`, qt(schema, "river_workflow_attempt_task")), params.WorkflowID, params.Attempt)
	}
	compat.Lock()
	defer compat.Unlock()
	arr := compat.attemptTasks[schema][params.WorkflowID]
	out := []*WorkflowAttemptTask{}
	for _, t := range arr {
		if params.Attempt == 0 || t.Attempt == params.Attempt {
			c := *t
			c.Metadata = append([]byte(nil), t.Metadata...)
			out = append(out, &c)
		}
	}
	return out, nil
}
func (e *Executor) WorkflowCancel(ctx context.Context, params *WorkflowCancelParams) ([]*rivertype.JobRow, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	jobs, err := e.allWorkflowJobs(ctx, params.Schema, params.WorkflowID, 10000)
	if err != nil {
		return nil, err
	}
	out := []*rivertype.JobRow{}
	now := params.CancelAttemptedAt
	if now.IsZero() {
		now = nowUTC()
	}
	for _, j := range jobs {
		if activeState(j.State) {
			row, err := e.Executor.JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{ID: j.ID, FinalizedAtDoUpdate: true, FinalizedAt: &now, Schema: params.Schema, StateDoUpdate: true, State: rivertype.JobStateCancelled})
			if err == nil {
				out = append(out, row)
			}
		}
	}
	if dbAvailable(e) {
		err := e.Executor.Exec(ctx, fmt.Sprintf(`UPDATE %s SET state = 'cancelled', finalized_at = $2, updated_at = $2 WHERE id = $1`, qt(params.Schema, "river_workflow")), params.WorkflowID, now)
		if err != nil {
			return out, err
		}
	} else {
		compat.Lock()
		if w := compat.workflows[params.Schema][params.WorkflowID]; w != nil {
			w.State = "cancelled"
			w.FinalizedAt = &now
			w.UpdatedAt = now
		}
		compat.Unlock()
	}
	return out, nil
}
func (e *Executor) WorkflowCancelWithDeletedDepsMany(ctx context.Context, params *WorkflowCancelWithDeletedDepsManyParams) (int64, error) {
	if params == nil {
		return 0, nil
	}
	var n int64
	for _, id := range params.WorkflowIDs {
		rows, err := e.WorkflowCancel(ctx, &WorkflowCancelParams{CancelAttemptedAt: params.WorkflowDepsFailedAt, Schema: params.Schema, WorkflowID: id})
		if err != nil {
			return n, err
		}
		n += int64(len(rows))
	}
	return n, nil
}
func (e *Executor) WorkflowCancelWithFailedDepsMany(ctx context.Context, params *WorkflowCancelWithFailedDepsManyParams) (int64, error) {
	if params == nil {
		return 0, nil
	}
	var n int64
	for _, id := range params.WorkflowIDs {
		rows, err := e.WorkflowCancel(ctx, &WorkflowCancelParams{CancelAttemptedAt: params.WorkflowDepsFailedAt, Schema: params.Schema, WorkflowID: id})
		if err != nil {
			return n, err
		}
		n += int64(len(rows))
	}
	return n, nil
}
func (e *Executor) WorkflowCleanupDeleteAttemptsByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteByWorkflowIDsParams) error {
	_ = ctx
	if params == nil {
		return nil
	}
	compat.Lock()
	defer compat.Unlock()
	m := compat.attempts[params.Schema]
	for _, id := range params.WorkflowIDs {
		delete(m, id)
	}
	return nil
}
func (e *Executor) WorkflowCleanupDeleteAttemptTasksByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteByWorkflowIDsParams) error {
	_ = ctx
	if params == nil {
		return nil
	}
	compat.Lock()
	defer compat.Unlock()
	m := compat.attemptTasks[params.Schema]
	for _, id := range params.WorkflowIDs {
		delete(m, id)
	}
	return nil
}
func (e *Executor) WorkflowCleanupDeleteDeadLetterJobsByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteByWorkflowIDsParams) error {
	return nil
}
func (e *Executor) WorkflowCleanupDeleteJobsByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteByWorkflowIDsParams) error {
	if params == nil {
		return nil
	}
	for _, id := range params.WorkflowIDs {
		jobs, err := e.allWorkflowJobs(ctx, params.Schema, id, 10000)
		if err != nil {
			return err
		}
		ids := []int64{}
		for _, j := range jobs {
			ids = append(ids, j.ID)
		}
		if _, err := e.JobDeleteByIDMany(ctx, &JobDeleteByIDManyParams{ID: ids, Schema: params.Schema}); err != nil {
			return err
		}
	}
	return nil
}
func (e *Executor) WorkflowCleanupDeleteSignalsByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteByWorkflowIDsParams) error {
	_ = ctx
	if params == nil {
		return nil
	}
	compat.Lock()
	defer compat.Unlock()
	m := compat.signals[params.Schema]
	for _, id := range params.WorkflowIDs {
		delete(m, id)
	}
	return nil
}
func (e *Executor) WorkflowCleanupDeleteTimersByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteByWorkflowIDsParams) error {
	if params == nil {
		return nil
	}
	return e.WorkflowTimerDeleteByWorkflowIDs(ctx, &WorkflowTimerDeleteByWorkflowIDsParams{Schema: params.Schema, WorkflowIDs: params.WorkflowIDs})
}
func (e *Executor) WorkflowCleanupDeleteWorkflowsByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteWorkflowsByWorkflowIDsParams) error {
	_ = ctx
	if params == nil {
		return nil
	}
	compat.Lock()
	defer compat.Unlock()
	m := compat.workflows[params.Schema]
	for _, id := range params.WorkflowIDs {
		delete(m, id)
	}
	return nil
}
func (e *Executor) WorkflowCleanupDeleteWorklistByWorkflowIDs(ctx context.Context, params *WorkflowCleanupDeleteByWorkflowIDsParams) error {
	_ = ctx
	if params == nil {
		return nil
	}
	compat.Lock()
	defer compat.Unlock()
	m := compat.worklists[params.Schema]
	for _, id := range params.WorkflowIDs {
		delete(m, id)
	}
	return nil
}
func (e *Executor) WorkflowCleanupListFinalizedIDs(ctx context.Context, params *WorkflowCleanupListFinalizedIDsParams) ([]string, error) {
	schema := ""
	max := 100
	var finalizedBefore time.Time
	state := ""
	if params != nil {
		schema = params.Schema
		max = limitDefault(params.LimitCount, max)
		finalizedBefore = params.FinalizedBefore
		state = params.State
	}
	if dbAvailable(e) {
		return scanJSON[[]string](ctx, e.Executor, fmt.Sprintf(`
			SELECT coalesce(json_agg(id ORDER BY id), '[]'::json)
			FROM (
				SELECT id FROM %s
				WHERE finalized_at IS NOT NULL
				  AND ($1::timestamptz IS NULL OR finalized_at < $1)
				  AND ($2::text = '' OR state = $2)
				ORDER BY id
				LIMIT $3
			) w
		`, qt(schema, "river_workflow")), nullableTime(finalizedBefore), state, max)
	}
	compat.Lock()
	defer compat.Unlock()
	out := []string{}
	for id, w := range compat.workflows[schema] {
		if w.FinalizedAt != nil && (finalizedBefore.IsZero() || w.FinalizedAt.Before(finalizedBefore)) && (state == "" || w.State == state) {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	if len(out) > max {
		out = out[:max]
	}
	return out, nil
}

func (e *Executor) WorkflowCleanupListFinalizedIDsWithoutJobs(ctx context.Context, params *WorkflowCleanupListFinalizedIDsWithoutJobsParams) ([]string, error) {
	schema := ""
	max := 100
	state := ""
	if params != nil {
		schema = params.Schema
		max = limitDefault(params.LimitCount, max)
		state = params.State
	}
	if dbAvailable(e) {
		return scanJSON[[]string](ctx, e.Executor, fmt.Sprintf(`
			SELECT coalesce(json_agg(id ORDER BY id), '[]'::json)
			FROM (
				SELECT w.id
				FROM %[1]s AS w
				WHERE w.finalized_at IS NOT NULL
				  AND ($1::text = '' OR w.state = $1)
				  AND NOT EXISTS (
					SELECT 1 FROM %[2]s AS j WHERE j.metadata ->> 'workflow_id' = w.id
				  )
				  AND NOT EXISTS (
					SELECT 1 FROM %[3]s AS j WHERE j.metadata ->> 'workflow_id' = w.id
				  )
				ORDER BY w.id
				LIMIT $2
			) w
		`, qt(schema, "river_workflow"), qt(schema, "river_job"), qt(schema, "river_job_dead_letter")), state, max)
	}
	compat.Lock()
	defer compat.Unlock()
	out := []string{}
	for id, w := range compat.workflows[schema] {
		if w.FinalizedAt != nil && (state == "" || w.State == state) {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	if len(out) > max {
		out = out[:max]
	}
	return out, nil
}

func (e *Executor) WorkflowCountIncompleteJobs(ctx context.Context, params *WorkflowCountIncompleteJobsParams) (int64, error) {
	if params == nil {
		return 0, nil
	}
	jobs, err := e.allWorkflowJobs(ctx, params.Schema, params.WorkflowID, 10000)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, j := range jobs {
		if activeState(j.State) {
			n++
		}
	}
	return n, nil
}
func (e *Executor) WorkflowFinalizeIfCompleteMany(ctx context.Context, params *WorkflowFinalizeIfCompleteManyParams) ([]string, error) {
	if params == nil {
		return []string{}, nil
	}
	out := []string{}
	for _, id := range params.WorkflowIDs {
		n, err := e.WorkflowCountIncompleteJobs(ctx, &WorkflowCountIncompleteJobsParams{Schema: params.Schema, WorkflowID: id})
		if err != nil {
			return out, err
		}
		if n == 0 {
			out = append(out, id)
			if dbAvailable(e) {
				err := e.Executor.Exec(ctx, fmt.Sprintf(`
					INSERT INTO %s (id, state, finalized_at, updated_at)
					VALUES ($1, 'completed', $2, $2)
					ON CONFLICT (id) DO UPDATE SET state = 'completed', finalized_at = excluded.finalized_at, updated_at = excluded.updated_at
				`, qt(params.Schema, "river_workflow")), id, params.Now)
				if err != nil {
					return out, err
				}
			} else {
				compat.Lock()
				if compat.workflows[params.Schema] == nil {
					compat.workflows[params.Schema] = map[string]*Workflow{}
				}
				w := compat.workflows[params.Schema][id]
				if w == nil {
					w = &Workflow{ID: id, CreatedAt: params.Now, CurrentAttempt: 1}
					compat.workflows[params.Schema][id] = w
				}
				w.State = "completed"
				w.FinalizedAt = &params.Now
				w.UpdatedAt = params.Now
				compat.Unlock()
			}
		}
	}
	return out, nil
}
func (e *Executor) WorkflowGetByID(ctx context.Context, params *WorkflowGetByIDParams) (*Workflow, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	schema := params.Schema
	if dbAvailable(e) {
		return scanJSON[*Workflow](ctx, e.Executor, fmt.Sprintf(`
			SELECT json_build_object('ID', id, 'Name', name, 'State', state, 'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'CurrentAttempt', current_attempt, 'CreatedAt', created_at, 'UpdatedAt', updated_at, 'FinalizedAt', finalized_at, 'WaitEvalCursorJobID', wait_eval_cursor_job_id)
			FROM %s WHERE id = $1
		`, qt(schema, "river_workflow")), params.WorkflowID)
	}
	compat.Lock()
	defer compat.Unlock()
	if w := compat.workflows[schema][params.WorkflowID]; w != nil {
		c := *w
		c.Metadata = append([]byte(nil), w.Metadata...)
		return &c, nil
	}
	return nil, rivertype.ErrNotFound
}
func (e *Executor) WorkflowGetFinalizationCandidates(ctx context.Context, params *WorkflowGetFinalizationCandidatesParams) ([]string, error) {
	if params == nil {
		params = &WorkflowGetFinalizationCandidatesParams{}
	}
	jobs, err := e.allWorkflowJobsAny(ctx, params.Schema, int(limitDefault(int(params.LimitCount), 1000))*20)
	if err != nil {
		return nil, err
	}
	groups := groupWorkflowJobs(jobs)
	out := []string{}
	for id, js := range groups {
		if id > params.AfterWorkflowID {
			done := true
			for _, j := range js {
				if activeState(j.State) {
					done = false
					break
				}
			}
			if done {
				out = append(out, id)
			}
		}
	}
	sort.Strings(out)
	limit := limitDefault(int(params.LimitCount), 100)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (e *Executor) WorkflowGetLegacyBackfillIDs(ctx context.Context, params *WorkflowGetLegacyBackfillIDsParams) ([]string, error) {
	if params == nil {
		return []string{}, nil
	}
	if dbAvailable(e) {
		legacyIndexesPresent, err := scanJSON[bool](ctx, e.Executor, `
			SELECT to_json(EXISTS (
				SELECT 1
				FROM pg_indexes
				WHERE schemaname = coalesce(nullif($1, ''), current_schema())
				  AND tablename = 'river_job'
				  AND indexdef LIKE '%metadata%workflow_id%'
			))
		`, params.Schema)
		if err != nil {
			return nil, err
		}
		if !legacyIndexesPresent {
			return []string{}, nil
		}
		return scanJSON[[]string](ctx, e.Executor, fmt.Sprintf(`
			SELECT coalesce(json_agg(id ORDER BY id), '[]'::json)
			FROM (
				SELECT DISTINCT j.metadata->>'workflow_id' AS id
				FROM %s AS j
				LEFT JOIN %s AS w ON w.id = j.metadata->>'workflow_id'
				WHERE j.metadata ? 'workflow_id'
				  AND j.metadata->>'workflow_id' > $1
				  AND w.id IS NULL
				ORDER BY id
				LIMIT $2
			) AS legacy
		`, qt(params.Schema, "river_job"), qt(params.Schema, "river_workflow")), params.AfterWorkflowID, limitDefault(int(params.LimitCount), 100))
	}
	return []string{}, nil
}
func (e *Executor) WorkflowHasWaitTasksMany(ctx context.Context, params *WorkflowHasWaitTasksManyParams) ([]string, error) {
	if params == nil {
		return []string{}, nil
	}
	out := []string{}
	for _, id := range params.WorkflowIDs {
		jobs, err := e.allWorkflowJobs(ctx, params.Schema, id, 10000)
		if err != nil {
			return out, err
		}
		for _, j := range jobs {
			if len(jobWaitRaw(j)) > 0 {
				out = append(out, id)
				break
			}
		}
	}
	return out, nil
}
func (e *Executor) WorkflowInitFromJobs(ctx context.Context, params *WorkflowInitFromJobsParams) ([]string, error) {
	if params == nil {
		return []string{}, nil
	}
	ids := []string{}
	for _, id := range params.WorkflowIDs {
		jobs, err := e.allWorkflowJobs(ctx, params.Schema, id, 1)
		if err == nil && len(jobs) > 0 {
			ids = append(ids, id)
		}
	}
	if err := e.WorkflowInsertMany(ctx, &WorkflowInsertManyParams{IDs: ids, Schema: params.Schema}); err != nil {
		return ids, err
	}
	return ids, nil
}
func (e *Executor) WorkflowInsertMany(ctx context.Context, params *WorkflowInsertManyParams) error {
	if params == nil {
		return nil
	}
	schema := params.Schema
	if dbAvailable(e) {
		now := nowUTC()
		return e.Executor.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s AS w (id, name, state, current_attempt, created_at, updated_at)
			SELECT ids.id, nullif(names.name, ''), 'active', 1, $3, $3
			FROM unnest($1::text[]) WITH ORDINALITY AS ids(id, ord)
			LEFT JOIN unnest($2::text[]) WITH ORDINALITY AS names(name, ord) USING (ord)
			WHERE ids.id <> ''
			ON CONFLICT (id) DO UPDATE SET name = coalesce(excluded.name, w.name), updated_at = excluded.updated_at
		`, qt(schema, "river_workflow")), params.IDs, params.Names, now)
	}
	compat.Lock()
	defer compat.Unlock()
	if compat.workflows[schema] == nil {
		compat.workflows[schema] = map[string]*Workflow{}
	}
	for i, id := range params.IDs {
		if id == "" {
			continue
		}
		if compat.workflows[schema][id] == nil {
			var name *string
			if i < len(params.Names) && params.Names[i] != "" {
				n := params.Names[i]
				name = &n
			}
			now := nowUTC()
			compat.workflows[schema][id] = &Workflow{CreatedAt: now, CurrentAttempt: 1, ID: id, Name: name, State: "active", UpdatedAt: now}
		}
	}
	return nil
}
func (e *Executor) WorkflowJobGetByTaskName(ctx context.Context, params *WorkflowJobGetByTaskNameParams) (*rivertype.JobRow, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	return e.workflowJobByTask(ctx, params.Schema, params.WorkflowID, params.TaskName)
}
func (e *Executor) WorkflowJobList(ctx context.Context, params *WorkflowJobListParams) ([]*WorkflowTaskWithJob, error) {
	if params == nil {
		return []*WorkflowTaskWithJob{}, nil
	}
	jobs, err := e.allWorkflowJobs(ctx, params.Schema, params.WorkflowID, maxInt(params.PaginationLimit+params.PaginationOffset, 10000))
	if err != nil {
		return nil, err
	}
	start := params.PaginationOffset
	if start > len(jobs) {
		return []*WorkflowTaskWithJob{}, nil
	}
	end := len(jobs)
	if params.PaginationLimit > 0 && start+params.PaginationLimit < end {
		end = start + params.PaginationLimit
	}
	out := []*WorkflowTaskWithJob{}
	for _, j := range jobs[start:end] {
		out = append(out, workflowTaskWithJob(j))
	}
	return out, nil
}
func (e *Executor) WorkflowListActive(ctx context.Context, params *WorkflowListParams) ([]*WorkflowListItem, error) {
	all, err := e.WorkflowListAll(ctx, params)
	if err != nil {
		return nil, err
	}
	out := []*WorkflowListItem{}
	for _, it := range all {
		if it.CountAvailable+it.CountPending+it.CountRetryable+it.CountRunning+it.CountScheduled > 0 {
			out = append(out, it)
		}
	}
	return out, nil
}
func (e *Executor) WorkflowListAll(ctx context.Context, params *WorkflowListParams) ([]*WorkflowListItem, error) {
	schema := ""
	limit := 100
	after := ""
	before := ""
	if params != nil {
		schema = params.Schema
		limit = limitDefault(params.PaginationLimit, limit)
		after = params.After
		before = params.Before
	}
	if dbAvailable(e) {
		return scanJSON[[]*WorkflowListItem](ctx, e.Executor, fmt.Sprintf(`
			SELECT coalesce(json_agg(json_build_object('ID', id, 'Name', name, 'CreatedAt', created_at,
				'CountAvailable', 0, 'CountCancelled', CASE WHEN state='cancelled' THEN 1 ELSE 0 END,
				'CountCompleted', CASE WHEN state='completed' THEN 1 ELSE 0 END,
				'CountDiscarded', CASE WHEN state='discarded' THEN 1 ELSE 0 END,
				'CountFailedDeps', 0, 'CountPending', 0, 'CountRetryable', CASE WHEN state='retryable' THEN 1 ELSE 0 END,
				'CountRunning', 0, 'CountScheduled', CASE WHEN state='active' THEN 1 ELSE 0 END) ORDER BY id), '[]'::json)
			FROM (SELECT * FROM %s WHERE ($1 = '' OR id > $1) AND ($2 = '' OR id < $2) ORDER BY id LIMIT $3) w
		`, qt(schema, "river_workflow")), after, before, limit)
	}
	jobs, err := e.allWorkflowJobsAny(ctx, schema, limit*50)
	if err != nil {
		return nil, err
	}
	groups := groupWorkflowJobs(jobs)
	ids := make([]string, 0, len(groups))
	for id := range groups {
		if after != "" && id <= after {
			continue
		}
		if before != "" && id >= before {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	out := []*WorkflowListItem{}
	for _, id := range ids {
		out = append(out, workflowListItemFromJobs(id, groups[id]))
	}
	return out, nil
}
func (e *Executor) WorkflowListByIDs(ctx context.Context, params *WorkflowListByIDsParams) ([]string, error) {
	if params == nil {
		return []string{}, nil
	}
	schema := params.Schema
	if dbAvailable(e) {
		return scanJSON[[]string](ctx, e.Executor, fmt.Sprintf(`SELECT coalesce(json_agg(id ORDER BY id), '[]'::json) FROM %s WHERE id = ANY($1::text[])`, qt(schema, "river_workflow")), params.WorkflowIDs)
	}
	compat.Lock()
	defer compat.Unlock()
	m := compat.workflows[schema]
	out := []string{}
	for _, id := range params.WorkflowIDs {
		if m[id] != nil {
			out = append(out, id)
		}
	}
	return out, nil
}
func (e *Executor) WorkflowListByIDsForWaitEval(ctx context.Context, params *WorkflowListByIDsForWaitEvalParams) ([]*WorkflowWaitWorkflow, error) {
	if params == nil {
		return []*WorkflowWaitWorkflow{}, nil
	}
	out := []*WorkflowWaitWorkflow{}
	for _, id := range params.WorkflowIDs {
		w, err := e.WorkflowGetByID(ctx, &WorkflowGetByIDParams{Schema: params.Schema, WorkflowID: id})
		if err == nil {
			out = append(out, &WorkflowWaitWorkflow{CreatedAt: w.CreatedAt, CurrentAttempt: w.CurrentAttempt, ID: w.ID, Metadata: append([]byte(nil), w.Metadata...)})
		}
	}
	return out, nil
}
func (e *Executor) WorkflowListInactive(ctx context.Context, params *WorkflowListParams) ([]*WorkflowListItem, error) {
	all, err := e.WorkflowListAll(ctx, params)
	if err != nil {
		return nil, err
	}
	out := []*WorkflowListItem{}
	for _, it := range all {
		if it.CountAvailable+it.CountPending+it.CountRetryable+it.CountRunning+it.CountScheduled == 0 {
			out = append(out, it)
		}
	}
	return out, nil
}
func (e *Executor) WorkflowLoadDepTasksAndIDs(ctx context.Context, params *WorkflowLoadDepTasksAndIDsParams) (map[string]*int64, error) {
	if params == nil {
		return map[string]*int64{}, nil
	}
	jobs, err := e.allWorkflowJobs(ctx, params.Schema, params.WorkflowID, 10000)
	if err != nil {
		return nil, err
	}
	by := taskMap(jobs)
	root := by[params.Task]
	out := map[string]*int64{}
	var visit func(string)
	visit = func(t string) {
		j := by[t]
		if j == nil {
			out[t] = nil
		} else {
			id := j.ID
			out[t] = &id
			if params.Recursive {
				for _, d := range jobDeps(j) {
					visit(d)
				}
			}
		}
	}
	if root != nil {
		for _, d := range jobDeps(root) {
			visit(d)
		}
	}
	return out, nil
}
func (e *Executor) WorkflowLoadJobsWithDeps(ctx context.Context, params *WorkflowLoadJobsWithDepsParams) ([]*WorkflowTaskWithJob, error) {
	if params == nil {
		return []*WorkflowTaskWithJob{}, nil
	}
	jobs, err := e.workflowJobsByIDs(ctx, params.Schema, params.JobIds)
	if err != nil {
		return nil, err
	}
	out := []*WorkflowTaskWithJob{}
	for _, j := range jobs {
		out = append(out, workflowTaskWithJob(j))
	}
	return out, nil
}
func (e *Executor) WorkflowLoadTaskWithDeps(ctx context.Context, params *WorkflowLoadTaskWithDepsParams) (*WorkflowTaskWithJob, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	j, err := e.workflowJobByTask(ctx, params.Schema, params.WorkflowID, params.Task)
	if err != nil {
		return nil, err
	}
	return workflowTaskWithJob(j), nil
}
func (e *Executor) WorkflowLoadTaskNamesByWorkflowID(ctx context.Context, params *WorkflowLoadTaskNamesByWorkflowIDParams) ([]string, error) {
	if params == nil {
		return []string{}, nil
	}
	jobs, err := e.allWorkflowJobs(ctx, params.Schema, params.WorkflowID, 10000)
	if err != nil {
		return nil, err
	}
	out := []string{}
	for _, j := range jobs {
		if t := jobTask(j); t != "" {
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out, nil
}
func (e *Executor) WorkflowLoadTasksByNames(ctx context.Context, params *WorkflowLoadTasksByNamesParams) ([]*WorkflowTask, error) {
	if params == nil {
		return []*WorkflowTask{}, nil
	}
	names := map[string]bool{}
	for _, n := range params.TaskNames {
		names[n] = true
	}
	jobs, err := e.allWorkflowJobs(ctx, params.Schema, params.WorkflowID, 10000)
	if err != nil {
		return nil, err
	}
	out := []*WorkflowTask{}
	for _, j := range jobs {
		if len(names) == 0 || names[jobTask(j)] {
			out = append(out, workflowTask(j))
		}
	}
	return out, nil
}
func (e *Executor) WorkflowLockByIDsSkipLocked(ctx context.Context, params *WorkflowLockByIDsSkipLockedParams) ([]string, error) {
	_ = ctx
	if params == nil {
		return []string{}, nil
	}
	limit := limitDefault(params.LimitCount, len(params.WorkflowIDs))
	out := append([]string(nil), params.WorkflowIDs...)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (e *Executor) WorkflowReadyTaskIDsByWorkflowIDs(ctx context.Context, params *WorkflowReadyTaskIDsByWorkflowIDsParams) ([]*WorkflowReadyTaskIDsByWorkflowIDsRow, error) {
	if params == nil {
		return []*WorkflowReadyTaskIDsByWorkflowIDsRow{}, nil
	}
	limit := limitDefault(params.LimitCount, 1000)
	out := []*WorkflowReadyTaskIDsByWorkflowIDsRow{}
	for _, id := range params.WorkflowIDs {
		jobs, err := e.allWorkflowJobs(ctx, params.Schema, id, 10000)
		if err != nil {
			return nil, err
		}
		by := taskMap(jobs)
		var count int64
		for _, j := range jobs {
			if readyWorkflowJob(j, by) {
				count++
				if len(out) < limit {
					out = append(out, &WorkflowReadyTaskIDsByWorkflowIDsRow{ID: j.ID, WorkflowID: id})
				}
			}
		}
		for _, r := range out {
			if r.WorkflowID == id {
				r.TotalCount = count
			}
		}
	}
	return out, nil
}
func (e *Executor) WorkflowRetry(ctx context.Context, params *WorkflowRetryParams) ([]*rivertype.JobRow, error) {
	if params == nil {
		return []*rivertype.JobRow{}, nil
	}
	jobs, err := e.allWorkflowJobs(ctx, params.Schema, params.WorkflowID, 10000)
	if err != nil {
		return nil, err
	}
	out := []*rivertype.JobRow{}
	now := params.Now
	if now.IsZero() {
		now = nowUTC()
	}
	for _, j := range jobs {
		switch params.Mode {
		case WorkflowRetryModeFailedOnly:
			if j.State != rivertype.JobStateDiscarded && j.State != rivertype.JobStateCancelled {
				continue
			}
		case WorkflowRetryModeFailedAndDownstream:
			if j.State == rivertype.JobStateCompleted {
				continue
			}
		}
		if finalizedState(j.State) {
			r, err := e.Executor.JobRetry(ctx, &riverdriver.JobRetryParams{ID: j.ID, Now: &now, Schema: params.Schema})
			if err == nil {
				out = append(out, r)
			}
		} else if j.State == rivertype.JobStatePending {
			r, err := e.WorkflowStageJobsByIDMany(ctx, &WorkflowStageJobsByIDManyParams{JobIDs: []int64{j.ID}, Schema: params.Schema, WorkflowStagedAt: now})
			if err == nil {
				out = append(out, r...)
			}
		}
	}
	return out, nil
}
func (e *Executor) WorkflowRetryLockAndCheckRunning(ctx context.Context, params *WorkflowRetryLockAndCheckRunningParams) (*WorkflowRetryLockAndCheckRunningResult, error) {
	if params == nil {
		return &WorkflowRetryLockAndCheckRunningResult{}, nil
	}
	n, err := e.WorkflowCountIncompleteJobs(ctx, &WorkflowCountIncompleteJobsParams{Schema: params.Schema, WorkflowID: params.WorkflowID})
	if err != nil {
		return nil, err
	}
	return &WorkflowRetryLockAndCheckRunningResult{WorkflowIsActive: n > 0}, nil
}

// ---- Signals/timers/wait/worklist ------------------------------------------------------------

func (e *Executor) WorkflowSignalInsert(ctx context.Context, params *WorkflowSignalInsertParams) (*WorkflowSignalInsertResult, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	schema := params.Schema
	attempt := 1
	if dbAvailable(e) {
		cur, err := scanJSON[int](ctx, e.Executor, fmt.Sprintf(`SELECT to_json(current_attempt) FROM %s WHERE id = $1`, qt(schema, "river_workflow")), params.WorkflowID)
		if err == nil {
			attempt = cur
		} else if err != rivertype.ErrNotFound {
			return nil, err
		}
		if params.RequestedAttempt != nil {
			attempt = *params.RequestedAttempt
		}
		payload := string(params.Payload)
		if payload == "" {
			payload = "{}"
		}
		metadata := string(params.Metadata)
		if metadata == "" {
			metadata = "{}"
		}
		source := string(params.Source)
		if source == "" {
			source = "{}"
		}
		if params.IdempotencyKey != "" {
			existing, err := scanJSON[*WorkflowSignalInsertResult](ctx, e.Executor, fmt.Sprintf(`
				SELECT json_build_object('ID', id, 'WorkflowID', workflow_id, 'Attempt', attempt, 'Key', key, 'IdempotencyKey', idempotency_key,
					'Payload', encode(convert_to(payload::text, 'UTF8'), 'base64'), 'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'Source', encode(convert_to(source::text, 'UTF8'), 'base64'),
					'CreatedAt', created_at, 'CurrentAttempt', $4::int, 'PayloadSemanticEqual', payload = $5::jsonb, 'SignalPresent', true, 'SkippedAsDuplicate', true)
				FROM %s WHERE workflow_id = $1 AND attempt = $2 AND idempotency_key = $3
			`, qt(schema, "river_workflow_signal")), params.WorkflowID, attempt, params.IdempotencyKey, cur, payload)
			if err == nil {
				return existing, nil
			}
			if err != rivertype.ErrNotFound {
				return nil, err
			}
		}
		res, err := scanJSON[*WorkflowSignalInsertResult](ctx, e.Executor, fmt.Sprintf(`
			INSERT INTO %s (workflow_id, attempt, key, idempotency_key, payload, metadata, source)
			VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb)
			RETURNING json_build_object('ID', id, 'WorkflowID', workflow_id, 'Attempt', attempt, 'Key', key, 'IdempotencyKey', idempotency_key,
				'Payload', encode(convert_to(payload::text, 'UTF8'), 'base64'), 'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'Source', encode(convert_to(source::text, 'UTF8'), 'base64'),
				'CreatedAt', created_at, 'CurrentAttempt', $8::int, 'PayloadSemanticEqual', true, 'SignalPresent', true, 'SkippedAsDuplicate', false)
		`, qt(schema, "river_workflow_signal")), params.WorkflowID, attempt, params.Key, params.IdempotencyKey, payload, metadata, source, cur)
		return res, err
	}
	if params.RequestedAttempt != nil {
		attempt = *params.RequestedAttempt
	}
	compat.Lock()
	defer compat.Unlock()
	if compat.signals[schema] == nil {
		compat.signals[schema] = map[string][]*WorkflowSignal{}
	}
	if params.IdempotencyKey != "" {
		for _, s := range compat.signals[schema][params.WorkflowID] {
			if s.IdempotencyKey == params.IdempotencyKey && s.Key == params.Key {
				c := *s
				return &WorkflowSignalInsertResult{WorkflowSignal: c, CurrentAttempt: attempt, PayloadSemanticEqual: string(s.Payload) == string(params.Payload), SignalPresent: true, SkippedAsDuplicate: true}, nil
			}
		}
	}
	compat.signalSeq++
	sig := &WorkflowSignal{Attempt: attempt, CreatedAt: nowUTC(), ID: compat.signalSeq, IdempotencyKey: params.IdempotencyKey, Key: params.Key, Metadata: append([]byte(nil), params.Metadata...), Payload: append([]byte(nil), params.Payload...), Source: append([]byte(nil), params.Source...), WorkflowID: params.WorkflowID}
	compat.signals[schema][params.WorkflowID] = append(compat.signals[schema][params.WorkflowID], sig)
	c := *sig
	return &WorkflowSignalInsertResult{WorkflowSignal: c, CurrentAttempt: attempt, PayloadSemanticEqual: true, SignalPresent: true}, nil
}

func filterSignals(in []*WorkflowSignal, attempt *int, cursor *int64, desc bool, keys map[string]bool, limit int) []*WorkflowSignal {
	out := []*WorkflowSignal{}
	for _, s := range in {
		if attempt != nil && s.Attempt != *attempt {
			continue
		}
		if cursor != nil {
			if desc && s.ID >= *cursor {
				continue
			}
			if !desc && s.ID <= *cursor {
				continue
			}
		}
		if len(keys) > 0 && !keys[s.Key] {
			continue
		}
		c := *s
		c.Metadata = append([]byte(nil), s.Metadata...)
		c.Payload = append([]byte(nil), s.Payload...)
		c.Source = append([]byte(nil), s.Source...)
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool {
		if desc {
			return out[i].ID > out[j].ID
		}
		return out[i].ID < out[j].ID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}
func keySet(keys []string) map[string]bool {
	m := map[string]bool{}
	for _, k := range keys {
		if k != "" {
			m[k] = true
		}
	}
	return m
}

func (e *Executor) WorkflowSignalList(ctx context.Context, params *WorkflowSignalListParams) ([]*WorkflowSignal, error) {
	if params == nil {
		return []*WorkflowSignal{}, nil
	}
	schema := params.Schema
	keys := map[string]bool{}
	var key string
	if params.Key != nil {
		keys[*params.Key] = true
		key = *params.Key
	}
	if dbAvailable(e) {
		attempt := 0
		if params.Attempt != nil {
			attempt = *params.Attempt
		}
		cursor := int64(0)
		if params.CursorID != nil {
			cursor = *params.CursorID
		}
		limit := limitDefault(params.LimitCount, 100)
		op := ">"
		order := "ASC"
		if params.Desc {
			op = "<"
			order = "DESC"
		}
		return scanJSON[[]*WorkflowSignal](ctx, e.Executor, fmt.Sprintf(`
			SELECT coalesce(json_agg(json_build_object('ID', id, 'WorkflowID', workflow_id, 'Attempt', attempt, 'Key', key, 'IdempotencyKey', idempotency_key,
				'Payload', encode(convert_to(payload::text, 'UTF8'), 'base64'), 'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'Source', encode(convert_to(source::text, 'UTF8'), 'base64'), 'CreatedAt', created_at) ORDER BY id %s), '[]'::json)
			FROM (SELECT * FROM %s WHERE workflow_id = $1 AND ($2::int = 0 OR attempt = $2) AND ($3 = '' OR key = $3) AND ($4::bigint = 0 OR id %s $4) ORDER BY id %s LIMIT $5) s
		`, order, qt(schema, "river_workflow_signal"), op, order), params.WorkflowID, attempt, key, cursor, limit)
	}
	compat.Lock()
	defer compat.Unlock()
	return filterSignals(compat.signals[schema][params.WorkflowID], params.Attempt, params.CursorID, params.Desc, keys, limitDefault(params.LimitCount, 100)), nil
}
func (e *Executor) WorkflowSignalListByEvidence(ctx context.Context, params *WorkflowSignalListByEvidenceParams) ([]*WorkflowSignal, error) {
	if params == nil {
		return []*WorkflowSignal{}, nil
	}
	a := params.Attempt
	return e.WorkflowSignalListByKeys(ctx, &WorkflowSignalListByKeysParams{Attempt: &a, CursorID: params.CursorID, Desc: params.Desc, Keys: params.Keys, LimitCount: params.LimitCount, Schema: params.Schema, WorkflowID: params.WorkflowID})
}
func (e *Executor) WorkflowSignalListByKeys(ctx context.Context, params *WorkflowSignalListByKeysParams) ([]*WorkflowSignal, error) {
	if params == nil {
		return []*WorkflowSignal{}, nil
	}
	schema := params.Schema
	if dbAvailable(e) {
		attempt := 0
		if params.Attempt != nil {
			attempt = *params.Attempt
		}
		cursor := int64(0)
		if params.CursorID != nil {
			cursor = *params.CursorID
		}
		limit := limitDefault(params.LimitCount, 100)
		op := ">"
		order := "ASC"
		if params.Desc {
			op = "<"
			order = "DESC"
		}
		return scanJSON[[]*WorkflowSignal](ctx, e.Executor, fmt.Sprintf(`
			SELECT coalesce(json_agg(json_build_object('ID', id, 'WorkflowID', workflow_id, 'Attempt', attempt, 'Key', key, 'IdempotencyKey', idempotency_key,
				'Payload', encode(convert_to(payload::text, 'UTF8'), 'base64'), 'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'Source', encode(convert_to(source::text, 'UTF8'), 'base64'), 'CreatedAt', created_at) ORDER BY id %s), '[]'::json)
			FROM (SELECT * FROM %s WHERE workflow_id = $1 AND ($2::int = 0 OR attempt = $2) AND (cardinality($3::text[]) = 0 OR key = ANY($3::text[])) AND ($4::bigint = 0 OR id %s $4) ORDER BY id %s LIMIT $5) s
		`, order, qt(schema, "river_workflow_signal"), op, order), params.WorkflowID, attempt, params.Keys, cursor, limit)
	}
	compat.Lock()
	defer compat.Unlock()
	return filterSignals(compat.signals[schema][params.WorkflowID], params.Attempt, params.CursorID, params.Desc, keySet(params.Keys), limitDefault(params.LimitCount, 100)), nil
}
func (e *Executor) WorkflowSignalListByWorkflowIDs(ctx context.Context, params *WorkflowSignalListByWorkflowIDsParams) ([]*WorkflowSignal, error) {
	if params == nil {
		return []*WorkflowSignal{}, nil
	}
	schema := params.Schema
	if dbAvailable(e) {
		return scanJSON[[]*WorkflowSignal](ctx, e.Executor, fmt.Sprintf(`
			SELECT coalesce(json_agg(json_build_object('ID', id, 'WorkflowID', workflow_id, 'Attempt', attempt, 'Key', key, 'IdempotencyKey', idempotency_key,
				'Payload', encode(convert_to(payload::text, 'UTF8'), 'base64'), 'Metadata', encode(convert_to(metadata::text, 'UTF8'), 'base64'), 'Source', encode(convert_to(source::text, 'UTF8'), 'base64'), 'CreatedAt', created_at) ORDER BY workflow_id, id), '[]'::json)
			FROM %s WHERE workflow_id = ANY($1::text[]) AND ($2::int = 0 OR attempt = $2) AND (cardinality($3::text[]) = 0 OR key = ANY($3::text[]))
		`, qt(schema, "river_workflow_signal")), params.WorkflowIDs, params.Attempt, params.Keys)
	}
	a := params.Attempt
	out := []*WorkflowSignal{}
	compat.Lock()
	defer compat.Unlock()
	for _, id := range params.WorkflowIDs {
		out = append(out, filterSignals(compat.signals[schema][id], &a, nil, false, keySet(params.Keys), 0)...)
	}
	return out, nil
}
func (e *Executor) WorkflowSignalStatsByWorkflowIDs(ctx context.Context, params *WorkflowSignalStatsByWorkflowIDsParams) ([]*WorkflowSignalStat, error) {
	if params == nil {
		return []*WorkflowSignalStat{}, nil
	}
	sigs, err := e.WorkflowSignalListByWorkflowIDs(ctx, &WorkflowSignalListByWorkflowIDsParams{Attempt: params.Attempt, Keys: params.Keys, Schema: params.Schema, WorkflowIDs: params.WorkflowIDs})
	if err != nil {
		return nil, err
	}
	m := map[string]*WorkflowSignalStat{}
	for _, s := range sigs {
		k := s.WorkflowID + "\x00" + s.Key
		st := m[k]
		if st == nil {
			st = &WorkflowSignalStat{Key: s.Key, WorkflowID: s.WorkflowID}
			m[k] = st
		}
		st.SignalCount++
		if s.ID > st.LastSignalID {
			st.LastSignalID = s.ID
		}
	}
	out := []*WorkflowSignalStat{}
	for _, v := range m {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].WorkflowID == out[j].WorkflowID {
			return out[i].Key < out[j].Key
		}
		return out[i].WorkflowID < out[j].WorkflowID
	})
	return out, nil
}
func (e *Executor) WorkflowStageJobsByIDMany(ctx context.Context, params *WorkflowStageJobsByIDManyParams) ([]*rivertype.JobRow, error) {
	if params == nil || len(params.JobIDs) == 0 {
		return []*rivertype.JobRow{}, nil
	}
	if dbAvailable(e) {
		return scanJSON[[]*rivertype.JobRow](ctx, e.Executor, fmt.Sprintf(`
			WITH updated AS (
				UPDATE %s SET state = 'available'::%s, scheduled_at = $2
				WHERE id = ANY($1::bigint[]) AND state = 'pending'::%s
				RETURNING *
			)
			SELECT coalesce(json_agg(%s ORDER BY j.id), '[]'::json) FROM updated AS j
		`, qt(params.Schema, "river_job"), qt(params.Schema, "river_job_state"), qt(params.Schema, "river_job_state"), jobRowJSONObjectSQL("j")), params.JobIDs, params.WorkflowStagedAt)
	}
	out := []*rivertype.JobRow{}
	for _, id := range params.JobIDs {
		r, err := e.Executor.JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{ID: id, Schema: params.Schema, StateDoUpdate: true, State: rivertype.JobStateAvailable})
		if err == nil {
			out = append(out, r)
		} else if err != rivertype.ErrNotFound {
			return out, err
		}
	}
	return out, nil
}
func (e *Executor) WorkflowTimerConsumeDue(ctx context.Context, params *WorkflowTimerConsumeDueParams) ([]*WorkflowTimer, error) {
	if params == nil {
		return []*WorkflowTimer{}, nil
	}
	schema := params.Schema
	limit := limitDefault(params.LimitCount, 100)
	if dbAvailable(e) {
		return scanJSON[[]*WorkflowTimer](ctx, e.Executor, fmt.Sprintf(`
			WITH due AS (
				DELETE FROM %s WHERE workflow_id IN (SELECT workflow_id FROM %s WHERE next_fire_at <= $1 ORDER BY next_fire_at, workflow_id LIMIT $2)
				RETURNING workflow_id, next_fire_at
			)
			SELECT coalesce(json_agg(json_build_object('WorkflowID', workflow_id, 'NextFireAt', next_fire_at) ORDER BY next_fire_at, workflow_id), '[]'::json) FROM due
		`, qt(schema, "river_workflow_timer"), qt(schema, "river_workflow_timer")), params.AsOf, limit)
	}
	compat.Lock()
	defer compat.Unlock()
	out := []*WorkflowTimer{}
	for id, t := range compat.timers[schema] {
		if !t.NextFireAt.After(params.AsOf) {
			c := *t
			out = append(out, &c)
			delete(compat.timers[schema], id)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}
func (e *Executor) WorkflowTimerDeleteByWorkflowIDs(ctx context.Context, params *WorkflowTimerDeleteByWorkflowIDsParams) error {
	if params == nil {
		return nil
	}
	schema := params.Schema
	if dbAvailable(e) {
		return e.Executor.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE workflow_id = ANY($1::text[])`, qt(schema, "river_workflow_timer")), params.WorkflowIDs)
	}
	compat.Lock()
	defer compat.Unlock()
	m := compat.timers[schema]
	for _, id := range params.WorkflowIDs {
		delete(m, id)
	}
	return nil
}
func (e *Executor) WorkflowTimerGetByWorkflowID(ctx context.Context, params *WorkflowTimerGetByWorkflowIDParams) (*WorkflowTimer, error) {
	if params == nil {
		return nil, errors.New("riverpro driver: nil params")
	}
	schema := params.Schema
	if dbAvailable(e) {
		return scanJSON[*WorkflowTimer](ctx, e.Executor, fmt.Sprintf(`SELECT json_build_object('WorkflowID', workflow_id, 'NextFireAt', next_fire_at) FROM %s WHERE workflow_id = $1`, qt(schema, "river_workflow_timer")), params.WorkflowID)
	}
	compat.Lock()
	defer compat.Unlock()
	if t := compat.timers[schema][params.WorkflowID]; t != nil {
		c := *t
		return &c, nil
	}
	return nil, rivertype.ErrNotFound
}
func (e *Executor) WorkflowTimerNextFireAtByWorkflowIDs(ctx context.Context, params *WorkflowTimerNextFireAtByWorkflowIDsParams) ([]*WorkflowTimerNextFireAtByWorkflowIDsRow, error) {
	if params == nil {
		return []*WorkflowTimerNextFireAtByWorkflowIDsRow{}, nil
	}
	schema := params.Schema
	if dbAvailable(e) {
		return scanJSON[[]*WorkflowTimerNextFireAtByWorkflowIDsRow](ctx, e.Executor, fmt.Sprintf(`
			SELECT coalesce(json_agg(json_build_object('WorkflowID', workflow_id, 'NextFireAt', next_fire_at) ORDER BY workflow_id), '[]'::json)
			FROM %s WHERE workflow_id = ANY($1::text[]) AND ($2::timestamptz = '0001-01-01 00:00:00+00'::timestamptz OR next_fire_at > $2)
		`, qt(schema, "river_workflow_timer")), params.WorkflowIDs, params.Now)
	}
	compat.Lock()
	defer compat.Unlock()
	out := []*WorkflowTimerNextFireAtByWorkflowIDsRow{}
	for _, id := range params.WorkflowIDs {
		if t := compat.timers[schema][id]; t != nil && (params.Now.IsZero() || t.NextFireAt.After(params.Now)) {
			out = append(out, &WorkflowTimerNextFireAtByWorkflowIDsRow{NextFireAt: t.NextFireAt, WorkflowID: id})
		}
	}
	return out, nil
}
func (e *Executor) WorkflowTimerUpsertMany(ctx context.Context, params *WorkflowTimerUpsertManyParams) error {
	if params == nil {
		return nil
	}
	schema := params.Schema
	if dbAvailable(e) {
		return e.Executor.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (workflow_id, next_fire_at)
			SELECT ids.id, fires.fire_at
			FROM unnest($1::text[]) WITH ORDINALITY AS ids(id, ord)
			JOIN unnest($2::timestamptz[]) WITH ORDINALITY AS fires(fire_at, ord) USING (ord)
			WHERE ids.id <> ''
			ON CONFLICT (workflow_id) DO UPDATE SET next_fire_at = excluded.next_fire_at
		`, qt(schema, "river_workflow_timer")), params.WorkflowIDs, params.NextFireAts)
	}
	compat.Lock()
	defer compat.Unlock()
	if compat.timers[schema] == nil {
		compat.timers[schema] = map[string]*WorkflowTimer{}
	}
	for i, id := range params.WorkflowIDs {
		if i < len(params.NextFireAts) {
			compat.timers[schema][id] = &WorkflowTimer{NextFireAt: params.NextFireAts[i], WorkflowID: id}
		}
	}
	return nil
}
func (e *Executor) WorkflowUnfinalizeIfActiveJobsMany(ctx context.Context, params *WorkflowUnfinalizeIfActiveJobsManyParams) ([]string, error) {
	if params == nil {
		return []string{}, nil
	}
	out := []string{}
	for _, id := range params.WorkflowIDs {
		n, err := e.WorkflowCountIncompleteJobs(ctx, &WorkflowCountIncompleteJobsParams{Schema: params.Schema, WorkflowID: id})
		if err != nil {
			return out, err
		}
		if n > 0 {
			out = append(out, id)
			compat.Lock()
			if w := compat.workflows[params.Schema][id]; w != nil {
				w.FinalizedAt = nil
				w.State = "active"
				w.UpdatedAt = params.Now
			}
			compat.Unlock()
		}
	}
	return out, nil
}
func (e *Executor) WorkflowWaitActivatableTaskIDsByWorkflowIDs(ctx context.Context, params *WorkflowWaitActivatableTaskIDsByWorkflowIDsParams) ([]*WorkflowWaitActivatableTaskIDsByWorkflowIDsRow, error) {
	if params == nil {
		return []*WorkflowWaitActivatableTaskIDsByWorkflowIDsRow{}, nil
	}
	rows, err := e.WorkflowReadyTaskIDsByWorkflowIDs(ctx, &WorkflowReadyTaskIDsByWorkflowIDsParams{LimitCount: params.LimitCount, Schema: params.Schema, WorkflowIDs: params.WorkflowIDs})
	if err != nil {
		return nil, err
	}
	out := []*WorkflowWaitActivatableTaskIDsByWorkflowIDsRow{}
	for _, r := range rows {
		out = append(out, &WorkflowWaitActivatableTaskIDsByWorkflowIDsRow{ID: r.ID, TotalCount: r.TotalCount, WorkflowID: r.WorkflowID})
	}
	return out, nil
}
func (e *Executor) WorkflowWaitActivateByJobIDMany(ctx context.Context, params *WorkflowWaitActivateByJobIDManyParams) ([]int64, error) {
	if params == nil {
		return []int64{}, nil
	}
	rows, err := e.WorkflowStageJobsByIDMany(ctx, &WorkflowStageJobsByIDManyParams{JobIDs: params.JobIDs, Schema: params.Schema, WorkflowStagedAt: params.Now})
	if err != nil {
		return nil, err
	}
	ids := []int64{}
	for _, r := range rows {
		ids = append(ids, r.ID)
	}
	return ids, nil
}
func (e *Executor) WorkflowWaitActiveTaskListByWorkflowIDs(ctx context.Context, params *WorkflowWaitActiveTaskListByWorkflowIDsParams) ([]*WorkflowWaitActiveTask, error) {
	if params == nil {
		return []*WorkflowWaitActiveTask{}, nil
	}
	limit := limitDefault(params.LimitCount, 1000)
	out := []*WorkflowWaitActiveTask{}
	for _, id := range params.WorkflowIDs {
		jobs, err := e.allWorkflowJobs(ctx, params.Schema, id, 10000)
		if err != nil {
			return nil, err
		}
		var count int64
		for _, j := range jobs {
			if len(jobWaitRaw(j)) > 0 && activeState(j.State) {
				count++
				if len(out) < limit {
					out = append(out, &WorkflowWaitActiveTask{ID: j.ID, Metadata: append([]byte(nil), j.Metadata...), WorkflowID: id})
				}
			}
		}
		for _, r := range out {
			if r.WorkflowID == id {
				r.TotalCount = count
			}
		}
	}
	return out, nil
}
func (e *Executor) WorkflowWaitDepOutputListByWorkflowTaskPairs(ctx context.Context, params *WorkflowWaitDepOutputListByWorkflowTaskPairsParams) ([]*WorkflowWaitDepOutput, error) {
	if params == nil {
		return []*WorkflowWaitDepOutput{}, nil
	}
	out := []*WorkflowWaitDepOutput{}
	for i, id := range params.WorkflowIDs {
		if i >= len(params.Tasks) {
			break
		}
		j, err := e.workflowJobByTask(ctx, params.Schema, id, params.Tasks[i])
		if err == nil {
			st := j.State
			out = append(out, &WorkflowWaitDepOutput{FinalizedAt: j.FinalizedAt, Output: j.Output(), State: &st, Task: params.Tasks[i], WorkflowID: id})
		}
	}
	return out, nil
}
func (e *Executor) WorkflowWaitEvalCursorUpdateByWorkflowIDMany(ctx context.Context, params *WorkflowWaitEvalCursorUpdateByWorkflowIDManyParams) error {
	if params == nil {
		return nil
	}
	schema := params.Schema
	if dbAvailable(e) {
		return e.Executor.Exec(ctx, fmt.Sprintf(`
			UPDATE %s AS w SET wait_eval_cursor_job_id = pairs.cursor_id, updated_at = now()
			FROM (
				SELECT ids.id, cursors.cursor_id
				FROM unnest($1::text[]) WITH ORDINALITY AS ids(id, ord)
				JOIN unnest($2::bigint[]) WITH ORDINALITY AS cursors(cursor_id, ord) USING (ord)
			) AS pairs
			WHERE w.id = pairs.id AND pairs.id <> ''
		`, qt(schema, "river_workflow")), params.WorkflowIDs, params.CursorJobIDs)
	}
	compat.Lock()
	defer compat.Unlock()
	m := compat.workflows[schema]
	for i, id := range params.WorkflowIDs {
		if i < len(params.CursorJobIDs) {
			if w := m[id]; w != nil {
				cid := params.CursorJobIDs[i]
				w.WaitEvalCursorJobID = &cid
				w.UpdatedAt = nowUTC()
			}
		}
	}
	return nil
}
func (e *Executor) WorkflowWaitUpdateMetadataByJobIDMany(ctx context.Context, params *WorkflowWaitUpdateMetadataByJobIDManyParams) error {
	if params == nil {
		return nil
	}
	if dbAvailable(e) {
		states := make([]string, len(params.JobIDs))
		for i := range states {
			if i < len(params.WaitStates) && len(params.WaitStates[i]) > 0 {
				states[i] = string(params.WaitStates[i])
			} else {
				states[i] = "{}"
			}
		}
		return e.Executor.Exec(ctx, fmt.Sprintf(`
			UPDATE %s AS j SET metadata = j.metadata || pairs.wait_state
			FROM (
				SELECT ids.id, states.wait_state::jsonb
				FROM unnest($1::bigint[]) WITH ORDINALITY AS ids(id, ord)
				JOIN unnest($2::text[]) WITH ORDINALITY AS states(wait_state, ord) USING (ord)
			) AS pairs
			WHERE j.id = pairs.id
		`, qt(params.Schema, "river_job")), params.JobIDs, states)
	}
	for i, id := range params.JobIDs {
		var meta []byte
		if i < len(params.WaitStates) {
			meta = params.WaitStates[i]
		}
		_, err := e.Executor.JobUpdate(ctx, &riverdriver.JobUpdateParams{ID: id, MetadataDoMerge: true, Metadata: meta, Schema: params.Schema})
		if err != nil && err != rivertype.ErrNotFound {
			return err
		}
	}
	return nil
}
func (e *Executor) WorkflowWorklistDeleteByWorkflowIDsReturningReasons(ctx context.Context, params *WorkflowWorklistDeleteByWorkflowIDsReturningReasonsParams) ([]*WorkflowWorklistDeleteByWorkflowIDsReturningReasonsRow, error) {
	if params == nil {
		return []*WorkflowWorklistDeleteByWorkflowIDsReturningReasonsRow{}, nil
	}
	schema := params.Schema
	if dbAvailable(e) {
		return scanJSON[[]*WorkflowWorklistDeleteByWorkflowIDsReturningReasonsRow](ctx, e.Executor, fmt.Sprintf(`
			WITH deleted AS (DELETE FROM %s WHERE workflow_id = ANY($1::text[]) RETURNING workflow_id, reason)
			SELECT coalesce(json_agg(json_build_object('WorkflowID', workflow_id, 'Reason', reason) ORDER BY workflow_id, reason), '[]'::json) FROM deleted
		`, qt(schema, "river_workflow_worklist")), params.WorkflowIDs)
	}
	compat.Lock()
	defer compat.Unlock()
	out := []*WorkflowWorklistDeleteByWorkflowIDsReturningReasonsRow{}
	m := compat.worklists[schema]
	for _, id := range params.WorkflowIDs {
		if w := m[id]; w != nil {
			out = append(out, &WorkflowWorklistDeleteByWorkflowIDsReturningReasonsRow{Reason: w.Reason, WorkflowID: id})
			delete(m, id)
		}
	}
	return out, nil
}
func (e *Executor) WorkflowWorklistInsertMany(ctx context.Context, params *WorkflowWorklistInsertManyParams) error {
	if params == nil {
		return nil
	}
	schema := params.Schema
	if dbAvailable(e) {
		return e.Executor.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (workflow_id, reason)
			SELECT id, $2 FROM unnest($1::text[]) AS id WHERE id <> ''
			ON CONFLICT (workflow_id, reason) DO NOTHING
		`, qt(schema, "river_workflow_worklist")), params.WorkflowIDs, params.Reason)
	}
	compat.Lock()
	defer compat.Unlock()
	if compat.worklists[schema] == nil {
		compat.worklists[schema] = map[string]*WorkflowWorklistItem{}
	}
	for _, id := range params.WorkflowIDs {
		if compat.worklists[schema][id] == nil {
			compat.worklistSeq++
			compat.worklists[schema][id] = &WorkflowWorklistItem{CreatedAt: nowUTC(), ID: compat.worklistSeq, Reason: params.Reason, WorkflowID: id}
		}
	}
	return nil
}
func (e *Executor) WorkflowWorklistListIDs(ctx context.Context, params *WorkflowWorklistListParams) ([]*WorkflowWorklistIDItem, error) {
	list, err := e.WorkflowWorklistList(ctx, params)
	if err != nil {
		return nil, err
	}
	out := []*WorkflowWorklistIDItem{}
	for _, it := range list {
		out = append(out, &WorkflowWorklistIDItem{ID: it.ID, WorkflowID: it.WorkflowID})
	}
	return out, nil
}
func (e *Executor) WorkflowWorklistList(ctx context.Context, params *WorkflowWorklistListParams) ([]*WorkflowWorklistItem, error) {
	schema := ""
	limit := 100
	after := int64(0)
	if params != nil {
		schema = params.Schema
		limit = limitDefault(params.LimitCount, limit)
		after = params.AfterID
	}
	if dbAvailable(e) {
		return scanJSON[[]*WorkflowWorklistItem](ctx, e.Executor, fmt.Sprintf(`
			SELECT coalesce(json_agg(json_build_object('ID', id, 'WorkflowID', workflow_id, 'Reason', reason, 'CreatedAt', created_at) ORDER BY id), '[]'::json)
			FROM (SELECT * FROM %s WHERE id > $1 ORDER BY id LIMIT $2) w
		`, qt(schema, "river_workflow_worklist")), after, limit)
	}
	compat.Lock()
	defer compat.Unlock()
	out := []*WorkflowWorklistItem{}
	for _, it := range compat.worklists[schema] {
		if it.ID > after {
			c := *it
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
