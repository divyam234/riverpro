package riverpro

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	prodriver "github.com/divyam234/riverpro/driver"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver"
	"github.com/robfig/cron/v3"
)

// ErrPeriodicJobAlreadyExists is returned when PeriodicJobInsert is called
// with an ID that already exists.
var ErrPeriodicJobAlreadyExists = prodriver.ErrPeriodicJobAlreadyExists

// PeriodicJobSpec is the complete persisted definition of a durable periodic
// job. JobArgs is preferred because River Pro derives Kind and JSON encoding
// from it. Kind and Args remain available for callers that already have an
// encoded payload.
type PeriodicJobSpec struct {
	ID      string
	JobArgs river.JobArgs

	// Kind and Args are the raw form of JobArgs. They are ignored when
	// JobArgs is non-nil.
	Kind string
	Args []byte

	Queue       string
	Priority    int
	MaxAttempts int
	Tags        []string
	Schedule    *PeriodicJobSchedule
	Paused      bool
}

// PeriodicJobInsertOpts describes a create-only durable periodic job.
type PeriodicJobInsertOpts = PeriodicJobSpec

// PeriodicJobUpsertOpts describes an idempotently reconciled durable periodic
// job. When its cron schedule is unchanged and NextRunAt is omitted, the
// existing database next_run_at is preserved.
type PeriodicJobUpsertOpts = PeriodicJobSpec

// PeriodicJobUpdateOpts patches an existing durable periodic job. Nil fields
// preserve their current values. Updating the schedule is the only operation
// that changes next_run_at.
type PeriodicJobUpdateOpts struct {
	JobArgs     river.JobArgs
	Queue       *string
	Priority    *int
	MaxAttempts *int
	Tags        *[]string
	Schedule    *PeriodicJobSchedule
}

// PeriodicJobSchedule describes when a periodic job runs.
type PeriodicJobSchedule struct {
	// CronExpression is a standard five-field cron expression or supported
	// descriptor. When set, the durable row remains after each execution.
	CronExpression string
	// CronTimezone is a tz database name. It defaults to UTC for cron jobs.
	CronTimezone string
	// NextRunAt is required for one-shot jobs. For cron jobs it optionally
	// overrides the first run; when omitted River Pro calculates the next cron
	// tick. Upsert preserves the stored next run when the schedule is unchanged.
	NextRunAt time.Time
}

// PeriodicJobListOpts controls pagination for PeriodicJobList.
type PeriodicJobListOpts struct {
	Max int
}

// PeriodicJobList returns durable periodic jobs (paused and active).
func (c *Client[TTx]) PeriodicJobList(ctx context.Context, opts *PeriodicJobListOpts) ([]*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	max := 0
	if opts != nil {
		max = opts.Max
	}
	return c.proDriver.GetProExecutor().PeriodicJobGetAll(ctx, &prodriver.PeriodicJobGetAllParams{Max: max, Schema: c.config.Schema})
}

// PeriodicJobListTx is the transaction variant of PeriodicJobList.
func (c *Client[TTx]) PeriodicJobListTx(ctx context.Context, tx TTx, opts *PeriodicJobListOpts) ([]*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	max := 0
	if opts != nil {
		max = opts.Max
	}
	return c.proDriver.UnwrapProExecutor(tx).PeriodicJobGetAll(ctx, &prodriver.PeriodicJobGetAllParams{Max: max, Schema: c.config.Schema})
}

// PeriodicJobGet returns a single durable periodic job by ID.
func (c *Client[TTx]) PeriodicJobGet(ctx context.Context, id string) (*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	return c.proDriver.GetProExecutor().PeriodicJobGetByID(ctx, &prodriver.PeriodicJobGetByIDParams{ID: id, Schema: c.config.Schema})
}

// PeriodicJobGetTx is the transaction variant of PeriodicJobGet.
func (c *Client[TTx]) PeriodicJobGetTx(ctx context.Context, tx TTx, id string) (*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	return c.proDriver.UnwrapProExecutor(tx).PeriodicJobGetByID(ctx, &prodriver.PeriodicJobGetByIDParams{ID: id, Schema: c.config.Schema})
}

// PeriodicJobInsert creates a durable periodic job and fails with
// ErrPeriodicJobAlreadyExists when the ID already exists.
func (c *Client[TTx]) PeriodicJobInsert(ctx context.Context, opts *PeriodicJobInsertOpts) (*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	params, err := buildPeriodicJobInsertParams(opts, c.config)
	if err != nil {
		return nil, err
	}
	return c.proDriver.GetProExecutor().PeriodicJobInsert(ctx, params)
}

// PeriodicJobInsertTx is the transaction variant of PeriodicJobInsert.
func (c *Client[TTx]) PeriodicJobInsertTx(ctx context.Context, tx TTx, opts *PeriodicJobInsertOpts) (*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	params, err := buildPeriodicJobInsertParams(opts, c.config)
	if err != nil {
		return nil, err
	}
	return c.proDriver.UnwrapProExecutor(tx).PeriodicJobInsert(ctx, params)
}

// PeriodicJobUpsert inserts or reconciles a complete durable periodic job
// definition. It preserves next_run_at when the cron schedule is unchanged and
// the caller did not explicitly provide NextRunAt.
func (c *Client[TTx]) PeriodicJobUpsert(ctx context.Context, opts *PeriodicJobUpsertOpts) (*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	params, err := buildPeriodicJobUpsertParams(opts, c.config)
	if err != nil {
		return nil, err
	}
	return c.proDriver.GetProExecutor().PeriodicJobUpsert(ctx, params)
}

// PeriodicJobUpsertTx is the transaction variant of PeriodicJobUpsert.
func (c *Client[TTx]) PeriodicJobUpsertTx(ctx context.Context, tx TTx, opts *PeriodicJobUpsertOpts) (*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	params, err := buildPeriodicJobUpsertParams(opts, c.config)
	if err != nil {
		return nil, err
	}
	return c.proDriver.UnwrapProExecutor(tx).PeriodicJobUpsert(ctx, params)
}

// PeriodicJobUpdate patches an existing durable periodic job. It never inserts
// a missing ID and never changes pause state.
func (c *Client[TTx]) PeriodicJobUpdate(ctx context.Context, id string, opts *PeriodicJobUpdateOpts) (*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	params, err := buildPeriodicJobUpdateParams(id, opts, c.config)
	if err != nil {
		return nil, err
	}
	return c.proDriver.GetProExecutor().PeriodicJobUpdate(ctx, params)
}

// PeriodicJobUpdateTx is the transaction variant of PeriodicJobUpdate.
func (c *Client[TTx]) PeriodicJobUpdateTx(ctx context.Context, tx TTx, id string, opts *PeriodicJobUpdateOpts) (*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	params, err := buildPeriodicJobUpdateParams(id, opts, c.config)
	if err != nil {
		return nil, err
	}
	return c.proDriver.UnwrapProExecutor(tx).PeriodicJobUpdate(ctx, params)
}

// PeriodicJobPause pauses an existing durable periodic job.
func (c *Client[TTx]) PeriodicJobPause(ctx context.Context, id string) (*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	return c.proDriver.GetProExecutor().PeriodicJobPause(ctx, &prodriver.PeriodicJobPauseParams{ID: id, Schema: c.config.Schema})
}

// PeriodicJobPauseTx is the transaction variant of PeriodicJobPause.
func (c *Client[TTx]) PeriodicJobPauseTx(ctx context.Context, tx TTx, id string) (*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	return c.proDriver.UnwrapProExecutor(tx).PeriodicJobPause(ctx, &prodriver.PeriodicJobPauseParams{ID: id, Schema: c.config.Schema})
}

// PeriodicJobResume resumes an existing durable periodic job without changing
// its stored next_run_at.
func (c *Client[TTx]) PeriodicJobResume(ctx context.Context, id string) (*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	return c.proDriver.GetProExecutor().PeriodicJobResume(ctx, &prodriver.PeriodicJobResumeParams{ID: id, Schema: c.config.Schema})
}

// PeriodicJobResumeTx is the transaction variant of PeriodicJobResume.
func (c *Client[TTx]) PeriodicJobResumeTx(ctx context.Context, tx TTx, id string) (*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	return c.proDriver.UnwrapProExecutor(tx).PeriodicJobResume(ctx, &prodriver.PeriodicJobResumeParams{ID: id, Schema: c.config.Schema})
}

// PeriodicJobDelete removes a durable periodic job by ID.
func (c *Client[TTx]) PeriodicJobDelete(ctx context.Context, id string) (*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	return c.proDriver.GetProExecutor().PeriodicJobDelete(ctx, &prodriver.PeriodicJobDeleteParams{ID: id, Schema: c.config.Schema})
}

// PeriodicJobDeleteTx is the transaction variant of PeriodicJobDelete.
func (c *Client[TTx]) PeriodicJobDeleteTx(ctx context.Context, tx TTx, id string) (*prodriver.PeriodicJob, error) {
	if err := c.requireDurablePeriodicJobsClient(); err != nil {
		return nil, err
	}
	return c.proDriver.UnwrapProExecutor(tx).PeriodicJobDelete(ctx, &prodriver.PeriodicJobDeleteParams{ID: id, Schema: c.config.Schema})
}

type periodicJobDefinition struct {
	id              string
	kind            string
	args            []byte
	queue           string
	priority        int
	maxAttempts     int
	tags            []string
	cronExpression  *string
	cronTimezone    string
	nextRunAt       time.Time
	nextRunExplicit bool
	paused          bool
}

func buildPeriodicJobInsertParams(opts *PeriodicJobInsertOpts, config *Config) (*prodriver.PeriodicJobInsertParams, error) {
	definition, err := buildPeriodicJobDefinition(opts, config)
	if err != nil {
		return nil, err
	}
	return &prodriver.PeriodicJobInsertParams{
		ID: definition.id, Kind: definition.kind, Args: definition.args,
		Queue: definition.queue, Priority: definition.priority, MaxAttempts: definition.maxAttempts,
		Tags: definition.tags, CronExpression: definition.cronExpression, CronTimezone: definition.cronTimezone,
		NextRunAt: definition.nextRunAt, Paused: definition.paused, Schema: schemaFromConfig(config),
	}, nil
}

func buildPeriodicJobUpsertParams(opts *PeriodicJobUpsertOpts, config *Config) (*prodriver.PeriodicJobUpsertParams, error) {
	definition, err := buildPeriodicJobDefinition(opts, config)
	if err != nil {
		return nil, err
	}
	return &prodriver.PeriodicJobUpsertParams{
		ID: definition.id, Kind: definition.kind, Args: definition.args,
		Queue: definition.queue, Priority: definition.priority, MaxAttempts: definition.maxAttempts,
		Tags: definition.tags, CronExpression: definition.cronExpression, CronTimezone: definition.cronTimezone,
		NextRunAt: definition.nextRunAt, ResetNextRunAt: definition.nextRunExplicit,
		Paused: definition.paused, Schema: schemaFromConfig(config),
	}, nil
}

func buildPeriodicJobDefinition(opts *PeriodicJobSpec, config *Config) (*periodicJobDefinition, error) {
	if opts == nil {
		return nil, errors.New("riverpro: nil periodic job options")
	}
	if opts.ID == "" {
		return nil, errors.New("riverpro: periodic job ID is empty")
	}
	kind, encodedArgs, err := encodePeriodicJobArgs(opts.JobArgs, opts.Kind, opts.Args)
	if err != nil {
		return nil, err
	}
	cronExpression, cronTimezone, nextRunAt, explicit, err := normalizePeriodicJobSchedule(opts.Schedule, time.Now())
	if err != nil {
		return nil, err
	}
	var jobInsertOpts river.InsertOpts
	if argsWithOpts, ok := opts.JobArgs.(river.JobArgsWithInsertOpts); ok {
		jobInsertOpts = argsWithOpts.InsertOpts()
	}
	queue := opts.Queue
	if queue == "" {
		queue = jobInsertOpts.Queue
	}
	if queue == "" {
		queue = river.QueueDefault
	}
	priority := opts.Priority
	if priority <= 0 {
		priority = jobInsertOpts.Priority
	}
	if priority <= 0 {
		priority = river.PriorityDefault
	}
	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = jobInsertOpts.MaxAttempts
	}
	if maxAttempts <= 0 {
		maxAttempts = river.MaxAttemptsDefault
	}
	tags := opts.Tags
	if tags == nil {
		tags = jobInsertOpts.Tags
	}
	return &periodicJobDefinition{
		id: opts.ID, kind: kind, args: encodedArgs, queue: queue, priority: priority,
		maxAttempts: maxAttempts, tags: append([]string(nil), tags...),
		cronExpression: cronExpression, cronTimezone: cronTimezone,
		nextRunAt: nextRunAt, nextRunExplicit: explicit, paused: opts.Paused,
	}, nil
}

func buildPeriodicJobUpdateParams(id string, opts *PeriodicJobUpdateOpts, config *Config) (*prodriver.PeriodicJobUpdateParams, error) {
	if id == "" {
		return nil, errors.New("riverpro: periodic job ID is empty")
	}
	if opts == nil {
		return nil, errors.New("riverpro: nil PeriodicJobUpdateOpts")
	}
	params := &prodriver.PeriodicJobUpdateParams{ID: id, Schema: schemaFromConfig(config)}
	if opts.JobArgs != nil {
		kind, encodedArgs, err := encodePeriodicJobArgs(opts.JobArgs, "", nil)
		if err != nil {
			return nil, err
		}
		params.SetArgs, params.Kind, params.Args = true, kind, encodedArgs
	}
	if opts.Queue != nil {
		params.SetQueue = true
		params.Queue = *opts.Queue
		if params.Queue == "" {
			params.Queue = river.QueueDefault
		}
	}
	if opts.Priority != nil {
		params.SetPriority = true
		params.Priority = *opts.Priority
		if params.Priority <= 0 {
			params.Priority = river.PriorityDefault
		}
	}
	if opts.MaxAttempts != nil {
		params.SetMaxAttempts = true
		params.MaxAttempts = *opts.MaxAttempts
		if params.MaxAttempts <= 0 {
			params.MaxAttempts = river.MaxAttemptsDefault
		}
	}
	if opts.Tags != nil {
		params.SetTags = true
		params.Tags = append([]string(nil), (*opts.Tags)...)
	}
	if opts.Schedule != nil {
		cronExpression, cronTimezone, nextRunAt, _, err := normalizePeriodicJobSchedule(opts.Schedule, time.Now())
		if err != nil {
			return nil, err
		}
		params.SetSchedule = true
		params.CronExpression = cronExpression
		params.CronTimezone = cronTimezone
		params.NextRunAt = nextRunAt
	}
	if !params.SetArgs && !params.SetQueue && !params.SetPriority && !params.SetMaxAttempts && !params.SetTags && !params.SetSchedule {
		return nil, errors.New("riverpro: periodic job update has no fields")
	}
	return params, nil
}

func encodePeriodicJobArgs(jobArgs river.JobArgs, kind string, encodedArgs []byte) (string, []byte, error) {
	if jobArgs != nil {
		kind = jobArgs.Kind()
		var err error
		encodedArgs, err = json.Marshal(jobArgs)
		if err != nil {
			return "", nil, fmt.Errorf("riverpro: encode periodic job args: %w", err)
		}
	}
	if kind == "" {
		return "", nil, errors.New("riverpro: periodic job kind is empty")
	}
	if len(encodedArgs) == 0 {
		encodedArgs = []byte(`{}`)
	}
	if !json.Valid(encodedArgs) {
		return "", nil, errors.New("riverpro: periodic job args are not valid JSON")
	}
	return kind, append([]byte(nil), encodedArgs...), nil
}

func normalizePeriodicJobSchedule(schedule *PeriodicJobSchedule, now time.Time) (*string, string, time.Time, bool, error) {
	if schedule == nil {
		return nil, "", time.Time{}, false, errors.New("riverpro: periodic job schedule is nil")
	}
	if schedule.CronExpression == "" {
		if schedule.NextRunAt.IsZero() {
			return nil, "", time.Time{}, false, errors.New("riverpro: one-shot periodic job requires NextRunAt")
		}
		return nil, "UTC", schedule.NextRunAt, true, nil
	}
	cronTimezone := schedule.CronTimezone
	if cronTimezone == "" {
		cronTimezone = "UTC"
	}
	calculated, err := cronNextAfter(schedule.CronExpression, cronTimezone, now)
	if err != nil {
		return nil, "", time.Time{}, false, err
	}
	nextRunAt := schedule.NextRunAt
	explicit := !nextRunAt.IsZero()
	if nextRunAt.IsZero() {
		nextRunAt = calculated
	}
	cronExpression := schedule.CronExpression
	return &cronExpression, cronTimezone, nextRunAt, explicit, nil
}

func schemaFromConfig(config *Config) string {
	if config == nil {
		return ""
	}
	return config.Schema
}

func (c *Client[TTx]) requireDurablePeriodicJobsClient() error {
	if c == nil || c.proDriver == nil {
		return errors.New("riverpro: client is not configured with a Pro driver")
	}
	return c.requireDurablePeriodicJobsEnabled()
}

// requireDurablePeriodicJobsEnabled returns prodriver.ErrNotSupported if
// DurablePeriodicJobs is disabled in the config.
func (c *Client[TTx]) requireDurablePeriodicJobsEnabled() error {
	if c == nil || c.config == nil || !c.config.DurablePeriodicJobs.Enabled {
		return prodriver.ErrNotSupported
	}
	return nil
}

// periodicEnqueuerLoop wakes up the durable periodic jobs enqueuer.
//
// By default it LISTENs on the periodic-job change topic and calls
// periodicEnqueueOnce on every notification. A 30s safety-net ticker
// runs even in LISTEN mode to catch any missed notifications (e.g. a
// change that happened before the listener was connected).
//
// Polling mode is used when:
//   - Config.DurablePeriodicJobs.PollOnly is true
//   - The underlying driver does not support listeners
//   - Listener Connect / Listen fails on startup
func (c *Client[TTx]) periodicEnqueuerLoop(ctx context.Context) {
	if c == nil || c.proDriver == nil || c.config == nil || !c.config.DurablePeriodicJobs.Enabled {
		return
	}
	if c.config.DurablePeriodicJobs.PollOnly {
		c.periodicEnqueuerPollingLoop(ctx)
		return
	}
	if !c.proDriver.SupportsListener() {
		c.periodicEnqueuerPollingLoop(ctx)
		return
	}
	c.periodicEnqueuerListenerLoop(ctx)
}

// periodicEnqueuerPollingLoop is the poll-only path. It runs
// periodicEnqueueOnce on every tick of DurablePeriodicJobs.PollInterval.
func (c *Client[TTx]) periodicEnqueuerPollingLoop(ctx context.Context) {
	interval := c.config.DurablePeriodicJobs.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = c.periodicEnqueueOnce(ctx)
		}
	}
}

// periodicEnqueuerListenerLoop is the LISTEN/NOTIFY path. It opens a
// dedicated connection via proDriver.GetListener, LISTENs on the
// schema-prefixed topic, and runs periodicEnqueueOnce on each
// notification. A 30s safety ticker catches any missed notifications.
func (c *Client[TTx]) periodicEnqueuerListenerLoop(ctx context.Context) {
	schema := c.config.Schema
	topic := prodriver.PeriodicJobChangeTopic(schema)

	listener := c.proDriver.GetListener(&riverdriver.GetListenenerParams{Schema: schema})
	if err := listener.Connect(ctx); err != nil {
		c.periodicEnqueuerPollingLoop(ctx)
		return
	}
	defer func() {
		_ = listener.Close(context.Background())
	}()
	if err := listener.Listen(ctx, topic); err != nil {
		c.periodicEnqueuerPollingLoop(ctx)
		return
	}

	// Run once at startup in case rows are already due.
	_ = c.periodicEnqueueOnce(ctx)

	const safetyInterval = 30 * time.Second
	safetyTicker := time.NewTicker(safetyInterval)
	defer safetyTicker.Stop()
	const waitTimeout = 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		case <-safetyTicker.C:
			_ = c.periodicEnqueueOnce(ctx)
		default:
			waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)
			notif, err := listener.WaitForNotification(waitCtx)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				// Transient listener error: back off briefly and
				// retry. The safety ticker will also wake us up.
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}
			_ = notif
			_ = c.periodicEnqueueOnce(ctx)
		}
	}
}

// periodicEnqueueOnce is a single iteration of the enqueuer loop. It is
// split out for testability. The work is gated on a Postgres advisory
// lock so only one client in the cluster runs it at a time.
func (c *Client[TTx]) periodicEnqueueOnce(ctx context.Context) error {
	if c == nil || c.proDriver == nil || c.config == nil || !c.config.DurablePeriodicJobs.Enabled {
		return nil
	}
	exec := c.proDriver.GetExecutor()
	if exec == nil {
		return nil
	}
	// Use a session-level advisory lock keyed on a per-process random
	// int63 (per AGENTS.md "rand.Int63()" convention). We unlock at
	// the end of this iteration.
	lockKey, err := periodicEnqueuerLockKey()
	if err != nil {
		return err
	}
	row := exec.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockKey)
	var locked bool
	if err := row.Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return nil
	}
	defer func() {
		_ = exec.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockKey)
	}()

	// Read all cron rows so we can compute the next tick for each.
	// One-shot rows (no cron_expression) are also due: the driver's
	// PeriodicJobEnqueueDue will insert their river_job row and
	// delete the durable row. Cron rows have their next_run_at
	// recomputed from the spec.
	rows, err := c.proDriver.GetProExecutor().PeriodicJobGetAll(ctx, &prodriver.PeriodicJobGetAllParams{Schema: c.config.Schema})
	if err != nil {
		return err
	}
	next := map[string]time.Time{}
	now := time.Now()
	anyDue := false
	for _, row := range rows {
		if row == nil {
			continue
		}
		// Skip rows whose stored next_run_at is in the future; the
		// driver will only claim rows where next_run_at <= now().
		if row.NextRunAt.After(now) {
			continue
		}
		anyDue = true
		if row.CronExpression == nil || *row.CronExpression == "" {
			continue
		}
		tick, err := cronNextAfter(*row.CronExpression, row.CronTimezone, now)
		if err != nil || tick.IsZero() {
			continue
		}
		next[row.ID] = tick
	}
	if !anyDue {
		return nil
	}
	_, err = c.proDriver.GetProExecutor().PeriodicJobEnqueueDue(ctx, &prodriver.PeriodicJobEnqueueDueParams{
		Max:       100,
		NextRunAt: next,
		Schema:    c.config.Schema,
	})
	return err
}

// periodicEnqueuerLockKey returns a stable per-process random int63 to
// use as the Postgres advisory lock key. Per AGENTS.md the key must be
// unique per test invocation because both riverprodatabasesql and
// riverpropgxv5 share the same database.
func periodicEnqueuerLockKey() (int64, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return 0, err
	}
	return n.Int64(), nil
}

// cronNextAfter returns the next fire time of the cron expression after
// `now`, in the given timezone (defaults to UTC).
func cronNextAfter(expr, tz string, now time.Time) (time.Time, error) {
	if strings.TrimSpace(expr) == "" {
		return time.Time{}, nil
	}
	loc := time.UTC
	if tz != "" {
		loaded, err := time.LoadLocation(tz)
		if err != nil {
			return time.Time{}, fmt.Errorf("riverpro: load cron timezone %q: %w", tz, err)
		}
		loc = loaded
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	sched, err := parser.Parse(expr)
	if err != nil {
		return time.Time{}, fmt.Errorf("riverpro: parse cron expression %q: %w", expr, err)
	}
	return sched.Next(now.In(loc)), nil
}

// sortedPeriodicJobIDs is removed; tests can use sort.Strings on the
// raw ID slice inline.
