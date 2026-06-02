package riverpro

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	prodriver "github.com/divyam234/riverpro/driver"
	"github.com/riverqueue/river/riverdriver"
	"github.com/robfig/cron/v3"
)

// PeriodicJobAddOpts describes a new durable periodic job to insert. Either
// Schedule.CronExpression (recurring) or Schedule.NextRunAt (one-shot) must
// be set.
type PeriodicJobAddOpts struct {
	ID          string
	Kind        string
	Args        []byte
	Queue       string
	Priority    int
	MaxAttempts int
	Tags        []string
	Schedule    *PeriodicJobSchedule
}

// PeriodicJobSchedule describes when a periodic job runs.
type PeriodicJobSchedule struct {
	// CronExpression is a standard 5-field cron spec. Mutually exclusive
	// with NextRunAt: when set, the row persists and fires on the cron.
	CronExpression string
	// CronTimezone is a tz database name (e.g. "UTC", "America/New_York").
	// Defaults to "UTC" when CronExpression is set.
	CronTimezone string
	// NextRunAt is the first fire time for a one-shot row. Mutually
	// exclusive with CronExpression.
	NextRunAt time.Time
}

// PeriodicJobListOpts controls pagination for PeriodicJobList.
type PeriodicJobListOpts struct {
	Max int
}

// PeriodicJobList returns durable periodic jobs (paused and active).
func (c *Client[TTx]) PeriodicJobList(ctx context.Context, opts *PeriodicJobListOpts) ([]*prodriver.PeriodicJob, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	if err := c.requireDurablePeriodicJobsEnabled(); err != nil {
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
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	if err := c.requireDurablePeriodicJobsEnabled(); err != nil {
		return nil, err
	}
	max := 0
	if opts != nil {
		max = opts.Max
	}
	return c.proDriver.UnwrapProExecutor(tx).PeriodicJobGetAll(ctx, &prodriver.PeriodicJobGetAllParams{Max: max, Schema: c.config.Schema})
}

// PeriodicJobGet returns a single durable periodic job by ID. Returns
// rivertype.ErrNotFound if no row matches.
func (c *Client[TTx]) PeriodicJobGet(ctx context.Context, id string) (*prodriver.PeriodicJob, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	if err := c.requireDurablePeriodicJobsEnabled(); err != nil {
		return nil, err
	}
	return c.proDriver.GetProExecutor().PeriodicJobGetByID(ctx, &prodriver.PeriodicJobGetByIDParams{ID: id, Schema: c.config.Schema})
}

// PeriodicJobGetTx is the transaction variant of PeriodicJobGet.
func (c *Client[TTx]) PeriodicJobGetTx(ctx context.Context, tx TTx, id string) (*prodriver.PeriodicJob, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	if err := c.requireDurablePeriodicJobsEnabled(); err != nil {
		return nil, err
	}
	return c.proDriver.UnwrapProExecutor(tx).PeriodicJobGetByID(ctx, &prodriver.PeriodicJobGetByIDParams{ID: id, Schema: c.config.Schema})
}

// PeriodicJobAdd inserts a new durable periodic job. If a row with the same
// ID already exists, it is updated (UPSERT).
func (c *Client[TTx]) PeriodicJobAdd(ctx context.Context, opts *PeriodicJobAddOpts) (*prodriver.PeriodicJob, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	if err := c.requireDurablePeriodicJobsEnabled(); err != nil {
		return nil, err
	}
	params, err := buildPeriodicJobInsertParams(opts, c.config)
	if err != nil {
		return nil, err
	}
	return c.proDriver.GetProExecutor().PeriodicJobInsert(ctx, params)
}

// PeriodicJobAddTx is the transaction variant of PeriodicJobAdd.
func (c *Client[TTx]) PeriodicJobAddTx(ctx context.Context, tx TTx, opts *PeriodicJobAddOpts) (*prodriver.PeriodicJob, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	if err := c.requireDurablePeriodicJobsEnabled(); err != nil {
		return nil, err
	}
	params, err := buildPeriodicJobInsertParams(opts, c.config)
	if err != nil {
		return nil, err
	}
	return c.proDriver.UnwrapProExecutor(tx).PeriodicJobInsert(ctx, params)
}

// buildPeriodicJobInsertParams validates an Add opts struct and converts
// it into the driver's PeriodicJobInsertParams.
func buildPeriodicJobInsertParams(opts *PeriodicJobAddOpts, config *Config) (*prodriver.PeriodicJobInsertParams, error) {
	if opts == nil {
		return nil, errors.New("riverpro: nil PeriodicJobAddOpts")
	}
	if opts.ID == "" {
		return nil, errors.New("riverpro: periodic job ID is empty")
	}
	if opts.Kind == "" {
		return nil, errors.New("riverpro: periodic job kind is empty")
	}
	if opts.Schedule == nil {
		return nil, errors.New("riverpro: periodic job schedule is nil")
	}
	if opts.Schedule.CronExpression == "" && opts.Schedule.NextRunAt.IsZero() {
		return nil, errors.New("riverpro: periodic job schedule must set CronExpression or NextRunAt")
	}
	cronExpr := opts.Schedule.CronExpression
	var cronExprPtr *string
	if cronExpr != "" {
		cronExprPtr = &cronExpr
	}
	cronTz := opts.Schedule.CronTimezone
	if cronExpr != "" && cronTz == "" {
		cronTz = "UTC"
	}
	nextRunAt := opts.Schedule.NextRunAt
	if nextRunAt.IsZero() {
		// Default to now + StartStaggerSpread to spread startup load.
		spread := time.Minute
		if config != nil && config.DurablePeriodicJobs.StartStaggerSpread > 0 {
			spread = config.DurablePeriodicJobs.StartStaggerSpread
		}
		nextRunAt = time.Now().Add(spread)
	}
	schema := ""
	if config != nil {
		schema = config.Schema
	}
	return &prodriver.PeriodicJobInsertParams{
		Args:           opts.Args,
		CronExpression: cronExprPtr,
		CronTimezone:   cronTz,
		ID:             opts.ID,
		Kind:           opts.Kind,
		MaxAttempts:    opts.MaxAttempts,
		NextRunAt:      nextRunAt,
		Priority:       opts.Priority,
		Queue:          opts.Queue,
		Schema:         schema,
		Tags:           opts.Tags,
	}, nil
}

// PeriodicJobDelete removes a durable periodic job by ID. Returns the
// deleted row, or rivertype.ErrNotFound if no row matched.
func (c *Client[TTx]) PeriodicJobDelete(ctx context.Context, id string) (*prodriver.PeriodicJob, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	if err := c.requireDurablePeriodicJobsEnabled(); err != nil {
		return nil, err
	}
	return c.proDriver.GetProExecutor().PeriodicJobDelete(ctx, &prodriver.PeriodicJobDeleteParams{ID: id, Schema: c.config.Schema})
}

// PeriodicJobDeleteTx is the transaction variant of PeriodicJobDelete.
func (c *Client[TTx]) PeriodicJobDeleteTx(ctx context.Context, tx TTx, id string) (*prodriver.PeriodicJob, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	if err := c.requireDurablePeriodicJobsEnabled(); err != nil {
		return nil, err
	}
	return c.proDriver.UnwrapProExecutor(tx).PeriodicJobDelete(ctx, &prodriver.PeriodicJobDeleteParams{ID: id, Schema: c.config.Schema})
}

// PeriodicJobPause marks a durable periodic job as paused. Paused jobs are
// not enqueued. Returns rivertype.ErrNotFound if no row matched; returns
// the row unchanged if it is already paused.
func (c *Client[TTx]) PeriodicJobPause(ctx context.Context, id string) (*prodriver.PeriodicJob, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	if err := c.requireDurablePeriodicJobsEnabled(); err != nil {
		return nil, err
	}
	now := time.Now()
	return c.proDriver.GetProExecutor().PeriodicJobPause(ctx, &prodriver.PeriodicJobPauseParams{ID: id, PausedAt: &now, Schema: c.config.Schema})
}

// PeriodicJobPauseTx is the transaction variant of PeriodicJobPause.
func (c *Client[TTx]) PeriodicJobPauseTx(ctx context.Context, tx TTx, id string) (*prodriver.PeriodicJob, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	if err := c.requireDurablePeriodicJobsEnabled(); err != nil {
		return nil, err
	}
	now := time.Now()
	return c.proDriver.UnwrapProExecutor(tx).PeriodicJobPause(ctx, &prodriver.PeriodicJobPauseParams{ID: id, PausedAt: &now, Schema: c.config.Schema})
}

// PeriodicJobResume clears the paused flag on a durable periodic job.
// Returns rivertype.ErrNotFound if no row matched; returns the row
// unchanged if it is not currently paused.
func (c *Client[TTx]) PeriodicJobResume(ctx context.Context, id string) (*prodriver.PeriodicJob, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	if err := c.requireDurablePeriodicJobsEnabled(); err != nil {
		return nil, err
	}
	return c.proDriver.GetProExecutor().PeriodicJobResume(ctx, &prodriver.PeriodicJobResumeParams{ID: id, Schema: c.config.Schema})
}

// PeriodicJobResumeTx is the transaction variant of PeriodicJobResume.
func (c *Client[TTx]) PeriodicJobResumeTx(ctx context.Context, tx TTx, id string) (*prodriver.PeriodicJob, error) {
	if c == nil || c.proDriver == nil {
		return nil, errors.New("riverpro: client is not configured with a Pro driver")
	}
	if err := c.requireDurablePeriodicJobsEnabled(); err != nil {
		return nil, err
	}
	return c.proDriver.UnwrapProExecutor(tx).PeriodicJobResume(ctx, &prodriver.PeriodicJobResumeParams{ID: id, Schema: c.config.Schema})
}

// PeriodicJobUpdateSchedule is removed; PeriodicJobAdd performs a full
// UPSERT, so callers can change a row's schedule (cron, timezone, next
// run) by calling Add with the same ID and the new spec.

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
	topic := prodriver.PeriodicJobChangeTopicSuffix
	if schema != "" {
		topic = schema + "." + prodriver.PeriodicJobChangeTopicSuffix
	}

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

