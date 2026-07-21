package riverbatch

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/divyam234/riverpro"
	prodriver "github.com/divyam234/riverpro/driver"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

const (
	MaxCountDefault     = 100
	MaxDelayDefault     = 5 * time.Second
	PollIntervalDefault = time.Second
)

type JobArgsWithBatchOpts interface {
	river.JobArgs
	BatchOpts() riverpro.BatchOpts
}

type ManyWorker[T JobArgsWithBatchOpts] interface {
	WorkMany(context.Context, []*river.Job[T]) error
}

type WorkerOpts struct {
	MaxCount     int
	MaxDelay     time.Duration
	PollInterval time.Duration
}

type worker[T JobArgsWithBatchOpts, TTx any] struct {
	river.WorkerDefaults[T]
	many ManyWorker[T]
	opts WorkerOpts
}

type funcManyWorker[T JobArgsWithBatchOpts] func(context.Context, []*river.Job[T]) error

func (f funcManyWorker[T]) WorkMany(ctx context.Context, jobs []*river.Job[T]) error {
	return f(ctx, jobs)
}

// Work gathers matching jobs until MaxCount is reached or MaxDelay elapses,
// then invokes mw once for the full batch.
func Work[T JobArgsWithBatchOpts, TTx any](ctx context.Context, mw ManyWorker[T], job *river.Job[T], opts *WorkerOpts) error {
	if job == nil {
		return errors.New("riverbatch: nil job")
	}
	if mw == nil {
		return errors.New("riverbatch: nil worker")
	}
	o, err := normalizeOpts(opts)
	if err != nil {
		return err
	}

	jobs := []*river.Job[T]{job}
	client, clientErr := riverpro.ClientFromContextSafely[TTx](ctx)
	batchKey := metadataString(job.Metadata, "riverpro_batch_key")
	if clientErr == nil && client != nil && client.ProExecutor() != nil && batchKey != "" && o.MaxCount > 1 {
		deadline := time.Now().Add(o.MaxDelay)
		for len(jobs) < o.MaxCount {
			rows, fetchErr := client.ProExecutor().JobGetAvailableForBatch(ctx, &prodriver.JobGetAvailableForBatchParams{
				AttemptedBy:      attemptedBy(job),
				BatchKey:         batchKey,
				BatchLeaderJobID: job.ID,
				Kind:             job.Kind,
				Max:              int32(o.MaxCount - len(jobs)),
				Queue:            job.Queue,
				Schema:           client.Schema(),
			})
			if fetchErr != nil {
				return batchResult(jobs, fetchErr)
			}
			for _, row := range rows {
				var args T
				if err := json.Unmarshal(row.EncodedArgs, &args); err != nil {
					return batchResult(jobs, err)
				}
				jobs = append(jobs, &river.Job[T]{JobRow: row, Args: args})
			}
			if len(jobs) >= o.MaxCount || !time.Now().Before(deadline) {
				break
			}

			wait := o.PollInterval
			if remaining := time.Until(deadline); wait > remaining {
				wait = remaining
			}
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return batchResult(jobs, context.Cause(ctx))
			case <-timer.C:
			}
		}
	}

	return batchResult(jobs, mw.WorkMany(ctx, jobs))
}

func Worker[T JobArgsWithBatchOpts, TTx any](mw ManyWorker[T], opts *WorkerOpts) river.Worker[T] {
	if mw == nil {
		panic("riverbatch: nil worker")
	}
	o, err := normalizeOpts(opts)
	if err != nil {
		panic(err)
	}
	return &worker[T, TTx]{many: mw, opts: o}
}

func WorkFunc[T JobArgsWithBatchOpts, TTx any](f func(context.Context, []*river.Job[T]) error, opts *WorkerOpts) river.Worker[T] {
	w, err := WorkFuncSafely[T, TTx](f, opts)
	if err != nil {
		panic(err)
	}
	return w
}

func WorkFuncSafely[T JobArgsWithBatchOpts, TTx any](f func(context.Context, []*river.Job[T]) error, opts *WorkerOpts) (river.Worker[T], error) {
	if f == nil {
		return nil, errors.New("riverbatch: nil work func")
	}
	o, err := normalizeOpts(opts)
	if err != nil {
		return nil, err
	}
	return &worker[T, TTx]{many: funcManyWorker[T](f), opts: o}, nil
}

func normalizeOpts(opts *WorkerOpts) (WorkerOpts, error) {
	o := WorkerOpts{MaxCount: MaxCountDefault, MaxDelay: MaxDelayDefault, PollInterval: PollIntervalDefault}
	if opts != nil {
		if opts.MaxCount != 0 {
			o.MaxCount = opts.MaxCount
		}
		if opts.MaxDelay != 0 {
			o.MaxDelay = opts.MaxDelay
		}
		if opts.PollInterval != 0 {
			o.PollInterval = opts.PollInterval
		}
	}
	if o.MaxCount <= 0 || o.MaxCount >= math.MaxInt32 {
		return o, errors.New("riverbatch: MaxCount must be > 0 and < math.MaxInt32")
	}
	if o.MaxDelay < 0 {
		return o, errors.New("riverbatch: MaxDelay must be >= 0")
	}
	if o.PollInterval < 0 {
		return o, errors.New("riverbatch: PollInterval must be >= 0")
	}
	return o, nil
}

func (w *worker[T, TTx]) Work(ctx context.Context, job *river.Job[T]) error {
	return Work[T, TTx](ctx, w.many, job, &w.opts)
}

func metadataString(data []byte, key string) string {
	if len(data) == 0 {
		return ""
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return ""
	}
	v, _ := m[key].(string)
	return v
}

func attemptedBy[T JobArgsWithBatchOpts](job *river.Job[T]) string {
	if job == nil || len(job.AttemptedBy) == 0 {
		return "riverbatch"
	}
	return job.AttemptedBy[len(job.AttemptedBy)-1]
}

func batchResult[T JobArgsWithBatchOpts](jobs []*river.Job[T], workErr error) error {
	if len(jobs) == 1 {
		if multi, ok := workErr.(*MultiError); !ok {
			return workErr
		} else {
			setJobs(multi, jobs)
			return multi
		}
	}

	if multi, ok := workErr.(*MultiError); ok {
		setJobs(multi, jobs)
		return multi
	}
	multi := NewMultiError()
	if workErr != nil {
		multi.generic = append(multi.generic, workErr)
	}
	setJobs(multi, jobs)
	return multi
}

type MultiError struct {
	generic []error
	byID    map[int64]error
	jobs    []*rivertype.JobRow
}

func NewMultiError() *MultiError { return &MultiError{byID: map[int64]error{}} }

func (e *MultiError) Add(job *rivertype.JobRow, err error) {
	if err == nil {
		return
	}
	if job == nil {
		e.generic = append(e.generic, err)
		return
	}
	e.AddByID(job.ID, err)
}

func (e *MultiError) AddByID(jobID int64, err error) {
	if err == nil {
		return
	}
	if e.byID == nil {
		e.byID = map[int64]error{}
	}
	e.byID[jobID] = err
}

func (e *MultiError) Get(job *rivertype.JobRow) error {
	if job == nil {
		return nil
	}
	return e.GetByID(job.ID)
}

func (e *MultiError) GetByID(jobID int64) error {
	if e == nil {
		return nil
	}
	return e.byID[jobID]
}

func (e *MultiError) Error() string {
	if e == nil {
		return ""
	}
	parts := make([]string, 0, len(e.generic)+len(e.byID))
	for _, err := range e.generic {
		parts = append(parts, err.Error())
	}
	ids := make([]int64, 0, len(e.byID))
	for id := range e.byID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		parts = append(parts, "job "+strconv.FormatInt(id, 10)+": "+e.byID[id].Error())
	}
	if len(parts) == 0 && len(e.jobs) > 0 {
		return "riverbatch: batch completed"
	}
	return strings.Join(parts, "; ")
}

func (e *MultiError) Is(target error) bool { _, ok := target.(*MultiError); return ok }

func (e *MultiError) Unwrap() []error {
	if e == nil {
		return nil
	}
	out := append([]error(nil), e.generic...)
	ids := make([]int64, 0, len(e.byID))
	for id := range e.byID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		out = append(out, e.byID[id])
	}
	return out
}

func (e *MultiError) Err() error {
	if e == nil || len(e.generic)+len(e.byID) == 0 {
		return nil
	}
	return e
}

func (e *MultiError) ErrorsByID() map[int64]error {
	if e == nil {
		return nil
	}
	out := make(map[int64]error, len(e.jobs))
	for _, job := range e.jobs {
		if job == nil {
			continue
		}
		errList := append([]error(nil), e.generic...)
		if err := e.byID[job.ID]; err != nil {
			errList = append(errList, err)
		}
		out[job.ID] = errors.Join(errList...)
	}
	return out
}

func (e *MultiError) Jobs() []*rivertype.JobRow {
	if e == nil {
		return nil
	}
	return e.jobs
}

func setJobs[T JobArgsWithBatchOpts](e *MultiError, jobs []*river.Job[T]) {
	e.jobs = make([]*rivertype.JobRow, 0, len(jobs))
	for _, job := range jobs {
		if job != nil && job.JobRow != nil {
			e.jobs = append(e.jobs, job.JobRow)
		}
	}
}
