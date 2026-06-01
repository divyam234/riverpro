package riverbatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/divyam234/riverpro"
	prodriver "github.com/divyam234/riverpro/driver"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver"
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

type worker[T JobArgsWithBatchOpts] struct {
	river.WorkerDefaults[T]
	many ManyWorker[T]
	f    func(context.Context, []*river.Job[T]) error
	opts WorkerOpts
}

func Work[T JobArgsWithBatchOpts, TTx any](ctx context.Context, mw ManyWorker[T], job *river.Job[T], opts *WorkerOpts) error {
	if mw == nil {
		return errors.New("riverbatch: nil worker")
	}
	if job == nil {
		return errors.New("riverbatch: nil job")
	}
	o, err := normalizeOpts(opts)
	if err != nil {
		return err
	}
	jobs := []*river.Job[T]{job}
	if client, err := riverpro.ClientFromContextSafely[TTx](ctx); err == nil && client != nil && client.ProExecutor() != nil && o.MaxCount > 1 {
		batchKey := metadataString(job.Metadata, "riverpro_batch_key")
		if batchKey != "" {
			rows, fetchErr := client.ProExecutor().JobGetAvailableForBatch(ctx, &prodriver.JobGetAvailableForBatchParams{
				AttemptedBy:      attemptedBy(job),
				BatchKey:         batchKey,
				BatchLeaderJobID: job.ID,
				Kind:             job.Kind,
				Max:              int32(o.MaxCount - 1),
				Queue:            job.Queue,
				Schema:           client.Schema(),
			})
			if fetchErr != nil {
				return fetchErr
			}
			for _, row := range rows {
				var args T
				if err := json.Unmarshal(row.EncodedArgs, &args); err != nil {
					return err
				}
				jobs = append(jobs, &river.Job[T]{JobRow: row, Args: args})
			}
		}
	}
	err = mw.WorkMany(ctx, jobs)
	if len(jobs) > 1 {
		completeFetchedPeers(ctx, jobs[1:], err)
	}
	return err
}

func Worker[T JobArgsWithBatchOpts, TTx any](mw ManyWorker[T], opts *WorkerOpts) river.Worker[T] {
	if mw == nil {
		panic("riverbatch: nil worker")
	}
	o, err := normalizeOpts(opts)
	if err != nil {
		panic(err)
	}
	return &worker[T]{many: mw, opts: o}
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
	return &worker[T]{f: f, opts: o}, nil
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
func (w *worker[T]) Work(ctx context.Context, job *river.Job[T]) error {
	if job == nil {
		return errors.New("riverbatch: nil job")
	}
	jobs := []*river.Job[T]{job}
	if w.many != nil {
		return w.many.WorkMany(ctx, jobs)
	}
	return w.f(ctx, jobs)
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

func completeFetchedPeers[T JobArgsWithBatchOpts](ctx context.Context, jobs []*river.Job[T], workErr error) {
	if len(jobs) == 0 || workErr != nil {
		return
	}
	client, err := riverpro.ClientFromContextSafely[any](ctx)
	if err != nil || client == nil || client.ProExecutor() == nil {
		return
	}
	ids := make([]int64, 0, len(jobs))
	for _, job := range jobs {
		if job != nil {
			ids = append(ids, job.ID)
		}
	}
	if len(ids) == 0 {
		return
	}
	now := time.Now()
	states := make([]rivertype.JobState, len(ids))
	finalized := make([]*time.Time, len(ids))
	for i := range ids {
		states[i] = rivertype.JobStateCompleted
		finalized[i] = &now
	}
	_, _ = client.ProExecutor().JobSetStateIfRunningMany(ctx, &riverdriver.JobSetStateIfRunningManyParams{ID: ids, FinalizedAt: finalized, State: states, Schema: client.Schema()})
}

type MultiError struct {
	generic []error
	byID    map[int64]error
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
		parts = append(parts, fmt.Sprintf("job %d: %v", id, e.byID[id]))
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
