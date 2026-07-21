package riverbatch

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/divyam234/riverpro"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/require"
)

type batchTestArgs struct{ Value int }

func (batchTestArgs) Kind() string                  { return "batch_test" }
func (batchTestArgs) BatchOpts() riverpro.BatchOpts { return riverpro.BatchOpts{} }

type batchTestWorker struct {
	called bool
	seen   []*river.Job[batchTestArgs]
	err    error
}

func (w *batchTestWorker) WorkMany(ctx context.Context, jobs []*river.Job[batchTestArgs]) error {
	w.called = true
	w.seen = jobs
	return w.err
}

func TestWorkDelegatesToManyWorker(t *testing.T) {
	t.Parallel()

	w := &batchTestWorker{}
	job := &river.Job[batchTestArgs]{JobRow: &rivertype.JobRow{ID: 11}, Args: batchTestArgs{Value: 7}}

	require.NoError(t, Work[batchTestArgs, any](context.Background(), w, job, nil))
	require.True(t, w.called)
	require.Equal(t, []*river.Job[batchTestArgs]{job}, w.seen)
}

func TestWorkValidatesInputsAndOpts(t *testing.T) {
	t.Parallel()

	job := &river.Job[batchTestArgs]{JobRow: &rivertype.JobRow{ID: 11}, Args: batchTestArgs{Value: 7}}

	require.ErrorContains(t, Work[batchTestArgs, any](context.Background(), nil, job, nil), "nil worker")
	require.ErrorContains(t, Work[batchTestArgs, any](context.Background(), &batchTestWorker{}, nil, nil), "nil job")
	require.ErrorContains(t, Work[batchTestArgs, any](context.Background(), &batchTestWorker{}, job, &WorkerOpts{MaxCount: 0 - 1}), "MaxCount")
	require.ErrorContains(t, Work[batchTestArgs, any](context.Background(), &batchTestWorker{}, job, &WorkerOpts{MaxCount: math.MaxInt32}), "MaxCount")
	require.ErrorContains(t, Work[batchTestArgs, any](context.Background(), &batchTestWorker{}, job, &WorkerOpts{MaxDelay: -time.Nanosecond}), "MaxDelay")
	require.ErrorContains(t, Work[batchTestArgs, any](context.Background(), &batchTestWorker{}, job, &WorkerOpts{PollInterval: -time.Nanosecond}), "PollInterval")
}

func TestWorkFuncAndWorkFuncSafely(t *testing.T) {
	t.Parallel()

	_, err := WorkFuncSafely[batchTestArgs, any](nil, nil)
	require.ErrorContains(t, err, "nil work func")

	called := false
	worker, err := WorkFuncSafely[batchTestArgs, any](func(ctx context.Context, jobs []*river.Job[batchTestArgs]) error {
		called = true
		require.Len(t, jobs, 1)
		require.EqualValues(t, 99, jobs[0].ID)
		return nil
	}, &WorkerOpts{MaxCount: 3, MaxDelay: time.Millisecond, PollInterval: time.Millisecond})
	require.NoError(t, err)
	require.NotNil(t, worker)

	require.NoError(t, worker.Work(context.Background(), &river.Job[batchTestArgs]{JobRow: &rivertype.JobRow{ID: 99}}))
	require.True(t, called)

	require.Panics(t, func() { WorkFunc[batchTestArgs, any](nil, nil) })
}

func TestWorkerWrapper(t *testing.T) {
	t.Parallel()

	w := &batchTestWorker{}
	worker := Worker[batchTestArgs, any](w, nil)
	require.NoError(t, worker.Work(context.Background(), &river.Job[batchTestArgs]{JobRow: &rivertype.JobRow{ID: 22}}))
	require.True(t, w.called)
	require.Panics(t, func() { Worker[batchTestArgs, any](nil, nil) })
}

func TestMultiError(t *testing.T) {
	t.Parallel()

	errOne := errors.New("one")
	errTwo := errors.New("two")
	errGeneric := errors.New("generic")

	multi := NewMultiError()
	require.Nil(t, multi.Err())
	multi.AddByID(2, errTwo)
	multi.AddByID(1, errOne)
	multi.Add(nil, errGeneric)
	multi.Add(&rivertype.JobRow{ID: 3}, nil)

	require.ErrorIs(t, multi, &MultiError{})
	require.Equal(t, errOne, multi.GetByID(1))
	require.Equal(t, errTwo, multi.Get(&rivertype.JobRow{ID: 2}))
	require.Nil(t, multi.Get(nil))
	require.Equal(t, multi, multi.Err())

	msg := multi.Error()
	require.True(t, strings.Contains(msg, "generic"))
	require.True(t, strings.Contains(msg, "job 1: one"))
	require.True(t, strings.Contains(msg, "job 2: two"))
	require.Len(t, multi.Unwrap(), 3)

	var nilMulti *MultiError
	require.Empty(t, nilMulti.Error())
	require.Nil(t, nilMulti.Err())
	require.Nil(t, nilMulti.Unwrap())
}

func TestBatchResultCompletesAllJobsOnSuccess(t *testing.T) {
	t.Parallel()

	jobs := []*river.Job[batchTestArgs]{
		{JobRow: &rivertype.JobRow{ID: 1}},
		{JobRow: &rivertype.JobRow{ID: 2}},
	}
	err := batchResult(jobs, nil)
	require.Error(t, err)

	result, ok := err.(interface {
		ErrorsByID() map[int64]error
		Jobs() []*rivertype.JobRow
	})
	require.True(t, ok)
	require.Equal(t, []*rivertype.JobRow{jobs[0].JobRow, jobs[1].JobRow}, result.Jobs())
	require.Contains(t, result.ErrorsByID(), int64(1))
	require.Contains(t, result.ErrorsByID(), int64(2))
	require.NoError(t, result.ErrorsByID()[1])
	require.NoError(t, result.ErrorsByID()[2])
}

func TestBatchResultAppliesRegularErrorToEveryJob(t *testing.T) {
	t.Parallel()

	workErr := errors.New("batch failed")
	jobs := []*river.Job[batchTestArgs]{
		{JobRow: &rivertype.JobRow{ID: 1}},
		{JobRow: &rivertype.JobRow{ID: 2}},
	}
	result := batchResult(jobs, workErr).(interface {
		ErrorsByID() map[int64]error
	})
	require.ErrorIs(t, result.ErrorsByID()[1], workErr)
	require.ErrorIs(t, result.ErrorsByID()[2], workErr)
}

func TestBatchResultPreservesPerJobErrors(t *testing.T) {
	t.Parallel()

	jobErr := errors.New("job failed")
	jobs := []*river.Job[batchTestArgs]{
		{JobRow: &rivertype.JobRow{ID: 1}},
		{JobRow: &rivertype.JobRow{ID: 2}},
	}
	multi := NewMultiError()
	multi.AddByID(2, jobErr)

	result := batchResult(jobs, multi).(interface {
		ErrorsByID() map[int64]error
	})
	require.NoError(t, result.ErrorsByID()[1])
	require.ErrorIs(t, result.ErrorsByID()[2], jobErr)
}
