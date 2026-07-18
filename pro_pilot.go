package riverpro

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivertype"

	prodriver "github.com/divyam234/riverpro/driver"
)

const (
	metadataKeyBatchKey     = "riverpro_batch_key"
	metadataKeyEphemeral    = "riverpro_ephemeral"
	metadataKeySequenceKey  = "riverpro_sequence_key"
	producerShutdownTimeout = 5 * time.Second
)

type proPilot[TTx any] struct {
	riverpilot.StandardPilot
	config *Config
	driver prodriver.ProDriver[TTx]
	params *riverpilot.PilotInitParams
}

func newProPilot[TTx any](driver prodriver.ProDriver[TTx], config *Config) *proPilot[TTx] {
	return &proPilot[TTx]{driver: driver, config: config}
}

func (p *proPilot[TTx]) PilotInit(archetype *baseservice.Archetype, params *riverpilot.PilotInitParams) {
	p.params = params
	p.StandardPilot.PilotInit(archetype, params)
}

func (p *proPilot[TTx]) JobInsertMany(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobInsertFastManyParams) ([]*riverdriver.JobInsertFastResult, error) {
	if params == nil || len(params.Jobs) == 0 {
		return p.StandardPilot.JobInsertMany(ctx, exec, params)
	}
	sequenceSeen := map[string]int{}
	sequenceKeys := make([]string, 0, len(params.Jobs))
	for _, job := range params.Jobs {
		if job == nil {
			continue
		}
		meta := metadataMap(job.Metadata)
		if p.isEphemeral(job) {
			meta[metadataKeyEphemeral] = true
		}
		if batchArgs, ok := job.Args.(JobArgsWithBatchOpts); ok {
			meta[metadataKeyBatchKey] = batchKey(job.Kind, job.EncodedArgs, batchArgs.BatchOpts().ByArgs, job.Args)
		}
		if seqArgs, ok := job.Args.(JobArgsWithSequenceOpts); ok {
			seqKey := sequenceKey(job.Queue, job.Kind, job.EncodedArgs, seqArgs.SequenceOpts(), job.Args)
			meta[metadataKeySequenceKey] = seqKey
			sequenceKeys = append(sequenceKeys, seqKey)
			active, err := sequenceActiveCount(ctx, exec, params.Schema, job.Queue, seqKey)
			if err != nil {
				return nil, err
			}
			if active+sequenceSeen[seqKey] > 0 && job.State != rivertype.JobStateScheduled {
				job.State = rivertype.JobStatePending
			}
			sequenceSeen[seqKey]++
		}
		data, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		job.Metadata = data
	}
	if len(sequenceKeys) > 0 && p.driver != nil {
		_, err := (&prodriver.Executor{Executor: exec}).SequenceAppendMany(ctx, &prodriver.SequenceAppendManyParams{Schema: params.Schema, SeqKeys: sequenceKeys})
		if err != nil {
			return nil, err
		}
	}
	return p.StandardPilot.JobInsertMany(ctx, exec, params)
}

func (p *proPilot[TTx]) JobGetAvailable(ctx context.Context, exec riverdriver.Executor, state riverpilot.ProducerState, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	if params == nil {
		return nil, nil
	}
	qc, ok := p.queueConfig(params.Queue)
	if ok && (qc.Concurrency.GlobalLimit > 0 || qc.Concurrency.LocalLimit > 0) {
		return (&prodriver.Executor{Executor: exec}).JobGetAvailableLimited(ctx, &prodriver.JobGetAvailableLimitedParams{
			JobGetAvailableParams: params,
			GlobalLimit:           int32(qc.Concurrency.GlobalLimit),
			LocalLimit:            int32(qc.Concurrency.LocalLimit),
			PartitionByArgs:       qc.Concurrency.Partition.ByArgs,
			PartitionByKind:       qc.Concurrency.Partition.ByKind,
		})
	}
	return p.StandardPilot.JobGetAvailable(ctx, exec, state, params)
}

func (p *proPilot[TTx]) JobSetStateIfRunningMany(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	updated, err := p.StandardPilot.JobSetStateIfRunningMany(ctx, exec, params)
	if err != nil || len(updated) == 0 {
		return updated, err
	}
	pe := &prodriver.Executor{Executor: exec}
	sequenceKeys := map[string]bool{}
	for _, job := range updated {
		if job == nil {
			continue
		}
		meta := metadataMap(job.Metadata)
		if key, _ := meta[metadataKeySequenceKey].(string); key != "" && isFinalState(job.State) {
			sequenceKeys[key] = true
		}
		if isTruthy(meta[metadataKeyEphemeral]) && job.State == rivertype.JobStateCompleted {
			_, _ = exec.JobDelete(ctx, &riverdriver.JobDeleteParams{ID: job.ID, Schema: schemaFromParams(params)})
		}
		if p.config != nil && p.config.DeadLetter.Enabled && job.State == rivertype.JobStateDiscarded {
			_ = copyJobToDeadLetter(ctx, exec, schemaFromParams(params), job.ID)
		}
	}
	if len(sequenceKeys) > 0 {
		keys := make([]string, 0, len(sequenceKeys))
		for key := range sequenceKeys {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		_, _ = pe.SequencePromote(ctx, &prodriver.SequencePromoteParams{Keys: keys, Schema: schemaFromParams(params), Now: ptrTimeProPilot(time.Now())})
	}
	return updated, nil
}

func (p *proPilot[TTx]) PeriodicJobGetAll(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobGetAllParams) ([]*riverpilot.PeriodicJob, error) {
	if p.config == nil || !p.config.DurablePeriodicJobs.Enabled {
		return p.StandardPilot.PeriodicJobGetAll(ctx, exec, params)
	}
	// Pro owns the table: the upstream enqueuer must do no work.
	return nil, nil
}

func (p *proPilot[TTx]) PeriodicJobKeepAliveAndReap(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobKeepAliveAndReapParams) ([]*riverpilot.PeriodicJob, error) {
	if p.config == nil || !p.config.DurablePeriodicJobs.Enabled {
		return p.StandardPilot.PeriodicJobKeepAliveAndReap(ctx, exec, params)
	}
	// Pro owns the table: the upstream enqueuer must do no work.
	return nil, nil
}

func (p *proPilot[TTx]) PeriodicJobUpsertMany(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobUpsertManyParams) ([]*riverpilot.PeriodicJob, error) {
	if p.config == nil || !p.config.DurablePeriodicJobs.Enabled {
		return p.StandardPilot.PeriodicJobUpsertMany(ctx, exec, params)
	}
	// Pro owns the table: the upstream enqueuer must do no work. User-side
	// mutations go through the durable periodic job client API.
	return nil, nil
}

func (p *proPilot[TTx]) ProducerInit(ctx context.Context, exec riverdriver.Executor, params *riverpilot.ProducerInitParams) (int64, riverpilot.ProducerState, error) {
	producer, err := (&prodriver.Executor{Executor: exec}).ProducerInsertOrUpdate(ctx, &prodriver.ProducerInsertOrUpdateParams{ID: params.ProducerID, ClientID: params.ClientID, QueueName: params.Queue, Schema: params.Schema})
	if err != nil {
		return 0, nil, err
	}
	return producer.ID, &proProducerState{}, nil
}

func (p *proPilot[TTx]) ProducerKeepAlive(ctx context.Context, exec riverdriver.Executor, params *riverdriver.ProducerKeepAliveParams) error {
	_, err := (&prodriver.Executor{Executor: exec}).ProducerKeepAlive(ctx, &prodriver.ProducerKeepAliveParams{ID: params.ID, QueueName: params.QueueName, Schema: params.Schema})
	return err
}

func (p *proPilot[TTx]) ProducerShutdown(ctx context.Context, exec riverdriver.Executor, params *riverpilot.ProducerShutdownParams) error {
	// River starts producer cleanup with very small retry deadlines (100ms,
	// 500ms, ...). A brief PostgreSQL lock can therefore emit noisy errors even
	// though cleanup succeeds on a later attempt. Producer removal is a bounded
	// shutdown operation, so give it one realistic budget independent of those
	// per-attempt deadlines.
	cleanupCtx, cancel := producerShutdownContext(ctx)
	defer cancel()
	return (&prodriver.Executor{Executor: exec}).ProducerDelete(cleanupCtx, &prodriver.ProducerDeleteParams{ID: params.ProducerID, Schema: params.Schema})
}

func producerShutdownContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), producerShutdownTimeout)
}

type proProducerState struct{}

func (s *proProducerState) JobFinish(job *rivertype.JobRow) {}

func (p *proPilot[TTx]) queueConfig(queue string) (QueueConfig, bool) {
	if queue == "" {
		queue = "default"
	}
	if p == nil || p.config == nil || p.config.ProQueues == nil {
		return QueueConfig{}, false
	}
	qc, ok := p.config.ProQueues[queue]
	return qc, ok
}

func (p *proPilot[TTx]) isEphemeral(job *riverdriver.JobInsertFastParams) bool {
	if job == nil {
		return false
	}
	if _, ok := job.Args.(JobArgsWithEphemeralOpts); ok {
		return true
	}
	qc, ok := p.queueConfig(job.Queue)
	return ok && qc.Ephemeral.Enabled
}

func metadataMap(data []byte) map[string]any {
	m := map[string]any{}
	if len(data) > 0 {
		_ = json.Unmarshal(data, &m)
	}
	return m
}

func isTruthy(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return t == "true"
	default:
		return false
	}
}

func batchKey(kind string, encodedArgs []byte, byArgs bool, args any) string {
	parts := []string{"kind=" + kind}
	if byArgs {
		parts = append(parts, "args="+selectedArgsFingerprint(encodedArgs, args, "batch"))
	}
	return stableKey(parts...)
}

func sequenceKey(queue, kind string, encodedArgs []byte, opts SequenceOpts, args any) string {
	parts := []string{}
	if opts.ByQueue {
		parts = append(parts, "queue="+queue)
	}
	if !opts.ExcludeKind {
		parts = append(parts, "kind="+kind)
	}
	if opts.ByArgs {
		parts = append(parts, "args="+selectedArgsFingerprint(encodedArgs, args, "sequence"))
	}
	if len(parts) == 0 {
		parts = append(parts, "kind="+kind)
	}
	return stableKey(parts...)
}

func selectedArgsFingerprint(encodedArgs []byte, args any, tag string) string {
	selected := selectedTaggedFields(args, tag)
	if len(selected) > 0 {
		data, _ := json.Marshal(selected)
		return string(data)
	}
	return string(encodedArgs)
}

func selectedTaggedFields(args any, tag string) map[string]any {
	out := map[string]any{}
	v := reflect.ValueOf(args)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return out
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return out
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		if !strings.Contains(f.Tag.Get("river"), tag) {
			continue
		}
		name := f.Tag.Get("json")
		if idx := strings.IndexByte(name, ','); idx >= 0 {
			name = name[:idx]
		}
		if name == "" {
			name = f.Name
		}
		if name == "-" {
			continue
		}
		out[name] = v.Field(i).Interface()
	}
	return out
}

func stableKey(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func sequenceActiveCount(ctx context.Context, exec riverdriver.Executor, schema, queue, key string) (int, error) {
	if exec == nil || key == "" {
		return 0, nil
	}
	query := fmt.Sprintf(`SELECT count(*) FROM %s WHERE queue = $1 AND metadata->>$2 = $3 AND state IN ('available','running','retryable','scheduled','pending')`, qTableName(schema, "river_job"))
	row := exec.QueryRow(ctx, query, queue, metadataKeySequenceKey, key)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func copyJobToDeadLetter(ctx context.Context, exec riverdriver.Executor, schema string, id int64) error {
	if exec == nil {
		return nil
	}
	return exec.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states, dead_lettered_at)
		SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states, now()
		FROM %s WHERE id = $1
		ON CONFLICT (id) DO NOTHING
	`, qTableName(schema, "river_job_dead_letter"), qTableName(schema, "river_job")), id)
}

func schemaFromParams(params *riverdriver.JobSetStateIfRunningManyParams) string {
	if params == nil {
		return ""
	}
	return params.Schema
}

func isFinalState(state rivertype.JobState) bool {
	switch state {
	case rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded:
		return true
	default:
		return false
	}
}

func qTableName(schema, table string) string {
	if schema == "" {
		schema = "river"
	}
	return quoteIdent(schema) + "." + quoteIdent(table)
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func ptrTimeProPilot(t time.Time) *time.Time { return &t }
