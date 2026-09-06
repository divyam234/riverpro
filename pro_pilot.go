package riverpro

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivertype"

	prodriver "github.com/divyam234/riverpro/driver"
	"github.com/divyam234/riverpro/riverworkflow"
)

const (
	metadataKeyBatchKey                    = "riverpro_batch_key"
	metadataKeyEphemeral                   = "riverpro_ephemeral"
	metadataKeySequenceKey                 = "riverpro_sequence_key"
	metadataKeySequenceContinueOnCancelled = "riverpro_sequence_continue_on_cancelled"
	metadataKeySequenceContinueOnDiscarded = "riverpro_sequence_continue_on_discarded"
	producerShutdownTimeout                = 5 * time.Second
)

type sequenceInsertJob struct {
	job   *riverdriver.JobInsertFastParams
	queue string
	key   string
}

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

	sequenceJobs := make([]sequenceInsertJob, 0, len(params.Jobs))
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
			if workflowID, _ := meta[riverworkflow.MetadataKeyWorkflowID].(string); workflowID != "" {
				return nil, errors.New("riverpro: sequences are not compatible with workflows")
			}
			opts := seqArgs.SequenceOpts()
			seqKey := sequenceKey(job.Queue, job.Kind, job.EncodedArgs, opts, job.Args)
			meta[metadataKeySequenceKey] = seqKey
			meta[metadataKeySequenceContinueOnCancelled] = opts.ContinueOnCancelled
			meta[metadataKeySequenceContinueOnDiscarded] = opts.ContinueOnDiscarded
			sequenceJobs = append(sequenceJobs, sequenceInsertJob{job: job, queue: job.Queue, key: seqKey})
			sequenceKeys = append(sequenceKeys, seqKey)
		}
		data, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		job.Metadata = data
	}

	activeCounts, err := sequenceActiveCounts(ctx, exec, params.Schema, sequenceJobs)
	if err != nil {
		return nil, err
	}
	sequenceSeen := make(map[string]int, len(sequenceJobs))
	for _, item := range sequenceJobs {
		lookupKey := sequenceLookupKey(item.queue, item.key)
		if activeCounts[lookupKey]+sequenceSeen[lookupKey] > 0 && item.job.State != rivertype.JobStateScheduled {
			item.job.State = rivertype.JobStatePending
		}
		sequenceSeen[lookupKey]++
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
		limitedParams := &prodriver.JobGetAvailableLimitedParams{
			JobGetAvailableParams: params,
			GlobalLimit:           int32(qc.Concurrency.GlobalLimit),
			LocalLimit:            int32(qc.Concurrency.LocalLimit),
			PartitionByArgs:       qc.Concurrency.Partition.ByArgs,
			PartitionByKind:       qc.Concurrency.Partition.ByKind,
		}
		if producerState, ok := state.(*proProducerState); ok {
			limitedParams.CurrentProducerPartitionKeys, limitedParams.CurrentProducerPartitionRunningCounts = producerState.snapshot()
		}
		jobs, err := (&prodriver.Executor{Executor: exec}).JobGetAvailableLimited(ctx, limitedParams)
		if err == nil {
			if producerState, ok := state.(*proProducerState); ok {
				producerState.add(jobs)
			}
		}
		return jobs, err
	}
	return p.StandardPilot.JobGetAvailable(ctx, exec, state, params)
}

func (p *proPilot[TTx]) JobGetStuck(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobGetStuckParams) ([]*rivertype.JobRow, error) {
	if params == nil {
		return nil, nil
	}
	staleAfter := 30 * time.Minute
	if p != nil && p.config != nil && p.config.ProducerStaleRetentionPeriod > 0 {
		staleAfter = p.config.ProducerStaleRetentionPeriod
	}
	return (&prodriver.Executor{Executor: exec}).JobGetStuckWithInactiveProducer(ctx, &prodriver.JobGetStuckWithInactiveProducerParams{
		JobGetStuckParams:    params,
		ProducerStaleHorizon: time.Now().Add(-staleAfter),
	})
}

func (p *proPilot[TTx]) JobRescueMany(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobRescueManyParams) (*struct{}, error) {
	if params == nil {
		return &struct{}{}, nil
	}
	staleAfter := 30 * time.Minute
	if p != nil && p.config != nil && p.config.ProducerStaleRetentionPeriod > 0 {
		staleAfter = p.config.ProducerStaleRetentionPeriod
	}
	err := (&prodriver.Executor{Executor: exec}).JobRescueManyWithInactiveProducer(ctx, &prodriver.JobRescueManyWithInactiveProducerParams{
		JobRescueManyParams:  params,
		ProducerStaleHorizon: time.Now().Add(-staleAfter),
	})
	return &struct{}{}, err
}

func (p *proPilot[TTx]) JobSetStateIfRunningMany(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	updated, err := p.StandardPilot.JobSetStateIfRunningMany(ctx, exec, params)
	if err != nil || len(updated) == 0 {
		return updated, err
	}
	pe := &prodriver.Executor{Executor: exec}
	sequenceKeys := map[string]bool{}
	var postErr error
	for _, job := range updated {
		if job == nil {
			continue
		}
		meta := metadataMap(job.Metadata)
		if key, _ := meta[metadataKeySequenceKey].(string); key != "" && sequenceShouldContinue(job.State, meta) {
			sequenceKeys[key] = true
		}
		if isTruthy(meta[metadataKeyEphemeral]) && job.State == rivertype.JobStateCompleted {
			if _, deleteErr := exec.JobDelete(ctx, &riverdriver.JobDeleteParams{ID: job.ID, Schema: schemaFromParams(params)}); deleteErr != nil {
				postErr = errors.Join(postErr, fmt.Errorf("delete ephemeral job %d: %w", job.ID, deleteErr))
			}
		}
	}
	if len(sequenceKeys) > 0 {
		keys := make([]string, 0, len(sequenceKeys))
		for key := range sequenceKeys {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if _, promoteErr := pe.SequencePromote(ctx, &prodriver.SequencePromoteParams{Keys: keys, Schema: schemaFromParams(params), Now: ptrTimeProPilot(time.Now())}); promoteErr != nil {
			postErr = errors.Join(postErr, fmt.Errorf("promote completed sequences: %w", promoteErr))
		}
	}
	return updated, postErr
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
	maxWorkers := int32(0)
	if p != nil && p.config != nil {
		if queueConfig, ok := p.config.Queues[params.Queue]; ok {
			maxWorkers = int32(queueConfig.MaxWorkers)
		}
	}
	producer, err := (&prodriver.Executor{Executor: exec}).ProducerInsertOrUpdate(ctx, &prodriver.ProducerInsertOrUpdateParams{
		ID: params.ProducerID, ClientID: params.ClientID, QueueName: params.Queue, MaxWorkers: maxWorkers, Schema: params.Schema,
	})
	if err != nil {
		return 0, nil, err
	}
	qc, _ := p.queueConfig(params.Queue)
	return producer.ID, &proProducerState{queue: params.Queue, partition: qc.Concurrency.Partition, running: map[string]int32{}}, nil
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

type proProducerState struct {
	mu        sync.Mutex
	queue     string
	partition PartitionConfig
	running   map[string]int32
}

func (s *proProducerState) JobFinish(job *rivertype.JobRow) {
	if s == nil || job == nil {
		return
	}
	key := concurrencyPartitionKey(job, s.partition)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running[key] <= 1 {
		delete(s.running, key)
		return
	}
	s.running[key]--
}

func (s *proProducerState) add(jobs []*rivertype.JobRow) {
	if s == nil || len(jobs) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, job := range jobs {
		if job != nil {
			s.running[concurrencyPartitionKey(job, s.partition)]++
		}
	}
}

func (s *proProducerState) snapshot() ([]string, []int32) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.running))
	for key := range s.running {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	counts := make([]int32, len(keys))
	for i, key := range keys {
		counts[i] = s.running[key]
	}
	return keys, counts
}

func concurrencyPartitionKey(job *rivertype.JobRow, partition PartitionConfig) string {
	parts := make([]string, 0, 2)
	if partition.ByKind {
		parts = append(parts, "kind="+job.Kind)
	}
	if partition.ByArgs != nil {
		args := string(job.EncodedArgs)
		if len(partition.ByArgs) > 0 {
			var raw map[string]json.RawMessage
			if json.Unmarshal(job.EncodedArgs, &raw) == nil {
				keys := append([]string(nil), partition.ByArgs...)
				sort.Strings(keys)
				var b strings.Builder
				b.WriteByte('{')
				for i, key := range keys {
					if i > 0 {
						b.WriteString(", ")
					}
					name, _ := json.Marshal(key)
					b.Write(name)
					b.WriteString(": ")
					if value, ok := raw[key]; ok {
						b.Write(value)
					} else {
						b.WriteString("null")
					}
				}
				b.WriteByte('}')
				args = b.String()
			}
		}
		parts = append(parts, "args="+args)
	}
	if len(parts) == 0 {
		return "queue=" + job.Queue
	}
	return strings.Join(parts, "|")
}

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

func sequenceActiveCounts(ctx context.Context, exec riverdriver.Executor, schema string, jobs []sequenceInsertJob) (map[string]int, error) {
	counts := make(map[string]int)
	if exec == nil || len(jobs) == 0 {
		return counts, nil
	}
	type lookup struct {
		Queue string `json:"queue"`
		Key   string `json:"key"`
	}
	unique := make(map[string]lookup, len(jobs))
	for _, item := range jobs {
		if item.key == "" {
			continue
		}
		unique[sequenceLookupKey(item.queue, item.key)] = lookup{Queue: item.queue, Key: item.key}
	}
	lookups := make([]lookup, 0, len(unique))
	for _, item := range unique {
		lookups = append(lookups, item)
	}
	payload, err := json.Marshal(lookups)
	if err != nil {
		return nil, err
	}
	type countRow struct {
		Queue string `json:"queue"`
		Key   string `json:"key"`
		Count int    `json:"count"`
	}
	var encodedCounts []byte
	err = exec.QueryRow(ctx, fmt.Sprintf(`
		WITH requested AS (
			SELECT queue, key
			FROM jsonb_to_recordset($1::jsonb) AS x(queue text, key text)
		), grouped AS (
			SELECT r.queue, r.key, count(j.id)::integer AS count
			FROM requested AS r
			LEFT JOIN %s AS j
			  ON j.queue = r.queue
			 AND j.metadata->>$2 = r.key
			 AND j.state IN ('available','running','retryable','scheduled','pending')
			GROUP BY r.queue, r.key
		)
		SELECT coalesce(json_agg(grouped ORDER BY queue, key), '[]'::json)
		FROM grouped
	`, qTableName(schema, "river_job")), payload, metadataKeySequenceKey).Scan(&encodedCounts)
	if err != nil {
		return nil, err
	}
	var grouped []countRow
	if err := json.Unmarshal(encodedCounts, &grouped); err != nil {
		return nil, err
	}
	for _, item := range grouped {
		counts[sequenceLookupKey(item.Queue, item.Key)] = item.Count
	}
	return counts, nil
}

func sequenceLookupKey(queue, key string) string {
	return queue + "\x00" + key
}

func schemaFromParams(params *riverdriver.JobSetStateIfRunningManyParams) string {
	if params == nil {
		return ""
	}
	return params.Schema
}

func sequenceShouldContinue(state rivertype.JobState, metadata map[string]any) bool {
	switch state {
	case rivertype.JobStateCompleted:
		return true
	case rivertype.JobStateCancelled:
		return isTruthy(metadata[metadataKeySequenceContinueOnCancelled])
	case rivertype.JobStateDiscarded:
		return isTruthy(metadata[metadataKeySequenceContinueOnDiscarded])
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
