-- River Pro clean-room sqlc query source.
-- These queries mirror the documented Pro driver executor surface. The in-tree
-- compatibility driver currently delegates much of the behavior through River's
-- OSS executor, but these query definitions make the Pro persistence contract
-- explicit and ready for generation with sqlc.

-- name: JobDeadLetterDeleteByID :one
DELETE FROM /* TEMPLATE: schema */river_job_dead_letter
WHERE id = @id
RETURNING *;

-- name: JobDeadLetterGetAll :many
SELECT *
FROM /* TEMPLATE: schema */river_job_dead_letter
ORDER BY finalized_at DESC NULLS LAST, id DESC;

-- name: JobDeadLetterGetByID :one
SELECT *
FROM /* TEMPLATE: schema */river_job_dead_letter
WHERE id = @id;

-- name: JobDeadLetterMoveByID :one
WITH moved AS (
    DELETE FROM /* TEMPLATE: schema */river_job_dead_letter AS d
    WHERE d.id = @id
    RETURNING d.*
), inserted AS (
    INSERT INTO /* TEMPLATE: schema */river_job (
        id, args, attempt, attempted_at, attempted_by, created_at, errors,
        finalized_at, kind, max_attempts, metadata, priority, queue, state,
        scheduled_at, tags, unique_key, unique_states
    )
    SELECT
        moved.id, moved.args, moved.attempt, NULL, NULL, moved.created_at, moved.errors,
        NULL, moved.kind, moved.max_attempts, moved.metadata, moved.priority, moved.queue, 'available',
        now(), moved.tags, moved.unique_key, moved.unique_states
    FROM moved
    ON CONFLICT (id) DO UPDATE SET
        state = 'available', finalized_at = NULL, scheduled_at = now()
    RETURNING *
)
SELECT * FROM inserted;

-- name: JobDeadLetterMoveDiscarded :many
WITH to_move AS (
    SELECT d.id
    FROM /* TEMPLATE: schema */river_job_dead_letter AS d
    WHERE d.finalized_at < @discarded_finalized_at_horizon
    ORDER BY d.finalized_at ASC NULLS LAST, d.id ASC
    LIMIT @max::integer
), moved AS (
    DELETE FROM /* TEMPLATE: schema */river_job_dead_letter d
    USING to_move
    WHERE d.id = to_move.id
    RETURNING d.*
), inserted AS (
    INSERT INTO /* TEMPLATE: schema */river_job (
        id, args, attempt, attempted_at, attempted_by, created_at, errors,
        finalized_at, kind, max_attempts, metadata, priority, queue, state,
        scheduled_at, tags, unique_key, unique_states
    )
    SELECT
        moved.id, moved.args, moved.attempt, NULL, NULL, moved.created_at, moved.errors,
        NULL, moved.kind, moved.max_attempts, moved.metadata, moved.priority, moved.queue, 'available',
        now(), moved.tags, moved.unique_key, moved.unique_states
    FROM moved
    ON CONFLICT (id) DO UPDATE SET
        state = 'available', finalized_at = NULL, scheduled_at = now()
    RETURNING *
)
SELECT * FROM inserted;

-- name: JobDeleteByIDMany :many
DELETE FROM /* TEMPLATE: schema */river_job
WHERE id = ANY(@ids::bigint[])
  AND state != 'running'
RETURNING *;

-- name: JobDeleteNonWorkflowBefore :execrows
DELETE FROM /* TEMPLATE: schema */river_job
WHERE NOT (metadata ? 'workflow_id')
  AND (
    (sqlc.arg(cancelled_do_delete)::boolean AND state = 'cancelled' AND finalized_at < sqlc.arg(cancelled_finalized_at_horizon)::timestamptz) OR
    (sqlc.arg(completed_do_delete)::boolean AND state = 'completed' AND finalized_at < sqlc.arg(completed_finalized_at_horizon)::timestamptz) OR
    (sqlc.arg(discarded_do_delete)::boolean AND state = 'discarded' AND finalized_at < sqlc.arg(discarded_finalized_at_horizon)::timestamptz)
  )
  AND (cardinality(sqlc.arg(queues_included)::text[]) = 0 OR queue = ANY(sqlc.arg(queues_included)::text[]))
  AND (cardinality(sqlc.arg(queues_excluded)::text[]) = 0 OR queue <> ALL(sqlc.arg(queues_excluded)::text[]));

-- name: JobGetAvailableForBatch :many
WITH candidates AS (
    SELECT j.id
    FROM /* TEMPLATE: schema */river_job AS j
    WHERE j.state = 'available'
      AND j.queue = @queue
      AND j.kind = @kind
      AND j.metadata->>'batch_id' = @batch_id
      AND (@batch_key::text = '' OR j.metadata->>'batch_key' = @batch_key)
      AND j.id <> @batch_leader_job_id
    ORDER BY j.priority ASC, j.scheduled_at ASC, j.id ASC
    LIMIT @max::integer
    FOR UPDATE SKIP LOCKED
)
UPDATE /* TEMPLATE: schema */river_job AS j
SET state = 'running', attempted_at = now(), attempted_by = array_append(coalesce(j.attempted_by, '{}'), @attempted_by)
FROM candidates
WHERE j.id = candidates.id
RETURNING j.*;

-- name: JobGetAvailableLimited :many
WITH input_args AS MATERIALIZED (
    SELECT sqlc.arg(current_producer_partition_keys)::text[] AS current_keys,
           sqlc.arg(current_producer_partition_running_counts)::integer[] AS current_counts
), candidates AS MATERIALIZED (
    SELECT j.id
    FROM /* TEMPLATE: schema */river_job AS j
    WHERE j.state = 'available'::/* TEMPLATE: schema */river_job_state
      AND j.queue = @queue
      AND j.scheduled_at <= @now
      AND (sqlc.arg(partition_by_kind)::boolean OR true)
      AND (sqlc.arg(partition_by_args)::text[] IS NOT NULL OR true)
      AND (coalesce(cardinality(sqlc.arg(available_partition_keys)::text[]), 0) = 0 OR true)
      AND (coalesce((SELECT cardinality(current_keys) FROM input_args), 0) >= 0 OR true)
    ORDER BY j.priority ASC, j.scheduled_at ASC, j.id ASC
    LIMIT least(
        @max_to_lock::integer,
        CASE WHEN @global_limit::integer > 0 THEN @global_limit::integer ELSE @max_to_lock::integer END,
        CASE WHEN @local_limit::integer > 0 THEN @local_limit::integer ELSE @max_to_lock::integer END
    )
    FOR UPDATE SKIP LOCKED
)
UPDATE /* TEMPLATE: schema */river_job AS j
SET state = 'running'::/* TEMPLATE: schema */river_job_state,
    attempt = j.attempt + 1,
    attempted_at = @now,
    attempted_by = array_append(
        CASE WHEN array_length(j.attempted_by, 1) >= @max_attempted_by::integer
             THEN j.attempted_by[array_length(j.attempted_by, 1) + 2 - @max_attempted_by::integer:]
             ELSE j.attempted_by
        END,
        @client_id
    )
FROM candidates
WHERE j.id = candidates.id
RETURNING j.*;

-- name: JobGetAvailablePartitionKeys :many
SELECT DISTINCT concat(queue, ':', kind)::text AS partition_key
FROM /* TEMPLATE: schema */river_job
WHERE queue = @queue
  AND state IN ('available', 'retryable')
ORDER BY partition_key;

-- name: PeriodicJobGetAll :many
SELECT *
FROM /* TEMPLATE: schema */river_periodic_job
WHERE updated_at >= @stale_updated_at_horizon
ORDER BY id
LIMIT @max::integer;

-- name: PeriodicJobGetByID :one
SELECT * FROM /* TEMPLATE: schema */river_periodic_job WHERE id = @id;

-- name: PeriodicJobKeepAliveAndReap :many
WITH keepalive AS (
    UPDATE /* TEMPLATE: schema */river_periodic_job
    SET updated_at = coalesce(sqlc.narg(now)::timestamptz, now())
    WHERE id = ANY(@ids::text[])
), reaped AS (
    DELETE FROM /* TEMPLATE: schema */river_periodic_job AS p
    WHERE p.id <> ALL(@ids::text[])
      AND p.updated_at < @stale_updated_at_horizon
    RETURNING p.*
)
SELECT * FROM reaped ORDER BY id;

-- name: PeriodicJobUpsertMany :many
INSERT INTO /* TEMPLATE: schema */river_periodic_job(id, next_run_at, updated_at)
SELECT ids.id, nexts.next_run_at, ups.updated_at
FROM unnest(sqlc.arg(ids)::text[]) WITH ORDINALITY AS ids(id, ord)
JOIN unnest(sqlc.arg(next_run_ats)::timestamptz[]) WITH ORDINALITY AS nexts(next_run_at, ord) USING (ord)
JOIN unnest(sqlc.arg(updated_ats)::timestamptz[]) WITH ORDINALITY AS ups(updated_at, ord) USING (ord)
ON CONFLICT (id) DO UPDATE SET next_run_at = excluded.next_run_at, updated_at = excluded.updated_at
RETURNING *;

-- name: PeriodicJobDelete :one
DELETE FROM /* TEMPLATE: schema */river_periodic_job
WHERE id = @id
RETURNING *;

-- name: PeriodicJobPause :one
UPDATE /* TEMPLATE: schema */river_periodic_job
SET paused_at = coalesce(sqlc.narg(paused_at)::timestamptz, now()),
    updated_at = now()
WHERE id = @id
  AND paused_at IS NULL
RETURNING *;

-- name: PeriodicJobResume :one
UPDATE /* TEMPLATE: schema */river_periodic_job
SET paused_at = NULL,
    next_run_at = CASE WHEN @set_next_run_at::boolean THEN sqlc.narg(next_run_at)::timestamptz ELSE next_run_at END,
    updated_at = now()
WHERE id = @id
  AND paused_at IS NOT NULL
RETURNING *;

-- name: ProducerDelete :exec
DELETE FROM /* TEMPLATE: schema */river_producer WHERE id = @id;

-- name: ProducerGetByID :one
SELECT * FROM /* TEMPLATE: schema */river_producer WHERE id = @id;

-- name: ProducerInsertOrUpdate :one
INSERT INTO /* TEMPLATE: schema */river_producer(id, client_id, queue_name, max_workers, metadata, paused_at, created_at, updated_at)
VALUES (@id, @client_id, @queue_name, @max_workers, @metadata, sqlc.narg(paused_at), coalesce(sqlc.narg(created_at)::timestamptz, now()), coalesce(sqlc.narg(updated_at)::timestamptz, now()))
ON CONFLICT (client_id, queue_name) DO UPDATE SET
    max_workers = excluded.max_workers,
    metadata = excluded.metadata,
    paused_at = excluded.paused_at,
    updated_at = excluded.updated_at
RETURNING *;

-- name: ProducerKeepAlive :one
UPDATE /* TEMPLATE: schema */river_producer
SET updated_at = now()
WHERE id = @id AND queue_name = @queue_name AND updated_at >= @stale_updated_at_horizon
RETURNING *;

-- name: ProducerListByQueue :many
SELECT p.*, count(j.id)::integer AS running
FROM /* TEMPLATE: schema */river_producer p
LEFT JOIN /* TEMPLATE: schema */river_job j ON j.queue = p.queue_name AND j.state = 'running'
WHERE p.queue_name = @queue_name
GROUP BY p.id
ORDER BY p.id;

-- name: ProducerUpdate :one
UPDATE /* TEMPLATE: schema */river_producer AS p
SET max_workers = CASE WHEN @max_workers_do_update::boolean THEN @max_workers ELSE p.max_workers END,
    metadata = CASE WHEN @metadata_do_update::boolean THEN @metadata ELSE p.metadata END,
    paused_at = CASE WHEN @paused_at_do_update::boolean THEN sqlc.narg(paused_at)::timestamptz ELSE p.paused_at END,
    updated_at = coalesce(sqlc.narg(updated_at)::timestamptz, now())
WHERE p.id = @id
RETURNING p.*;

-- name: QueueGetMetadataForInsert :many
SELECT name, metadata->'concurrency' AS concurrency
FROM /* TEMPLATE: schema */river_queue
WHERE name = ANY(@names::text[])
ORDER BY name;

-- name: SequenceAppendMany :execrows
INSERT INTO /* TEMPLATE: schema */river_job_sequence(key)
SELECT unnest(@seq_keys::text[])
ON CONFLICT (key) DO NOTHING;

-- name: SequenceList :many
SELECT *
FROM /* TEMPLATE: schema */river_job_sequence
ORDER BY key
LIMIT @max_count::integer;

-- name: SequencePromote :many
WITH promoted AS (
    UPDATE /* TEMPLATE: schema */river_job
    SET state = 'available', scheduled_at = coalesce(sqlc.narg(now)::timestamptz, now())
    WHERE metadata->>'sequence_key' = ANY(@keys::text[])
      AND state = 'pending'
    RETURNING metadata->>'sequence_key' AS key
)
SELECT DISTINCT key FROM promoted ORDER BY key;

-- name: SequencePromoteFromTable :execrows
WITH keys AS (
    SELECT key FROM /* TEMPLATE: schema */river_job_sequence ORDER BY key LIMIT @max::integer
)
UPDATE /* TEMPLATE: schema */river_job
SET state = 'available', scheduled_at = coalesce(sqlc.narg(now)::timestamptz, now())
WHERE metadata->>'sequence_key' IN (SELECT key FROM keys)
  AND state = 'pending';

-- name: SequenceScanAndPromoteStalled :many
SELECT key
FROM /* TEMPLATE: schema */river_job_sequence
WHERE key > @last_sequence_key
ORDER BY key
LIMIT @max::integer;

-- name: WorkflowAttemptInsert :one
INSERT INTO /* TEMPLATE: schema */river_workflow_attempt(workflow_id, attempt, reset_history, retry_mode, triggered_by)
VALUES (@workflow_id, @attempt, @reset_history, @retry_mode, @triggered_by)
ON CONFLICT (workflow_id, attempt) DO UPDATE SET
    reset_history = excluded.reset_history,
    retry_mode = excluded.retry_mode,
    triggered_by = excluded.triggered_by
RETURNING *;

-- name: WorkflowAttemptListByWorkflowID :many
SELECT * FROM /* TEMPLATE: schema */river_workflow_attempt
WHERE workflow_id = @workflow_id
ORDER BY attempt;

-- name: WorkflowAttemptTaskInsert :one
INSERT INTO /* TEMPLATE: schema */river_workflow_attempt_task(workflow_id, attempt, task, job_id, state, attempt_count, errors, metadata, finalized_at)
VALUES (@workflow_id, @attempt, @task, @job_id, @state, @attempt_count, @errors, @metadata, sqlc.narg(finalized_at))
ON CONFLICT (workflow_id, attempt, task) DO UPDATE SET
    job_id = excluded.job_id,
    state = excluded.state,
    attempt_count = excluded.attempt_count,
    errors = excluded.errors,
    metadata = excluded.metadata,
    finalized_at = excluded.finalized_at
RETURNING *;

-- name: WorkflowAttemptTaskListByWorkflowID :many
SELECT * FROM /* TEMPLATE: schema */river_workflow_attempt_task
WHERE workflow_id = @workflow_id AND attempt = @attempt
ORDER BY task;

-- name: WorkflowCancel :many
WITH jobs AS (
    SELECT j.id FROM /* TEMPLATE: schema */river_job AS j WHERE j.metadata->>'workflow_id' = @workflow_id FOR UPDATE
)
UPDATE /* TEMPLATE: schema */river_job AS j
SET state = CASE WHEN j.state = 'running' THEN j.state ELSE 'cancelled' END,
    finalized_at = CASE WHEN j.state = 'running' THEN j.finalized_at ELSE @cancel_attempted_at END,
    metadata = jsonb_set(j.metadata, '{cancel_attempted_at}', to_jsonb(@cancel_attempted_at::timestamptz), true)
FROM jobs
WHERE j.id = jobs.id AND j.state NOT IN ('completed', 'discarded', 'cancelled')
RETURNING j.*;

-- name: WorkflowCancelWithDeletedDepsMany :execrows
UPDATE /* TEMPLATE: schema */river_job
SET state = 'cancelled', finalized_at = @workflow_deps_failed_at
WHERE metadata->>'workflow_id' = ANY(@workflow_ids::text[])
  AND state = 'pending';

-- name: WorkflowCancelWithFailedDepsMany :execrows
UPDATE /* TEMPLATE: schema */river_job
SET state = 'cancelled', finalized_at = @workflow_deps_failed_at
WHERE metadata->>'workflow_id' = ANY(@workflow_ids::text[])
  AND state = 'pending';

-- name: WorkflowCleanupDeleteAttemptTasksByWorkflowIDs :exec
DELETE FROM /* TEMPLATE: schema */river_workflow_attempt_task WHERE workflow_id = ANY(@workflow_ids::text[]);

-- name: WorkflowCleanupDeleteAttemptsByWorkflowIDs :exec
DELETE FROM /* TEMPLATE: schema */river_workflow_attempt WHERE workflow_id = ANY(@workflow_ids::text[]);

-- name: WorkflowCleanupDeleteDeadLetterJobsByWorkflowIDs :exec
DELETE FROM /* TEMPLATE: schema */river_job_dead_letter WHERE metadata->>'workflow_id' = ANY(@workflow_ids::text[]);

-- name: WorkflowCleanupDeleteJobsByWorkflowIDs :exec
DELETE FROM /* TEMPLATE: schema */river_job WHERE metadata->>'workflow_id' = ANY(@workflow_ids::text[]);

-- name: WorkflowCleanupDeleteSignalsByWorkflowIDs :exec
DELETE FROM /* TEMPLATE: schema */river_workflow_signal WHERE workflow_id = ANY(@workflow_ids::text[]);

-- name: WorkflowCleanupDeleteTimersByWorkflowIDs :exec
DELETE FROM /* TEMPLATE: schema */river_workflow_timer WHERE workflow_id = ANY(@workflow_ids::text[]);

-- name: WorkflowCleanupDeleteWorkflowsByWorkflowIDs :exec
DELETE FROM /* TEMPLATE: schema */river_workflow
WHERE id = ANY(@workflow_ids::text[]) AND state = @state;

-- name: WorkflowCleanupDeleteWorklistByWorkflowIDs :exec
DELETE FROM /* TEMPLATE: schema */river_workflow_worklist WHERE workflow_id = ANY(@workflow_ids::text[]);

-- name: WorkflowCleanupListFinalizedIDs :many
SELECT id FROM /* TEMPLATE: schema */river_workflow
WHERE state = @state AND finalized_at < @finalized_before
ORDER BY finalized_at ASC, id ASC
LIMIT @limit_count::integer;

-- name: WorkflowCountIncompleteJobs :one
SELECT count(*)
FROM /* TEMPLATE: schema */river_job
WHERE metadata->>'workflow_id' = @workflow_id
  AND id <> @supervisor_job_id
  AND state NOT IN ('completed', 'cancelled', 'discarded');

-- name: WorkflowFinalizeIfCompleteMany :many
WITH stats AS (
    SELECT metadata->>'workflow_id' AS workflow_id,
           bool_and(state IN ('completed', 'cancelled', 'discarded')) AS all_finalized,
           bool_or(state = 'discarded') AS any_discarded,
           bool_or(state = 'cancelled') AS any_cancelled
    FROM /* TEMPLATE: schema */river_job
    WHERE metadata->>'workflow_id' = ANY(@workflow_ids::text[])
    GROUP BY metadata->>'workflow_id'
), updated AS (
    UPDATE /* TEMPLATE: schema */river_workflow w
    SET finalized_at = @now,
        updated_at = @now,
        state = CASE WHEN stats.any_discarded THEN 'discarded' WHEN stats.any_cancelled THEN 'cancelled' ELSE 'completed' END
    FROM stats
    WHERE w.id = stats.workflow_id AND stats.all_finalized AND w.finalized_at IS NULL
    RETURNING w.id
)
SELECT updated.id FROM updated ORDER BY updated.id;

-- name: WorkflowGetByID :one
SELECT * FROM /* TEMPLATE: schema */river_workflow WHERE id = @workflow_id;

-- name: WorkflowGetFinalizationCandidates :many
SELECT id FROM /* TEMPLATE: schema */river_workflow
WHERE id > @after_workflow_id AND finalized_at IS NULL
ORDER BY id
LIMIT @limit_count::integer;

-- name: WorkflowGetLegacyBackfillIDs :many
SELECT DISTINCT metadata->>'workflow_id' AS id
FROM /* TEMPLATE: schema */river_job
WHERE metadata ? 'workflow_id' AND metadata->>'workflow_id' > @after_workflow_id
ORDER BY id
LIMIT @limit_count::integer;

-- name: WorkflowHasWaitTasksMany :many
SELECT DISTINCT metadata->>'workflow_id' AS workflow_id
FROM /* TEMPLATE: schema */river_job
WHERE metadata->>'workflow_id' = ANY(@workflow_ids::text[])
  AND metadata ? 'workflow_wait'
ORDER BY workflow_id;

-- name: WorkflowInitFromJobs :many
INSERT INTO /* TEMPLATE: schema */river_workflow(id, name, state, metadata, current_attempt)
SELECT DISTINCT metadata->>'workflow_id', nullif(metadata->>'workflow_name', ''), 'active', '{}'::jsonb, 1
FROM /* TEMPLATE: schema */river_job
WHERE metadata->>'workflow_id' = ANY(@workflow_ids::text[])
ON CONFLICT (id) DO NOTHING
RETURNING id;

-- name: WorkflowInsertMany :exec
INSERT INTO /* TEMPLATE: schema */river_workflow(id, name)
SELECT ids.id, nullif(names.name, '')
FROM unnest(@ids::text[]) WITH ORDINALITY AS ids(id, ord)
LEFT JOIN unnest(@names::text[]) WITH ORDINALITY AS names(name, ord) USING (ord)
ON CONFLICT (id) DO NOTHING;

-- name: WorkflowJobGetByTaskName :one
SELECT * FROM /* TEMPLATE: schema */river_job
WHERE metadata->>'workflow_id' = @workflow_id
  AND metadata->>'workflow_task' = @task_name;

-- name: WorkflowJobList :many
SELECT j.*, coalesce(j.metadata->'workflow_deps', '[]'::jsonb) AS deps
FROM /* TEMPLATE: schema */river_job j
WHERE metadata->>'workflow_id' = @workflow_id
ORDER BY id
LIMIT @pagination_limit::integer OFFSET @pagination_offset::integer;

-- name: WorkflowListAll :many
SELECT w.id, w.name, w.created_at,
       count(j.id) FILTER (WHERE j.state = 'available')::integer AS count_available,
       count(j.id) FILTER (WHERE j.state = 'cancelled')::integer AS count_cancelled,
       count(j.id) FILTER (WHERE j.state = 'completed')::integer AS count_completed,
       count(j.id) FILTER (WHERE j.state = 'discarded')::integer AS count_discarded,
       0::integer AS count_failed_deps,
       count(j.id) FILTER (WHERE j.state = 'pending')::integer AS count_pending,
       count(j.id) FILTER (WHERE j.state = 'retryable')::integer AS count_retryable,
       count(j.id) FILTER (WHERE j.state = 'running')::integer AS count_running,
       count(j.id) FILTER (WHERE j.state = 'scheduled')::integer AS count_scheduled
FROM /* TEMPLATE: schema */river_workflow w
LEFT JOIN /* TEMPLATE: schema */river_job j ON j.metadata->>'workflow_id' = w.id
WHERE (@after::text = '' OR w.id > @after) AND (@before::text = '' OR w.id < @before)
GROUP BY w.id, w.name, w.created_at
ORDER BY w.id
LIMIT @pagination_limit::integer;

-- name: WorkflowListActive :many
SELECT w.id, w.name, w.created_at,
       count(j.id) FILTER (WHERE j.state = 'available')::integer AS count_available,
       count(j.id) FILTER (WHERE j.state = 'cancelled')::integer AS count_cancelled,
       count(j.id) FILTER (WHERE j.state = 'completed')::integer AS count_completed,
       count(j.id) FILTER (WHERE j.state = 'discarded')::integer AS count_discarded,
       0::integer AS count_failed_deps,
       count(j.id) FILTER (WHERE j.state = 'pending')::integer AS count_pending,
       count(j.id) FILTER (WHERE j.state = 'retryable')::integer AS count_retryable,
       count(j.id) FILTER (WHERE j.state = 'running')::integer AS count_running,
       count(j.id) FILTER (WHERE j.state = 'scheduled')::integer AS count_scheduled
FROM /* TEMPLATE: schema */river_workflow w
LEFT JOIN /* TEMPLATE: schema */river_job j ON j.metadata->>'workflow_id' = w.id
WHERE w.finalized_at IS NULL
  AND (@after::text = '' OR w.id > @after)
  AND (@before::text = '' OR w.id < @before)
GROUP BY w.id, w.name, w.created_at
ORDER BY w.id
LIMIT @pagination_limit::integer;

-- name: WorkflowListInactive :many
SELECT w.id, w.name, w.created_at,
       count(j.id) FILTER (WHERE j.state = 'available')::integer AS count_available,
       count(j.id) FILTER (WHERE j.state = 'cancelled')::integer AS count_cancelled,
       count(j.id) FILTER (WHERE j.state = 'completed')::integer AS count_completed,
       count(j.id) FILTER (WHERE j.state = 'discarded')::integer AS count_discarded,
       0::integer AS count_failed_deps,
       count(j.id) FILTER (WHERE j.state = 'pending')::integer AS count_pending,
       count(j.id) FILTER (WHERE j.state = 'retryable')::integer AS count_retryable,
       count(j.id) FILTER (WHERE j.state = 'running')::integer AS count_running,
       count(j.id) FILTER (WHERE j.state = 'scheduled')::integer AS count_scheduled
FROM /* TEMPLATE: schema */river_workflow w
LEFT JOIN /* TEMPLATE: schema */river_job j ON j.metadata->>'workflow_id' = w.id
WHERE w.finalized_at IS NOT NULL
  AND (@after::text = '' OR w.id > @after)
  AND (@before::text = '' OR w.id < @before)
GROUP BY w.id, w.name, w.created_at
ORDER BY w.id
LIMIT @pagination_limit::integer;

-- name: WorkflowListByIDs :many
SELECT id FROM /* TEMPLATE: schema */river_workflow WHERE id = ANY(@workflow_ids::text[]) ORDER BY id;

-- name: WorkflowListByIDsForWaitEval :many
SELECT w.id, w.metadata, w.current_attempt, w.created_at
FROM /* TEMPLATE: schema */river_workflow AS w
WHERE w.id = ANY(@workflow_ids::text[])
ORDER BY w.id;

-- name: WorkflowLoadDepTasksAndIDs :many
SELECT metadata->>'workflow_task' AS task, id
FROM /* TEMPLATE: schema */river_job
WHERE metadata->>'workflow_id' = @workflow_id
  AND (metadata->'workflow_deps') ? @task;

-- name: WorkflowLoadJobsWithDeps :many
SELECT j.*, coalesce(j.metadata->'workflow_deps', '[]'::jsonb) AS deps
FROM /* TEMPLATE: schema */river_job j
WHERE id = ANY(@job_ids::bigint[])
ORDER BY id;

-- name: WorkflowLoadTaskNamesByWorkflowID :many
SELECT metadata->>'workflow_task' AS task
FROM /* TEMPLATE: schema */river_job
WHERE metadata->>'workflow_id' = @workflow_id
ORDER BY task;

-- name: WorkflowLoadTaskWithDeps :one
SELECT j.*, coalesce(j.metadata->'workflow_deps', '[]'::jsonb) AS deps
FROM /* TEMPLATE: schema */river_job j
WHERE metadata->>'workflow_id' = @workflow_id
  AND metadata->>'workflow_task' = @task;

-- name: WorkflowLoadTasksByNames :many
SELECT id, metadata->>'workflow_task' AS task, state, metadata->'workflow_deps' AS deps, metadata->>'workflow_id' AS workflow_id
FROM /* TEMPLATE: schema */river_job
WHERE metadata->>'workflow_id' = @workflow_id
  AND metadata->>'workflow_task' = ANY(@task_names::text[])
ORDER BY task;

-- name: WorkflowLockByIDsSkipLocked :many
SELECT id FROM /* TEMPLATE: schema */river_workflow
WHERE id = ANY(@workflow_ids::text[])
ORDER BY id
LIMIT @limit_count::integer
FOR UPDATE SKIP LOCKED;

-- name: WorkflowReadyTaskIDsByWorkflowIDs :many
WITH workflow_jobs AS (
    SELECT id, metadata->>'workflow_id' workflow_id, metadata->>'workflow_task' task, metadata->'workflow_deps' deps, state
    FROM /* TEMPLATE: schema */river_job
    WHERE metadata->>'workflow_id' = ANY(@workflow_ids::text[])
), ready AS (
    SELECT j.id, j.workflow_id
    FROM workflow_jobs j
    WHERE j.state = 'pending'
      AND NOT EXISTS (
        SELECT 1 FROM jsonb_array_elements_text(coalesce(j.deps, '[]'::jsonb)) dep(name)
        LEFT JOIN workflow_jobs d ON d.workflow_id = j.workflow_id AND d.task = dep.name
        WHERE d.state IS DISTINCT FROM 'completed'
      )
)
SELECT id, workflow_id, count(*) OVER () AS total_count
FROM ready
ORDER BY id
LIMIT @limit_count::integer;

-- name: WorkflowRetryLockAndCheckRunning :one
SELECT EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */river_job AS j
    WHERE j.metadata->>'workflow_id' = @workflow_id AND j.state IN ('available', 'pending', 'retryable', 'running', 'scheduled')
) AS workflow_is_active
FROM /* TEMPLATE: schema */river_workflow AS w
WHERE w.id = @workflow_id
FOR UPDATE;

-- name: WorkflowRetry :many
WITH jobs AS (
    UPDATE /* TEMPLATE: schema */river_job AS j
    SET state = CASE WHEN j.metadata->'workflow_deps' = '[]'::jsonb OR j.metadata->'workflow_deps' IS NULL THEN 'available' ELSE 'pending' END,
        finalized_at = NULL,
        scheduled_at = @now,
        errors = '{}'
    WHERE j.metadata->>'workflow_id' = @workflow_id
      AND (@mode = 'all' OR j.state IN ('discarded', 'cancelled'))
    RETURNING j.*
), wf AS (
    UPDATE /* TEMPLATE: schema */river_workflow AS w
    SET current_attempt = w.current_attempt + 1, finalized_at = NULL, state = 'active', updated_at = @now
    WHERE w.id = @workflow_id
)
SELECT * FROM jobs ORDER BY id;

-- name: WorkflowSignalInsert :one
WITH wf AS (
    SELECT w.id, w.current_attempt FROM /* TEMPLATE: schema */river_workflow AS w WHERE w.id = @workflow_id
), inserted AS (
    INSERT INTO /* TEMPLATE: schema */river_workflow_signal(workflow_id, attempt, key, idempotency_key, payload, metadata, source)
    SELECT @workflow_id, coalesce(sqlc.narg(requested_attempt)::integer, wf.current_attempt), @key, @idempotency_key, @payload, @metadata, @source
    FROM wf
    ON CONFLICT (workflow_id, attempt, idempotency_key) WHERE idempotency_key <> '' DO NOTHING
    RETURNING *, (SELECT current_attempt FROM wf) AS current_attempt
)
SELECT i.id,
       i.workflow_id,
       i.attempt,
       i.key,
       i.idempotency_key,
       i.payload,
       i.metadata,
       i.source,
       i.created_at,
       i.current_attempt,
       true::boolean AS payload_semantic_equal,
       true::boolean AS signal_present,
       false::boolean AS skipped_as_duplicate
FROM inserted AS i;

-- name: WorkflowSignalList :many
SELECT * FROM /* TEMPLATE: schema */river_workflow_signal
WHERE workflow_id = @workflow_id
  AND (sqlc.narg(attempt)::integer IS NULL OR attempt = sqlc.narg(attempt)::integer)
  AND (sqlc.narg(key)::text IS NULL OR key = sqlc.narg(key)::text)
  AND (sqlc.narg(cursor_id)::bigint IS NULL OR CASE WHEN @sort_desc::boolean THEN id < sqlc.narg(cursor_id)::bigint ELSE id > sqlc.narg(cursor_id)::bigint END)
ORDER BY CASE WHEN @sort_desc::boolean THEN -id ELSE id END
LIMIT @limit_count::integer;

-- name: WorkflowSignalListByEvidence :many
SELECT * FROM /* TEMPLATE: schema */river_workflow_signal
WHERE workflow_id = @workflow_id
  AND attempt = @attempt
  AND (cardinality(@keys::text[]) = 0 OR key = ANY(@keys::text[]))
  AND (cardinality(@last_included_signal_ids::bigint[]) = 0 OR id <> ALL(@last_included_signal_ids::bigint[]))
  AND (sqlc.narg(cursor_id)::bigint IS NULL OR CASE WHEN @sort_desc::boolean THEN id < sqlc.narg(cursor_id)::bigint ELSE id > sqlc.narg(cursor_id)::bigint END)
ORDER BY CASE WHEN @sort_desc::boolean THEN -id ELSE id END
LIMIT @limit_count::integer;

-- name: WorkflowSignalListByKeys :many
SELECT * FROM /* TEMPLATE: schema */river_workflow_signal
WHERE workflow_id = @workflow_id
  AND key = ANY(@keys::text[])
  AND (sqlc.narg(attempt)::integer IS NULL OR attempt = sqlc.narg(attempt)::integer)
  AND (sqlc.narg(cursor_id)::bigint IS NULL OR CASE WHEN @sort_desc::boolean THEN id < sqlc.narg(cursor_id)::bigint ELSE id > sqlc.narg(cursor_id)::bigint END)
ORDER BY CASE WHEN @sort_desc::boolean THEN -id ELSE id END
LIMIT @limit_count::integer;

-- name: WorkflowSignalListByWorkflowIDs :many
SELECT * FROM /* TEMPLATE: schema */river_workflow_signal
WHERE workflow_id = ANY(@workflow_ids::text[])
  AND attempt = @attempt
  AND (cardinality(@keys::text[]) = 0 OR key = ANY(@keys::text[]))
ORDER BY workflow_id, id;

-- name: WorkflowSignalStatsByWorkflowIDs :many
SELECT workflow_id, key, count(*) AS signal_count, max(id) AS last_signal_id
FROM /* TEMPLATE: schema */river_workflow_signal
WHERE workflow_id = ANY(@workflow_ids::text[])
  AND attempt = @attempt
  AND (cardinality(@keys::text[]) = 0 OR key = ANY(@keys::text[]))
GROUP BY workflow_id, key
ORDER BY workflow_id, key;

-- name: WorkflowStageJobsByIDMany :many
UPDATE /* TEMPLATE: schema */river_job
SET state = 'available', scheduled_at = @workflow_staged_at
WHERE id = ANY(@job_ids::bigint[])
  AND state = 'pending'
RETURNING *;

-- name: WorkflowTimerConsumeDue :many
DELETE FROM /* TEMPLATE: schema */river_workflow_timer AS t
WHERE t.workflow_id IN (
    SELECT ti.workflow_id FROM /* TEMPLATE: schema */river_workflow_timer AS ti
    WHERE ti.next_fire_at <= @as_of
    ORDER BY ti.next_fire_at, ti.workflow_id
    LIMIT @limit_count::integer
)
RETURNING t.*;

-- name: WorkflowTimerDeleteByWorkflowIDs :exec
DELETE FROM /* TEMPLATE: schema */river_workflow_timer WHERE workflow_id = ANY(@workflow_ids::text[]);

-- name: WorkflowTimerGetByWorkflowID :one
SELECT * FROM /* TEMPLATE: schema */river_workflow_timer WHERE workflow_id = @workflow_id;

-- name: WorkflowTimerNextFireAtByWorkflowIDs :many
SELECT workflow_id, next_fire_at
FROM /* TEMPLATE: schema */river_workflow_timer
WHERE workflow_id = ANY(@workflow_ids::text[]) AND next_fire_at > @now
ORDER BY next_fire_at, workflow_id;

-- name: WorkflowTimerUpsertMany :exec
INSERT INTO /* TEMPLATE: schema */river_workflow_timer(workflow_id, next_fire_at)
SELECT ids.workflow_id, nexts.next_fire_at
FROM unnest(sqlc.arg(workflow_ids)::text[]) WITH ORDINALITY AS ids(workflow_id, ord)
JOIN unnest(sqlc.arg(next_fire_ats)::timestamptz[]) WITH ORDINALITY AS nexts(next_fire_at, ord) USING (ord)
ON CONFLICT (workflow_id) DO UPDATE SET next_fire_at = excluded.next_fire_at;

-- name: WorkflowUnfinalizeIfActiveJobsMany :many
WITH active AS (
    SELECT DISTINCT metadata->>'workflow_id' AS workflow_id
    FROM /* TEMPLATE: schema */river_job
    WHERE metadata->>'workflow_id' = ANY(@workflow_ids::text[])
      AND state IN ('available', 'pending', 'retryable', 'running', 'scheduled')
), updated AS (
    UPDATE /* TEMPLATE: schema */river_workflow w
    SET finalized_at = NULL, state = 'active', updated_at = @now
    FROM active
    WHERE w.id = active.workflow_id
    RETURNING w.id
)
SELECT updated.id FROM updated ORDER BY updated.id;

-- name: WorkflowWaitActivatableTaskIDsByWorkflowIDs :many
SELECT j.id, j.metadata->>'workflow_id' AS workflow_id, count(*) OVER () AS total_count
FROM /* TEMPLATE: schema */river_job AS j
WHERE j.metadata->>'workflow_id' = ANY(@workflow_ids::text[])
  AND j.state = 'pending'
  AND j.metadata ? 'workflow_wait'
ORDER BY j.id
LIMIT @limit_count::integer;

-- name: WorkflowWaitActivateByJobIDMany :many
UPDATE /* TEMPLATE: schema */river_job
SET state = 'available', scheduled_at = @now
WHERE id = ANY(@job_ids::bigint[]) AND state = 'pending'
RETURNING id;

-- name: WorkflowWaitActiveTaskListByWorkflowIDs :many
SELECT j.id, j.metadata, j.metadata->>'workflow_id' AS workflow_id, count(*) OVER () AS total_count
FROM /* TEMPLATE: schema */river_job AS j
WHERE j.metadata->>'workflow_id' = ANY(@workflow_ids::text[])
  AND j.state = 'pending'
  AND j.metadata ? 'workflow_wait'
ORDER BY j.id
LIMIT @limit_count::integer;

-- name: WorkflowWaitDepOutputListByWorkflowTaskPairs :many
SELECT metadata->>'workflow_id' AS workflow_id,
       metadata->>'workflow_task' AS task,
       state,
       finalized_at,
       metadata->'workflow_output' AS output
FROM /* TEMPLATE: schema */river_job
WHERE metadata->>'workflow_id' = ANY(@workflow_ids::text[])
  AND metadata->>'workflow_task' = ANY(@tasks::text[])
ORDER BY workflow_id, task;

-- name: WorkflowWaitEvalCursorUpdateByWorkflowIDMany :exec
UPDATE /* TEMPLATE: schema */river_workflow AS w
SET wait_eval_cursor_job_id = pairs.cursor_job_id,
    updated_at = now()
FROM (
    SELECT ids.workflow_id, cursors.cursor_job_id
    FROM unnest(sqlc.arg(workflow_ids)::text[]) WITH ORDINALITY AS ids(workflow_id, ord)
    JOIN unnest(sqlc.arg(cursor_job_ids)::bigint[]) WITH ORDINALITY AS cursors(cursor_job_id, ord) USING (ord)
) AS pairs
WHERE w.id = pairs.workflow_id;

-- name: WorkflowWaitUpdateMetadataByJobIDMany :exec
UPDATE /* TEMPLATE: schema */river_job AS j
SET metadata = jsonb_set(j.metadata, '{workflow_wait_state}', pairs.wait_state::jsonb, true)
FROM (
    SELECT ids.job_id, states.wait_state
    FROM unnest(sqlc.arg(job_ids)::bigint[]) WITH ORDINALITY AS ids(job_id, ord)
    JOIN unnest(sqlc.arg(wait_states)::jsonb[]) WITH ORDINALITY AS states(wait_state, ord) USING (ord)
) AS pairs
WHERE j.id = pairs.job_id;

-- name: WorkflowWorklistDeleteByWorkflowIDsReturningReasons :many
DELETE FROM /* TEMPLATE: schema */river_workflow_worklist
WHERE workflow_id = ANY(@workflow_ids::text[])
RETURNING workflow_id, reason;

-- name: WorkflowWorklistInsertMany :exec
INSERT INTO /* TEMPLATE: schema */river_workflow_worklist(workflow_id, reason)
SELECT unnest(@workflow_ids::text[]), @reason::smallint
ON CONFLICT (workflow_id, reason) DO NOTHING;

-- name: WorkflowWorklistList :many
SELECT * FROM /* TEMPLATE: schema */river_workflow_worklist
WHERE id > @after_id
ORDER BY id
LIMIT @limit_count::integer;

-- name: WorkflowWorklistListIDs :many
SELECT id, workflow_id FROM /* TEMPLATE: schema */river_workflow_worklist
WHERE id > @after_id
ORDER BY id
LIMIT @limit_count::integer;
