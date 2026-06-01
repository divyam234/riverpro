-- River Pro compatibility schema.
--
-- This clean-room migration line stores the durable state needed by the Pro API
-- surface implemented in this repository: batches/dead-letter helpers,
-- sequences, durable periodic jobs, producer heartbeats, and workflow runtime
-- metadata. It intentionally uses additive tables alongside River OSS tables.

CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_job_dead_letter (
    LIKE /* TEMPLATE: schema */river_job INCLUDING DEFAULTS INCLUDING CONSTRAINTS
);

ALTER TABLE /* TEMPLATE: schema */river_job_dead_letter
    ADD COLUMN IF NOT EXISTS dead_lettered_at timestamptz NOT NULL DEFAULT now();

CREATE UNIQUE INDEX IF NOT EXISTS river_job_dead_letter_id_idx
    ON /* TEMPLATE: schema */river_job_dead_letter(id);

CREATE INDEX IF NOT EXISTS river_job_dead_letter_finalized_at_idx
    ON /* TEMPLATE: schema */river_job_dead_letter(finalized_at, id);

CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_periodic_job (
    id text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    next_run_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT river_periodic_job_id_length CHECK (char_length(id) > 0 AND char_length(id) < 512)
);

CREATE INDEX IF NOT EXISTS river_periodic_job_next_run_at_idx
    ON /* TEMPLATE: schema */river_periodic_job(next_run_at, id);

CREATE INDEX IF NOT EXISTS river_periodic_job_updated_at_idx
    ON /* TEMPLATE: schema */river_periodic_job(updated_at, id);

CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_producer (
    id bigserial PRIMARY KEY,
    client_id text NOT NULL,
    queue_name text NOT NULL,
    max_workers integer NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    paused_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT river_producer_client_id_length CHECK (char_length(client_id) > 0 AND char_length(client_id) < 128),
    CONSTRAINT river_producer_queue_name_length CHECK (char_length(queue_name) > 0 AND char_length(queue_name) < 128),
    CONSTRAINT river_producer_max_workers_nonnegative CHECK (max_workers >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS river_producer_client_queue_idx
    ON /* TEMPLATE: schema */river_producer(client_id, queue_name);

CREATE INDEX IF NOT EXISTS river_producer_queue_updated_at_idx
    ON /* TEMPLATE: schema */river_producer(queue_name, updated_at, id);

CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_job_sequence (
    id bigserial PRIMARY KEY,
    key text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT river_job_sequence_key_length CHECK (char_length(key) > 0 AND char_length(key) < 512)
);

CREATE INDEX IF NOT EXISTS river_job_sequence_key_idx
    ON /* TEMPLATE: schema */river_job_sequence(key);

CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_workflow (
    id text PRIMARY KEY,
    name text,
    state text NOT NULL DEFAULT 'active',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    current_attempt integer NOT NULL DEFAULT 1,
    wait_eval_cursor_job_id bigint,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    finalized_at timestamptz,
    CONSTRAINT river_workflow_id_length CHECK (char_length(id) > 0 AND char_length(id) < 512),
    CONSTRAINT river_workflow_name_length CHECK (name IS NULL OR char_length(name) < 512),
    CONSTRAINT river_workflow_current_attempt_positive CHECK (current_attempt > 0),
    CONSTRAINT river_workflow_state_known CHECK (state IN ('active', 'cancelled', 'completed', 'discarded', 'failed', 'retryable'))
);

CREATE INDEX IF NOT EXISTS river_workflow_state_id_idx
    ON /* TEMPLATE: schema */river_workflow(state, id);

CREATE INDEX IF NOT EXISTS river_workflow_finalized_at_idx
    ON /* TEMPLATE: schema */river_workflow(finalized_at, id)
    WHERE finalized_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_workflow_attempt (
    workflow_id text NOT NULL REFERENCES /* TEMPLATE: schema */river_workflow(id) ON DELETE CASCADE,
    attempt integer NOT NULL,
    reset_history boolean NOT NULL DEFAULT false,
    retry_mode text NOT NULL DEFAULT '',
    triggered_by jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, attempt),
    CONSTRAINT river_workflow_attempt_positive CHECK (attempt > 0)
);

CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_workflow_attempt_task (
    workflow_id text NOT NULL,
    attempt integer NOT NULL,
    task text NOT NULL,
    job_id bigint NOT NULL,
    state /* TEMPLATE: schema */river_job_state NOT NULL,
    attempt_count integer NOT NULL DEFAULT 0,
    errors jsonb[] NOT NULL DEFAULT '{}',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    finalized_at timestamptz,
    PRIMARY KEY (workflow_id, attempt, task),
    FOREIGN KEY (workflow_id, attempt) REFERENCES /* TEMPLATE: schema */river_workflow_attempt(workflow_id, attempt) ON DELETE CASCADE,
    CONSTRAINT river_workflow_attempt_task_name_length CHECK (char_length(task) > 0 AND char_length(task) < 512),
    CONSTRAINT river_workflow_attempt_task_attempt_count_nonnegative CHECK (attempt_count >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS river_workflow_attempt_task_job_id_idx
    ON /* TEMPLATE: schema */river_workflow_attempt_task(job_id);

CREATE INDEX IF NOT EXISTS river_workflow_attempt_task_workflow_state_idx
    ON /* TEMPLATE: schema */river_workflow_attempt_task(workflow_id, state, job_id);

CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_workflow_signal (
    id bigserial PRIMARY KEY,
    workflow_id text NOT NULL REFERENCES /* TEMPLATE: schema */river_workflow(id) ON DELETE CASCADE,
    attempt integer NOT NULL,
    key text NOT NULL,
    idempotency_key text NOT NULL DEFAULT '',
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT river_workflow_signal_attempt_positive CHECK (attempt > 0),
    CONSTRAINT river_workflow_signal_key_length CHECK (char_length(key) > 0 AND char_length(key) < 512)
);

CREATE UNIQUE INDEX IF NOT EXISTS river_workflow_signal_idempotency_idx
    ON /* TEMPLATE: schema */river_workflow_signal(workflow_id, attempt, idempotency_key)
    WHERE idempotency_key <> '';

CREATE INDEX IF NOT EXISTS river_workflow_signal_workflow_attempt_key_id_idx
    ON /* TEMPLATE: schema */river_workflow_signal(workflow_id, attempt, key, id);

CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_workflow_timer (
    workflow_id text PRIMARY KEY REFERENCES /* TEMPLATE: schema */river_workflow(id) ON DELETE CASCADE,
    next_fire_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS river_workflow_timer_next_fire_at_idx
    ON /* TEMPLATE: schema */river_workflow_timer(next_fire_at, workflow_id);

CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_workflow_worklist (
    id bigserial PRIMARY KEY,
    workflow_id text NOT NULL REFERENCES /* TEMPLATE: schema */river_workflow(id) ON DELETE CASCADE,
    reason smallint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS river_workflow_worklist_workflow_reason_idx
    ON /* TEMPLATE: schema */river_workflow_worklist(workflow_id, reason);

CREATE INDEX IF NOT EXISTS river_workflow_worklist_id_idx
    ON /* TEMPLATE: schema */river_workflow_worklist(id);
