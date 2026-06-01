CREATE TYPE river_job_state AS ENUM (
    'available', 'cancelled', 'completed', 'discarded', 'pending', 'retryable', 'running', 'scheduled'
);

CREATE TABLE river_job (
    id bigserial PRIMARY KEY,
    args jsonb NOT NULL DEFAULT '{}',
    attempt smallint NOT NULL DEFAULT 0,
    attempted_at timestamptz,
    attempted_by text[],
    created_at timestamptz NOT NULL DEFAULT now(),
    errors jsonb[],
    finalized_at timestamptz,
    kind text NOT NULL,
    max_attempts smallint NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}',
    priority smallint NOT NULL DEFAULT 1,
    queue text NOT NULL DEFAULT 'default',
    state river_job_state NOT NULL DEFAULT 'available',
    scheduled_at timestamptz NOT NULL DEFAULT now(),
    tags varchar(255)[] NOT NULL DEFAULT '{}',
    unique_key bytea,
    unique_states bit(8)
);

CREATE TABLE river_queue (
    name text PRIMARY KEY NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    paused_at timestamptz,
    updated_at timestamptz NOT NULL
);

CREATE TABLE river_job_dead_letter (
    LIKE river_job INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    dead_lettered_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX river_job_dead_letter_id_idx ON river_job_dead_letter(id);

CREATE TABLE river_periodic_job (
    id text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    next_run_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX river_producer_client_queue_key ON river_producer(client_id, queue_name);

CREATE TABLE river_producer (
    id bigserial PRIMARY KEY,
    client_id text NOT NULL,
    queue_name text NOT NULL,
    max_workers integer NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    paused_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE river_job_sequence (
    id bigserial PRIMARY KEY,
    key text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE river_workflow (
    id text PRIMARY KEY,
    name text,
    state text NOT NULL DEFAULT 'active',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    current_attempt integer NOT NULL DEFAULT 1,
    wait_eval_cursor_job_id bigint,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    finalized_at timestamptz
);

CREATE TABLE river_workflow_attempt (
    workflow_id text NOT NULL REFERENCES river_workflow(id) ON DELETE CASCADE,
    attempt integer NOT NULL,
    reset_history boolean NOT NULL DEFAULT false,
    retry_mode text NOT NULL DEFAULT '',
    triggered_by jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, attempt)
);

CREATE TABLE river_workflow_attempt_task (
    workflow_id text NOT NULL,
    attempt integer NOT NULL,
    task text NOT NULL,
    job_id bigint NOT NULL,
    state river_job_state NOT NULL,
    attempt_count integer NOT NULL DEFAULT 0,
    errors jsonb[] NOT NULL DEFAULT '{}',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    finalized_at timestamptz,
    PRIMARY KEY (workflow_id, attempt, task),
    FOREIGN KEY (workflow_id, attempt) REFERENCES river_workflow_attempt(workflow_id, attempt) ON DELETE CASCADE
);

CREATE TABLE river_workflow_signal (
    id bigserial PRIMARY KEY,
    workflow_id text NOT NULL REFERENCES river_workflow(id) ON DELETE CASCADE,
    attempt integer NOT NULL,
    key text NOT NULL,
    idempotency_key text NOT NULL DEFAULT '',
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX river_workflow_worklist_workflow_reason_key ON river_workflow_worklist(workflow_id, reason);
CREATE UNIQUE INDEX river_workflow_signal_idempotency_key_idx ON river_workflow_signal(workflow_id, attempt, idempotency_key) WHERE idempotency_key <> '';

CREATE TABLE river_workflow_timer (
    workflow_id text PRIMARY KEY REFERENCES river_workflow(id) ON DELETE CASCADE,
    next_fire_at timestamptz NOT NULL
);

CREATE TABLE river_workflow_worklist (
    id bigserial PRIMARY KEY,
    workflow_id text NOT NULL REFERENCES river_workflow(id) ON DELETE CASCADE,
    reason smallint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
