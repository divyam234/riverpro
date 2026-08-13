CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */river_job_sequence_inbox (
    id bigserial PRIMARY KEY,
    key text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT river_job_sequence_inbox_key_length CHECK (char_length(key) > 0 AND char_length(key) < 512)
);

CREATE INDEX IF NOT EXISTS river_job_sequence_inbox_key_id_idx
    ON /* TEMPLATE: schema */river_job_sequence_inbox(key, id);
