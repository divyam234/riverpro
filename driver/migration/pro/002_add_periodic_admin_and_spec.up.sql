-- River Pro migration 002: full durable periodic job spec + admin support.
--
-- Adds to river_periodic_job:
--   * paused_at           — operator can pause a job to stop scheduling it
--   * kind / args / queue / priority / max_attempts / tags
--                         — the full job spec so the Pro enqueuer can build
--                           a river_job row without any Go callback
--   * cron_expression / cron_timezone
--                         — optional schedule; when set, the enqueuer loop
--                           computes next_run_at by parsing cron. NULL cron
--                           means a one-shot at next_run_at (row is deleted
--                           after the first fire).
--
-- The partial index over (next_run_at) WHERE paused_at IS NULL keeps the
-- enqueue-loop scan cheap as the table grows.

ALTER TABLE /* TEMPLATE: schema */river_periodic_job
    ADD COLUMN IF NOT EXISTS paused_at timestamptz,
    ADD COLUMN IF NOT EXISTS kind text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS args jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS queue text NOT NULL DEFAULT 'default',
    ADD COLUMN IF NOT EXISTS priority smallint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_attempts smallint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS tags text[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS cron_expression text,
    ADD COLUMN IF NOT EXISTS cron_timezone text NOT NULL DEFAULT 'UTC';

CREATE INDEX IF NOT EXISTS river_periodic_job_active_next_run_at_idx
    ON /* TEMPLATE: schema */river_periodic_job(next_run_at, id)
    WHERE paused_at IS NULL;

CREATE INDEX IF NOT EXISTS river_periodic_job_kind_idx
    ON /* TEMPLATE: schema */river_periodic_job(kind)
    WHERE kind <> '';
