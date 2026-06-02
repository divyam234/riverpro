DROP INDEX IF EXISTS /* TEMPLATE: schema */river_periodic_job_kind_idx;
DROP INDEX IF EXISTS /* TEMPLATE: schema */river_periodic_job_active_next_run_at_idx;

ALTER TABLE /* TEMPLATE: schema */river_periodic_job
    DROP COLUMN IF EXISTS cron_timezone,
    DROP COLUMN IF EXISTS cron_expression,
    DROP COLUMN IF EXISTS tags,
    DROP COLUMN IF EXISTS max_attempts,
    DROP COLUMN IF EXISTS priority,
    DROP COLUMN IF EXISTS queue,
    DROP COLUMN IF EXISTS args,
    DROP COLUMN IF EXISTS kind,
    DROP COLUMN IF EXISTS paused_at;
