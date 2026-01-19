-- todo: do in the app. Both timestamps and uuids
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS job_groups (
  job_group    text PRIMARY KEY,
  locked_by    text,
  locked_until timestamptz,
  updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_group    text NOT NULL REFERENCES job_groups(job_group) ON DELETE CASCADE,

  occurred_at  timestamptz NOT NULL,
  run_after    timestamptz NOT NULL DEFAULT now(),

  type         text NOT NULL,
  payload      jsonb NOT NULL,

  attempts     int NOT NULL DEFAULT 0,
  max_attempts int NOT NULL DEFAULT 25,

  status       text NOT NULL CHECK (status IN ('queued','running','succeeded','failed','dead')),
  started_at   timestamptz,
  finished_at  timestamptz,
  last_error   text,
  
  locked_by    text,
  locked_until timestamptz
);

CREATE INDEX IF NOT EXISTS jobs_ready_idx
  ON jobs (status, run_after, job_group, occurred_at, id);

CREATE INDEX IF NOT EXISTS jobs_group_order_idx
  ON jobs (job_group, status, occurred_at, id);

CREATE INDEX IF NOT EXISTS jobs_running_expiry_idx
  ON jobs (status, locked_until);

