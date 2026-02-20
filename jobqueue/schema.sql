-- todo: do in the app. Both timestamps and uuids
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS job_groups (
  job_group    text PRIMARY KEY,
  locked_by    text,
  locked_until timestamptz,
  -- TODO: what is this used for?
  updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_group    text NOT NULL REFERENCES job_groups(job_group) ON DELETE CASCADE,

  ordering_seq        bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
  run_after    timestamptz NOT NULL DEFAULT now(),

  type         text NOT NULL,
  payload      jsonb NOT NULL,

  attempts     int NOT NULL DEFAULT 0,
  max_attempts int NOT NULL DEFAULT 25,

  -- TODO: make status an enum
  status       text NOT NULL CHECK (status IN ('queued','running','succeeded','failed','dead')),
  started_at   timestamptz,
  finished_at  timestamptz,
  last_error   text,
  
  -- TODO: why are these used?
  locked_by    text,
  locked_until timestamptz
);

CREATE INDEX IF NOT EXISTS jobs_ready_idx
  ON jobs (status, run_after, job_group, ordering_seq, id);

CREATE INDEX IF NOT EXISTS jobs_group_order_idx
  ON jobs (job_group, status, ordering_seq, id);

CREATE INDEX IF NOT EXISTS jobs_running_expiry_idx
  ON jobs (status, locked_until);

CREATE INDEX IF NOT EXISTS available_job_groups_idx
  ON job_groups (locked_until)
  WHERE locked_until IS NULL;
  -- WHERE locked_until IS NULL OR locked_until < now(); -- now() is not immutable so postgres does not accept this index predicate :(. Can we fix this?

