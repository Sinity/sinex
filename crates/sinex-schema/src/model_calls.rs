//! Operational accounting survives replay and removal of response content.
//! Only the writer can inspect or modify these hash-only reservations.
pub(crate) const POLICIES: &str = "CREATE TABLE derivation.model_policies (
  policy_event uuid PRIMARY KEY,
  policy_hash bytea NOT NULL CHECK (octet_length(policy_hash) = 32),
  authorized_at timestamptz NOT NULL
)";

pub(crate) const CALLS: &str = "CREATE TABLE derivation.model_calls (
  request_hash bytea PRIMARY KEY CHECK (octet_length(request_hash) = 32),
  policy_event uuid NOT NULL,
  bucket_hash bytea NOT NULL CHECK (octet_length(bucket_hash) = 32),
  currency text NOT NULL,
  unit text NOT NULL,
  period_start timestamptz NOT NULL,
  period_end timestamptz NOT NULL CHECK (period_end > period_start),
  limit_units numeric NOT NULL CHECK (limit_units >= 0 AND limit_units = trunc(limit_units)),
  reserved_units numeric NOT NULL CHECK (reserved_units >= 0 AND reserved_units = trunc(reserved_units)),
  actual_units numeric CHECK (actual_units >= 0 AND actual_units = trunc(actual_units)),
  state text NOT NULL CHECK (state IN ('reserved', 'uncertain', 'recorded')),
  response_event uuid,
  output_hash bytea CHECK (output_hash IS NULL OR octet_length(output_hash) = 32),
  receipt_hash bytea CHECK (receipt_hash IS NULL OR octet_length(receipt_hash) = 32),
  overrun boolean NOT NULL DEFAULT false,
  reason text,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CHECK ((state = 'recorded' AND actual_units IS NOT NULL AND response_event IS NOT NULL AND output_hash IS NOT NULL)
    OR (state IN ('reserved', 'uncertain') AND actual_units IS NULL AND response_event IS NULL AND output_hash IS NULL)),
  CHECK (state <> 'uncertain' OR reason IS NOT NULL)
)";

pub(crate) const BUCKET: &str =
    "CREATE INDEX model_calls_bucket ON derivation.model_calls (bucket_hash)";

pub(crate) const RECEIPT: &str = "CREATE UNIQUE INDEX model_calls_receipt ON derivation.model_calls (receipt_hash) WHERE receipt_hash IS NOT NULL";
