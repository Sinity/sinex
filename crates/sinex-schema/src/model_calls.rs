//! Operational accounting survives replay and removal of response content.
//! Only the writer can inspect or modify these hash-only reservations.
pub(crate) const POLICIES: &str = "CREATE TABLE derivation.model_policies (
  policy_event uuid PRIMARY KEY,
  policy_hash bytea NOT NULL CHECK (octet_length(policy_hash) = 32),
  authorized_at timestamptz NOT NULL
)";

pub(crate) const CALLS: &str = "CREATE TABLE derivation.model_calls (
  entry_id uuid PRIMARY KEY,
  request_hash bytea UNIQUE CHECK (request_hash IS NULL OR octet_length(request_hash) = 32),
  policy_event uuid NOT NULL,
  policy_definition_hash bytea NOT NULL CHECK (octet_length(policy_definition_hash) = 32),
  policy_name text NOT NULL,
  policy_version text NOT NULL,
  operation text NOT NULL,
  account_scope text NOT NULL,
  bucket_hash bytea NOT NULL CHECK (octet_length(bucket_hash) = 32),
  currency text NOT NULL,
  unit text NOT NULL,
  period_start_ns numeric NOT NULL CHECK (period_start_ns = trunc(period_start_ns) AND period_start_ns NOT IN ('NaN'::numeric,'Infinity'::numeric,'-Infinity'::numeric)),
  period_end_ns numeric NOT NULL CHECK (period_end_ns = trunc(period_end_ns) AND period_end_ns NOT IN ('NaN'::numeric,'Infinity'::numeric,'-Infinity'::numeric) AND period_end_ns > period_start_ns),
  limit_units numeric NOT NULL CHECK (limit_units >= 0 AND limit_units = trunc(limit_units)),
  reserved_units numeric CHECK (reserved_units >= 0 AND reserved_units = trunc(reserved_units)),
  actual_units numeric CHECK (actual_units >= 0 AND actual_units = trunc(actual_units)),
  state text NOT NULL CHECK (state IN ('reserved', 'uncertain', 'recorded', 'actual_unbound')),
  response_event uuid,
  output_hash bytea CHECK (output_hash IS NULL OR octet_length(output_hash) = 32),
  receipt_hash bytea CHECK (receipt_hash IS NULL OR octet_length(receipt_hash) = 32),
  receipt_event uuid,
  overrun boolean NOT NULL DEFAULT false,
  reason text,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CHECK ((state = 'actual_unbound' AND request_hash IS NULL AND reserved_units IS NULL AND actual_units IS NOT NULL
      AND receipt_hash IS NOT NULL AND receipt_event IS NOT NULL AND response_event IS NULL AND output_hash IS NULL)
    OR (state = 'recorded' AND request_hash IS NOT NULL AND actual_units IS NOT NULL
      AND response_event IS NOT NULL AND output_hash IS NOT NULL)
    OR (state IN ('reserved', 'uncertain') AND request_hash IS NOT NULL AND reserved_units IS NOT NULL
      AND actual_units IS NULL AND response_event IS NULL AND output_hash IS NULL AND receipt_hash IS NULL AND receipt_event IS NULL)),
  CHECK (state <> 'uncertain' OR reason IS NOT NULL)
)";

pub(crate) const BUCKET: &str =
    "CREATE INDEX model_calls_bucket ON derivation.model_calls (bucket_hash)";

pub(crate) const RECEIPT: &str = "CREATE UNIQUE INDEX model_calls_receipt ON derivation.model_calls (receipt_hash) WHERE receipt_hash IS NOT NULL";

pub(crate) fn tables() -> Vec<String> {
    [POLICIES, CALLS, BUCKET, RECEIPT]
        .into_iter()
        .map(String::from)
        .collect()
}
pub(crate) fn statements() -> Vec<String> {
    let mut out = tables();
    out.push("GRANT SELECT,INSERT,UPDATE,DELETE ON derivation.model_policies,derivation.model_calls TO sinex_writer".into());
    out.push(
        "REVOKE ALL ON derivation.model_policies,derivation.model_calls FROM sinex_reader,PUBLIC"
            .into(),
    );
    out
}
pub(crate) fn before(statement: &str) -> bool {
    !statement.contains("derivation.model_policies")
        && !statement.contains("derivation.model_calls")
}
