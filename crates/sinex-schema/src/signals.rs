//! Private materialized host checkpoint; ordinary events own rules and deliveries.
pub(crate) const CHECKPOINT: &str = "CREATE TABLE core.signal_checkpoints (
  host text PRIMARY KEY,
  revision bigint NOT NULL CHECK (revision >= 1),
  state jsonb NOT NULL CHECK (jsonb_typeof(state) = 'object')
)";

pub(crate) fn statements() -> Vec<String> {
    vec![
        CHECKPOINT.into(),
        "GRANT SELECT,INSERT,UPDATE,DELETE ON core.signal_checkpoints TO sinex_writer".into(),
        "REVOKE ALL ON core.signal_checkpoints FROM sinex_reader".into(),
    ]
}
