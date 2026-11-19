//! A compact index into copy readings. The event payload still holds the facts.

use sinex_model::{EventPayload, event_types::library::FileCopy};

pub const EVENT_TYPE: &str = FileCopy::EVENT_TYPE;

pub(crate) const TABLE: &str = "CREATE TABLE core.library_copy_fields (
  event_id uuid PRIMARY KEY,
  content_hash bytea,
  path_hash bytea
)";

pub(crate) const INDEXES: &[&str] = &[
    "CREATE INDEX library_copy_fields_content ON core.library_copy_fields (content_hash)",
    "CREATE INDEX library_copy_fields_path ON core.library_copy_fields (path_hash)",
];

pub(crate) fn upgrade() -> Vec<String> {
    let fill = format!(
        "INSERT INTO core.library_copy_fields (event_id, content_hash, path_hash)
         SELECT id, sha256(convert_to(payload->>'content', 'UTF8')),
                    sha256(convert_to(payload->>'path', 'UTF8'))
         FROM core.events WHERE event_type = '{EVENT_TYPE}'"
    );
    let mut statements = vec![TABLE.into(), fill];
    statements.extend(INDEXES.iter().map(|sql| (*sql).into()));
    statements
}
