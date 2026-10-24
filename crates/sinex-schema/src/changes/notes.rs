//! Only note metadata changes at version 2. The predecessor definitions were
//! captured from the released v1 native binary, not reconstructed from v2.
use crate::{Error, event_types, kind_view};
use serde_json::Value;
use sqlx::{Executor, PgConnection};

type RegistryRow = (String, Value, String, Vec<String>, String);

pub(super) async fn apply(conn: &mut PgConnection) -> Result<(), Error> {
    let expected: RegistryRow = serde_json::from_str(include_str!("notes/v1-registry.json"))
        .expect("frozen note registry row");
    let current: Option<RegistryRow> = sqlx::query_as(
        "SELECT name,payload_schema,key_kind,key_fields,replace_rule FROM registry.event_types WHERE name='note.document' FOR UPDATE",
    ).fetch_optional(&mut *conn).await?;
    let view: Option<String> = sqlx::query_scalar(
        "SELECT pg_get_viewdef(c.oid,true) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='event_types' AND c.relname='note_document' AND c.relkind='v'",
    ).fetch_optional(&mut *conn).await?;
    if current.as_ref() != Some(&expected)
        || view.as_deref().map(str::trim) != Some(include_str!("notes/v1-view.sql").trim())
    {
        return Err(Error::Shape(1));
    }
    let kind = event_types::ALL
        .iter()
        .find(|k| k.event_type == "note.document")
        .expect("registered note payload");
    // New fields append to the old columns. PostgreSQL preserves the view's
    // identity, dependencies and existing ACLs when replacing that projection.
    conn.execute(kind_view(kind, true).as_str()).await?;
    sqlx::query("UPDATE registry.event_types SET payload_schema=$1 WHERE name='note.document'")
        .bind((kind.json_schema)())
        .execute(&mut *conn)
        .await?;
    conn.execute("GRANT SELECT ON event_types.note_document TO sinex_reader,sinex_writer")
        .await?;
    Ok(())
}
