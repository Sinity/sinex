use sqlx::{Executor, PgConnection};

pub(crate) async fn remove_version_4(conn: &mut PgConnection) {
    for sql in [
        "DROP VIEW event_types.sinex_telemetry",
        "DROP VIEW event_types.tag_applied",
        "DROP VIEW event_types.activity_span",
        "DROP VIEW event_types.conversation_day",
        "DROP VIEW event_types.rest_fused",
        "DROP VIEW event_types.document_text",
        "DROP VIEW telemetry.visible_events",
        "DELETE FROM registry.event_types WHERE name IN ('sinex.telemetry', 'tag.applied', 'activity.span', 'conversation.day', 'rest.fused', 'document.text')",
    ] {
        conn.execute(sql).await.unwrap();
    }
}
