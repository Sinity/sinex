use sqlx::{Executor, PgConnection};

pub(crate) async fn remove_foundations(conn: &mut PgConnection) {
    for sql in [
        "DROP VIEW event_types.sinex_telemetry",
        "DROP VIEW event_types.tag_applied",
        "DROP VIEW event_types.activity_span",
        "DROP VIEW event_types.conversation_day",
        "DROP VIEW event_types.rest_fused",
        "DROP VIEW event_types.document_text",
        "DROP VIEW event_types.activity_window",
        "DROP VIEW event_types.activity_session",
        "DROP VIEW event_types.input_coverage, event_types.work_episode, event_types.work_suspension, event_types.work_resumption, event_types.context_pack, event_types.work_act, event_types.activity_day",
        "DELETE FROM registry.event_types WHERE name IN ('input.coverage','work.episode','work.suspension','work.resumption','context.pack','work.act','activity.day')",
        "DELETE FROM registry.event_types WHERE name IN ('activity.window', 'activity.session')",
        "DELETE FROM core.counters WHERE name = 'excised'",
        "DROP TABLE core.event_children",
        // Dropping a column keeps its ordinal slot; rebuild the predecessor layout.
        "CREATE TEMP TABLE predecessor_waiting AS SELECT derivation, key, since FROM derivation.waiting",
        "DROP TABLE derivation.waiting",
        "CREATE TABLE derivation.waiting (derivation text NOT NULL, key jsonb NOT NULL, since timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (derivation, key))",
        "INSERT INTO derivation.waiting SELECT * FROM predecessor_waiting",
        "DROP TABLE predecessor_waiting",
        "GRANT SELECT ON derivation.waiting TO sinex_reader, sinex_writer",
        "GRANT INSERT, UPDATE, DELETE ON derivation.waiting TO sinex_writer",
        "CREATE SCHEMA telemetry",
        "CREATE TABLE telemetry.events (LIKE core.events INCLUDING ALL)",
        "DELETE FROM registry.event_types WHERE name IN ('sinex.telemetry', 'tag.applied', 'activity.span', 'conversation.day', 'rest.fused', 'document.text')",
    ] {
        conn.execute(sql).await.unwrap();
    }
}

pub(crate) async fn remove_version_4(conn: &mut PgConnection) {
    conn.execute("DROP VIEW event_types.operator_mark, event_types.query_saved")
        .await
        .unwrap();
    conn.execute("DELETE FROM registry.event_types WHERE name IN ('operator.mark','query.saved')")
        .await
        .unwrap();
}
