use sqlx::{Executor, PgConnection};

pub(crate) async fn downgrade(conn: &mut PgConnection) {
    conn.execute("DROP VIEW core.visible_events CASCADE")
        .await
        .unwrap();
    conn.execute("ALTER TABLE core.events DROP COLUMN kind_version, DROP COLUMN validation")
        .await
        .unwrap();
    conn.execute("ALTER TABLE telemetry.events DROP COLUMN kind_version, DROP COLUMN validation")
        .await
        .unwrap();
    conn.execute(
        "ALTER TABLE core.admission_stage DROP COLUMN kind_version, DROP COLUMN validation",
    )
    .await
    .unwrap();
    conn.execute("DROP TABLE core.replay_refusals; DROP TABLE registry.kind_prefixes")
        .await
        .unwrap();
    conn.execute("ALTER TABLE registry.event_types RENAME TO test_declarations; CREATE TABLE registry.event_types (name text PRIMARY KEY, payload_schema jsonb, key_fields text[], replace_rule text NOT NULL)").await.unwrap();
    conn.execute("INSERT INTO registry.event_types SELECT DISTINCT ON (name) name,payload_schema,ARRAY[]::text[],replace_rule FROM registry.test_declarations ORDER BY name,version DESC; DROP TABLE registry.test_declarations; DROP FUNCTION registry.keep_definition()").await.unwrap();
}
