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

/// Synthetic downgrades leave PostgreSQL's dropped-column positions behind.
/// Compare every column attribute except those unused physical positions.
pub(crate) async fn catalog(conn: &mut PgConnection) -> Vec<String> {
    let mut rows = sinex_schema::catalog(conn).await.unwrap();
    for row in &mut rows {
        let Some((column, attributes)) = row.split_once(' ') else {
            continue;
        };
        if [
            "core.events.kind_version",
            "core.events.validation",
            "telemetry.events.kind_version",
            "telemetry.events.validation",
            "core.admission_stage.kind_version",
            "core.admission_stage.validation",
        ]
        .contains(&column)
        {
            let (_, attributes) = attributes.split_once(' ').expect("catalog column position");
            *row = format!("{column} {attributes}");
        }
    }
    rows
}
