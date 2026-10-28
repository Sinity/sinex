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
    conn.execute("ALTER TABLE registry.event_types RENAME TO test_declarations; CREATE TABLE registry.event_types (name text PRIMARY KEY, payload_schema jsonb, key_kind text NOT NULL DEFAULT 'location', key_fields text[], replace_rule text NOT NULL)").await.unwrap();
    conn.execute("INSERT INTO registry.event_types (name,payload_schema,key_fields,replace_rule) SELECT DISTINCT ON (name) name,payload_schema,ARRAY[]::text[],replace_rule FROM registry.test_declarations ORDER BY name,version DESC; DROP TABLE registry.test_declarations; DROP FUNCTION registry.keep_definition()").await.unwrap();
}

/// Reconstructing an older schema drops columns, but PostgreSQL keeps their
/// positions. Columns added again therefore have different positions.
pub(crate) fn without_positions(catalog: Vec<String>) -> Vec<String> {
    catalog
        .into_iter()
        .map(|row| match row.split_once(' ') {
            Some((name, rest)) if name.matches('.').count() == 2 => {
                let rest = rest.split_once(' ').map_or(rest, |(_, after)| after);
                format!("{name} {rest}")
            }
            _ => row,
        })
        .collect()
}
