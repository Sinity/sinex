//! Source positions: each binding's cursor and carried parser state, committed
//! by admission with the events they cover.
use crate::Result;
use serde_json::Value;
use sqlx::PgConnection;

/// The stored cursor and carry for `binding`, if it has read anything.
pub async fn load(conn: &mut PgConnection, binding: &str) -> Result<Option<(Value, Value)>> {
    Ok(sqlx::query!(
        "SELECT cursor, carry FROM raw.positions WHERE binding = $1",
        binding
    )
    .fetch_optional(&mut *conn)
    .await?
    .map(|r| (r.cursor, r.carry)))
}

/// The newest material registered for an input, so a growing input's next part
/// links to it.
pub async fn last_material(
    conn: &mut PgConnection,
    binding: &str,
    locator: &str,
    host: &str,
) -> Result<Option<(sinex_model::MaterialId, u32)>> {
    Ok(sqlx::query!(
        r#"SELECT m.id, m.part_index FROM raw.materials m JOIN raw.inputs i ON i.id = m.input_id
           WHERE i.binding = $1 AND i.locator = $2 AND i.host = $3
           ORDER BY m.part_index DESC LIMIT 1"#,
        binding,
        locator,
        host
    )
    .fetch_optional(&mut *conn)
    .await?
    .map(|r| {
        (
            sinex_model::MaterialId(r.id),
            u32::try_from(r.part_index).unwrap_or(0),
        )
    }))
}

/// Retained record outcomes. Only events establish a known privacy class;
/// failures alone leave it unknown.
#[derive(Default)]
pub struct AccountedRecords {
    pub anchors: Vec<i64>,
    pub class: Option<sinex_model::Class>,
}

/// Exact locations only: an isolated later outcome proves no earlier record.
pub async fn accounted_records(
    conn: &mut PgConnection,
    material: sinex_model::MaterialId,
) -> Result<AccountedRecords> {
    let rows = sqlx::query!(
        r#"SELECT anchor AS "anchor!", class AS "class?" FROM core.events WHERE material_id = $1
           UNION SELECT anchor, NULL::text FROM raw.failures WHERE material_id = $1
           ORDER BY 1"#,
        material.0,
    )
    .fetch_all(conn)
    .await?;
    let mut records = AccountedRecords::default();
    for row in rows {
        records.anchors.push(row.anchor);
        if let Some(class) = row.class {
            let class = crate::privacy::parse_class(&class)?;
            records.class = Some(records.class.map_or(class, |c| c.max(class)));
        }
    }
    records.anchors.dedup();
    Ok(records)
}
