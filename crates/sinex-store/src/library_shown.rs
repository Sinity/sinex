//! The library's shown-value table (`core.library_shown`): one row per
//! target and tag or property. The library computes the rows; this module
//! only stores and reads them, inside the caller's transaction.
use crate::Result;
use serde_json::Value;
use sqlx::PgConnection;

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub item: String,
    /// The target's canonical text.
    pub target: String,
    /// `tag` or `property`.
    pub kind: String,
    pub key: String,
    pub state: String,
    pub level: String,
    pub value: Option<Value>,
    pub supporters: Value,
}

/// Replace every row of these items with `rows`.
pub async fn replace(conn: &mut PgConnection, items: &[String], rows: &[Row]) -> Result<()> {
    sqlx::query!("DELETE FROM core.library_shown WHERE item = ANY($1)", items)
        .execute(&mut *conn)
        .await?;
    if rows.is_empty() {
        return Ok(());
    }
    let column = |f: fn(&Row) -> String| rows.iter().map(f).collect::<Vec<_>>();
    let values: Vec<Option<Value>> = rows.iter().map(|r| r.value.clone()).collect();
    let supporters: Vec<Value> = rows.iter().map(|r| r.supporters.clone()).collect();
    sqlx::query!(
        "INSERT INTO core.library_shown (item, target, kind, key, state, level, value, supporters)
         SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[],
                              $6::text[], $7::jsonb[], $8::jsonb[])",
        &column(|r| r.item.clone()),
        &column(|r| r.target.clone()),
        &column(|r| r.kind.clone()),
        &column(|r| r.key.clone()),
        &column(|r| r.state.clone()),
        &column(|r| r.level.clone()),
        &values as &[Option<Value>],
        &supporters,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The rows of one item, by target, kind and key.
pub async fn of_item(conn: &mut PgConnection, item: &str) -> Result<Vec<Row>> {
    Ok(sqlx::query_as!(
        Row,
        r#"SELECT item, target, kind, key, state, level, value, supporters
           FROM core.library_shown WHERE item = $1 ORDER BY target, kind, key"#,
        item
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// Every row, by target, kind and key.
pub async fn all(conn: &mut PgConnection) -> Result<Vec<Row>> {
    Ok(sqlx::query_as!(
        Row,
        r#"SELECT item, target, kind, key, state, level, value, supporters
           FROM core.library_shown ORDER BY target, kind, key"#
    )
    .fetch_all(&mut *conn)
    .await?)
}
