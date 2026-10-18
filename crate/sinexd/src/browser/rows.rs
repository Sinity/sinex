//! Reads one Chromium row as JSON, after checking inside SQLite that it fits
//! the writer's step size limit.
use serde_json::{Value, json};
use sinex_store::MAX_STEP_BYTES;
use sqlx::{
    Column, Row, TypeInfo, ValueRef,
    sqlite::{SqliteConnection, SqliteRow},
};
use std::collections::HashMap;

pub(super) struct ProviderRows {
    /// Per table, an SQL expression for the encoded size of one row.
    cost: HashMap<&'static str, String>,
}
fn sum(items: &[String]) -> String {
    match items {
        [] => "0".into(),
        [one] => one.clone(),
        _ => {
            let mid = items.len() / 2;
            format!("({}+{})", sum(&items[..mid]), sum(&items[mid..]))
        }
    }
}
impl ProviderRows {
    pub(super) async fn new(connection: &mut SqliteConnection) -> Result<Self, String> {
        let mut cost = HashMap::new();
        for table in ["visits", "urls", "downloads", "downloads_url_chains"] {
            let name_bytes:i64=sqlx::query_scalar(&format!("SELECT coalesce(sum(length(CAST(name AS BLOB))+16),0) FROM pragma_table_xinfo('{table}') WHERE hidden!=1")).fetch_one(&mut *connection).await.map_err(|_|"Chromium column metadata unavailable")?;
            if name_bytes <= 0 || name_bytes > MAX_STEP_BYTES as i64 {
                return Err("Chromium column names exceed step size limit".into());
            }
            let names: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT name FROM pragma_table_xinfo('{table}') WHERE hidden!=1 ORDER BY cid"
            ))
            .fetch_all(&mut *connection)
            .await
            .map_err(|_| "Chromium column names unavailable")?;
            // An estimate computed inside SQLite; the encoded row is checked
            // again against the step limit when it is admitted.
            let terms: Vec<String> = names.iter().map(|name| {
                let quoted = format!("\"{}\"",name.replace('"', "\"\""));
                let key_bytes = serde_json::to_vec(name).map_or(0, |n| n.len()+2);
                format!("({key_bytes}+CASE typeof({quoted}) WHEN 'text' THEN length(CAST({quoted} AS BLOB)) WHEN 'blob' THEN length({quoted}) ELSE 64 END)")
            }).collect();
            cost.insert(table, sum(&terms));
        }
        Ok(Self { cost })
    }
    pub(super) async fn read(
        &self,
        connection: &mut SqliteConnection,
        table: &'static str,
        id: i64,
        index: Option<i64>,
    ) -> Result<Option<Value>, String> {
        let expression = self.cost.get(table).ok_or("unsupported Chromium table")?;
        let condition = if index.is_some() {
            "id=? AND chain_index=?"
        } else {
            "id=?"
        };
        let budget_sql = format!("SELECT {expression} FROM {table} WHERE {condition}");
        let mut query = sqlx::query_scalar::<_, i64>(&budget_sql).bind(id);
        if let Some(index) = index {
            query = query.bind(index);
        }
        let Some(bytes) = query
            .fetch_optional(&mut *connection)
            .await
            .map_err(|_| "Chromium row budget unavailable")?
        else {
            return Ok(None);
        };
        if bytes < 0 || bytes > MAX_STEP_BYTES as i64 {
            return Err(
                "Chromium row exceeds step size limit before provider transfer; poll incomplete"
                    .into(),
            );
        }
        let value_sql = format!("SELECT * FROM {table} WHERE {condition}");
        let mut query = sqlx::query(&value_sql).bind(id);
        if let Some(index) = index {
            query = query.bind(index);
        }
        query
            .fetch_optional(&mut *connection)
            .await
            .map_err(|_| "Chromium original row unavailable")?
            .map(decode)
            .transpose()
    }
}
/// Decode every column. Non-finite reals keep their bits and blobs stay byte
/// arrays, so no value is lost in JSON.
fn decode(row: SqliteRow) -> Result<Value, String> {
    let mut value = serde_json::Map::new();
    for c in row.columns() {
        let name = c.name();
        let raw = row
            .try_get_raw(name)
            .map_err(|_| "browser column unavailable")?;
        let field = if raw.is_null() {
            Value::Null
        } else {
            match raw.type_info().name() {
                "INTEGER" => json!(
                    row.try_get::<i64, _>(name)
                        .map_err(|_| "browser integer decoding")?
                ),
                "REAL" => {
                    let real = row
                        .try_get::<f64, _>(name)
                        .map_err(|_| "browser real decoding")?;
                    if real.is_finite() {
                        json!(real)
                    } else {
                        json!({"sqlite_real_bits": format!("{:016x}", real.to_bits())})
                    }
                }
                "BLOB" => json!({
                    "sqlite_blob": row
                        .try_get::<Vec<u8>, _>(name)
                        .map_err(|_| "browser blob decoding")?
                }),
                _ => json!(
                    row.try_get::<String, _>(name)
                        .map_err(|_| "browser text decoding")?
                ),
            }
        };
        value.insert(name.to_owned(), field);
    }
    Ok(Value::Object(value))
}

#[cfg(test)]
#[path = "rows_tests.rs"]
mod tests;
