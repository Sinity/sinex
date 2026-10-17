//! Bound provider transfer before decoding full original SQLite values.
use super::*;
use std::collections::HashMap;

pub(super) struct ProviderRows {
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
                return Err("Chromium column names exceed retention budget".into());
            }
            let names: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT name FROM pragma_table_xinfo('{table}') WHERE hidden!=1 ORDER BY cid"
            ))
            .fetch_all(&mut *connection)
            .await
            .map_err(|_| "Chromium column names unavailable")?;
            // These scalar lengths stay inside SQLite; encoded expansion is
            // checked again by the existing admission budget after transfer.
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
                "Chromium row exceeds retention budget before provider transfer; poll incomplete"
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
#[cfg(test)]
#[path = "rows_tests.rs"]
mod tests;
