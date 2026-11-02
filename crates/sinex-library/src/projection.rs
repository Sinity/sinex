//! The shown-value projection (`core.library_shown`): for every target and
//! tag or property, the value [`crate::shown`] chooses from the current
//! curation records. The writer refreshes it inside every transaction that
//! changes a curation record (a request, a replay, an archive or a restore),
//! for exactly the items those records are about, so it always equals the
//! function over the live records.
use crate::curation::{self, CURATION, Snapshot, target_text};
use crate::shown::{self, Shown, Values};
use serde_json::{Value, json};
use sinex_model::event_types::library::Curation;
use sinex_store::Connection as Conn;
use sinex_store::library_shown::Row;
use std::collections::BTreeSet;

/// The rows of these values: one per tag and per property.
pub fn rows(values: &[Values]) -> Vec<Row> {
    let mut out = Vec::new();
    for v in values {
        let target = target_text(&v.target);
        for tag in &v.tags {
            out.push(Row {
                item: v.target.item.clone(),
                target: target.clone(),
                kind: "tag".into(),
                key: tag.tag.clone(),
                state: text(&tag.state),
                level: text(&tag.level),
                value: None,
                supporters: json!({ "applied_by": tag.applied_by, "rejected_by": tag.rejected_by }),
            });
        }
        for (name, shown) in &v.properties {
            let (state, level, value, supporters) = match shown {
                Shown::Unknown => continue,
                Shown::Empty { level, supporters } => {
                    ("empty", text(level), None, json!(supporters))
                }
                Shown::Value {
                    value,
                    level,
                    supporters,
                } => ("value", text(level), Some(value.clone()), json!(supporters)),
                Shown::Conflict { level, values } => ("conflict", text(level), None, json!(values)),
            };
            out.push(Row {
                item: v.target.item.clone(),
                target: target.clone(),
                kind: "property".into(),
                key: name.clone(),
                state: state.into(),
                level,
                value,
                supporters,
            });
        }
    }
    out
}

fn text<T: serde::Serialize>(value: &T) -> String {
    match serde_json::to_value(value).expect("states serialize") {
        Value::String(s) => s,
        other => other.to_string(),
    }
}

/// The rows the function gives for these items from their live records.
pub async fn compute(conn: &mut Conn, items: &[String]) -> curation::Result<Vec<Row>> {
    let mut snapshot = Snapshot::new();
    snapshot
        .load_where(conn, &["target", "item"], items)
        .await?;
    let current: Vec<Curation> = snapshot
        .current()?
        .into_iter()
        .filter(|r| r.target.as_ref().is_some_and(|t| items.contains(&t.item)))
        .collect();
    Ok(rows(&shown::values(&current)))
}

/// Recompute the rows of these items.
pub async fn refresh_items(conn: &mut Conn, items: &[String]) -> curation::Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    let rows = compute(conn, items).await?;
    sinex_store::library_shown::replace(conn, items, &rows).await?;
    Ok(())
}

/// Refresh the items whose shown values can have changed since commit
/// position `since`: those of every curation record admitted, archived or
/// restored since, and of every record such a withdrawal names, through
/// withdrawals of withdrawals.
pub async fn refresh_since(conn: &mut Conn, since: i64) -> curation::Result<usize> {
    let rows = sinex_store::payloads::changed_since(conn, CURATION, since).await?;
    if rows.is_empty() {
        return Ok(0);
    }
    let changed: Vec<Curation> = curation::decode(CURATION, rows)?;
    let mut items: BTreeSet<String> = BTreeSet::new();
    let mut snapshot = Snapshot::new();
    let mut frontier: BTreeSet<String> = BTreeSet::new();
    for record in &changed {
        frontier.insert(record.root.clone());
        if let Some(target) = &record.target {
            items.insert(target.item.clone());
        }
    }
    let mut followed: BTreeSet<String> = BTreeSet::new();
    while !frontier.is_empty() {
        let roots: Vec<String> = std::mem::take(&mut frontier)
            .into_iter()
            .filter(|root| followed.insert(root.clone()))
            .collect();
        snapshot.load_roots(conn, roots.iter().cloned()).await?;
        let named: Vec<String> = snapshot
            .records()
            .values()
            .chain(changed.iter())
            .filter(|r| roots.contains(&r.root))
            .flat_map(|r| r.withdraws.iter())
            .filter_map(|w| authored::Target::parse(w).ok())
            .map(|w| w.root().to_owned())
            .collect();
        frontier.extend(named.into_iter().filter(|root| !followed.contains(root)));
    }
    items.extend(
        snapshot
            .records()
            .values()
            .filter_map(|r| r.target.as_ref().map(|t| t.item.clone())),
    );
    let items: Vec<String> = items.into_iter().collect();
    refresh_items(conn, &items).await?;
    Ok(items.len())
}

#[cfg(test)]
mod tests;
