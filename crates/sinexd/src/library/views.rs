//! Reading the library as the operator sees it: an item, a collection, a
//! ranking.
use super::{R, e};
use serde_json::{Value, json};
use sinex_library::{rank, reads, shown};
use sinex_model::event_types::library::Assignment;
use sqlx::PgConnection;
use std::collections::HashMap;

/// One item, as the operator reads it.
pub(crate) async fn item_view(conn: &mut PgConnection, item: &str) -> R<Value> {
    let it = reads::item(conn, item)
        .await
        .map_err(e)?
        .ok_or_else(|| format!("no library item {item}"))?;
    let revisions = reads::revisions(conn, item).await.map_err(e)?;
    let renditions = reads::renditions(conn, item).await.map_err(e)?;
    let assignments = reads::assignments(conn, item).await.map_err(e)?;
    let ratings = reads::ratings(conn, item).await.map_err(e)?;
    let views = reads::views(conn, item).await.map_err(e)?;
    let entries = reads::entries_of(conn, item).await.map_err(e)?;
    let copies = reads::copies_at(conn, &it.copies).await.map_err(e)?;
    let mut fields: HashMap<&str, Vec<Assignment>> = HashMap::new();
    for a in &assignments {
        if a.field != "tag" {
            fields.entry(a.field.as_str()).or_default().push(a.clone());
        }
    }
    let tags: Vec<Assignment> = assignments
        .iter()
        .filter(|a| a.field == "tag")
        .cloned()
        .collect();
    let shown: serde_json::Map<String, Value> = fields
        .into_iter()
        .map(|(f, a)| {
            let v = match shown::property(&a) {
                shown::Shown::Unknown => json!({ "state": "unknown" }),
                shown::Shown::Empty { by } => json!({ "state": "empty", "assignment": by }),
                shown::Shown::Value { value, by, level } => {
                    json!({ "state": "value", "value": value, "level": level, "assignment": by })
                }
                shown::Shown::Conflict { level, values } => {
                    json!({ "state": "conflict", "level": level, "values": values })
                }
            };
            (f.to_owned(), v)
        })
        .collect();
    let available: Vec<Value> = copies
        .iter()
        .map(|a| {
            json!({
                "path": a.path,
                "present": a.vanished_at.is_none(),
                "same_bytes_as_item": a.content == it.content,
                "stored": a.stored,
            })
        })
        .collect();
    Ok(json!({
        "item": it.item,
        "label": it.label,
        "revision": it.revision,
        "withdrawn": it.withdrawn_at.is_some(),
        "content": it.content,
        "media_type": it.media_type,
        "revisions": revisions.iter().map(|r| json!({"revision": r.revision, "content": r.content, "at": r.at})).collect::<Vec<_>>(),
        "renditions": renditions.iter().filter(|r| r.withdrawn_at.is_none()).map(|r| json!({
            "rendition": r.rendition, "revision": r.revision, "role": r.role, "media_type": r.media_type,
            "width": r.width, "height": r.height, "duration_ms": r.duration_ms, "content": r.content})).collect::<Vec<_>>(),
        "copies": available,
        "tags": shown::tags(&tags).into_iter().map(|t| json!({"tag": t.tag, "level": t.level})).collect::<Vec<_>>(),
        "properties": shown,
        "ratings": ratings.iter().filter(|r| r.withdrawn_at.is_none()).map(|r| json!({"scale": r.scale, "value": r.value, "of": r.of, "by": r.by})).collect::<Vec<_>>(),
        "views": {
            "count": views.iter().map(|v| u64::from(v.count)).sum::<u64>(),
            "seconds": views.iter().filter_map(|v| v.seconds).sum::<f64>(),
            "last": views.iter().map(|v| v.started_at).max_by_key(|t| t.unix_ns()),
        },
        "collections": entries.iter().filter(|en| en.withdrawn_at.is_none()).map(|en| en.collection.clone()).collect::<Vec<_>>(),
    }))
}

/// A collection's entries in order, or every collection.
pub(crate) async fn collection_view(conn: &mut PgConnection, name: Option<&str>) -> R<Value> {
    match name {
        None => {
            let all = reads::collections(conn).await.map_err(e)?;
            let mut rows = Vec::new();
            for c in all {
                let n = reads::entries(conn, &c.collection).await.map_err(e)?.len();
                rows.push(json!({ "name": c.name, "collection": c.collection, "entries": n, "saved_query": c.saved_query }));
            }
            Ok(json!({ "collections": rows }))
        }
        Some(name) => {
            let c = reads::collection_named(conn, name)
                .await
                .map_err(e)?
                .ok_or_else(|| format!("no collection named {name}"))?;
            let entries = reads::entries(conn, &c.collection).await.map_err(e)?;
            Ok(json!({
                "name": c.name,
                "collection": c.collection,
                "entries": entries.iter().map(|en| json!({"entry": en.entry, "item": en.target.item, "revision": en.target.revision, "position": en.position})).collect::<Vec<_>>(),
            }))
        }
    }
}

/// Items ranked from pairwise comparisons on a scale.
pub(crate) async fn rank_view(conn: &mut PgConnection, scale: &str) -> R<Value> {
    let judgements: Vec<(String, String)> = reads::comparisons(conn, scale)
        .await
        .map_err(e)?
        .into_iter()
        .map(|c| (c.winner, c.loser))
        .collect();
    let ranked = rank::bradley_terry(&judgements);
    let next = rank::next_pair(&ranked, &judgements);
    Ok(json!({
        "scale": scale,
        "comparisons": judgements.len(),
        "ranking": ranked.iter().map(|r| json!({"item": r.item, "strength": r.strength, "wins": r.wins, "losses": r.losses})).collect::<Vec<_>>(),
        "compare_next": next.map(|(a, b)| json!([a, b])),
    }))
}
