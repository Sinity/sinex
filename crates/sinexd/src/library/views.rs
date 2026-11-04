//! Reading the library as the operator sees it: an item with its shown
//! values and curation history, a collection in order, a ranking.
use super::{R, e};
use serde_json::{Value, json};
use sinex_library::curation::Snapshot;
use sinex_library::plan::{Scale, entry_position};
use sinex_library::{rank, reads, shown};
use sinex_model::event_types::library::{Curation, CurationKind, Target};
use sqlx::PgConnection;
use std::collections::{BTreeMap, BTreeSet};

/// One item, as the operator reads it.
pub(crate) async fn item_view(conn: &mut PgConnection, item: &str) -> R<Value> {
    let it = reads::item(conn, item)
        .await
        .map_err(e)?
        .ok_or_else(|| format!("no library item {item}"))?;
    let revisions = reads::revisions(conn, item).await.map_err(e)?;
    let renditions = reads::renditions(conn, item).await.map_err(e)?;
    let views = reads::views(conn, item).await.map_err(e)?;
    let copies = reads::copies_at(conn, &it.copies).await.map_err(e)?;

    let mut snapshot = Snapshot::new();
    snapshot
        .load_where(conn, &["target", "item"], &[item.to_owned()])
        .await
        .map_err(e)?;
    let mut current_entries = Vec::new();
    let mut history = Vec::new();
    let about: Vec<Curation> = snapshot
        .records()
        .values()
        .filter(|record| record.target.as_ref().is_some_and(|t| t.item == item))
        .cloned()
        .collect();
    for record in &about {
        let state = snapshot.state(&record.root).map_err(e)?;
        let revision = state
            .revisions
            .iter()
            .find(|r| r.revision.rev == record.rev)
            .expect("a loaded revision has a state");
        history.push(json!({
            "record": format!("{}@{}", record.root, record.rev),
            "kind": record.kind,
            "key": record.key,
            "value": record.value,
            "target": record.target,
            "origin": record.origin,
            "cites": record.cites,
            "author": record.author,
            "at": record.at,
            "request": record.request,
            "current": revision.current,
            "withdrawn": revision.withdrawn,
        }));
        if record.kind == CurationKind::Entry && revision.current {
            current_entries.push(record.clone());
        }
    }
    history.reverse();
    let current: Vec<Curation> = snapshot
        .current()
        .map_err(e)?
        .into_iter()
        .filter(|record| record.target.as_ref().is_some_and(|t| t.item == item))
        .collect();
    let whole = Target {
        item: item.to_owned(),
        ..Target::default()
    };
    // Tags and properties are the projection's rows; ratings and notes are
    // each author's own and are listed from the current records.
    let shown_rows = sinex_store::library_shown::of_item(conn, item)
        .await
        .map_err(e)?;
    let mut targets: BTreeMap<String, (Vec<Value>, serde_json::Map<String, Value>)> =
        BTreeMap::new();
    for row in shown_rows {
        let (tags, properties) = targets.entry(row.target.clone()).or_default();
        match row.kind.as_str() {
            "tag" => tags.push(json!({
                "tag": row.key,
                "state": row.state,
                "level": row.level,
                "applied_by": row.supporters["applied_by"],
                "rejected_by": row.supporters["rejected_by"],
            })),
            _ => {
                let mut shown = json!({ "state": row.state, "level": row.level });
                if row.state == "conflict" {
                    shown["values"] = row.supporters;
                } else {
                    shown["supporters"] = row.supporters;
                }
                if let Some(value) = row.value {
                    shown["value"] = value;
                }
                properties.insert(row.key, shown);
            }
        }
    }
    let values = shown::values(&current);
    let whole_text = sinex_library::curation::target_text(&whole);
    let (own_tags, own_properties) = targets.remove(&whole_text).unwrap_or_default();
    let own = values.iter().find(|v| v.target == whole);
    let parts: Vec<Value> = targets
        .into_iter()
        .map(|(target, (tags, properties))| {
            json!({
                "target": serde_json::from_str::<Value>(&target).unwrap_or(Value::String(target)),
                "tags": tags,
                "properties": properties,
            })
        })
        .collect();
    let collections = entries_with_names(conn, &mut snapshot, current_entries).await?;

    let available: Vec<Value> = copies
        .iter()
        .map(|a| {
            json!({
                "path": a.path,
                "present": a.vanished_at.is_none(),
                "same_bytes_as_item": a.content == it.content,
                "stored": a.stored,
                "move_candidates": a.move_candidates,
            })
        })
        .collect();
    let known: Vec<f64> = views.iter().filter_map(|v| v.seconds).collect();
    let unknown = views.iter().filter(|v| v.seconds.is_none()).count();
    Ok(json!({
        "item": it.item,
        "label": it.label,
        "revision": it.revision,
        "content": it.content,
        "media_type": it.media_type,
        "revisions": revisions.iter().map(|r| json!({"revision": r.revision, "replaces": r.replaces, "content": r.content, "at": r.at})).collect::<Vec<_>>(),
        "renditions": renditions.iter().map(|r| json!({
            "rendition": r.rendition, "revision": r.revision, "role": r.role, "media_type": r.media_type,
            "width": r.width, "height": r.height, "duration_ms": r.duration_ms, "content": r.content})).collect::<Vec<_>>(),
        "copies": available,
        "tags": own_tags,
        "properties": own_properties,
        "ratings": own.map(|v| json!(v.ratings)).unwrap_or(json!([])),
        "notes": own.map(|v| json!(v.notes)).unwrap_or(json!([])),
        "parts": parts,
        "views": {
            "count": views.iter().map(|v| u64::from(v.count)).sum::<u64>(),
            // Unknown durations are not zero: the sum is a lower bound
            // while any view's duration is unknown.
            "seconds_known": known.iter().sum::<f64>(),
            "views_without_duration": unknown,
            "last": views.iter().map(|v| v.started_at).max_by_key(|t| t.unix_ns()),
        },
        "collections": collections,
        "history": history,
    }))
}

/// Each current entry of the item, with its collection's current name.
async fn entries_with_names(
    conn: &mut PgConnection,
    snapshot: &mut Snapshot,
    entries: Vec<Curation>,
) -> R<Vec<Value>> {
    let roots: BTreeSet<String> = entries
        .iter()
        .filter_map(|entry| entry.key.clone())
        .collect();
    snapshot.load_roots(conn, roots).await.map_err(e)?;
    let mut out = Vec::new();
    for entry in entries {
        let collection = entry.key.clone().unwrap_or_default();
        let heads = snapshot.heads(&collection).map_err(e)?;
        let names: Vec<Option<String>> = heads
            .iter()
            .filter_map(|head| snapshot.record(head))
            .map(|record| record.key.clone())
            .collect();
        out.push(json!({
            "entry": entry.root,
            "collection": collection,
            "names": names,
            "revision": entry.target.and_then(|t| t.revision),
        }));
    }
    Ok(out)
}

/// The one current collection with this name.
async fn collection_named(
    conn: &mut PgConnection,
    snapshot: &mut Snapshot,
    name: &str,
) -> R<String> {
    let holders = snapshot
        .named(conn, CurationKind::Collection, name)
        .await
        .map_err(e)?;
    match holders.len() {
        0 => Err(format!("no collection named {name}")),
        1 => Ok(holders.into_keys().next().expect("one")),
        _ => Err(format!(
            "several collections are named {name}: {}",
            holders.into_keys().collect::<Vec<_>>().join(", ")
        )),
    }
}

/// A collection's current entries, by position.
async fn entries(
    conn: &mut PgConnection,
    snapshot: &mut Snapshot,
    collection: &str,
) -> R<Vec<Curation>> {
    snapshot
        .load_where(conn, &["key"], &[collection.to_owned()])
        .await
        .map_err(e)?;
    let mut out: Vec<(authored::SortKey, Curation)> = snapshot
        .current()
        .map_err(e)?
        .into_iter()
        .filter(|r| r.kind == CurationKind::Entry && r.key.as_deref() == Some(collection))
        .filter_map(|r| {
            Some((
                authored::SortKey {
                    key: entry_position(&r)?,
                    root: r.root.clone(),
                },
                r,
            ))
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out.into_iter().map(|(_, r)| r).collect())
}

/// A collection's entries in order, or every collection.
pub(crate) async fn collection_view(conn: &mut PgConnection, name: Option<&str>) -> R<Value> {
    let mut snapshot = Snapshot::new();
    match name {
        None => {
            snapshot
                .load_kind(conn, CurationKind::Collection)
                .await
                .map_err(e)?;
            let collections: Vec<Curation> = snapshot
                .current()
                .map_err(e)?
                .into_iter()
                .filter(|r| r.kind == CurationKind::Collection)
                .collect();
            let mut rows = Vec::new();
            for c in collections {
                let n = entries(conn, &mut snapshot, &c.root).await?.len();
                rows.push(json!({
                    "name": c.key,
                    "collection": c.root,
                    "revision": c.rev,
                    "entries": n,
                }));
            }
            Ok(json!({ "collections": rows }))
        }
        Some(name) => {
            let root = collection_named(conn, &mut snapshot, name).await?;
            let entries = entries(conn, &mut snapshot, &root).await?;
            let heads = snapshot.heads(&root).map_err(e)?;
            Ok(json!({
                "name": name,
                "collection": root,
                "revisions": heads.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "entries": entries.iter().map(|en| {
                    let target = en.target.clone().unwrap_or_default();
                    json!({
                        "entry": en.root,
                        "record": format!("{}@{}", en.root, en.rev),
                        "item": target.item,
                        "revision": target.revision,
                        "rendition": target.rendition,
                        "part": target.part,
                        "position": entry_position(en),
                        "author": en.author,
                    })
                }).collect::<Vec<_>>(),
            }))
        }
    }
}

/// Items ranked on a scale from its comparisons, with the next set worth
/// judging.
pub(crate) async fn rank_view(
    conn: &mut PgConnection,
    scale: &str,
    judges: Option<&str>,
    collection: Option<&str>,
    k: Option<u64>,
    rule: Option<&str>,
) -> R<Value> {
    let mut snapshot = Snapshot::new();
    let holders = snapshot
        .named(conn, CurationKind::Scale, scale)
        .await
        .map_err(e)?;
    let definition = match holders.len() {
        0 => return Err(format!("no scale `{scale}`")),
        1 => {
            let heads = holders.into_values().next().expect("one");
            heads
                .first()
                .and_then(|head| snapshot.record(head))
                .and_then(|record| record.value.as_ref())
                .and_then(|value| Scale::from_value(scale, value))
        }
        _ => return Err(format!("several scales are named `{scale}`")),
    };
    snapshot
        .load_where(conn, &["key"], &[scale.to_owned()])
        .await
        .map_err(e)?;
    let current = snapshot.current().map_err(e)?;
    let judgments = rank::judgments(&current, scale);
    let mut candidates: BTreeSet<String> = BTreeSet::new();
    if let Some(name) = collection {
        let root = collection_named(conn, &mut snapshot, name).await?;
        for entry in entries(conn, &mut snapshot, &root).await? {
            if let Some(target) = entry.target {
                candidates.insert(target.item);
            }
        }
    } else {
        candidates.extend(
            judgments
                .iter()
                .flat_map(|j| j.order.iter().flatten().cloned()),
        );
    }
    let set = rank::judge_set(judges);
    let items: Vec<String> = candidates.iter().cloned().collect();
    let options = ranking::FitOptions {
        judges: set.clone(),
        ..ranking::FitOptions::default()
    };
    let fit = ranking::fit(&items, &judgments, &options).map_err(e)?;
    let k = usize::try_from(k.unwrap_or(2)).map_err(e)?;
    let rule = match rule.unwrap_or("information") {
        "information" => ranking::SelectionRule::Information,
        "random" => ranking::SelectionRule::Random {
            seed: fit.judgment_count as u64,
        },
        "uncompared-first" => ranking::SelectionRule::UncomparedFirst,
        other => {
            return Err(format!(
                "no selection rule `{other}`; rules: information, random, uncompared-first"
            ));
        }
    };
    let next = if items.len() >= k {
        let proposal = ranking::next_set(&fit, &items, k, rule).map_err(e)?;
        json!({
            "rule": format!("{:?}", proposal.rule),
            "items": proposal.seats.iter().map(|seat| json!({
                "item": seat.item,
                "why": format!("{:?}", seat.why),
            })).collect::<Vec<_>>(),
        })
    } else {
        Value::Null
    };
    let unranked = fit.items.iter().filter(|item| item.support == 0).count();
    let mut components: BTreeMap<usize, usize> = BTreeMap::new();
    for item in &fit.items {
        *components.entry(item.component).or_default() += 1;
    }
    Ok(json!({
        "scale": scale,
        "question": definition.map(|d| d.question),
        "judges": match &set {
            ranking::JudgeSet::All => json!("all"),
            ranking::JudgeSet::Named(names) => json!(names),
        },
        "comparisons": fit.judgment_count,
        "method": fit.method,
        "fit": match fit.status {
            ranking::FitStatus::Converged { iterations } => json!({"status": "converged", "iterations": iterations}),
            ranking::FitStatus::Stopped { iterations, change } => json!({"status": "stopped", "iterations": iterations, "change": change}),
        },
        "unranked": unranked,
        "groups": components.len(),
        "ranking": fit.items.iter().map(|item| json!({
            "item": item.item,
            "score": item.score,
            "interval": [item.interval.0, item.interval.1],
            "rank": item.rank,
            "rank_range": [item.rank_range.best, item.rank_range.worst],
            "support": item.support,
            "group": item.component,
        })).collect::<Vec<_>>(),
        "judge_next": next,
    }))
}
