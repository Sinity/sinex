//! Curation: tags, properties, collections, ratings, comparisons, views, and
//! curation imported from other applications' sidecar files.
use super::{Library, R, e};
use serde_json::{Value, json};
use sinex_library::{import, items, reads};
use sinex_model::Timestamp;
use sinex_model::event_types::library::{
    Assignment, Collection, Comparison, Entry, Level, LibraryRecord, Rating, Target, View,
};
use sinex_sources::sources::library as library_source;
use sqlx::PgConnection;
use std::path::{Path, PathBuf};

impl Library<'_> {
    pub async fn tag(
        &self,
        conn: &mut PgConnection,
        item: &str,
        tag: &str,
        actor: &str,
    ) -> R<Value> {
        self.item_or(conn, item).await?;
        let tag = tag.trim();
        if tag.is_empty() || tag == "-" {
            return Err("a tag needs text".into());
        }
        let existing = reads::assignments(conn, item).await.map_err(e)?;
        if existing.iter().any(|a| {
            a.withdrawn_at.is_none()
                && a.level == Level::Override
                && a.field == "tag"
                && a.value.as_deref() == Some(tag)
                && a.by == actor
        }) {
            return Ok(json!({ "item": item, "tag": tag, "already": true }));
        }
        let a = Self::assignment(item, "tag", Some(tag.to_owned()), Level::Override, None);
        let id = a.assignment.clone();
        self.write(vec![LibraryRecord::Assignment(a)], actor, "tag")
            .await?;
        Ok(json!({ "item": item, "tag": tag, "assignment": id }))
    }

    pub async fn untag(
        &self,
        conn: &mut PgConnection,
        item: &str,
        tag: &str,
        actor: &str,
    ) -> R<Value> {
        let mine: Vec<Assignment> = reads::assignments(conn, item)
            .await
            .map_err(e)?
            .into_iter()
            .filter(|a| {
                a.withdrawn_at.is_none()
                    && a.field == "tag"
                    && a.value.as_deref() == Some(tag)
                    && a.by == actor
            })
            .collect();
        if mine.is_empty() {
            return Err(format!(
                "{actor} has no assignment of `{tag}` to {item}; to remove an imported tag, tag it `-{tag}`"
            ));
        }
        let n = mine.len();
        self.write(
            mine.into_iter().map(Self::withdraw).collect(),
            actor,
            "untag",
        )
        .await?;
        Ok(json!({ "item": item, "tag": tag, "withdrawn": n }))
    }

    pub async fn set(
        &self,
        conn: &mut PgConnection,
        item: &str,
        field: &str,
        value: Option<String>,
        actor: &str,
    ) -> R<Value> {
        self.item_or(conn, item).await?;
        if field == "tag" {
            return Err("tags are set with library-tag".into());
        }
        let mut records: Vec<LibraryRecord> = reads::assignments(conn, item)
            .await
            .map_err(e)?
            .into_iter()
            .filter(|a| {
                a.withdrawn_at.is_none()
                    && a.field == field
                    && a.level == Level::Override
                    && a.by == actor
            })
            .map(Self::withdraw)
            .collect();
        records.push(LibraryRecord::Assignment(Self::assignment(
            item,
            field,
            value.clone(),
            Level::Override,
            None,
        )));
        self.write(records, actor, "set").await?;
        Ok(json!({ "item": item, "field": field, "value": value }))
    }

    pub async fn collect(
        &self,
        conn: &mut PgConnection,
        collection: &str,
        item: &str,
        actor: &str,
    ) -> R<Value> {
        let it = self.item_or(conn, item).await?;
        let mut records = Vec::new();
        let c = match reads::collection_named(conn, collection).await.map_err(e)? {
            Some(c) => c,
            None => {
                let c = Collection {
                    collection: items::new_id(),
                    name: collection.to_owned(),
                    saved_query: None,
                    selected_from: None,
                    by: String::new(),
                    at: Timestamp::now(),
                    withdrawn_at: None,
                };
                records.push(LibraryRecord::Collection(c.clone()));
                c
            }
        };
        let last = reads::entries(conn, &c.collection)
            .await
            .map_err(e)?
            .iter()
            .map(|en| en.position)
            .fold(0.0f64, f64::max);
        let entry = Entry {
            entry: items::new_id(),
            collection: c.collection.clone(),
            target: Target {
                item: item.to_owned(),
                revision: Some(it.revision),
                rendition: None,
                part: None,
            },
            position: last + 1.0,
            by: String::new(),
            at: Timestamp::now(),
            withdrawn_at: None,
        };
        let id = entry.entry.clone();
        records.push(LibraryRecord::Entry(entry));
        self.write(records, actor, "collect").await?;
        Ok(json!({ "collection": c.name, "entry": id, "position": last + 1.0 }))
    }

    pub async fn uncollect(
        &self,
        conn: &mut PgConnection,
        collection: &str,
        item: &str,
        actor: &str,
    ) -> R<Value> {
        let c = reads::collection_named(conn, collection)
            .await
            .map_err(e)?
            .ok_or_else(|| format!("no collection named {collection}"))?;
        let gone: Vec<LibraryRecord> = reads::entries(conn, &c.collection)
            .await
            .map_err(e)?
            .into_iter()
            .filter(|en| en.target.item == item)
            .map(|mut en| {
                en.withdrawn_at = Some(Timestamp::now());
                LibraryRecord::Entry(en)
            })
            .collect();
        if gone.is_empty() {
            return Err(format!("{item} is not in {collection}"));
        }
        let n = gone.len();
        self.write(gone, actor, "uncollect").await?;
        Ok(json!({ "collection": collection, "item": item, "entries_withdrawn": n }))
    }

    pub async fn rate(
        &self,
        conn: &mut PgConnection,
        item: &str,
        value: f64,
        scale: &str,
        of: f64,
        actor: &str,
    ) -> R<Value> {
        self.item_or(conn, item).await?;
        if !(0.0..=of).contains(&value) {
            return Err(format!("a rating on {scale} is between 0 and {of}"));
        }
        let r = Rating {
            item: item.to_owned(),
            scale: scale.to_owned(),
            value,
            of,
            level: Level::Override,
            by: String::new(),
            at: Timestamp::now(),
            withdrawn_at: None,
        };
        self.write(vec![LibraryRecord::Rating(r)], actor, "rate")
            .await?;
        Ok(json!({ "item": item, "scale": scale, "value": value, "of": of }))
    }

    pub async fn compare(
        &self,
        conn: &mut PgConnection,
        winner: &str,
        loser: &str,
        scale: &str,
        actor: &str,
    ) -> R<Value> {
        if winner == loser {
            return Err("an item is not compared with itself".into());
        }
        self.item_or(conn, winner).await?;
        self.item_or(conn, loser).await?;
        let c = Comparison {
            comparison: items::new_id(),
            scale: scale.to_owned(),
            winner: winner.to_owned(),
            loser: loser.to_owned(),
            by: String::new(),
            at: Timestamp::now(),
            withdrawn_at: None,
        };
        self.write(vec![LibraryRecord::Comparison(c)], actor, "compare")
            .await?;
        Ok(json!({ "scale": scale, "winner": winner, "loser": loser }))
    }

    pub async fn viewed(
        &self,
        conn: &mut PgConnection,
        item: &str,
        seconds: Option<f64>,
        player: &str,
        actor: &str,
    ) -> R<Value> {
        let it = self.item_or(conn, item).await?;
        let now = Timestamp::now();
        let started = seconds
            .and_then(|s| Timestamp::from_unix_ns(now.unix_ns() - (s * 1e9) as i128).ok())
            .unwrap_or(now);
        let v = View {
            view: items::new_id(),
            target: Target {
                item: item.to_owned(),
                revision: Some(it.revision),
                rendition: None,
                part: None,
            },
            player: player.to_owned(),
            started_at: started,
            seconds,
            count: 1,
            level: Level::Override,
            by: String::new(),
        };
        self.write(vec![LibraryRecord::View(v)], actor, "viewed")
            .await?;
        Ok(json!({ "item": item, "seconds": seconds, "player": player }))
    }

    /// Bring in curation from sidecar files beside media under `path`.
    pub async fn import(&self, conn: &mut PgConnection, path: &Path, actor: &str) -> R<Value> {
        let root =
            std::fs::canonicalize(path).map_err(|err| format!("{}: {err}", path.display()))?;
        let found =
            tokio::task::spawn_blocking(move || -> Vec<(PathBuf, String, Vec<import::Sidecar>)> {
                let mut out = Vec::new();
                let mut files = Vec::new();
                media_files(&root, &mut files);
                for f in files {
                    let sidecars: Vec<import::Sidecar> = import::sidecars_of(&f)
                        .into_iter()
                        .filter_map(|(p, fmt)| import::read(&p, fmt))
                        .collect();
                    if sidecars.is_empty() {
                        continue;
                    }
                    if let Ok(d) = library_source::digest(&f) {
                        let mut sc = sidecars;
                        // A site's MD5 that disagrees with the file says the
                        // sidecar is about other bytes.
                        sc.retain(|s| s.md5.as_deref().is_none_or(|m| m == d.md5));
                        if !sc.is_empty() {
                            out.push((f, format!("blake3:{}", d.blake3), sc));
                        }
                    }
                }
                out
            })
            .await
            .map_err(e)?;
        let (mut matched, mut unmatched, mut values) = (0, 0, 0);
        let mut records = Vec::new();
        for (file, content, sidecars) in found {
            let items = reads::items_with(conn, &content).await.map_err(e)?;
            if items.is_empty() {
                unmatched += 1;
                continue;
            }
            matched += 1;
            for it in items {
                let existing = reads::assignments(conn, &it.item).await.map_err(e)?;
                for sc in &sidecars {
                    let source_ref = sc
                        .source_ref
                        .clone()
                        .or_else(|| Some(file.display().to_string()));
                    for v in &sc.values {
                        let dup = existing.iter().any(|a| {
                            a.withdrawn_at.is_none()
                                && a.level == Level::Import
                                && a.field == v.field
                                && a.value.as_deref() == Some(v.value.as_str())
                        });
                        if dup {
                            continue;
                        }
                        values += 1;
                        records.push(LibraryRecord::Assignment(Self::assignment(
                            &it.item,
                            &v.field,
                            Some(v.value.clone()),
                            Level::Import,
                            source_ref.clone(),
                        )));
                    }
                }
            }
        }
        for chunk in records.chunks(2000) {
            self.write(chunk.to_vec(), actor, "import").await?;
        }
        Ok(json!({
            "files_with_sidecars_matched": matched,
            "files_with_sidecars_not_in_library": unmatched,
            "values_imported": values,
        }))
    }
}

fn media_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        match entry.file_type() {
            Ok(t) if t.is_dir() => media_files(&p, out),
            Ok(t) if t.is_file() && !name.ends_with(".json") && !name.ends_with(".txt") => {
                out.push(p)
            }
            _ => {}
        }
    }
}
