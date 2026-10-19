//! The media library's operations (architecture §6.6), shared by the API and
//! `sinexd admin library-*`. Every change is a library request: its records
//! are stored as the request's own bytes and admitted through the one writer
//! (`sinex-sources`'s `library.request`), attributed to the caller. Reads
//! come from live events through `sinex_library::db`.
use crate::context::Sink;
use crate::writer::{Priority, WriterHandle};
use serde_json::{Value, json};
use sinex_library::{adopt, db, import, policy, rank, render};
use sinex_model::kinds::library::{
    Acquisition, Assignment, Collection, Comparison, Entry, Item, Level, LibraryRecord, Rating,
    Rendition, RenditionRole, Target, View,
};
use sinex_model::{RunId, Timestamp};
use sinex_protocol::Request;
use sinex_sources::sources::library as lib;
use sinex_sources::{Limits, drivers};
use sinex_store::Blobs;
use sqlx::PgConnection;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Who intake's own records are attributed to.
pub const INTAKE: &str = "library-intake";

/// What a library operation writes with.
pub struct Lib<'a> {
    pub writer: &'a WriterHandle,
    pub blobs: &'a Blobs,
    pub host: &'a str,
}

type R<T> = Result<T, String>;

fn e(err: impl std::fmt::Display) -> String {
    err.to_string()
}

impl Lib<'_> {
    /// Store and admit one library request. All its records commit together
    /// or none do (one material, admitted in one batch when it fits).
    pub async fn write(&self, records: Vec<LibraryRecord>, actor: &str, action: &str) -> R<Value> {
        if records.is_empty() {
            return Ok(json!({ "records": 0 }));
        }
        let n = records.len();
        let host = self.host.to_owned();
        let binding = format!("library.request:{actor}@{host}");
        let sink = Sink(self.blobs.clone());
        let actor_owned = actor.to_owned();
        let action = action.to_owned();
        let batches = tokio::task::spawn_blocking(move || {
            let acquired = lib::request_material(&records, &actor_owned, &host, &action);
            drivers::batches(
                &lib::request_source(),
                &binding,
                RunId::new(),
                acquired,
                Value::Null,
                &sink,
                Limits {
                    max_events: usize::MAX,
                    max_bytes: usize::MAX,
                },
            )
        })
        .await
        .map_err(e)?
        .map_err(e)?;
        let (mut admitted, mut replaced, mut failures) = (0, 0, 0);
        for b in batches {
            let o = self.writer.submit(Priority::Live, b).await.map_err(e)?;
            admitted += o.admitted;
            replaced += o.replaced;
            failures += o.failures;
        }
        if failures > 0 {
            return Err(format!(
                "{failures} of {n} library records failed to read back"
            ));
        }
        Ok(json!({ "records": n, "admitted": admitted, "replaced": replaced }))
    }

    /// Scan a library root, admit what changed, and make or continue items.
    pub async fn add(&self, conn: &mut PgConnection, path: &Path, reference: bool) -> R<Value> {
        let given =
            std::fs::canonicalize(path).map_err(|err| format!("{}: {err}", path.display()))?;
        // A single file is scanned within its folder, alone: nothing else in
        // the folder is read or reported missing.
        let (root, only) = if given.is_dir() {
            (given, None)
        } else {
            let parent = given
                .parent()
                .ok_or_else(|| format!("{}: no folder holds it", given.display()))?
                .to_path_buf();
            let name = given
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .ok_or_else(|| format!("{}: not a file name", given.display()))?;
            (parent, Some(name))
        };
        let root_text = root.display().to_string();
        // Seed the scan's memory from what is already admitted, so a second
        // add of the same root hashes only what changed.
        let known = db::acquisitions(conn, self.host, &root_text)
            .await
            .map_err(e)?;
        let memory =
            lib::ScanMemory::seed(known.iter().filter(|a| a.vanished_at.is_none()).filter_map(
                |a| {
                    let relative = Path::new(&a.path).strip_prefix(&root).ok()?;
                    let mtime_ns = a.modified_at.map(|t| t.unix_ns())?;
                    Some((
                        relative.to_string_lossy().into_owned(),
                        lib::Known {
                            size: a.size,
                            mtime_ns,
                            device: a.device,
                            inode: a.inode,
                            content: a.content.clone(),
                        },
                    ))
                },
            ));
        let binding = format!("library.scan:{root_text}@{}", self.host);
        let mut cursor = sinex_store::positions::load(conn, &binding)
            .await
            .ok()
            .flatten()
            .map(|(c, _)| c)
            .unwrap_or(Value::Null);
        let scan = std::sync::Arc::new(lib::LibraryScan {
            root: root.clone(),
            host: self.host.to_owned(),
            keep_bytes: !reference,
            probe: true,
            bytes_per_poll: 1 << 30,
            only,
        });
        let mut memory = Some(memory);
        let (mut files, mut seen) = (0usize, 0usize);
        loop {
            let s = scan.clone();
            let mut m = memory.take().expect("memory is returned each poll");
            let c = cursor.clone();
            let sink = Sink(self.blobs.clone());
            let b = binding.clone();
            let (m, out) = tokio::task::spawn_blocking(move || {
                let acquired = s.poll(&c, &mut m, &sink);
                let out = acquired.and_then(|a| {
                    if let Some(reason) = &a.observation.unavailable {
                        return Err(drivers::DriverError::Io(reason.clone()));
                    }
                    let next = a
                        .material
                        .as_ref()
                        .map(|mat| ((mat.cursor_at)(mat.bytes.len()), mat.records.len()));
                    let batches = drivers::batches(
                        &lib::scan_source(),
                        &b,
                        RunId::new(),
                        a,
                        Value::Null,
                        &sink,
                        Limits::default(),
                    )?;
                    Ok((next, batches))
                });
                (m, out)
            })
            .await
            .map_err(e)?;
            memory = Some(m);
            let (next, batches) = out.map_err(e)?;
            for b in batches {
                let o = self.writer.submit(Priority::Bulk, b).await.map_err(e)?;
                seen += o.admitted + o.replaced;
            }
            match next {
                Some((c, records)) => {
                    cursor = c;
                    files += records;
                }
                None => break,
            }
        }
        // Adopt: every present acquisition under the root, against the items
        // that adopt its path.
        let acquisitions = db::acquisitions(conn, self.host, &root_text)
            .await
            .map_err(e)?;
        let keys: Vec<String> = acquisitions
            .iter()
            .flat_map(|a| {
                let mut k = vec![adopt::key(&a.host, &a.path)];
                if let Some(from) = &a.moved_from {
                    k.push(adopt::key(&a.host, from));
                }
                k
            })
            .collect();
        let mut owners: HashMap<String, Item> = HashMap::new();
        for item in db::owners(conn, &keys).await.map_err(e)? {
            for k in &item.acquisitions {
                owners.insert(k.clone(), item.clone());
            }
        }
        let now = Timestamp::now();
        let mut records = Vec::new();
        let (mut new_items, mut revisions, mut moves) = (0, 0, 0);
        // Moves first, then the rest, so a rename continues its item before
        // anything else claims the bytes.
        let mut ordered: Vec<&Acquisition> = acquisitions.iter().collect();
        ordered.sort_by_key(|a| a.moved_from.is_none());
        for a in ordered {
            let out = adopt::adopt(
                a,
                |k| owners.get(k).cloned().map(|item| adopt::Owner { item }),
                INTAKE,
                now,
            );
            for r in &out {
                if let LibraryRecord::Item(i) = r {
                    match (i.revision, owners.values().any(|o| o.item == i.item)) {
                        (1, false) => new_items += 1,
                        (_, true) if a.moved_from.is_some() => moves += 1,
                        (r, true) if r > 1 => revisions += 1,
                        _ => {}
                    }
                    owners.retain(|_, o| o.item != i.item);
                    for k in &i.acquisitions {
                        owners.insert(k.clone(), i.clone());
                    }
                }
            }
            records.extend(out);
        }
        let mut written = 0;
        for chunk in records.chunks(2000) {
            let v = self.write(chunk.to_vec(), INTAKE, "intake").await?;
            written += v["records"].as_u64().unwrap_or(0);
        }
        Ok(json!({
            "root": root_text,
            "stored_bytes": !reference,
            "files_described": files,
            "acquisitions_admitted": seen,
            "items_new": new_items,
            "items_moved": moves,
            "item_revisions": revisions,
            "library_records_written": written,
        }))
    }

    async fn item_or(&self, conn: &mut PgConnection, item: &str) -> R<Item> {
        match db::item(conn, item).await.map_err(e)? {
            Some(i) if i.withdrawn_at.is_none() => Ok(i),
            Some(_) => Err(format!("item {item} was withdrawn")),
            None => Err(format!("no library item {item}")),
        }
    }

    fn assignment(
        item: &str,
        field: &str,
        value: Option<String>,
        level: Level,
        source_ref: Option<String>,
    ) -> Assignment {
        Assignment {
            assignment: adopt::mint(),
            target: Target {
                item: item.to_owned(),
                ..Target::default()
            },
            field: field.to_owned(),
            value,
            level,
            source_ref,
            by: String::new(),
            at: Timestamp::now(),
            withdrawn_at: None,
        }
    }

    fn withdraw(mut a: Assignment) -> LibraryRecord {
        a.withdrawn_at = Some(Timestamp::now());
        LibraryRecord::Assignment(a)
    }

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
        let existing = db::assignments(conn, item).await.map_err(e)?;
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
        let mine: Vec<Assignment> = db::assignments(conn, item)
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
        let mut records: Vec<LibraryRecord> = db::assignments(conn, item)
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
        let c = match db::collection_named(conn, collection).await.map_err(e)? {
            Some(c) => c,
            None => {
                let c = Collection {
                    collection: adopt::mint(),
                    name: collection.to_owned(),
                    saved_query: None,
                    frozen_from: None,
                    by: String::new(),
                    at: Timestamp::now(),
                    withdrawn_at: None,
                };
                records.push(LibraryRecord::Collection(c.clone()));
                c
            }
        };
        let last = db::entries(conn, &c.collection)
            .await
            .map_err(e)?
            .iter()
            .map(|en| en.position)
            .fold(0.0f64, f64::max);
        let entry = Entry {
            entry: adopt::mint(),
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
        let c = db::collection_named(conn, collection)
            .await
            .map_err(e)?
            .ok_or_else(|| format!("no collection named {collection}"))?;
        let gone: Vec<LibraryRecord> = db::entries(conn, &c.collection)
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
            comparison: adopt::mint(),
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
            view: adopt::mint(),
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

    /// A file holding exactly `content` for a player: a present copy whose
    /// bytes still match, or the stored bytes written out to the cache.
    async fn local_copy(
        &self,
        conn: &mut PgConnection,
        content: &str,
        media_type: &str,
    ) -> R<(PathBuf, bool)> {
        for a in db::acquisitions_of(conn, content).await.map_err(e)? {
            if a.vanished_at.is_some() {
                continue;
            }
            let p = PathBuf::from(&a.path);
            let size = a.size;
            let c = content.to_owned();
            let ok = tokio::task::spawn_blocking(move || render::verified(&p, size, &c))
                .await
                .map_err(e)?;
            if ok {
                return Ok((PathBuf::from(&a.path), true));
            }
        }
        let blobs = self.blobs.clone();
        let content = content.to_owned();
        let target = render::cache_path(&cache_dir(), &content, media_type);
        tokio::task::spawn_blocking(move || -> R<(PathBuf, bool)> {
            if render::verified(&target, std::fs::metadata(&target).map(|m| m.len()).unwrap_or(0), &content) {
                return Ok((target, false));
            }
            if !blobs.contains(&content).map_err(e)? {
                return Err(format!(
                    "no verified copy of {content} on disk, and its bytes are not stored (indexed by reference)"
                ));
            }
            if let Some(dir) = target.parent() {
                std::fs::create_dir_all(dir).map_err(e)?;
            }
            // Streamed: a long video is never held in memory.
            let tmp = target.with_extension("partial");
            let mut file = std::io::BufWriter::new(std::fs::File::create(&tmp).map_err(e)?);
            blobs.copy_to(&content, &mut file).map_err(e)?;
            std::io::Write::flush(&mut file).map_err(e)?;
            drop(file);
            std::fs::rename(&tmp, &target).map_err(e)?;
            Ok((target, false))
        })
        .await
        .map_err(e)?
    }

    /// Where to open an item: the target is fixed now, so a later change to
    /// the item never swaps what was asked for.
    pub async fn open(
        &self,
        conn: &mut PgConnection,
        item: &str,
        rendition: Option<&str>,
    ) -> R<Value> {
        let it = self.item_or(conn, item).await?;
        let (content, media_type, rendition_id) = match rendition {
            Some(id) => {
                let r = db::renditions(conn, item)
                    .await
                    .map_err(e)?
                    .into_iter()
                    .find(|r| r.rendition == id && r.withdrawn_at.is_none())
                    .ok_or_else(|| format!("{item} has no rendition {id}"))?;
                (r.content, r.media_type, Some(r.rendition))
            }
            None => (it.content.clone(), it.media_type.clone(), None),
        };
        let (path, on_disk) = self.local_copy(conn, &content, &media_type).await?;
        Ok(json!({
            "item": item,
            "revision": it.revision,
            "rendition": rendition_id,
            "content": content,
            "media_type": media_type,
            "path": path,
            "verified_copy_on_disk": on_disk,
            "player": render::player_for(&media_type, None),
        }))
    }

    pub async fn preview(&self, conn: &mut PgConnection, item: &str, actor: &str) -> R<Value> {
        let it = self.item_or(conn, item).await?;
        if !render::previewable(&it.media_type) {
            return Err(format!("{} has no still preview ({})", item, it.media_type));
        }
        let renditions = db::renditions(conn, item).await.map_err(e)?;
        let original = renditions
            .iter()
            .find(|r| {
                r.role == RenditionRole::Original
                    && r.revision == it.revision
                    && r.withdrawn_at.is_none()
            })
            .map(|r| r.rendition.clone());
        let (input, _) = self.local_copy(conn, &it.content, &it.media_type).await?;
        let blobs = self.blobs.clone();
        let out = std::env::temp_dir().join(format!("sinex-preview-{}.webp", adopt::mint()));
        let o = out.clone();
        let (content, w, h) =
            tokio::task::spawn_blocking(move || -> R<(String, Option<u32>, Option<u32>)> {
                render::make_preview(&input, &o)?;
                let d = lib::digest(&o).map_err(e)?;
                let (w, h, _) = lib::dimensions(&d.head, &d.media_type);
                let file = std::fs::File::open(&o).map_err(e)?;
                let name = blobs.put_reader(std::io::BufReader::new(file)).map_err(e)?;
                let _ = std::fs::remove_file(&o);
                Ok((name, w, h))
            })
            .await
            .map_err(e)??;
        let r = Rendition {
            rendition: adopt::mint(),
            item: item.to_owned(),
            revision: it.revision,
            role: RenditionRole::Preview,
            content: content.clone(),
            media_type: "image/webp".into(),
            width: w,
            height: h,
            duration_ms: None,
            made_from: original,
            made_with: render::preview_settings(),
            by: String::new(),
            at: Timestamp::now(),
            withdrawn_at: None,
        };
        let id = r.rendition.clone();
        self.write(vec![LibraryRecord::Rendition(r)], actor, "preview")
            .await?;
        Ok(json!({ "item": item, "rendition": id, "content": content, "width": w, "height": h }))
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
                    if let Ok(d) = lib::digest(&f) {
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
            let items = db::items_with(conn, &content).await.map_err(e)?;
            if items.is_empty() {
                unmatched += 1;
                continue;
            }
            matched += 1;
            for it in items {
                let existing = db::assignments(conn, &it.item).await.map_err(e)?;
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

/// Where stored bytes are written out for players.
pub fn cache_dir() -> PathBuf {
    std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))
        .unwrap_or_else(std::env::temp_dir)
        .join("sinex/library")
}

/// One item, as the operator reads it.
pub async fn item_view(conn: &mut PgConnection, item: &str) -> R<Value> {
    let it = db::item(conn, item)
        .await
        .map_err(e)?
        .ok_or_else(|| format!("no library item {item}"))?;
    let revisions = db::revisions(conn, item).await.map_err(e)?;
    let renditions = db::renditions(conn, item).await.map_err(e)?;
    let assignments = db::assignments(conn, item).await.map_err(e)?;
    let ratings = db::ratings(conn, item).await.map_err(e)?;
    let views = db::views(conn, item).await.map_err(e)?;
    let entries = db::entries_of(conn, item).await.map_err(e)?;
    let copies = db::acquisitions_at(conn, &it.acquisitions)
        .await
        .map_err(e)?;
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
            let v = match policy::property(&a) {
                policy::Shown::Unknown => json!({ "state": "unknown" }),
                policy::Shown::Empty { by } => json!({ "state": "empty", "assignment": by }),
                policy::Shown::Value { value, by, level } => {
                    json!({ "state": "value", "value": value, "level": level, "assignment": by })
                }
                policy::Shown::Conflict { level, values } => {
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
        "tags": policy::tags(&tags).into_iter().map(|t| json!({"tag": t.tag, "level": t.level})).collect::<Vec<_>>(),
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
pub async fn collection_view(conn: &mut PgConnection, name: Option<&str>) -> R<Value> {
    match name {
        None => {
            let all = db::collections(conn).await.map_err(e)?;
            let mut rows = Vec::new();
            for c in all {
                let n = db::entries(conn, &c.collection).await.map_err(e)?.len();
                rows.push(json!({ "name": c.name, "collection": c.collection, "entries": n, "saved_query": c.saved_query }));
            }
            Ok(json!({ "collections": rows }))
        }
        Some(name) => {
            let c = db::collection_named(conn, name)
                .await
                .map_err(e)?
                .ok_or_else(|| format!("no collection named {name}"))?;
            let entries = db::entries(conn, &c.collection).await.map_err(e)?;
            Ok(json!({
                "name": c.name,
                "collection": c.collection,
                "entries": entries.iter().map(|en| json!({"entry": en.entry, "item": en.target.item, "revision": en.target.revision, "position": en.position})).collect::<Vec<_>>(),
            }))
        }
    }
}

/// Items ranked from pairwise comparisons on a scale.
pub async fn rank_view(conn: &mut PgConnection, scale: &str) -> R<Value> {
    let judgements: Vec<(String, String)> = db::comparisons(conn, scale)
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

/// Run one library command: reads need only `conn`; writes need the writer.
pub async fn dispatch(
    conn: &mut PgConnection,
    writer: Option<&WriterHandle>,
    blobs: &Blobs,
    host: &str,
    request: Request,
    actor: &str,
) -> R<Value> {
    let reads_only = matches!(
        request,
        Request::LibraryItem { .. }
            | Request::LibraryCollection { .. }
            | Request::LibraryRank { .. }
    );
    let writer = match (writer, reads_only) {
        (Some(w), _) => w,
        (None, true) => {
            return match request {
                Request::LibraryItem { item } => item_view(conn, &item).await,
                Request::LibraryCollection { collection } => {
                    collection_view(conn, collection.as_deref()).await
                }
                Request::LibraryRank { scale } => {
                    rank_view(conn, scale.as_deref().unwrap_or("overall")).await
                }
                _ => unreachable!("reads only"),
            };
        }
        (None, false) => {
            return Err("this daemon has no writer; writes go through `sinexd run`".into());
        }
    };
    let lib = Lib {
        writer,
        blobs,
        host,
    };
    let number = |s: &str, what: &str| {
        s.trim()
            .parse::<f64>()
            .map_err(|_| format!("{what} must be a number, not `{s}`"))
    };
    match request {
        Request::LibraryAdd { path, reference } => {
            lib.add(
                conn,
                std::path::Path::new(&path),
                reference.unwrap_or(false),
            )
            .await
        }
        Request::LibraryItem { item } => item_view(conn, &item).await,
        Request::LibraryTag { item, tag } => lib.tag(conn, &item, &tag, actor).await,
        Request::LibraryUntag { item, tag } => lib.untag(conn, &item, &tag, actor).await,
        Request::LibrarySet {
            item,
            field,
            value,
            empty,
        } => {
            let value = match (value, empty.unwrap_or(false)) {
                (Some(_), true) => return Err("give a value or --empty, not both".into()),
                (None, false) => return Err("give a value, or --empty".into()),
                (v, _) => v,
            };
            lib.set(conn, &item, &field, value, actor).await
        }
        Request::LibraryCollect { collection, item } => {
            lib.collect(conn, &collection, &item, actor).await
        }
        Request::LibraryUncollect { collection, item } => {
            lib.uncollect(conn, &collection, &item, actor).await
        }
        Request::LibraryCollection { collection } => {
            collection_view(conn, collection.as_deref()).await
        }
        Request::LibraryRate {
            item,
            value,
            scale,
            of,
        } => {
            let value = number(&value, "a rating")?;
            let of = of
                .as_deref()
                .map(|o| number(o, "--of"))
                .transpose()?
                .unwrap_or(5.0);
            lib.rate(
                conn,
                &item,
                value,
                scale.as_deref().unwrap_or("stars"),
                of,
                actor,
            )
            .await
        }
        Request::LibraryCompare {
            winner,
            loser,
            scale,
        } => {
            lib.compare(
                conn,
                &winner,
                &loser,
                scale.as_deref().unwrap_or("overall"),
                actor,
            )
            .await
        }
        Request::LibraryRank { scale } => {
            rank_view(conn, scale.as_deref().unwrap_or("overall")).await
        }
        Request::LibraryOpen { item, rendition } => {
            lib.open(conn, &item, rendition.as_deref()).await
        }
        Request::LibraryViewed {
            item,
            seconds,
            player,
        } => {
            let seconds = seconds
                .as_deref()
                .map(|s| number(s, "--seconds"))
                .transpose()?;
            lib.viewed(
                conn,
                &item,
                seconds,
                player.as_deref().unwrap_or("unknown"),
                actor,
            )
            .await
        }
        Request::LibraryPreview { item } => lib.preview(conn, &item, actor).await,
        Request::LibraryImport { path } => {
            lib.import(conn, std::path::Path::new(&path), actor).await
        }
        _ => unreachable!("only library requests reach here"),
    }
}

#[cfg(test)]
mod tests;
