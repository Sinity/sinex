//! Intake: scanning a library root and making or continuing items from what
//! the scan saw.
use super::{INTAKE, Library, R, e};
use serde_json::{Value, json};
use sinex_library::{items, reads};
use sinex_model::Timestamp;
use sinex_model::event_types::library::{FileCopy, Item, LibraryRecord};
use sinex_sources::drivers;
use sinex_sources::sources::library as library_source;
use sqlx::PgConnection;
use std::collections::HashMap;
use std::path::Path;

impl Library<'_> {
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
        let binding = format!("library.scan:{root_text}@{}", self.host);
        // Seed the scan's state from what is already admitted, so a second
        // add of the same root hashes only what changed.
        let known = reads::copies(conn, self.host, &root_text)
            .await
            .map_err(e)?;
        let state = library_source::ScanState::seed(
            known
                .iter()
                .filter(|a| a.vanished_at.is_none())
                .filter_map(|a| {
                    let relative = Path::new(&a.path).strip_prefix(&root).ok()?;
                    let mtime_ns = a.modified_at.map(|t| t.unix_ns())?;
                    Some((
                        relative.to_string_lossy().into_owned(),
                        library_source::Known {
                            size: a.size,
                            mtime_ns,
                            device: a.device,
                            inode: a.inode,
                            content: a.content.clone(),
                        },
                    ))
                }),
        );
        let mut cursor = sinex_store::positions::load(conn, &binding)
            .await
            .ok()
            .flatten()
            .map(|(c, _)| c)
            .unwrap_or(Value::Null);
        let scan = std::sync::Arc::new(library_source::LibraryScan {
            root: root.clone(),
            host: self.host.to_owned(),
            keep_bytes: !reference,
            probe: true,
            bytes_per_poll: 1 << 30,
            only,
        });
        let mut state = Some(state);
        let (mut files, mut seen) = (0usize, 0usize);
        loop {
            let s = scan.clone();
            let mut m = state.take().expect("state is returned each poll");
            let c = cursor.clone();
            let sink = crate::material_sink::Sink(self.blobs.clone());
            let (m, out) = tokio::task::spawn_blocking(move || {
                let acquired = s.poll(&c, &mut m, &sink);
                let out = acquired.and_then(|a| {
                    if let Some(reason) = &a.observation.unavailable {
                        return Err(drivers::DriverError::Io(reason.clone()));
                    }
                    let next = a
                        .material
                        .as_ref()
                        .map(|mat| {
                            Ok::<_, drivers::DriverError>((
                                (mat.cursor_at)(mat.bytes.len())?,
                                mat.records.len(),
                            ))
                        })
                        .transpose()?;
                    Ok((next, a))
                });
                (m, out)
            })
            .await
            .map_err(e)?;
            state = Some(m);
            let (next, acquired) = out.map_err(e)?;
            let admitted = crate::authored::submit(
                conn,
                self.writer,
                self.blobs,
                std::sync::Arc::new(library_source::scan_source()),
                &binding,
                acquired,
            )
            .await?;
            seen += admitted.outcome.admitted + admitted.outcome.replaced;
            match next {
                Some((c, records)) => {
                    cursor = c;
                    files += records;
                }
                None => break,
            }
        }
        // Every present copy under the root, against the items its
        // path belongs to.
        let copies = reads::copies(conn, self.host, &root_text)
            .await
            .map_err(e)?;
        let keys: Vec<String> = copies
            .iter()
            .flat_map(|a| {
                let mut k = vec![items::copy_key(&a.host, &a.path)];
                if let Some(from) = &a.moved_from {
                    k.push(items::copy_key(&a.host, from));
                }
                k
            })
            .collect();
        let mut holders: HashMap<String, Item> = HashMap::new();
        for item in reads::items_at(conn, &keys).await.map_err(e)? {
            for k in &item.copies {
                holders.insert(k.clone(), item.clone());
            }
        }
        let now = Timestamp::now();
        let mut records = Vec::new();
        let (mut new_items, mut revisions, mut moves) = (0, 0, 0);
        // Moves first, then the rest, so a rename continues its item before
        // anything else claims the bytes.
        let mut ordered: Vec<&FileCopy> = copies.iter().collect();
        ordered.sort_by_key(|a| a.moved_from.is_none());
        for a in ordered {
            let out = items::records_for(a, |k| holders.get(k).cloned(), INTAKE, now);
            for r in &out {
                if let LibraryRecord::Item(i) = r {
                    match (i.revision, holders.values().any(|o| o.item == i.item)) {
                        (1, false) => new_items += 1,
                        (_, true) if a.moved_from.is_some() => moves += 1,
                        (r, true) if r > 1 => revisions += 1,
                        _ => {}
                    }
                    holders.retain(|_, o| o.item != i.item);
                    for k in &i.copies {
                        holders.insert(k.clone(), i.clone());
                    }
                }
            }
            records.extend(out);
        }
        let mut written = 0;
        for chunk in records.chunks(2000) {
            let v = self.write(conn, chunk.to_vec(), INTAKE, "intake").await?;
            written += v["records"].as_u64().unwrap_or(0);
        }
        Ok(json!({
            "root": root_text,
            "stored_bytes": !reference,
            "files_described": files,
            "copies_admitted": seen,
            "items_new": new_items,
            "items_moved": moves,
            "item_revisions": revisions,
            "library_records_written": written,
        }))
    }
}
