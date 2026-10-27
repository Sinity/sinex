//! Intake: scanning a library root and making or continuing items from what
//! the scan saw.
use super::{INTAKE, Library, R, e};
use serde_json::{Value, json};
use sinex_library::{items, reads};
use sinex_model::Timestamp;
use sinex_model::event_types::library::{FileCopy, Item, LibraryRecord};
use sinex_sources::sources::library as library_source;
use sinex_sources::{Reader, drivers};
use sqlx::PgConnection;
use std::collections::HashMap;
use std::path::Path;

impl Library<'_> {
    /// Scan a library root, admit what changed, and make or continue items.
    pub(crate) async fn add(
        &self,
        conn: &mut PgConnection,
        path: &Path,
        reference: bool,
    ) -> R<Value> {
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
        let mut reader = library_source::LibraryReader::new(library_source::LibraryScan {
            root: root.clone(),
            host: self.host.to_owned(),
            keep_bytes: !reference,
            probe: true,
            bytes_per_poll: 1 << 30,
            only,
        });
        // What the scan knew when it last stopped, so a second add of the
        // same root hashes only what changed.
        let (_stop, stop) = tokio::sync::watch::channel(false);
        crate::binding_reader::restore_reader(conn, self.blobs, &mut reader, &binding, None, &stop)
            .await?;
        let mut cursor = sinex_store::positions::load(conn, &binding)
            .await
            .map_err(e)?
            .map(|(c, _)| c)
            .unwrap_or(Value::Null);
        let mut reader = Some(reader);
        let (mut files, mut seen) = (0usize, 0usize);
        loop {
            let mut r = reader.take().expect("the reader is returned each poll");
            let c = cursor.clone();
            let (r, out) = tokio::task::spawn_blocking(move || {
                let out = r.poll(&c).and_then(|a| {
                    if let Some(reason) = &a.observation.unavailable {
                        return Err(drivers::DriverError::Io(reason.clone()));
                    }
                    let next = a
                        .material
                        .as_ref()
                        .map(|m| {
                            let length = match &m.stored {
                                Some(drivers::StoredBytes::FramedMemory(bytes)) => bytes.len(),
                                _ => m.bytes.len(),
                            };
                            Ok::<_, drivers::DriverError>(((m.cursor_at)(length)?, m.records.len()))
                        })
                        .transpose()?;
                    Ok((next, a))
                });
                (r, out)
            })
            .await
            .map_err(e)?;
            reader = Some(r);
            let (next, acquired) = out.map_err(e)?;
            let admitted = crate::requests::submit(
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
