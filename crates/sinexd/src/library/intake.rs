//! Intake: scanning a library root and making or continuing items from what
//! the scan saw.
use super::{INTAKE, Library, R, e};
use serde_json::{Value, json};
use sinex_library::{items, reads};
use sinex_model::Timestamp;
use sinex_model::event_types::library::{FileCopy, Item, LibraryRecord};
use sinex_sources::{Settings, sources};
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
        let format = sources::format("library").ok_or("library format is not registered")?;
        let mut settings = Settings::new(self.host);
        settings.options = json!({"reference": reference});
        let binding = (format.plan.ok_or("library format has no planner")?)(path, &settings)?
            .into_iter()
            .next()
            .ok_or("library input has no binding")?;
        let root_text = binding.input.display().to_string();
        let reader = crate::binding_reader::BindingReader::new(format, binding)?;
        crate::intake::request_class(conn, reader.source.as_ref()).await?;
        let counts =
            crate::binding_reader::once(conn, self.blobs, self.writer, self.host, vec![reader])
                .await?;
        let files = counts.events_offered;
        let seen = counts.admitted + counts.replaced;
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
