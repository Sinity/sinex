//! Intake: scanning a library root and making or continuing items from what
//! the scan saw.
use super::{Library, R, e};
use serde_json::{Value, json};
use sinex_library::reads;
use sinex_model::event_types::library::FileCopy;
use sinex_source_core::binding::Settings;
use sinex_sources::sources;
use sqlx::PgConnection;
use std::cmp::Reverse;
use std::collections::BTreeMap;
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
        let selected = std::fs::canonicalize(path).map_err(e)?;
        let stored = sinex_store::bindings::for_source(conn, "library.scan")
            .await
            .map_err(e)?;
        let mut roots = std::collections::BTreeSet::new();
        for root in self.roots.iter().cloned().chain(
            stored
                .into_iter()
                .filter(|binding| binding.settings["host"] == self.host)
                .map(|binding| std::path::PathBuf::from(binding.input)),
        ) {
            let Ok(root) = std::fs::canonicalize(&root) else {
                continue;
            };
            let root = if root.is_file() {
                root.parent()
                    .ok_or("library root has no parent")?
                    .to_path_buf()
            } else {
                root
            };
            if selected.starts_with(&root) {
                roots.insert(root);
            }
        }
        if roots.len() > 1 {
            return Err("library selection belongs to several configured roots".into());
        }
        if let Some(root) = roots.first() {
            settings.options["root"] = json!(root);
        }
        let binding = (format.plan.ok_or("library format has no planner")?)(path, &settings)?
            .into_iter()
            .next()
            .ok_or("library input has no binding")?;
        let root_text = binding.input.display().to_string();
        let before = reads::copies(conn, self.host, &root_text)
            .await
            .map_err(e)?;
        let reader = crate::binding_reader::BindingReader::new(format, binding)?;
        crate::intake::request_class(conn, reader.source.as_ref()).await?;
        let counts =
            crate::binding_reader::once(conn, self.blobs, self.writer, self.host, vec![reader])
                .await?;
        let files = counts.events_offered;
        let seen = counts.admitted;
        let after = reads::copies(conn, self.host, &root_text)
            .await
            .map_err(e)?;
        let keys: std::collections::BTreeSet<_> =
            after.iter().map(|copy| copy.item.clone()).collect();
        let computed = crate::derivation_runner::compute_now(
            conn,
            self.writer,
            &sinex_derivations::library::LibraryItems,
            keys.into_iter().map(Value::String).collect(),
            self.host,
            self.time_settings.clone(),
        )
        .await?;
        let mut new_items = 0;
        let mut revisions = 0;
        let mut moves = 0;
        let before_by_item = copies_by_item(&before);
        for (item, copy) in copies_by_item(&after) {
            let old = before_by_item.get(item).copied();
            if copy.vanished_at.is_some() {
                continue;
            }
            match old {
                None => new_items += 1,
                Some(old) if old.content != copy.content => revisions += 1,
                Some(old) if old.path != copy.path && copy.moved_from.is_some() => moves += 1,
                _ => {}
            }
        }
        let written = computed.admitted;
        Ok(json!({
            "root": root_text,
            "stored_bytes": !reference,
            "files_described": files,
            "copies_admitted": seen,
            "scan_failures": counts.to_json()["failures"],
            "items_new": new_items,
            "items_moved": moves,
            "item_revisions": revisions,
            "library_records_written": written,
        }))
    }
}

/// Present copies take precedence over removals when describing an item.
fn copies_by_item(copies: &[FileCopy]) -> BTreeMap<&str, &FileCopy> {
    let mut latest: BTreeMap<&str, &FileCopy> = BTreeMap::new();
    for copy in copies {
        if latest.get(copy.item.as_str()).is_none_or(|old| {
            (copy.vanished_at.is_some(), Reverse(copy.part_index))
                < (old.vanished_at.is_some(), Reverse(old.part_index))
        }) {
            latest.insert(copy.item.as_str(), copy);
        }
    }
    latest
}
