//! Intake: scanning a library root and making or continuing items from what
//! the scan saw.
use super::{Library, R, e};
use serde_json::{Value, json};
use sinex_library::reads;
use sinex_source_core::binding::Settings;
use sinex_sources::sources;
use sqlx::PgConnection;
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
        let before = reads::copies(conn, self.host, &root_text)
            .await
            .map_err(e)?;
        let reader = crate::binding_reader::BindingReader::new(format, binding)?;
        crate::intake::request_class(conn, reader.source.as_ref()).await?;
        let counts =
            crate::binding_reader::once(conn, self.blobs, self.writer, self.host, vec![reader])
                .await?;
        let files = counts.events_offered;
        let seen = counts.admitted + counts.replaced;
        let after = reads::copies(conn, self.host, &root_text)
            .await
            .map_err(e)?;
        let keys: std::collections::BTreeSet<_> =
            after.iter().map(|copy| copy.item.clone()).collect();
        crate::derivation_runner::compute_now(
            conn,
            self.writer,
            &sinex_derivations::library::LibraryItems,
            keys.into_iter().map(Value::String).collect(),
            self.host,
        )
        .await?;
        let mut new_items = 0;
        let mut revisions = 0;
        let mut moves = 0;
        for copy in &after {
            let old = before
                .iter()
                .filter(|old| old.item == copy.item)
                .min_by_key(|old| (old.vanished_at.is_some(), std::cmp::Reverse(old.part_index)));
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
        let written = (new_items + revisions) * 2 + moves;
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
