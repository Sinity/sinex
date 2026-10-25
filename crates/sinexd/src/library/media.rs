//! Handing items to players and making previews: a verified local copy of the
//! exact bytes, or the stored bytes written out to the cache.
use super::{Library, R, e};
use serde_json::{Value, json};
use sinex_library::{items, reads, render};
use sinex_model::Timestamp;
use sinex_model::event_types::library::{LibraryRecord, Rendition, RenditionRole};
use sinex_sources::sources::library as library_source;
use sqlx::PgConnection;
use std::path::PathBuf;

impl Library<'_> {
    /// A file holding exactly `content` for a player: a present copy whose
    /// bytes still match, or the stored bytes written out to the cache.
    async fn local_copy(
        &self,
        conn: &mut PgConnection,
        content: &str,
        media_type: &str,
    ) -> R<(PathBuf, bool)> {
        for a in reads::acquisitions_of(conn, content).await.map_err(e)? {
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
            // Plain media streams to the cache; delta-encoded content is
            // decoded whole first.
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
                let r = reads::renditions(conn, item)
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
        let renditions = reads::renditions(conn, item).await.map_err(e)?;
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
        let out = std::env::temp_dir().join(format!("sinex-preview-{}.webp", items::new_id()));
        let o = out.clone();
        let (stored, w, h) = tokio::task::spawn_blocking(
            move || -> R<(sinex_model::Cited, Option<u32>, Option<u32>)> {
                render::make_preview(&input, &o)?;
                let d = library_source::digest(&o).map_err(e)?;
                let (w, h, _) = library_source::dimensions(&d.head, &d.media_type);
                let file = std::fs::File::open(&o).map_err(e)?;
                let size = file.metadata().map_err(e)?.len();
                let blob = blobs.put_reader(std::io::BufReader::new(file)).map_err(e)?;
                let _ = std::fs::remove_file(&o);
                Ok((sinex_model::Cited { blob, size }, w, h))
            },
        )
        .await
        .map_err(e)??;
        let content = stored.blob.clone();
        let r = Rendition {
            rendition: items::new_id(),
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
        self.write_citing(
            conn,
            vec![LibraryRecord::Rendition(r)],
            actor,
            "preview",
            vec![stored],
        )
        .await?;
        Ok(json!({ "item": item, "rendition": id, "content": content, "width": w, "height": h }))
    }
}

/// Where stored bytes are written out for players.
fn cache_dir() -> PathBuf {
    std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))
        .unwrap_or_else(std::env::temp_dir)
        .join("sinex/library")
}
