//! Publish page payload and dense map as one evidence material.
use sinex_model::{Class, MaterialRef, SqliteImagePublication, WrappedMaterialKey};
use sinex_sources::drivers::sqlite_image::Image;
use sinex_store::{Blobs, sealed};
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

type PageHash = Arc<dyn Fn(&[u8; 32]) -> [u8; 32] + Send + Sync>;

pub(crate) fn store(
    blobs: &Blobs,
    image: &Image,
    recipient: Option<&sealed::Recipient>,
) -> Result<SqliteImagePublication, String> {
    let meta = image.metadata.clone();
    let key = if meta.class == Class::Sealed {
        Some(sealed::new_key().map_err(err)?)
    } else {
        None
    };
    if key.is_some() && recipient.is_none() {
        return Err("sealed SQLite image needs the configured public recipient".into());
    }
    let naming = key
        .as_ref()
        .map(|k| blake3::derive_key("sinex 2026 SQLite image naming key", &k.key));
    let page_hash: PageHash = match naming {
        Some(k) => Arc::new(move |h| *blake3::keyed_hash(&k, h).as_bytes()),
        None => Arc::new(|h| *h),
    };
    let reader = image.reader(page_hash.clone(), false).map_err(err)?;
    let blob = match &key {
        Some(k) => blobs.put_sealed_reader(reader, k),
        None => blobs.put_reader(reader),
    }
    .map_err(err)?;
    verify_installed(blobs, image, key.as_ref(), recipient, &blob)
}

fn verify_installed(
    blobs: &Blobs,
    image: &Image,
    key: Option<&sealed::DataKey>,
    recipient: Option<&sealed::Recipient>,
    blob: &str,
) -> Result<SqliteImagePublication, String> {
    let mut meta = image.metadata.clone();
    let naming = key.map(|k| blake3::derive_key("sinex 2026 SQLite image naming key", &k.key));
    let page_hash: PageHash = match naming {
        Some(k) => Arc::new(move |h| *blake3::keyed_hash(&k, h).as_bytes()),
        None => Arc::new(|h| *h),
    };
    let extents = image.extents(&blob)?;
    meta.validate(&extents)?;
    meta.image_hash = verify_layout(
        blobs,
        &meta,
        &extents,
        blob,
        key,
        Some(image),
        page_hash,
        naming,
    )?;
    fill_costs(
        blobs,
        &mut meta,
        &extents,
        blob,
        image.previous.as_ref().map(|p| &p.metadata),
    )?;
    let wrapped = key
        .as_ref()
        .map(|k| {
            Ok::<_, String>(WrappedMaterialKey {
                material: meta.material,
                key_ref: k.key_ref.clone(),
                wrapped: sealed::wrap(k, recipient.expect("checked recipient")).map_err(err)?,
            })
        })
        .transpose()?;
    let material = MaterialRef {
        id: meta.material,
        input: None,
        blob: blob.to_owned(),
        length: meta.evidence_bytes,
        part_index: 0,
        previous_part: None,
        parser: "sqlite-image".into(),
        parser_version: 1,
        manifest: serde_json::json!({"role":"sqlite-image","sqlite_database":meta.database,"sqlite_image":meta}),
    };
    let publication = SqliteImagePublication {
        metadata: meta,
        material,
        extents,
        key: wrapped,
    };
    publication.validate()?;
    Ok(publication)
}

fn verify_layout(
    blobs: &Blobs,
    meta: &sinex_model::SqliteImageMeta,
    extents: &[sinex_model::ImageExtent],
    blob: &str,
    key: Option<&sealed::DataKey>,
    image: Option<&Image>,
    page_hash: PageHash,
    naming: Option<[u8; 32]>,
) -> Result<String, String> {
    // Verify exact reconstructed pages against the still-pinned acquisition view,
    // and each dense map entry against the corresponding page. Hashes alone do not
    // establish that a patch's extent layout selects the right bytes.
    if key.is_none() {
        let check = match image {
            Some(image) => image.checker()?,
            None => Arc::new(|| Ok(())),
        };
        drop(
            blobs
                .read_plain(&blob, Some(meta.evidence_bytes), move || {
                    check().map_err(|e| std::io::Error::other(e).into())
                })
                .map_err(err)?,
        );
    }
    let evidence_extent = [sinex_model::ImageExtent {
        blob: blob.to_owned(),
        offset: 0,
        length: meta.evidence_bytes,
    }];
    let mut evidence: Box<dyn sinex_sources::drivers::export_archive::group::ReadSeek + '_> =
        match &key {
            Some(k) => Box::new(
                blobs
                    .read_sealed(
                        &blob,
                        Some(meta.evidence_bytes),
                        k,
                        crate::sealed_access::read_limits(meta.evidence_bytes)?,
                        || Ok(()),
                    )
                    .map_err(err)?,
            ),
            None => Box::new(blobs.assemble(&evidence_extent).map_err(err)?),
        };
    evidence
        .seek(SeekFrom::Start(meta.map_offset))
        .map_err(err)?;
    let mut full_hash = match naming {
        Some(k) => blake3::Hasher::new_keyed(&k),
        None => blake3::Hasher::new(),
    };
    let mut source = image
        .map(|image| image.reader(page_hash.clone(), true).map_err(err))
        .transpose()?;
    let mut full: Box<dyn Read + '_> = match &key {
        Some(k) => Box::new(
            blobs
                .read_sealed(
                    &blob,
                    Some(meta.evidence_bytes),
                    k,
                    crate::sealed_access::read_limits(meta.evidence_bytes)?,
                    || Ok(()),
                )
                .map_err(err)?,
        ),
        None => Box::new(blobs.assemble(&extents).map_err(err)?),
    };
    let mut expected = vec![0; meta.page_size as usize];
    let mut actual = vec![0; meta.page_size as usize];
    for _ in 0..meta.page_count {
        full.read_exact(&mut actual).map_err(err)?;
        if let Some(source) = &mut source {
            source.read_exact(&mut expected).map_err(err)?;
            if actual != expected {
                return Err("SQLite retained extents differ from the pinned image".into());
            }
        }
        full_hash.update(&actual);
        let mut stored = [0; 32];
        evidence.read_exact(&mut stored).map_err(err)?;
        if stored != page_hash(blake3::hash(&actual).as_bytes()) {
            return Err("SQLite page map differs from retained image".into());
        }
    }
    let hash = format!("blake3:{}", full_hash.finalize().to_hex());
    if key.is_none() && meta.image_hash != hash {
        return Err("SQLite image digest differs after storage".into());
    }
    Ok(hash)
}
fn fill_costs(
    blobs: &Blobs,
    meta: &mut sinex_model::SqliteImageMeta,
    extents: &[sinex_model::ImageExtent],
    blob: &str,
    previous: Option<&sinex_model::SqliteImageMeta>,
) -> Result<(), String> {
    let stored = blobs.stored_size(&blob).map_err(err)?;
    if meta.full {
        meta.base_stored_bytes = stored;
        meta.patch_stored_bytes = 0;
    } else {
        let previous = previous.ok_or("SQLite patch has no previous image")?;
        if meta.previous != Some(previous.revision)
            || meta.base != previous.base
            || meta.class != previous.class
            || meta.database != previous.database
            || meta.host != previous.host
        {
            return Err("SQLite stored cost predecessor differs from reserved image".into());
        }
        meta.base_stored_bytes = previous.base_stored_bytes;
        meta.patch_stored_bytes = previous
            .patch_stored_bytes
            .checked_add(stored)
            .ok_or("SQLite stored patch cost overflow")?;
    }
    let mut unique = std::collections::BTreeSet::new();
    for e in extents {
        unique.insert(&e.blob);
    }
    meta.read_stored_bytes = unique.into_iter().try_fold(0u64, |sum, name| {
        sum.checked_add(blobs.stored_size(name).map_err(err)?)
            .ok_or_else(|| "SQLite read cost overflow".to_owned())
    })?;
    Ok(())
}

/// Restart verifies only reserved retained bytes, never a new provider view.
/// The frozen whole-image digest and dense map cover the exact original layout.
pub(crate) fn verify_retained_plan(
    blobs: &Blobs,
    plan: &sinex_store::native_materials::PendingImage,
    previous: Option<&sinex_model::SqliteImageMeta>,
) -> Result<SqliteImagePublication, String> {
    let sinex_store::native_materials::PendingImage::AwaitingCosts(expected) = plan;
    expected.validate()?;
    if expected.metadata.class == Class::Sealed {
        return Err("unpriced sealed image is unsupported".into());
    }
    let mut completed = expected.clone();
    let hash = verify_layout(
        blobs,
        &completed.metadata,
        &completed.extents,
        &completed.material.blob,
        None,
        None,
        Arc::new(|h| *h),
        None,
    )?;
    if hash != completed.metadata.image_hash {
        return Err("reserved SQLite image digest differs after restart".into());
    }
    fill_costs(
        blobs,
        &mut completed.metadata,
        &completed.extents,
        &completed.material.blob,
        previous,
    )?;
    completed.material.manifest["sqlite_image"] =
        serde_json::to_value(&completed.metadata).map_err(err)?;
    completed.validate()?;
    Ok(completed)
}

/// An unpriced image plan keeps its pinned view until the installed CAS winner
/// has passed the same page/map reconstruction checks as ordinary publication.
pub(crate) struct PreparedImage {
    blobs: Blobs,
    image: Arc<Image>,
    expected: SqliteImagePublication,
}
impl PreparedImage {
    pub fn plan(&self) -> Result<sinex_store::native_materials::PendingImage, String> {
        sinex_store::native_materials::PendingImage::awaiting(self.expected.clone()).map_err(err)
    }
    pub fn verify(self) -> Result<SqliteImagePublication, String> {
        let mut completed = verify_installed(
            &self.blobs,
            &self.image,
            None,
            None,
            &self.expected.material.blob,
        )?;
        completed.material.parser = self.expected.material.parser.clone();
        completed.material.parser_version = self.expected.material.parser_version;
        self.image.retain(completed.clone())?;
        Ok(completed)
    }
}
pub(crate) fn prepare_plain(
    blobs: &Blobs,
    image: Arc<Image>,
    parser: &str,
    parser_version: u32,
) -> Result<(PreparedImage, sinex_store::blobs::PreparedPlain), String> {
    if image.metadata.class == Class::Sealed {
        return Err("sealed image requires its keyed publication path".into());
    }
    let stage = blobs
        .prepare_plain(image.reader(Arc::new(|h| *h), false).map_err(err)?)
        .map_err(err)?;
    if stage.length() != image.metadata.evidence_bytes {
        return Err("prepared image length differs from capture".into());
    }
    let mut metadata = image.metadata.clone();
    metadata.base_stored_bytes = 0;
    metadata.patch_stored_bytes = 0;
    metadata.read_stored_bytes = 0;
    let extents = image.extents(&stage.name())?;
    let material = MaterialRef {
        id: metadata.material,
        input: None,
        blob: stage.name(),
        length: metadata.evidence_bytes,
        part_index: 0,
        previous_part: None,
        parser: parser.into(),
        parser_version,
        manifest: serde_json::json!({"role":"sqlite-image","sqlite_database":metadata.database,"sqlite_image":metadata}),
    };
    let expected = SqliteImagePublication {
        metadata,
        material,
        extents,
        key: None,
    };
    expected.validate()?;
    Ok((
        PreparedImage {
            blobs: blobs.clone(),
            image,
            expected,
        },
        stage,
    ))
}

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

#[cfg(test)]
mod tests;
