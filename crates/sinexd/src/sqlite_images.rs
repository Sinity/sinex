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
    let mut meta = image.metadata.clone();
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
    let extents = image.extents(&blob)?;
    meta.validate(&extents)?;
    // Verify exact reconstructed pages against the still-pinned acquisition view,
    // and each dense map entry against the corresponding page. Hashes alone do not
    // establish that a patch's extent layout selects the right bytes.
    if key.is_none() {
        blobs.verify(&blob).map_err(err)?;
    }
    let evidence_extent = [sinex_model::ImageExtent {
        blob: blob.clone(),
        offset: 0,
        length: meta.evidence_bytes,
    }];
    let mut evidence: Box<dyn sinex_sources::drivers::export_archive::group::ReadSeek> = match &key
    {
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
    let mut full_hash = match naming {
        Some(k) => blake3::Hasher::new_keyed(&k),
        None => blake3::Hasher::new(),
    };
    let mut source = image.reader(page_hash.clone(), true).map_err(err)?;
    let mut plain = if key.is_none() {
        Some(blobs.assemble(&extents).map_err(err)?)
    } else {
        None
    };
    let mut expected = vec![0; meta.page_size as usize];
    let mut actual = vec![0; meta.page_size as usize];
    for n in 0..meta.page_count {
        source.read_exact(&mut expected).map_err(err)?;
        match &mut plain {
            Some(reader) => reader.read_exact(&mut actual).map_err(err)?,
            None => {
                evidence
                    .seek(SeekFrom::Start(n * u64::from(meta.page_size)))
                    .map_err(err)?;
                evidence.read_exact(&mut actual).map_err(err)?;
            }
        }
        if actual != expected {
            return Err("SQLite retained extents differ from the pinned image".into());
        }
        full_hash.update(&actual);
        evidence
            .seek(SeekFrom::Start(meta.map_offset + n * 32))
            .map_err(err)?;
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
    meta.image_hash = hash;
    let stored = blobs.stored_size(&blob).map_err(err)?;
    if meta.full {
        meta.base_stored_bytes = stored;
        meta.patch_stored_bytes = 0;
    } else {
        let previous = &image
            .previous
            .as_ref()
            .ok_or("SQLite patch has no previous image")?
            .metadata;
        meta.base_stored_bytes = previous.base_stored_bytes;
        meta.patch_stored_bytes = previous
            .patch_stored_bytes
            .checked_add(stored)
            .ok_or("SQLite stored patch cost overflow")?;
    }
    let mut unique = std::collections::BTreeSet::new();
    for e in &extents {
        unique.insert(&e.blob);
    }
    meta.read_stored_bytes = unique.into_iter().try_fold(0u64, |sum, name| {
        sum.checked_add(blobs.stored_size(name).map_err(err)?)
            .ok_or_else(|| "SQLite read cost overflow".to_owned())
    })?;
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
        blob,
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
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
