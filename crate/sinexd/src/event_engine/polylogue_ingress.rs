//! Verify externally produced Polylogue observations against registered bytes
//! before event-engine admission can treat their anchors as evidence.

use crate::runtime::content_store::ContentStoreManager;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sinex_db::DbPool;
use sinex_db::models::Blob;
use sinex_db::repositories::DbPoolExt;
use sinex_db::schema::defs::records::SourceMaterialRecord;
use sinex_primitives::domain::MaterialStatus;
use sinex_primitives::events::admission::EventIntent;
use sinex_primitives::events::builder::Provenance;
use sinex_primitives::polylogue_protocol::PolylogueRevisionManifest;
use sinex_primitives::{Id, JsonValue, Uuid};
use std::collections::BTreeMap;

use super::SinexError;

pub(super) enum PolylogueProofFailure {
    Retry(SinexError),
    Reject(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AnchorClaim {
    protocol_version: String,
    semantics_version: u16,
    manifest_digest: String,
    revision_id: String,
    session_id: String,
    origin: String,
    native_id: String,
    record_id: String,
    record_kind: String,
    material_id: Uuid,
    segment_index: i32,
    line_index: usize,
    seq: u64,
    record_sha256: String,
}

fn reject(reason: impl Into<String>) -> PolylogueProofFailure {
    PolylogueProofFailure::Reject(reason.into())
}

async fn registered_file(
    pool: &DbPool,
    material_id: Uuid,
    require_cas: bool,
) -> Result<SourceMaterialRecord, PolylogueProofFailure> {
    let record = pool
        .source_materials()
        .get_by_id(Id::<SourceMaterialRecord>::from_uuid(material_id))
        .await
        .map_err(|error| {
            PolylogueProofFailure::Retry(
                SinexError::database("could not inspect Polylogue material registration")
                    .with_source(error),
            )
        })?
        .ok_or_else(|| {
            PolylogueProofFailure::Retry(SinexError::network(
                "Polylogue material is not registered",
            ))
        })?;
    if record.status != MaterialStatus::Completed {
        return Err(PolylogueProofFailure::Retry(SinexError::network(
            "Polylogue material is not complete",
        )));
    }
    if record
        .metadata
        .get("material_type")
        .and_then(JsonValue::as_str)
        != Some("file")
        || record
            .metadata
            .get("source_uri")
            .and_then(JsonValue::as_str)
            .is_none()
    {
        return Err(reject(
            "Polylogue evidence must be registered file material",
        ));
    }
    if require_cas && record.optional_blob_id.is_none() {
        return Err(reject(
            "Polylogue original-backed publication requires retained CAS material",
        ));
    }
    Ok(record)
}

async fn registered_path(
    pool: &DbPool,
    content_store: Option<&ContentStoreManager>,
    record: &SourceMaterialRecord,
) -> Result<std::path::PathBuf, PolylogueProofFailure> {
    if let Some(blob_id) = record.optional_blob_id {
        let store = content_store.ok_or_else(|| {
            PolylogueProofFailure::Retry(SinexError::network("Polylogue CAS reader is unavailable"))
        })?;
        let blob = pool
            .blobs()
            .get_by_id(Id::<Blob>::from_uuid(blob_id))
            .await
            .map_err(|error| {
                PolylogueProofFailure::Retry(
                    SinexError::database("could not inspect Polylogue blob").with_source(error),
                )
            })?
            .ok_or_else(|| {
                PolylogueProofFailure::Retry(SinexError::network(
                    "registered Polylogue blob is unavailable",
                ))
            })?;
        return store
            .get_blob_path(&blob.content_key())
            .await
            .map(|path| path.into_std_path_buf())
            .map_err(|error| {
                PolylogueProofFailure::Retry(
                    SinexError::network("could not resolve Polylogue CAS bytes").with_source(error),
                )
            });
    }
    record
        .metadata
        .get("source_uri")
        .and_then(JsonValue::as_str)
        .map(std::path::PathBuf::from)
        .ok_or_else(|| reject("file material has no declared file authority"))
}

async fn read_registered_manifest(
    pool: &DbPool,
    content_store: Option<&ContentStoreManager>,
    record: &SourceMaterialRecord,
) -> Result<Vec<u8>, PolylogueProofFailure> {
    let path = registered_path(pool, content_store, record).await?;
    let bytes = tokio::fs::read(path).await.map_err(|error| {
        PolylogueProofFailure::Retry(
            SinexError::network("registered manifest bytes are unavailable").with_source(error),
        )
    })?;
    if let Some(expected) = record
        .metadata
        .get("polylogue_sha256")
        .and_then(JsonValue::as_str)
        && format!("{:x}", Sha256::digest(&bytes)) != expected
    {
        return Err(PolylogueProofFailure::Retry(SinexError::network(
            "retained manifest failed byte integrity",
        )));
    }
    Ok(bytes)
}

async fn verify_original_materials(
    pool: &DbPool,
    content_store: Option<&ContentStoreManager>,
    evidence: &sinex_primitives::events::admission::PolylogueRevisionEvidence,
    manifest: &PolylogueRevisionManifest,
) -> Result<(), PolylogueProofFailure> {
    use sinex_primitives::polylogue_protocol::{
        PolylogueOriginalRole, original_material_commitment,
    };
    use tokio::io::AsyncReadExt;

    if evidence.originals.is_empty() {
        if evidence.normalizer_version.is_some()
            || manifest.content_digest.provider_digest.is_some()
        {
            return Err(reject(
                "Polylogue source commitment has no original witnesses",
            ));
        }
        return Ok(());
    }
    let normalizer = evidence
        .normalizer_version
        .as_deref()
        .filter(|version| !version.trim().is_empty())
        .ok_or_else(|| reject("Polylogue original witnesses have no normalizer version"))?;
    let commitment = original_material_commitment(normalizer, &evidence.originals)
        .map_err(|error| reject(format!("invalid original witness commitment: {error}")))?;
    if manifest.content_digest.provider_digest.as_deref() != Some(commitment.as_str()) {
        return Err(reject(
            "Polylogue original witness commitment contradicts manifest",
        ));
    }
    if !evidence
        .originals
        .iter()
        .any(|original| original.role == PolylogueOriginalRole::ProviderNative)
    {
        return Err(reject(
            "Polylogue publication has no acquired provider witness",
        ));
    }
    let links = pool
        .source_materials()
        .links_from(evidence.manifest_material_id)
        .await
        .map_err(|error| {
            PolylogueProofFailure::Retry(
                SinexError::database("could not inspect original material backing links")
                    .with_source(error),
            )
        })?;
    let mut names = std::collections::HashSet::new();
    for original in &evidence.originals {
        let role = serde_json::to_value(original.role)
            .map_err(|error| reject(format!("invalid original witness role: {error}")))?;
        if original.source_ref.trim().is_empty()
            || !names.insert((role.to_string(), original.source_ref.clone()))
        {
            return Err(reject(
                "Polylogue original witness names are empty or duplicate",
            ));
        }
        if !links.iter().any(|link| {
            link.to_material_id == original.material_id
                && link.relation_type == "backed_by"
                && link
                    .metadata
                    .get("normalizer_version")
                    .and_then(JsonValue::as_str)
                    == Some(normalizer)
                && link.metadata.get("source_ref").and_then(JsonValue::as_str)
                    == Some(original.source_ref.as_str())
        }) {
            return Err(PolylogueProofFailure::Retry(SinexError::network(
                "Polylogue original witness backing link is unavailable",
            )));
        }
        let record = registered_file(pool, original.material_id, true).await?;
        if record
            .metadata
            .get("polylogue_sha256")
            .and_then(JsonValue::as_str)
            != Some(original.sha256.as_str())
            || record
                .metadata
                .get("source_ref")
                .and_then(JsonValue::as_str)
                != Some(original.source_ref.as_str())
            || record.metadata.get("polylogue_original_role") != Some(&role)
            || record.metadata.get("acquisition")
                != Some(
                    &serde_json::to_value(&original.acquisition)
                        .map_err(|error| reject(format!("invalid acquisition witness: {error}")))?,
                )
        {
            return Err(reject(
                "registered original material contradicts its witness",
            ));
        }
        let path = registered_path(pool, content_store, &record).await?;
        let mut file = tokio::fs::File::open(path).await.map_err(|error| {
            PolylogueProofFailure::Retry(
                SinexError::network("original material bytes are unavailable").with_source(error),
            )
        })?;
        let mut hash = Sha256::new();
        let mut size = 0_u64;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let count = file.read(&mut buffer).await.map_err(|error| {
                PolylogueProofFailure::Retry(
                    SinexError::network("could not verify original material bytes")
                        .with_source(error),
                )
            })?;
            if count == 0 {
                break;
            }
            size = size
                .checked_add(count as u64)
                .ok_or_else(|| reject("original size overflows u64"))?;
            hash.update(&buffer[..count]);
        }
        if size != original.size_bytes || format!("{:x}", hash.finalize()) != original.sha256 {
            return Err(PolylogueProofFailure::Retry(SinexError::network(
                "retained original material failed byte integrity",
            )));
        }
    }
    Ok(())
}

/// The manifest is metadata on the raw intent; segment bytes are read only
/// from already registered source materials. Any missing material leaves the
/// physical raw message unsettled for redelivery, while a contradictory proof
/// is a terminal admission rejection routed through the existing DLQ owner.
pub(super) async fn validate_polylogue_intent(
    pool: &DbPool,
    content_store: Option<&ContentStoreManager>,
    intent: &EventIntent,
) -> Result<(), PolylogueProofFailure> {
    let has_polylogue = intent
        .events
        .iter()
        .any(|event| event.source.to_string() == "integration.polylogue");
    if !has_polylogue {
        if intent.polylogue_revision.is_some() {
            return Err(reject(
                "Polylogue revision evidence attached to another source",
            ));
        }
        return Ok(());
    }
    if intent.source_id != "integration.polylogue"
        || intent
            .events
            .iter()
            .any(|event| event.source.to_string() != "integration.polylogue")
    {
        return Err(reject("Polylogue revision intent mixes source identities"));
    }
    let evidence = intent
        .polylogue_revision
        .as_ref()
        .ok_or_else(|| reject("Polylogue intent has no material revision evidence"))?;
    let manifest_record = registered_file(
        pool,
        evidence.manifest_material_id,
        !evidence.originals.is_empty(),
    )
    .await?;
    let manifest_bytes = read_registered_manifest(pool, content_store, &manifest_record).await?;
    let manifest = PolylogueRevisionManifest::from_json(&manifest_bytes)
        .map_err(|error| reject(format!("invalid Polylogue manifest: {error}")))?;
    manifest
        .validate_compatibility()
        .map_err(|error| reject(format!("unsupported Polylogue manifest: {error}")))?;
    let manifest_digest = format!("{:x}", Sha256::digest(&manifest_bytes));

    verify_original_materials(pool, content_store, evidence, &manifest).await?;

    let mut segments = BTreeMap::new();
    for (&segment_index, &material_id) in &evidence.segment_material_ids {
        let record = registered_file(pool, material_id, !evidence.originals.is_empty()).await?;
        let path = registered_path(pool, content_store, &record).await?;
        segments.insert(segment_index, path);
    }
    let verified_manifest = manifest.clone();
    let offsets = tokio::task::spawn_blocking(move || verified_manifest.verify_files(&segments))
        .await
        .map_err(|error| {
            PolylogueProofFailure::Retry(
                SinexError::network("Polylogue verification worker unavailable").with_source(error),
            )
        })?
        .map_err(|error| {
            if error.is_retryable() {
                PolylogueProofFailure::Retry(error)
            } else {
                reject(format!("Polylogue revision verification failed: {error}"))
            }
        })?;

    for event in &intent.events {
        let claim: AnchorClaim = serde_json::from_value(event.payload.clone())
            .map_err(|error| reject(format!("invalid Polylogue anchor claim: {error}")))?;
        let anchor = manifest
            .anchors
            .get(&claim.record_id)
            .ok_or_else(|| reject("Polylogue anchor claim names no manifest record"))?;
        if claim.protocol_version != manifest.protocol_version
            || claim.semantics_version != manifest.semantics_version
            || claim.manifest_digest != manifest_digest
            || claim.revision_id != manifest.revision_id
            || claim.session_id != manifest.session_id
            || claim.origin != manifest.origin
            || claim.native_id != manifest.native_id
            || claim.record_kind != anchor.kind
            || claim.segment_index != anchor.segment_index
            || claim.line_index != anchor.line_index
            || claim.seq != anchor.seq
            || claim.record_sha256 != anchor.sha256
            || event.event_type.to_string()
                != format!("integration.polylogue.{}.observed", anchor.kind)
            || evidence.segment_material_ids.get(&anchor.segment_index) != Some(&claim.material_id)
        {
            return Err(reject(
                "Polylogue anchor claim contradicts verified material",
            ));
        }
        let Provenance::Material {
            id, anchor_byte, ..
        } = &event.provenance
        else {
            return Err(reject("Polylogue observation has no material provenance"));
        };
        if id.to_uuid() != claim.material_id
            || *anchor_byte
                != *offsets
                    .get(&claim.record_id)
                    .ok_or_else(|| reject("Polylogue anchor cannot resolve"))?
        {
            return Err(reject(
                "Polylogue event provenance contradicts verified anchor",
            ));
        }
    }
    Ok(())
}
