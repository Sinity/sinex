//! Verify externally produced Polylogue observations against registered bytes
//! before event-engine admission can treat their anchors as evidence.

use serde::Deserialize;
use sha2::{Digest, Sha256};
use sinex_db::DbPool;
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

/// The manifest is metadata on the raw intent; segment bytes are read only
/// from already registered source materials. Any missing material leaves the
/// physical raw message unsettled for redelivery, while a contradictory proof
/// is a terminal admission rejection routed through the existing DLQ owner.
pub(super) async fn validate_polylogue_intent(
    pool: &DbPool,
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
    let manifest_bytes = evidence.manifest_json.as_bytes();
    let manifest = PolylogueRevisionManifest::from_json(manifest_bytes)
        .map_err(|error| reject(format!("invalid Polylogue manifest: {error}")))?;
    manifest
        .validate_compatibility()
        .map_err(|error| reject(format!("unsupported Polylogue manifest: {error}")))?;
    let manifest_digest = format!("{:x}", Sha256::digest(manifest_bytes));

    let mut segments = BTreeMap::new();
    for (&segment_index, &material_id) in &evidence.segment_material_ids {
        let record = pool
            .source_materials()
            .get_by_id(Id::<SourceMaterialRecord>::from_uuid(material_id))
            .await
            .map_err(|error| {
                PolylogueProofFailure::Retry(
                    SinexError::database("could not inspect Polylogue segment registration")
                        .with_source(error),
                )
            })?
            .ok_or_else(|| {
                PolylogueProofFailure::Retry(SinexError::network(format!(
                    "Polylogue segment material {material_id} is not registered"
                )))
            })?;
        if record.status != MaterialStatus::Completed {
            return Err(PolylogueProofFailure::Retry(SinexError::network(format!(
                "Polylogue segment material {material_id} is not complete"
            ))));
        }
        let source_uri = record
            .metadata
            .get("source_uri")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| reject("Polylogue segment must be a registered file material"))?;
        if record
            .metadata
            .get("material_type")
            .and_then(JsonValue::as_str)
            != Some("file")
        {
            return Err(reject(
                "Polylogue segment must be a registered file material",
            ));
        }
        let bytes = tokio::fs::read(source_uri).await.map_err(|error| {
            PolylogueProofFailure::Retry(
                SinexError::network("could not read registered Polylogue segment")
                    .with_context("material_id", material_id.to_string())
                    .with_source(error),
            )
        })?;
        segments.insert(segment_index, bytes);
    }
    manifest
        .verify(&segments)
        .map_err(|error| reject(format!("Polylogue revision verification failed: {error}")))?;

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
                != manifest
                    .anchor_byte(&segments, &claim.record_id)
                    .map_err(|error| reject(format!("Polylogue anchor cannot resolve: {error}")))?
        {
            return Err(reject(
                "Polylogue event provenance contradicts verified anchor",
            ));
        }
    }
    Ok(())
}
