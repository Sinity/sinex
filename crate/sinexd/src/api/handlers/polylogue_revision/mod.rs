//! Authenticated local-file publication. Operations own prepared coordinates;
//! native CAS, raw consumer settlement, event rows and DLQ own outcomes.
mod materials;

use crate::api::{rpc_server::RpcAuthContext, service_container::ServiceContainer};
use crate::runtime::intent_partitions::{
    IntentPartition, intent_header, intent_partition, partition_event_intent,
};
use crate::runtime::{NatsPublisher, nats_publisher::RawIntentPublishAck};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sinex_db::DbPoolExt;
use sinex_primitives::domain::HostName;
use sinex_primitives::events::{
    admission::{EventIntent, PolylogueRevisionEvidence},
    builder::Provenance,
};
use sinex_primitives::polylogue_protocol::{
    PolylogueOriginalRole, PolylogueOriginalWitness, PolylogueRevisionManifest,
    canonical_json_bytes, original_material_commitment,
};
use sinex_primitives::rpc::polylogue::*;
use sinex_primitives::{DynamicPayload, Id, Result, SinexError, Uuid};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Serialize, Deserialize)]
struct Prepared {
    manifest: PolylogueRevisionManifest,
    material_refs: PolyloguePublicationMaterials,
    event_ids: BTreeMap<String, Uuid>,
}

#[derive(Clone, Serialize, Deserialize)]
struct PublicationPlan {
    // The exact content-free envelope metadata is prepared once with the partition
    // boundaries. Transport retries preserve it together with assigned child IDs.
    header: EventIntent,
    partitions: Vec<IntentPartition>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Evidence {
    phase: PolyloguePublicationPhase,
    plan: PublicationPlan,
    publish_acks: Vec<RawIntentPublishAck>,
}

impl Evidence {
    fn validate(&self, child_count: usize) -> Result<()> {
        let mut end = 0;
        for part in &self.plan.partitions {
            if part.start != end || part.end <= part.start || part.end > child_count {
                return Err(SinexError::processing(
                    "stored publication partitions are inconsistent",
                ));
            }
            end = part.end;
        }
        if !self.plan.header.events.is_empty()
            || end != child_count
            || self.plan.partitions.is_empty()
            || self.publish_acks.len() > self.plan.partitions.len()
            || self.phase
                != if self.complete() {
                    PolyloguePublicationPhase::Published
                } else {
                    PolyloguePublicationPhase::MaterialsReady
                }
        {
            return Err(SinexError::processing(
                "stored publication progress is inconsistent",
            ));
        }
        Ok(())
    }

    fn complete(&self) -> bool {
        !self.plan.partitions.is_empty() && self.publish_acks.len() == self.plan.partitions.len()
    }
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn logical_request(req: &PolylogueRevisionPublishRequest) -> Result<Value> {
    if req.request_id.trim().is_empty()
        || req.normalizer_version.trim().is_empty()
        || req.originals.is_empty()
    {
        return Err(SinexError::validation(
            "publication requires request, normalizer and retained originals",
        ));
    }
    let mut seen = BTreeSet::new();
    for original in &req.originals {
        original.acquisition.validate(original.size_bytes)?;
        if original.source_ref.trim().is_empty()
            || original.media_type.trim().is_empty()
            || !valid_hash(&original.sha256)
            || !seen.insert((
                serde_json::to_string(&original.role)?,
                original.source_ref.clone(),
            ))
        {
            return Err(SinexError::validation(
                "invalid or repeated retained original descriptor",
            ));
        }
    }
    if !req
        .originals
        .iter()
        .any(|v| v.role == PolylogueOriginalRole::ProviderNative)
    {
        return Err(SinexError::validation(
            "publication has no retained provider input",
        ));
    }
    if !valid_hash(&req.manifest.sha256) || req.segments.iter().any(|v| !valid_hash(&v.sha256)) {
        return Err(SinexError::validation(
            "publication digest must be lowercase SHA-256",
        ));
    }
    let mut value = serde_json::to_value(req)?;
    value
        .as_object_mut()
        .ok_or_else(|| SinexError::invalid_state("publication request shape"))?
        .remove("request_id");
    value["manifest"]
        .as_object_mut()
        .ok_or_else(|| SinexError::invalid_state("manifest descriptor shape"))?
        .remove("file_path");
    for field in ["segments", "originals"] {
        for item in value[field]
            .as_array_mut()
            .ok_or_else(|| SinexError::invalid_state("publication descriptor array"))?
        {
            item.as_object_mut()
                .ok_or_else(|| SinexError::invalid_state("publication descriptor shape"))?
                .remove("file_path");
        }
    }
    Ok(value)
}

fn originals(
    req: &PolylogueRevisionPublishRequest,
    prepared: &Prepared,
) -> Vec<PolylogueOriginalWitness> {
    req.originals
        .iter()
        .zip(&prepared.material_refs.original_material_ids)
        .map(|(input, id)| PolylogueOriginalWitness {
            role: input.role,
            source_ref: input.source_ref.clone(),
            material_id: *id,
            sha256: input.sha256.clone(),
            size_bytes: input.size_bytes,
            media_type: input.media_type.clone(),
            acquisition: input.acquisition.clone(),
        })
        .collect()
}

fn validate_manifest(req: &PolylogueRevisionPublishRequest, prepared: &Prepared) -> Result<()> {
    let manifest = &prepared.manifest;
    manifest.validate_compatibility()?;
    let declared = std::iter::once(&manifest.head_segment)
        .chain(&manifest.segments)
        .collect::<Vec<_>>();
    if req.segments.len() != declared.len() {
        return Err(SinexError::validation(
            "publication segment set differs from manifest",
        ));
    }
    let mut names = BTreeSet::new();
    for input in &req.segments {
        if !names.insert(&input.name)
            || !declared.iter().any(|d| {
                d.filename == input.name
                    && d.sha256 == input.sha256
                    && u64::try_from(d.size_bytes).ok() == Some(input.size_bytes)
            })
        {
            return Err(SinexError::validation(
                "publication segment descriptor contradicts manifest",
            ));
        }
    }
    let commitment =
        original_material_commitment(&req.normalizer_version, &originals(req, prepared))?;
    if manifest.content_digest.provider_digest.as_deref() != Some(commitment.as_str()) {
        return Err(SinexError::validation(
            "manifest does not bind these retained originals and normalizer",
        ));
    }
    Ok(())
}

async fn read_manifest(
    services: &ServiceContainer,
    path: &std::path::Path,
    declared_size: u64,
) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    services
        .content
        .content_store()
        .ensure_ingest_size_allowed(declared_size)?;
    let file = tokio::fs::File::open(path).await.map_err(SinexError::io)?;
    let metadata = file.metadata().await.map_err(SinexError::io)?;
    if !metadata.is_file() || metadata.len() != declared_size {
        return Err(SinexError::validation(
            "manifest locator is not the declared regular artifact",
        ));
    }
    let bound = declared_size
        .checked_add(1)
        .ok_or_else(|| SinexError::validation("manifest size is unsupported"))?;
    let mut bytes = Vec::new();
    file.take(bound)
        .read_to_end(&mut bytes)
        .await
        .map_err(SinexError::io)?;
    if u64::try_from(bytes.len()).ok() != Some(declared_size) {
        return Err(SinexError::validation(
            "manifest changed while reading exact descriptor",
        ));
    }
    Ok(bytes)
}

pub async fn handle_polylogue_revision_publish(
    services: &ServiceContainer,
    req: PolylogueRevisionPublishRequest,
    auth: &RpcAuthContext,
) -> Result<PolylogueRevisionPublicationResponse> {
    let fingerprint = digest(&canonical_json_bytes(&logical_request(&req)?)?);
    let repo = services.publication_coordination_pool().state();
    let existing = repo
        .find_polylogue_publication(auth.actor_id(), &req.request_id)
        .await?;
    let candidate = if let Some(record) = &existing {
        serde_json::from_value::<Prepared>(
            record
                .scope
                .as_ref()
                .and_then(|v| v.get("prepared"))
                .cloned()
                .ok_or_else(|| SinexError::invalid_state("publication preparation absent"))?,
        )?
    } else {
        // Only a new preparation reads the caller's manifest. All subsequent
        // custody and decoding resolve registered immutable material.
        let bytes = read_manifest(
            services,
            std::path::Path::new(&req.manifest.file_path),
            req.manifest.size_bytes,
        )
        .await?;
        if u64::try_from(bytes.len()).ok() != Some(req.manifest.size_bytes)
            || digest(&bytes) != req.manifest.sha256
        {
            return Err(SinexError::validation(
                "manifest locator contradicts exact descriptor",
            ));
        }
        let manifest = PolylogueRevisionManifest::from_json(&bytes)?;
        let material_refs = PolyloguePublicationMaterials {
            manifest_material_id: Uuid::now_v7(),
            segment_material_ids: std::iter::once(&manifest.head_segment)
                .chain(&manifest.segments)
                .map(|v| (v.index, Uuid::now_v7()))
                .collect(),
            original_material_ids: req.originals.iter().map(|_| Uuid::now_v7()).collect(),
        };
        let event_ids = manifest
            .anchors
            .keys()
            .map(|id| (id.clone(), Uuid::now_v7()))
            .collect();
        Prepared {
            manifest,
            material_refs,
            event_ids,
        }
    };
    validate_manifest(&req, &candidate)?;
    let record = repo
        .prepare_polylogue_publication(
            auth.actor_id(),
            &req.request_id,
            &fingerprint,
            json!({"prepared":candidate}),
        )
        .await?;
    let prepared: Prepared = serde_json::from_value(
        record
            .scope
            .as_ref()
            .and_then(|v| v.get("prepared"))
            .cloned()
            .ok_or_else(|| SinexError::invalid_state("publication preparation absent"))?,
    )?;
    let mut verified: Option<EventIntent> = None;
    loop {
        let (mut tx, record) = repo
            .lock_polylogue_publication(auth.actor_id(), &req.request_id)
            .await?;
        let old: Option<Evidence> = record
            .preview_summary
            .clone()
            .map(serde_json::from_value)
            .transpose()?;
        if let Some(evidence) = &old {
            evidence.validate(prepared.event_ids.len())?;
            if evidence.complete() {
                tx.commit().await.map_err(|error| {
                    SinexError::processing("release publication lock").with_source(error)
                })?;
                return response(
                    services,
                    &req.request_id,
                    &record,
                    &prepared,
                    Some(evidence),
                )
                .await;
            }
        }
        if verified.is_none() {
            verified = Some(stage_publication(services, &req, &prepared).await?);
        }
        let intent = verified
            .as_mut()
            .ok_or_else(|| SinexError::processing("verified publication unavailable"))?;
        let Some(mut evidence) = old else {
            let evidence = Evidence {
                phase: PolyloguePublicationPhase::MaterialsReady,
                plan: PublicationPlan {
                    header: intent_header(intent),
                    partitions: partition_event_intent(intent)?,
                },
                publish_acks: Vec::new(),
            };
            evidence.validate(prepared.event_ids.len())?;
            repo.record_polylogue_publication_evidence(
                &mut tx,
                record.id,
                serde_json::to_value(&evidence)?,
                false,
            )
            .await?;
            // The exact plan is durable before any physical envelope is sent.
            tx.commit().await.map_err(|error| {
                SinexError::processing("commit publication plan").with_source(error)
            })?;
            continue;
        };
        let events = std::mem::take(&mut intent.events);
        *intent = evidence.plan.header.clone();
        intent.events = events;
        let partition = intent_partition(
            intent,
            evidence.plan.partitions[evidence.publish_acks.len()],
        )?;
        let client = services
            .nats_client()
            .ok_or_else(|| SinexError::service("publication requires NATS"))?
            .clone();
        let ack =
            NatsPublisher::with_namespace(client, services.nats_namespace().map(str::to_owned))
                .publish_intent_with_ack(&partition, sinex_primitives::transport::Class::Critical)
                .await?;
        evidence.publish_acks.push(ack);
        let complete = evidence.complete();
        evidence.phase = if complete {
            PolyloguePublicationPhase::Published
        } else {
            PolyloguePublicationPhase::MaterialsReady
        };
        repo.record_polylogue_publication_evidence(
            &mut tx,
            record.id,
            serde_json::to_value(&evidence)?,
            complete,
        )
        .await?;
        // A missing PubAck/commit leaves this partition owed. Existing native
        // same-ID handling owns any physical redelivery; no exactly-once claim.
        tx.commit().await.map_err(|error| {
            SinexError::processing("commit publication receipt").with_source(error)
        })?;
        if complete {
            return response(
                services,
                &req.request_id,
                &record,
                &prepared,
                Some(&evidence),
            )
            .await;
        }
    }
}

async fn stage_publication(
    services: &ServiceContainer,
    req: &PolylogueRevisionPublishRequest,
    prepared: &Prepared,
) -> Result<EventIntent> {
    let manifest_record = materials::stage_artifact(
        services,
        prepared.material_refs.manifest_material_id,
        &req.manifest,
        json!({"polylogue_role":"manifest"}),
    )
    .await?;
    let manifest_path = materials::artifact_path(services, &manifest_record).await?;
    let stored_manifest_bytes =
        read_manifest(services, &manifest_path, req.manifest.size_bytes).await?;
    let stored_manifest = PolylogueRevisionManifest::from_json(&stored_manifest_bytes)?;
    if digest(&stored_manifest_bytes) != req.manifest.sha256
        || serde_json::to_value(&stored_manifest)? != serde_json::to_value(&prepared.manifest)?
    {
        return Err(SinexError::validation(
            "stored manifest differs from preparation",
        ));
    }
    let mut files = BTreeMap::new();
    for descriptor in
        std::iter::once(&prepared.manifest.head_segment).chain(&prepared.manifest.segments)
    {
        let input = req
            .segments
            .iter()
            .find(|v| v.name == descriptor.filename)
            .ok_or_else(|| SinexError::validation("missing segment"))?;
        let artifact = PolylogueArtifactInput {
            file_path: input.file_path.clone(),
            sha256: input.sha256.clone(),
            size_bytes: input.size_bytes,
        };
        let id = *prepared
            .material_refs
            .segment_material_ids
            .get(&descriptor.index)
            .ok_or_else(|| SinexError::invalid_state("prepared segment ID absent"))?;
        let record = materials::stage_artifact(
            services,
            id,
            &artifact,
            json!({"polylogue_role":"segment", "polylogue_segment_name":input.name}),
        )
        .await?;
        files.insert(
            descriptor.index,
            materials::artifact_path(services, &record).await?,
        );
    }
    for original in originals(req, prepared) {
        let input = req
            .originals
            .iter()
            .find(|v| v.role == original.role && v.source_ref == original.source_ref)
            .ok_or_else(|| SinexError::invalid_state("prepared original absent"))?;
        materials::stage_artifact(services, original.material_id, &PolylogueArtifactInput { file_path: input.file_path.clone(), sha256: original.sha256.clone(), size_bytes: original.size_bytes }, json!({"polylogue_original_role":original.role,"source_ref":original.source_ref,"acquisition":original.acquisition,"media_type":original.media_type,"normalizer_version":req.normalizer_version})).await?;
        for child in std::iter::once(&prepared.material_refs.manifest_material_id)
            .chain(prepared.material_refs.segment_material_ids.values())
        {
            services.pool().source_materials().link_backing_material(*child, original.material_id, json!({"normalizer_version":req.normalizer_version,"source_ref":original.source_ref})).await?;
        }
    }
    let manifest = prepared.manifest.clone();
    let offsets = tokio::task::spawn_blocking(move || manifest.verify_files(&files))
        .await
        .map_err(|error| {
            SinexError::processing("publication verification worker failed").with_source(error)
        })??;
    let mut anchors = prepared.manifest.anchors.iter().collect::<Vec<_>>();
    anchors.sort_by_key(|(_, anchor)| anchor.seq);
    let mut events = Vec::with_capacity(anchors.len());
    for (record_id, anchor) in anchors {
        let material_id = *prepared
            .material_refs
            .segment_material_ids
            .get(&anchor.segment_index)
            .ok_or_else(|| SinexError::invalid_state("prepared anchor material absent"))?;
        let mut event = DynamicPayload::new("integration.polylogue", format!("integration.polylogue.{}.observed", anchor.kind), json!({
            "protocol_version":prepared.manifest.protocol_version,"semantics_version":prepared.manifest.semantics_version,
            "manifest_digest":req.manifest.sha256,"revision_id":prepared.manifest.revision_id,"session_id":prepared.manifest.session_id,
            "origin":prepared.manifest.origin,"native_id":prepared.manifest.native_id,"record_id":record_id,"record_kind":anchor.kind,
            "material_id":material_id,"segment_index":anchor.segment_index,"line_index":anchor.line_index,"seq":anchor.seq,"record_sha256":anchor.sha256
        })).from_material(Id::<sinex_primitives::events::SourceMaterial>::from_uuid(material_id)).build()?.to_json_event()?;
        event.id = Some(Id::from_uuid(
            *prepared
                .event_ids
                .get(record_id)
                .ok_or_else(|| SinexError::invalid_state("prepared child ID absent"))?,
        ));
        event.provenance = Provenance::from_material(
            Id::<sinex_primitives::events::SourceMaterial>::from_uuid(material_id),
            *offsets
                .get(record_id)
                .ok_or_else(|| SinexError::invalid_state("verified child offset absent"))?,
            None,
            None,
        );
        events.push(event);
    }
    let mut intent = EventIntent::new(
        "integration.polylogue",
        "polylogue-external-publication",
        "1",
        events,
        HostName::from_static("polylogue-producer"),
    );
    intent.polylogue_revision = Some(PolylogueRevisionEvidence {
        manifest_material_id: prepared.material_refs.manifest_material_id,
        segment_material_ids: prepared.material_refs.segment_material_ids.clone(),
        normalizer_version: Some(req.normalizer_version.clone()),
        originals: originals(req, prepared),
    });
    Ok(intent)
}

pub async fn handle_polylogue_revision_status(
    services: &ServiceContainer,
    req: PolylogueRevisionStatusRequest,
    auth: &RpcAuthContext,
) -> Result<PolylogueRevisionPublicationResponse> {
    let record = services
        .publication_coordination_pool()
        .state()
        .find_polylogue_publication(auth.actor_id(), &req.request_id)
        .await?
        .ok_or_else(|| SinexError::not_found("publication request is not prepared"))?;
    let prepared = serde_json::from_value(
        record
            .scope
            .as_ref()
            .and_then(|v| v.get("prepared"))
            .cloned()
            .ok_or_else(|| SinexError::invalid_state("publication preparation absent"))?,
    )?;
    let evidence = record
        .preview_summary
        .clone()
        .map(serde_json::from_value)
        .transpose()?;
    response(
        services,
        &req.request_id,
        &record,
        &prepared,
        evidence.as_ref(),
    )
    .await
}

async fn response(
    services: &ServiceContainer,
    request_id: &str,
    record: &sinex_db::repositories::state::OperationRecord,
    prepared: &Prepared,
    evidence: Option<&Evidence>,
) -> Result<PolylogueRevisionPublicationResponse> {
    if let Some(evidence) = evidence {
        evidence.validate(prepared.event_ids.len())?;
    }
    let complete = evidence.filter(|v| v.complete());
    let mut state = complete.map(|_| PolyloguePublicationState::RawAccepted);
    let mut phase = evidence.map_or(PolyloguePublicationPhase::Prepared, |v| v.phase);
    let mut detail = evidence.map_or_else(
        || "No native publication receipt yet".to_string(),
        |v| {
            format!(
                "{} of {} physical envelopes accepted; whole publication remains owed",
                v.publish_acks.len(),
                v.plan.partitions.len()
            )
        },
    );
    if let Some(evidence) = complete {
        detail = "JetStream accepted all prepared envelopes; settlement remains pending".into();
        let mut persisted = BTreeSet::new();
        let mut rejected = BTreeSet::new();
        let ids = prepared.event_ids.values().copied().collect::<Vec<_>>();
        for chunk in ids.chunks(1000) {
            let typed = chunk.iter().copied().map(Id::from_uuid).collect::<Vec<_>>();
            for event in services.pool().events().get_by_ids(&typed).await? {
                if let Some(id) = event.id {
                    persisted.insert(id.to_uuid());
                }
            }
            rejected.extend(
                services
                    .pool()
                    .dlq_events()
                    .unresolved_polylogue_raw_ingest_event_ids(chunk)
                    .await?,
            );
        }
        // A durable row is the native success witness for this exact prepared
        // interpretation. A later duplicate's debt cannot revoke that custody.
        rejected.retain(|id| !persisted.contains(id));
        if ids
            .iter()
            .all(|id| persisted.contains(id) || rejected.contains(id))
        {
            if let (Some(topology), Some(client)) =
                (services.raw_activity_topology(), services.nats_client())
            {
                if evidence
                    .publish_acks
                    .iter()
                    .all(|ack| topology.events_stream.to_string() == ack.stream)
                {
                    let js = async_nats::jetstream::new(client.clone());
                    if let Ok(stream) = js.get_stream(topology.events_stream.to_string()).await {
                        if let Ok(mut consumer) = stream
                            .get_consumer::<async_nats::jetstream::consumer::pull::Config>(
                                &topology.consumer_durable,
                            )
                            .await
                        {
                            if let Ok(info) = consumer.info().await {
                                if evidence
                                    .publish_acks
                                    .iter()
                                    .all(|ack| info.ack_floor.stream_sequence >= ack.sequence)
                                {
                                    state = Some(if rejected.is_empty() {
                                        PolyloguePublicationState::PersistedConfirmed
                                    } else {
                                        PolyloguePublicationState::DurableDebt
                                    });
                                    phase = PolyloguePublicationPhase::Settled;
                                    detail="Native raw owner settled every prepared physical envelope; child outcomes resolve in events/DLQ".into();
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(PolylogueRevisionPublicationResponse {
        request_id: request_id.into(),
        state,
        detail,
        phase,
        operation_id: record.id.to_uuid(),
        material_refs: prepared.material_refs.clone(),
        event_ids: prepared.event_ids.values().copied().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::polylogue_protocol::{
        PolylogueAcquisition, PolylogueAcquisitionKind, PolylogueByteWindow,
    };

    #[test]
    fn stored_partition_progress_rejects_gaps_overlap_and_empty_plans() {
        let header = EventIntent::new(
            "synthetic",
            "synthetic",
            "1",
            Vec::new(),
            HostName::from_static("synthetic-host"),
        );
        let valid = Evidence {
            phase: PolyloguePublicationPhase::MaterialsReady,
            plan: PublicationPlan {
                header,
                partitions: vec![
                    IntentPartition { start: 0, end: 1 },
                    IntentPartition { start: 1, end: 3 },
                ],
            },
            publish_acks: vec![RawIntentPublishAck {
                stream: "synthetic-raw".into(),
                sequence: 1,
                duplicate: false,
            }],
        };
        valid.validate(3).unwrap();
        assert!(!valid.complete(), "a durable prefix is still owed work");
        for second_start in [0, 2] {
            let mut malformed = valid.clone();
            malformed.plan.partitions[1].start = second_start;
            assert!(malformed.validate(3).is_err());
        }
        let mut malformed = valid.clone();
        malformed.plan.partitions.clear();
        assert!(malformed.validate(0).is_err());
        malformed = valid;
        malformed.plan.partitions[1].end = 1;
        assert!(malformed.validate(3).is_err());
    }

    fn request() -> PolylogueRevisionPublishRequest {
        PolylogueRevisionPublishRequest {
            request_id: "fixture-request".into(),
            normalizer_version: "fixture-normalizer-v1".into(),
            manifest: PolylogueArtifactInput {
                file_path: "/first/manifest.json".into(),
                sha256: "a".repeat(64),
                size_bytes: 10,
            },
            segments: vec![PolylogueSegmentInput {
                name: "head.ndjson".into(),
                file_path: "/first/head.ndjson".into(),
                sha256: "b".repeat(64),
                size_bytes: 20,
            }],
            originals: vec![PolylogueOriginalInput {
                role: PolylogueOriginalRole::ProviderNative,
                source_ref: "polylogue.raw:fixture".into(),
                file_path: "/first/provider.jsonl".into(),
                sha256: "c".repeat(64),
                size_bytes: 30,
                media_type: "application/x-ndjson".into(),
                acquisition: PolylogueAcquisition {
                    kind: PolylogueAcquisitionKind::Unknown,
                    byte_window: None,
                    addressing_mode: None,
                    tables: None,
                },
            }],
        }
    }

    #[test]
    fn publication_fingerprint_excludes_refreshed_locators_but_binds_original_bytes_and_normalizer()
    -> Result<()> {
        let mut req = request();
        let first = canonical_json_bytes(&logical_request(&req)?)?;
        req.request_id = "another-scope".into();
        req.manifest.file_path = "/retry/manifest.json".into();
        req.segments[0].file_path = "/retry/head.ndjson".into();
        req.originals[0].file_path = "/retry/provider.jsonl".into();
        assert_eq!(first, canonical_json_bytes(&logical_request(&req)?)?);
        req.originals[0].sha256 = "d".repeat(64);
        assert_ne!(first, canonical_json_bytes(&logical_request(&req)?)?);
        req.originals[0].sha256 = "c".repeat(64);
        req.normalizer_version = "fixture-normalizer-v2".into();
        assert_ne!(first, canonical_json_bytes(&logical_request(&req)?)?);
        Ok(())
    }

    #[test]
    fn publication_original_descriptor_refuses_duplicate_and_contradictory_acquisition() {
        let mut req = request();
        req.originals.push(req.originals[0].clone());
        assert!(logical_request(&req).is_err());
        req.originals.pop();
        req.originals[0].acquisition.byte_window = Some(PolylogueByteWindow { start: 0, end: 30 });
        assert!(logical_request(&req).is_err());
        req.originals[0].acquisition.kind = PolylogueAcquisitionKind::ByteWindow;
        assert!(logical_request(&req).is_ok());
        req.originals[0].size_bytes = 0;
        req.originals[0].acquisition.byte_window = Some(PolylogueByteWindow { start: 30, end: 30 });
        assert!(logical_request(&req).is_ok());
        req.originals[0].acquisition.tables = Some(vec!["messages".into()]);
        assert!(logical_request(&req).is_err());
    }

    #[test]
    fn publication_acquisition_requires_nullable_coordinate_keys() {
        let full =
            json!({"kind":"unknown","byte_window":null,"addressing_mode":null,"tables":null});
        assert!(serde_json::from_value::<PolylogueAcquisition>(full.clone()).is_ok());
        for key in ["byte_window", "addressing_mode", "tables"] {
            let mut omitted = full.clone();
            omitted.as_object_mut().unwrap().remove(key);
            assert!(
                serde_json::from_value::<PolylogueAcquisition>(omitted).is_err(),
                "omitted {key}"
            );
        }
    }
}
