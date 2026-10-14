//! The shared ordered partition plan for native event-intent publication.
//!
//! Plans preserve the original envelope header and child order. Durable callers
//! record the plan and header before publishing and reuse them on retry.

use std::io::{self, Write};

use serde::{Deserialize, Serialize};
use sinex_primitives::SinexError;
use sinex_primitives::events::admission::EventIntent;

use super::RuntimeResult;
use super::nats_payload::{NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES, ensure_nats_payload_fits};

/// A nonempty contiguous range in the original ordered event vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct IntentPartition {
    pub start: usize,
    pub end: usize,
}

/// Compute the complete plan before any publish. Each child is serialized once
/// into a byte counter; exact header size includes the complete revision proof.
pub(crate) fn partition_event_intent(intent: &EventIntent) -> RuntimeResult<Vec<IntentPartition>> {
    partition_with_limit(intent, NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES)
}

/// Preserve the exact envelope metadata without cloning its child vector.
/// The empty-events value is preparation state and must never be published.
pub(crate) fn intent_header(intent: &EventIntent) -> EventIntent {
    let EventIntent {
        envelope_version,
        source_id,
        parser_id,
        parser_version,
        polylogue_revision,
        events: _,
        admitted_at,
        admitted_by,
    } = intent;
    EventIntent {
        envelope_version: envelope_version.clone(),
        source_id: source_id.clone(),
        parser_id: parser_id.clone(),
        parser_version: parser_version.clone(),
        polylogue_revision: polylogue_revision.clone(),
        events: Vec::new(),
        admitted_at: *admitted_at,
        admitted_by: admitted_by.clone(),
    }
}

/// Construct a prepared subset with every original metadata field intact.
/// Capacity is checked again so a changed header cannot silently change a plan.
pub(crate) fn intent_partition(
    intent: &EventIntent,
    partition: IntentPartition,
) -> RuntimeResult<EventIntent> {
    if partition.start >= partition.end || partition.end > intent.events.len() {
        return Err(SinexError::invalid_state(
            "Invalid prepared event-intent partition",
        ));
    }
    let mut chunk = intent_header(intent);
    chunk.events = intent.events[partition.start..partition.end].to_vec();
    ensure_nats_payload_fits(
        "event intent envelope",
        &chunk.source_id,
        serialized_len(&chunk)?,
    )?;
    Ok(chunk)
}

fn partition_with_limit(intent: &EventIntent, limit: usize) -> RuntimeResult<Vec<IntentPartition>> {
    intent.validate()?;
    let header = serialized_len(&intent_header(intent))?;
    if header > limit {
        return Err(capacity_error(
            "Event-intent header exceeds publish capacity",
            header,
            limit,
        ));
    }
    let mut partitions = Vec::new();
    let mut start = 0;
    let mut payload_bytes = header;
    for (index, event) in intent.events.iter().enumerate() {
        let child_bytes = serialized_len(event)?;
        let single_bytes = header.saturating_add(child_bytes);
        if single_bytes > limit {
            return Err(capacity_error(
                "Single event-intent child exceeds publish capacity",
                single_bytes,
                limit,
            )
            .with_context("child_index", index));
        }
        let candidate = payload_bytes
            .saturating_add(child_bytes)
            .saturating_add(usize::from(index > start));
        if candidate > limit {
            partitions.push(IntentPartition { start, end: index });
            start = index;
            payload_bytes = single_bytes;
        } else {
            payload_bytes = candidate;
        }
    }
    partitions.push(IntentPartition {
        start,
        end: intent.events.len(),
    });
    Ok(partitions)
}

fn capacity_error(message: &str, payload_bytes: usize, limit: usize) -> SinexError {
    SinexError::resource_exhausted(message)
        .with_context("payload_bytes", payload_bytes)
        .with_context("max_payload_bytes", limit)
}

#[derive(Default)]
struct ByteCounter(usize);

impl Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialized_len<T: Serialize + ?Sized>(value: &T) -> RuntimeResult<usize> {
    let mut counter = ByteCounter::default();
    serde_json::to_writer(&mut counter, value).map_err(SinexError::from)?;
    Ok(counter.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_primitives::domain::HostName;
    use sinex_primitives::events::EventId;
    use sinex_primitives::{DynamicPayload, Id, Uuid};

    fn intent_with_children(sizes: &[usize]) -> EventIntent {
        let events = sizes
            .iter()
            .map(|size| {
                let mut event = DynamicPayload::new(
                    "synthetic",
                    "partition.example",
                    json!({"body": "x".repeat(*size)}),
                )
                .from_parents([EventId::from_uuid(Uuid::now_v7())])
                .unwrap()
                .build()
                .unwrap();
                event.id = Some(Id::new());
                event
            })
            .collect();
        EventIntent::new(
            "synthetic-source",
            "synthetic-parser",
            "1",
            events,
            HostName::from_static("synthetic-host"),
        )
    }

    fn add_proof(intent: &mut EventIntent, source_ref: String) {
        intent.polylogue_revision = Some(serde_json::from_value(json!({
            "manifest_material_id": Uuid::now_v7(), "segment_material_ids": {"-1": Uuid::now_v7()},
            "normalizer_version": "synthetic-v1",
            "originals": [{"role":"provider_native", "source_ref":source_ref, "material_id":Uuid::now_v7(), "sha256":"a".repeat(64),
                "size_bytes":1, "media_type":"application/json", "acquisition":{"kind":"full_snapshot","byte_window":null,"addressing_mode":null,"tables":null}}]
        })).unwrap());
    }

    #[test]
    fn exact_sizing_matches_serde_with_all_header_fields_and_proof() {
        let mut intent = intent_with_children(&[3, 7]);
        for with_proof in [false, true] {
            if with_proof {
                add_proof(&mut intent, "synthetic:\"exact\\referenceé".into());
            }
            let mut rebuilt = intent_header(&intent);
            assert!(rebuilt.events.is_empty());
            rebuilt.events = intent.events.clone();
            assert_eq!(
                serde_json::to_vec(&intent).unwrap(),
                serde_json::to_vec(&rebuilt).unwrap()
            );
            let expected = serialized_len(&intent_header(&intent)).unwrap()
                + intent
                    .events
                    .iter()
                    .map(|event| serialized_len(event).unwrap())
                    .sum::<usize>()
                + 1;
            assert_eq!(expected, serde_json::to_vec(&intent).unwrap().len());
        }
    }

    #[test]
    fn proof_header_affects_stable_partitions_and_every_subset_retains_metadata() {
        let mut intent = intent_with_children(&[100, 100, 100]);
        let limit = serialized_len(&intent).unwrap();
        add_proof(&mut intent, "synthetic:retained-version".repeat(10));
        let plan = partition_with_limit(&intent, limit).unwrap();
        assert!(plan.len() > 1);
        assert_eq!(plan, partition_with_limit(&intent, limit).unwrap());
        let mut original_header = serde_json::to_value(&intent).unwrap();
        original_header.as_object_mut().unwrap().remove("events");
        let mut child_ids = Vec::new();
        for partition in plan {
            let chunk = intent_partition(&intent, partition).unwrap();
            assert!(serialized_len(&chunk).unwrap() <= limit);
            child_ids.extend(chunk.event_ids());
            let mut chunk_header = serde_json::to_value(&chunk).unwrap();
            chunk_header.as_object_mut().unwrap().remove("events");
            assert_eq!(chunk_header, original_header);
        }
        assert_eq!(child_ids, intent.event_ids());
    }

    #[test]
    fn exact_boundary_fits_and_one_extra_byte_splits() {
        let intent = intent_with_children(&[10, 10]);
        let exact = serialized_len(&intent).unwrap();
        assert_eq!(
            partition_with_limit(&intent, exact).unwrap(),
            vec![IntentPartition { start: 0, end: 2 }]
        );
        assert_eq!(
            partition_with_limit(&intent, exact - 1).unwrap(),
            vec![
                IntentPartition { start: 0, end: 1 },
                IntentPartition { start: 1, end: 2 }
            ]
        );
    }

    #[test]
    fn one_oversize_child_or_proof_header_is_resource_failure_before_publication() {
        let intent = intent_with_children(&[NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES]);
        assert_eq!(
            partition_event_intent(&intent).unwrap_err().kind(),
            sinex_primitives::error::SinexErrorKind::ResourceExhausted
        );
        let mut intent = intent_with_children(&[1]);
        add_proof(
            &mut intent,
            "x".repeat(NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES),
        );
        assert_eq!(
            partition_event_intent(&intent).unwrap_err().kind(),
            sinex_primitives::error::SinexErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn invalid_prepared_partition_cannot_drop_or_invent_children() {
        let intent = intent_with_children(&[1, 1]);
        for partition in [
            IntentPartition { start: 0, end: 0 },
            IntentPartition { start: 1, end: 0 },
            IntentPartition { start: 0, end: 3 },
        ] {
            assert!(intent_partition(&intent, partition).is_err());
        }
    }
}
