//! Build the wire form of committed events, for the confirmed stream, a
//! snapshot page and a fetch by ID alike.
use crate::{Error, Result};
use sinex_model::{Class, EventId, KindRef, MaterialId, RecordLocation, RunId, Timestamp};
use sinex_protocol::{ConfirmedEvent, PayloadIdentity, WithheldEvent, WithheldReason};
use sinex_store::confirmed::Row;
use std::collections::BTreeMap;

/// The complete events for `rows`, in order, each with its kind version's
/// registered definition hash.
pub(crate) async fn build(
    conn: &mut sqlx::PgConnection,
    rows: Vec<Row>,
) -> Result<Vec<ConfirmedEvent>> {
    let mut kinds = rows
        .iter()
        .map(|row| kind(row))
        .collect::<Result<Vec<_>>>()?;
    kinds.sort();
    kinds.dedup();
    let definitions: BTreeMap<_, _> = sinex_schema::kinds::load(conn, &kinds)
        .await?
        .into_iter()
        .map(|(kind, definition)| (kind, definition.definition_hash()))
        .collect();
    rows.into_iter()
        .map(|row| {
            let reference = kind(&row)?;
            let hash = definitions.get(&reference).cloned().ok_or_else(|| {
                Error::internal(format!(
                    "kind {} version {} is not registered",
                    reference.name, reference.version
                ))
            })?;
            event(row, hash)
        })
        .collect()
}

fn kind(row: &Row) -> Result<KindRef> {
    Ok(KindRef {
        name: row.event_type.clone(),
        version: u64::try_from(row.kind_version)?,
    })
}

fn class(name: &str) -> Result<Class> {
    serde_json::from_value(serde_json::Value::String(name.into())).map_err(Error::from)
}

fn event(row: Row, kind_definition_hash: String) -> Result<ConfirmedEvent> {
    let source_ref = row
        .source_version
        .map(|version| {
            u32::try_from(version).map(|version| sinex_model::SourceRef {
                name: row.source.clone(),
                version,
            })
        })
        .transpose()?;
    let location = match (row.material_id, row.record_offset) {
        (Some(material), Some(record_offset)) => Some(RecordLocation {
            material: MaterialId(material),
            record_offset: u64::try_from(record_offset)?,
        }),
        _ => None,
    };
    Ok(ConfirmedEvent {
        source_ref,
        source_fingerprint: row.source_fingerprint,
        producer_family: row.producer_family,
        producer_identity: row.producer_identity,
        kind_version: u64::try_from(row.kind_version)?,
        kind_definition_hash,
        validation: serde_json::from_value(serde_json::Value::String(row.validation))?,
        commit_seq: row.commit_seq,
        id: EventId(row.id),
        event_type: row.event_type,
        source: row.source,
        run: RunId(row.run_id),
        location,
        output_index: u64::try_from(row.output_index)?,
        parents: row
            .parents
            .unwrap_or_default()
            .into_iter()
            .map(EventId)
            .collect(),
        ts_orig: row.ts_orig.map(Timestamp),
        ts_quality: row.ts_quality,
        ts_coided: row.ts_coided.map(Timestamp),
        ts_persisted: Timestamp(row.ts_persisted),
        record_state: row.record_state,
        class: class(&row.class)?,
        equivalence_key: row.equivalence_key,
        reading_order: row.reading_order,
        record_hash: row
            .record_hash
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        payload_identity: PayloadIdentity::for_payload(&row.payload),
        payload: row.payload,
    })
}

/// The body-free form of an event.
pub(crate) fn withheld(
    commit_seq: i64,
    event: uuid::Uuid,
    event_type: String,
    class_name: &str,
    reason: WithheldReason,
) -> Result<WithheldEvent> {
    Ok(WithheldEvent {
        commit_seq,
        event: EventId(event),
        event_type,
        class: class(class_name)?,
        reason,
    })
}
