use crate::EventRecord;
use crate::repositories::common::DbResult;
use sinex_primitives::Id;
use sinex_primitives::Timestamp;
use sinex_primitives::domain::{AutomatonModel, SyntheticTemporalPolicy, TemporalSourceType};
use sinex_primitives::events::{EventId, SourceMaterial};
use sinex_primitives::non_empty::NonEmptyVec;

use crate::JsonValue;
use crate::models::{Event, Provenance};

pub fn records_to_events(records: Vec<EventRecord>) -> DbResult<Vec<Event<JsonValue>>> {
    let mut events = Vec::with_capacity(records.len());
    for record in records {
        events.push(record.try_to_event()?);
    }
    Ok(events)
}

pub trait EventRecordExt {
    fn try_to_event(self) -> DbResult<Event<JsonValue>>;
}

fn parse_optional_enum<T>(
    value: Option<String>,
    field: &str,
    event_id: uuid::Uuid,
) -> DbResult<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match value {
        Some(raw) => raw.parse::<T>().map(Some).map_err(|error| {
            sinex_primitives::SinexError::invalid_state(
                format!("event record has invalid {field}",),
            )
            .with_context("event_id", event_id.to_string())
            .with_context("field", field.to_string())
            .with_context("value", raw)
            .with_context("parse_error", error.to_string())
        }),
        None => Ok(None),
    }
}

impl EventRecordExt for EventRecord {
    fn try_to_event(self) -> DbResult<Event<JsonValue>> {
        use crate::models::{EventId, OffsetKind, Provenance, SourceMaterial};

        let provenance = match (
            self.source_event_ids,
            self.source_material_id,
            self.anchor_byte,
        ) {
            (Some(event_ids), None, _) if !event_ids.is_empty() => {
                let ids: Vec<EventId> = event_ids.into_iter().map(EventId::from_uuid).collect();
                let non_empty = NonEmptyVec::from_vec(ids).ok_or_else(|| {
                    sinex_primitives::SinexError::invalid_state(
                        "source_event_ids unexpectedly empty after non-empty guard",
                    )
                })?;
                // Populate operation_id from the event-level created_by_operation_id column
                let operation_id = self
                    .created_by_operation_id
                    .map(sinex_primitives::Id::from_uuid);
                Provenance::Derived {
                    source_event_ids: non_empty,
                    operation_id,
                }
            }
            (None, Some(material_id), Some(anchor_byte)) => {
                let offset_kind = match self.offset_kind.as_deref() {
                    Some(raw) => OffsetKind::try_from_wire_str(raw)
                        .map_err(|err| err.with_context("event_id", self.id.to_string()))?,
                    None => OffsetKind::Byte,
                };

                Provenance::Material {
                    id: Id::<SourceMaterial>::from_uuid(material_id),
                    anchor_byte,
                    offset_start: self.offset_start,
                    offset_end: self.offset_end,
                    offset_kind,
                }
            }
            (Some(_), Some(_), _) => {
                return Err(sinex_primitives::SinexError::invalid_state(
                    "event record contains both derived and material provenance",
                ));
            }
            (None, Some(_), None) => {
                return Err(sinex_primitives::SinexError::invalid_state(
                    "material provenance missing anchor_byte",
                ));
            }
            (None, None, _) => {
                return Err(sinex_primitives::SinexError::invalid_state(
                    "event record missing provenance",
                ));
            }
            (Some(_event_ids), None, _) => {
                return Err(sinex_primitives::SinexError::invalid_state(format!(
                    "source_event_ids present but empty for event {}",
                    self.id
                )));
            }
        };

        let ts_orig = if let Some(subnano) = self.ts_orig_subnano {
            Timestamp::from_postgres_timestamp(self.ts_orig.inner(), subnano)
        } else {
            self.ts_orig
        };

        Ok(Event::<JsonValue> {
            id: Some(EventId::from_uuid(self.id)),
            source: self.source.into(),
            event_type: self.event_type.into(),
            host: self.host.into(),
            payload: self.payload,
            ts_orig: Some(ts_orig),
            ts_quality: parse_optional_enum::<TemporalSourceType>(
                self.ts_quality,
                "ts_quality",
                self.id,
            )?,
            module_run_id: self.module_run_id,
            payload_schema_id: self.payload_schema_id,
            provenance,
            anchor_payload_hash: self.anchor_payload_hash.clone(),
            associated_blob_ids: self
                .associated_blob_ids
                .map(|ids| ids.into_iter().collect()),
            temporal_policy: parse_optional_enum::<SyntheticTemporalPolicy>(
                self.temporal_policy,
                "temporal_policy",
                self.id,
            )?,
            semantics_version: self.semantics_version,
            scope_key: self.scope_key,
            equivalence_key: self.equivalence_key,
            created_by_operation_id: self.created_by_operation_id,
            automaton_model: parse_optional_enum::<AutomatonModel>(
                self.automaton_model,
                "automaton_model",
                self.id,
            )?,
        })
    }
}

pub type ExtractedProvenance = (
    Option<Vec<EventId>>,
    Option<Id<SourceMaterial>>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<i64>,
);

pub fn extract_provenance(event: &Event<JsonValue>) -> DbResult<ExtractedProvenance> {
    match &event.provenance {
        Provenance::Derived {
            source_event_ids, ..
        } => {
            let ids = source_event_ids.iter().copied().collect();
            Ok((Some(ids), None, None, None, None, None))
        }
        Provenance::Material {
            id,
            anchor_byte,
            offset_start,
            offset_end,
            offset_kind,
        } => {
            let kind = if offset_start.is_some() && offset_end.is_some() {
                Some(offset_kind.as_wire_str().to_string())
            } else {
                None
            };
            Ok((
                None,
                Some(*id),
                *offset_start,
                *offset_end,
                kind,
                Some(*anchor_byte),
            ))
        }
    }
}

#[cfg(test)]
#[path = "conversions_test.rs"]
mod tests;
