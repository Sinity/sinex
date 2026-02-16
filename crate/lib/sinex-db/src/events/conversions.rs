use crate::error::{db_error, DbResult};
use serde_json::Value as JsonValue;
use sinex_primitives::events::{Event, EventId, OffsetKind, Provenance, SourceMaterial};
use sinex_primitives::ids::Id;
use sinex_primitives::non_empty::NonEmptyVec;
use sinex_schema::schema::records::EventRecord;
use sinex_schema::ulid::Ulid;

/// Convert a list of database records to `Event<JsonValue>` domain objects.
///
/// Reconstructs provenance information and handles timestamp adjustments for each record.
pub fn records_to_events(records: Vec<EventRecord>) -> DbResult<Vec<Event<JsonValue>>> {
    let mut events = Vec::with_capacity(records.len());
    for record in records {
        events.push(record.try_to_event()?);
    }
    Ok(events)
}

/// Trait to convert DB records to Domain events
pub trait EventRecordExt {
    fn try_to_event(self) -> DbResult<Event<JsonValue>>;
}

impl EventRecordExt for EventRecord {
    fn try_to_event(self) -> DbResult<Event<JsonValue>> {
        let provenance = match (
            self.source_event_ids,
            self.source_material_id,
            self.anchor_byte,
        ) {
            // Synthesis provenance
            (Some(event_ids), None, _) if !event_ids.is_empty() => {
                let ids: Vec<EventId> = event_ids.into_iter().map(EventId::from_ulid).collect();
                let non_empty = NonEmptyVec::from_vec(ids).ok_or_else(|| {
                    db_error(
                        sqlx::Error::Protocol("source_event_ids unexpectedly empty".into()),
                        "convert event record provenance",
                    )
                })?;
                Provenance::Synthesis {
                    source_event_ids: non_empty,
                    operation_id: None,
                }
            }
            // Material provenance
            (None, Some(material_id), Some(anchor_byte)) => {
                let offset_kind = match self.offset_kind.as_deref() {
                    Some("line") => OffsetKind::Line,
                    Some("rowid") => OffsetKind::Record,
                    Some("logical") => OffsetKind::Character,
                    Some("byte") | None => OffsetKind::Byte,
                    Some(other) => {
                        tracing::warn!(
                            offset_kind = other,
                            "Unknown offset_kind, defaulting to byte"
                        );
                        OffsetKind::Byte
                    }
                };

                Provenance::Material {
                    id: Id::<SourceMaterial>::from_ulid(material_id),
                    anchor_byte,
                    offset_start: self.offset_start,
                    offset_end: self.offset_end,
                    offset_kind,
                }
            }
            // Error cases
            (Some(_), Some(_), _) => {
                return Err(db_error(
                    sqlx::Error::Protocol(
                        "event record contains both synthesis and material provenance".into(),
                    ),
                    "convert event record provenance",
                ));
            }
            (None, Some(_), None) => {
                return Err(db_error(
                    sqlx::Error::Protocol("material provenance missing anchor_byte".into()),
                    "convert event record provenance",
                ));
            }
            (None, None, _) => {
                return Err(db_error(
                    sqlx::Error::Protocol("event record missing provenance".into()),
                    "convert event record provenance",
                ));
            }
            (Some(_event_ids), None, _) => {
                return Err(db_error(
                    sqlx::Error::Protocol(format!(
                        "source_event_ids present but empty for event {}",
                        self.id
                    )),
                    "convert event record provenance",
                ));
            }
        };

        // Timestamp nanosecond adjustment
        let mut ts_orig = self.ts_orig;
        if let Some(subnano) = self.ts_orig_subnano {
            if let Some(adjusted) = ts_orig.checked_add(time::Duration::nanoseconds(subnano as i64))
            {
                ts_orig = adjusted.into();
            }
        }

        Ok(Event::<JsonValue> {
            id: Some(EventId::from_ulid(self.id)),
            source: self.source.into(),
            event_type: self.event_type.into(),
            host: self.host.into(),
            payload: self.payload,
            ts_orig: Some(ts_orig),
            ingestor_version: self.ingestor_version,
            payload_schema_id: self.payload_schema_id,
            provenance,
            associated_blob_ids: self
                .associated_blob_ids
                .map(|ids| ids.into_iter().collect()),
        })
    }
}

/// Extracted provenance tuple: (source_event_ids, source_material_id, offset_start, offset_end, offset_kind, anchor_byte).
///
/// Used for preparing event records for database insertion.
pub type ExtractedProvenance = (
    Option<Vec<Ulid>>,
    Option<Ulid>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<i64>,
);

/// Extract provenance information from an Event for storage in the database.
///
/// Returns a tuple containing the provenance fields ready for insertion:
/// - source_event_ids (for synthesis provenance)
/// - source_material_id, offset_start, offset_end, offset_kind, anchor_byte (for material provenance)
pub fn extract_provenance(event: &Event<JsonValue>) -> ExtractedProvenance {
    match &event.provenance {
        Provenance::Synthesis {
            source_event_ids, ..
        } => {
            let ulids = source_event_ids.iter().map(|id| *id.as_ulid()).collect();
            (Some(ulids), None, None, None, None, None)
        }
        Provenance::Material {
            id,
            anchor_byte,
            offset_start,
            offset_end,
            offset_kind,
        } => {
            let kind = Some(offset_kind.as_wire_str().to_string());
            (
                None,
                Some(*id.as_ulid()),
                *offset_start,
                *offset_end,
                kind,
                Some(*anchor_byte),
            )
        }
        _ => (None, None, None, None, None, None),
    }
}
