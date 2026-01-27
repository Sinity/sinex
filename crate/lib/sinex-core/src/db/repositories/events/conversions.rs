use crate::repositories::common::DbResult;
use crate::types::non_empty::NonEmptyVec;
use crate::types::Id;
use crate::EventRecord;
use sinex_schema::ulid::Ulid;

use crate::models::{Event, JsonValue, Provenance};
use crate::types::domain::{EventSource, EventType, HostName};
use chrono::{DateTime, Utc};

pub fn records_to_events(records: Vec<EventRecord>) -> DbResult<Vec<Event<JsonValue>>> {
    let mut events = Vec::with_capacity(records.len());
    for record in records {
        events.push(record.try_to_event()?);
    }
    Ok(events)
}

#[derive(Debug, sqlx::FromRow)]
pub struct EventSearchRow {
    pub id: Ulid,
    pub source: EventSource,
    pub event_type: EventType,
    pub host: HostName,
    pub ts_ingest: DateTime<Utc>,
    pub payload: JsonValue,
    pub score: Option<f64>,
    pub snippet: Option<String>,
}

pub(crate) trait EventRecordExt {
    fn try_to_event(self) -> DbResult<Event<JsonValue>>;
}

impl EventRecordExt for EventRecord {
    fn try_to_event(self) -> DbResult<Event<JsonValue>> {
        use crate::db::models::event::{EventId, OffsetKind, Provenance, SourceMaterial};
        use crate::repositories::common::db_error;

        let provenance = match (
            self.source_event_ids,
            self.source_material_id,
            self.anchor_byte,
        ) {
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
                    sqlx::Error::Protocol(
                        format!("source_event_ids present but empty for event {}", self.id).into(),
                    ),
                    "convert event record provenance",
                ));
            }
        };

        let mut ts_orig = self.ts_orig;
        if let Some(subnano) = self.ts_orig_subnano {
            if let Some(adjusted) =
                ts_orig.checked_add_signed(chrono::Duration::nanoseconds(subnano as i64))
            {
                ts_orig = adjusted;
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

pub type ExtractedProvenance = (
    Option<Vec<Ulid>>,
    Option<Ulid>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<i64>,
);

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
    }
}
