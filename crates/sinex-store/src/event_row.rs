//! Stored event columns and conversion to the shared event model.
use crate::{Error, Result};
use serde_json::Value;
use sinex_model::{
    Class, DbCheck, EquivalenceKey, Event, EventId, MaterialId, Origin, ReadingOrder,
    RecordLocation, RecordState, ReplaceRule, TimeQuality, Timestamp, TsOrig,
};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct EventRow {
    pub commit_seq: i64,
    pub id: Uuid,
    pub event_type: String,
    pub kind_version: i64,
    pub validation: String,
    pub source: String,
    pub run_id: Uuid,
    pub material_id: Option<Uuid>,
    pub record_offset: Option<i64>,
    pub output_index: i64,
    pub parents: Option<Vec<Uuid>>,
    pub ts_orig: Option<OffsetDateTime>,
    pub ts_quality: String,
    pub ts_coided: Option<OffsetDateTime>,
    pub ts_persisted: OffsetDateTime,
    pub payload: Value,
    pub equivalence_key: Option<String>,
    pub reading_order: i64,
    pub record_hash: Vec<u8>,
    pub record_state: String,
    pub class: String,
    pub replace_rule: Option<String>,
}

impl TryFrom<EventRow> for Event {
    type Error = Error;
    fn try_from(row: EventRow) -> Result<Self> {
        let EventRow {
            id,
            event_type,
            kind_version,
            validation,
            replace_rule,
            source,
            material_id,
            record_offset,
            output_index,
            parents,
            ts_orig,
            ts_quality,
            payload,
            equivalence_key,
            reading_order,
            record_hash,
            record_state,
            class,
            ..
        } = row;
        let origin = match (material_id, record_offset, parents) {
            (Some(material), Some(record_offset), _) => Origin::Record {
                location: RecordLocation {
                    material: MaterialId(material),
                    record_offset: u64::try_from(record_offset)
                        .map_err(|_| crate::Error::Invalid("negative record offset".into()))?,
                },
                output_index: u64::try_from(output_index)
                    .map_err(|_| crate::Error::Invalid("negative output_index".into()))?,
            },
            (_, _, Some(parents)) => Origin::Derived {
                parents: parents.into_iter().map(EventId).collect(),
            },
            _ => return Err(crate::Error::Invalid(format!("event {id} has no origin"))),
        };
        let quality: TimeQuality = stored(&ts_quality, "time quality")?;
        Ok(Event {
            id: EventId(id),
            event_type,
            kind_version: u64::try_from(kind_version)
                .map_err(|_| crate::Error::Invalid("invalid kind version".into()))?,
            validation: stored(&validation, "validation")?,
            source,
            origin,
            ts_orig: TsOrig::maybe(ts_orig.map(Timestamp), quality),
            payload,
            equivalence_key: equivalence_key.map(EquivalenceKey),
            replace: replace_rule
                .as_deref()
                .map(|r| stored::<ReplaceRule>(r, "replacement rule"))
                .transpose()?,
            reading_order: ReadingOrder(reading_order),
            record_hash: record_hash
                .try_into()
                .map_err(|_| crate::Error::Invalid("record hash is not 32 bytes".into()))?,
            record_state: stored::<RecordState>(&record_state, "record state")?,
            class: stored::<Class>(&class, "class")?,
            owned_blobs: Vec::new(),
        })
    }
}

pub(crate) fn stored<T: DbCheck>(text: &str, column: &str) -> Result<T> {
    T::from_db(text).ok_or_else(|| Error::Invalid(format!("unknown {column} {text:?}")))
}
