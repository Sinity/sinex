//! Committed events in full, as the confirmed stream, a snapshot page and a
//! fetch by ID deliver them. Payloads are read through `core.visible_events`,
//! so a class the connection's grants do not lift arrives withheld.
use crate::Result;
use std::collections::BTreeMap;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub id: Uuid,
    pub commit_seq: i64,
    pub event_type: String,
    pub kind_version: i64,
    pub validation: String,
    pub source: String,
    pub source_version: Option<i64>,
    pub source_fingerprint: Option<String>,
    pub producer_family: Option<String>,
    pub producer_identity: Option<String>,
    pub run_id: Uuid,
    pub material_id: Option<Uuid>,
    pub record_offset: Option<i64>,
    pub output_index: i64,
    pub parents: Option<Vec<Uuid>>,
    pub ts_orig: Option<OffsetDateTime>,
    pub ts_quality: String,
    pub ts_coided: Option<OffsetDateTime>,
    pub ts_persisted: OffsetDateTime,
    pub payload: serde_json::Value,
    pub equivalence_key: Option<String>,
    pub reading_order: i64,
    pub record_hash: Vec<u8>,
    pub record_state: String,
    pub class: String,
}

/// The live or archived events among `ids`, in the requested order; a
/// tombstoned or unknown ID is omitted.
pub async fn rows(conn: &mut sqlx::PgConnection, ids: &[Uuid]) -> Result<Vec<Row>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let found = sqlx::query_as!(Row,
        r#"SELECT e.id AS "id!",e.commit_seq AS "commit_seq!",e.event_type AS "event_type!",
          e.kind_version AS "kind_version!",e.validation AS "validation!",e.source AS "source!",
          e.source_version,e.source_fingerprint,e.producer_family,e.producer_identity,
          e.run_id AS "run_id!",e.material_id,e.record_offset,e.output_index AS "output_index!",e.parents,
          e.ts_orig,e.ts_quality AS "ts_quality!",e.ts_coided,e.ts_persisted AS "ts_persisted!",
          e.payload AS "payload!",e.equivalence_key,e.reading_order AS "reading_order!",
          e.record_hash AS "record_hash!",e.record_state AS "record_state!",e.class AS "class!"
        FROM unnest($1::uuid[]) selected(id) CROSS JOIN LATERAL (
          SELECT v.* FROM core.visible_events v WHERE v.id=selected.id OFFSET 0) e
        WHERE NOT EXISTS(SELECT 1 FROM core.event_tombstones t WHERE t.event_id=e.id)"#,
        ids
    )
    .fetch_all(conn)
    .await?;
    let found: BTreeMap<_, _> = found.into_iter().map(|row| (row.id, row)).collect();
    Ok(ids.iter().filter_map(|id| found.get(id).cloned()).collect())
}
