//! Bounded inspection over canonical acquisition observations. No provider reads.
use crate::{COUNTER, Error, ReadPool, Result, WriterPool};
use serde_json::{Value, json};
use sinex_model::acquisition::{CollectorScope, OutcomeRefs, PollSchedule, SourcesRequest};
use sqlx::Connection;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

const INPUTS: usize = 16;
const HISTORY: usize = 8;
const RECEIPTS: usize = 16;

impl WriterPool {
    pub(crate) async fn schedule_source(
        &mut self,
        observation: Uuid,
        schedule: PollSchedule,
    ) -> Result<()> {
        schedule
            .validate()
            .map_err(|_| Error::Invalid("invalid acquisition schedule"))?;
        let mut tx = self.connection.begin().await?;
        let seq: i64 = sqlx::query_scalar(COUNTER).fetch_one(&mut *tx).await?;
        let count = sqlx::query("UPDATE raw.observations SET commit_seq=$3,metadata=jsonb_set(metadata,'{_sinex_acquisition_v1,schedule}',$2) WHERE id=$1 AND status IN ('completed','unavailable') AND metadata->'_sinex_acquisition_v1'->>'version'='1' AND metadata->'_sinex_acquisition_v1'->>'owner_run'=$4 AND NOT (metadata->'_sinex_acquisition_v1' ? 'schedule')")
            .bind(observation).bind(serde_json::to_value(schedule).map_err(|_| Error::Invalid("invalid schedule encoding"))?).bind(seq).bind(self.run_id.to_string()).execute(&mut *tx).await?.rows_affected();
        if count != 1 {
            return Err(Error::Invalid("source settlement cannot be rescheduled"));
        }
        tx.commit().await?;
        Ok(())
    }
}

impl ReadPool {
    pub async fn inspect_sources(
        &self,
        request: SourcesRequest,
        active: Vec<CollectorScope>,
    ) -> Result<Value> {
        request
            .validate()
            .map_err(|_| Error::Invalid("invalid source inspection"))?;
        let now = OffsetDateTime::now_utc();
        let mut lease = self.read_lease().await?;
        let mut tx = lease.connection().begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SET LOCAL statement_timeout='2000ms'")
            .execute(&mut *tx)
            .await?;
        let seq: i64 = sqlx::query_scalar("SELECT seq FROM core.commit_counter WHERE one")
            .fetch_one(&mut *tx)
            .await?;
        let mut rows: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('source',i.binding,'input',i.id,'locator',i.locator,'representation',CASE WHEN i.binding IN ('terminal.atuin-history','browser.history.chromium') THEN 'canonical_sqlite_rows' ELSE 'original_zsh_history' END,'sqlite_snapshots',CASE WHEN i.binding='terminal.atuin-history' THEN 'unavailable_until_1c' WHEN i.binding='browser.history.chromium' THEN 'periodic_full_bases; page_patches_unavailable' ELSE NULL END,'last_run',to_jsonb(o),'cursor',(SELECT position FROM raw.source_cursors WHERE binding=i.binding AND input=i.id::text),'read_checkpoint',CASE WHEN i.binding IN ('terminal.atuin-history','browser.history.chromium') THEN (SELECT carry FROM raw.source_cursors WHERE binding=i.binding AND input=i.id::text) ELSE NULL END,'first_record_error',(SELECT to_jsonb(f) FROM raw.record_failures f JOIN raw.source_material_registry fm ON fm.id=f.material_id WHERE f.observation_id=o.id ORDER BY fm.staged_at,fm.id,f.anchor_byte LIMIT 1)) FROM raw.inputs i LEFT JOIN LATERAL (SELECT * FROM raw.observations WHERE input_id=i.id ORDER BY seq DESC LIMIT 1) o ON true WHERE ($1::uuid IS NULL OR i.id=$1) AND ($2::uuid IS NULL OR i.id>$2) ORDER BY i.id LIMIT $3")
            .bind(request.input).bind(request.after_input).bind((INPUTS+1) as i64).fetch_all(&mut *tx).await?;
        let inputs_more = rows.len() > INPUTS;
        rows.truncate(INPUTS);
        for row in &mut rows {
            let input: Uuid = serde_json::from_value(row["input"].clone())
                .map_err(|_| Error::Invalid("invalid input identity"))?;
            let last = &row["last_run"];
            let schedule = serde_json::from_value::<PollSchedule>(
                last["metadata"]["_sinex_acquisition_v1"]["schedule"].clone(),
            )
            .ok()
            .filter(|s| s.validate().is_ok());
            let owner_run = last["metadata"]["_sinex_acquisition_v1"]["owner_run"]
                .as_str()
                .and_then(|s| Uuid::parse_str(s).ok());
            let applies = sinex_model::acquisition::current_scope(
                &active,
                row["source"].as_str().unwrap_or(""),
                row["locator"].as_str().unwrap_or(""),
                owner_run,
            );
            let state = if applies {
                sinex_model::acquisition::state(last["status"].as_str(), schedule.as_ref(), now)
            } else {
                sinex_model::acquisition::AcquisitionState::Unknown
            };
            row["acquisition"] = json!({"state":state,"attempt_started_at":last["observed_at"],"schedule":schedule,"schedule_applicability":if applies{"current_collector_run"}else{"historical_or_unknown; not the current configured collector run"},"schedule_evidence":if schedule.is_some(){"collector_observed_v1"}else{"unknown"},"coverage":"unknown; acquisition outcomes do not prove historical capture","cause":last["reason"],"overdue_cause":"unknown; a missed planned start is not a capture-stop time or reading-poll deadline"});
            sqlx::query("SAVEPOINT source_detail")
                .execute(&mut *tx)
                .await?;
            sqlx::query("SET LOCAL statement_timeout='100ms'")
                .execute(&mut *tx)
                .await?;
            let inspected = tokio::time::timeout(
                std::time::Duration::from_millis(200),
                inspect_detail(&mut tx, row, &request, input),
            )
            .await;
            if !matches!(inspected, Ok(Ok(()))) {
                // Roll back a failed optional statement while keeping the primary
                // source snapshot and latest outcome available.
                if sqlx::query("ROLLBACK TO SAVEPOINT source_detail")
                    .execute(&mut *tx)
                    .await
                    .is_err()
                {
                    return Err(Error::Invalid("source detail connection unavailable"));
                }
                row["inspection"] = json!({"status":"unavailable","reason":"optional inspection budget or evidence unavailable","retry":"sources --input INPUT"});
                row["outcome_reference_evidence"] = json!({"success_known":false,"failure_known":false,"reason":"inspection unavailable"});
                row["last_success"] = Value::Null;
                row["last_failure"] = Value::Null;
                row["history"] =
                    json!({"complete":false,"rows":[],"reason":"inspection unavailable"});
                row["receipts"] =
                    json!({"complete":false,"rows":[],"reason":"inspection unavailable"});
            } else {
                row["inspection"] =
                    json!({"status":"available","statement_budget_ms":100,"input_budget_ms":200});
            }
            sqlx::query("SET LOCAL statement_timeout='2000ms'")
                .execute(&mut *tx)
                .await?;
            sqlx::query("RELEASE SAVEPOINT source_detail")
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        lease.finish();
        for source in &mut rows {
            if !source["first_record_error"].is_null() {
                let failure = &mut source["first_record_error"];
                let material: Uuid = serde_json::from_value(failure["material_id"].clone())
                    .map_err(|_| Error::Invalid("record failure material unavailable"))?;
                let anchor = failure["anchor_byte"]
                    .as_i64()
                    .ok_or(Error::Invalid("record failure anchor unavailable"))?;
                match self.retained_record(material, anchor, None, &[]).await {
                    Ok((record, end)) => {
                        failure["record"] = record;
                        failure["end"] = json!(end);
                        failure["evidence_status"] = json!("available");
                    }
                    Err(_) => {
                        failure["record"] = Value::Null;
                        failure["end"] = Value::Null;
                        failure["evidence_status"] = json!("unavailable");
                    }
                }
                failure["representation"] = json!("canonical_sqlite_row");
            }
        }
        let next_input = inputs_more
            .then(|| rows.last().map(|r| r["input"].clone()))
            .flatten();
        Ok(
            json!({"notice":sinex_model::DATA_NOTICE,"sources":rows,"page":{"size":INPUTS,"complete":!inputs_more,"next_after_input":next_input,"continuation":"sources --after-input NEXT"},"resolved":{"now":now.format(&Rfc3339).map_err(|_| Error::Invalid("source clock encoding"))?,"commit_seq":seq},"read_as":"Current retained observations; pages use current snapshots. History continues toward older sequence numbers; acquisition receipts continue by interpretation ID."}),
        )
    }
}

async fn inspect_detail(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &mut Value,
    request: &SourcesRequest,
    input: Uuid,
) -> Result<()> {
    let last = row["last_run"].clone();
    let refs = serde_json::from_value::<OutcomeRefs>(
        last["metadata"]["_sinex_acquisition_v1"]["outcomes"].clone(),
    )
    .ok();
    let id = last["id"].as_str().and_then(|s| Uuid::parse_str(s).ok());
    let success = if last["status"] == "completed" {
        id
    } else {
        refs.as_ref().and_then(|r| r.last_success)
    };
    let failure = if last["status"] == "unavailable" || last["status"] == "interrupted" {
        id
    } else {
        refs.as_ref().and_then(|r| r.last_failure)
    };
    for (key, reference) in [("last_success", success), ("last_failure", failure)] {
        row[key] = match reference {
            Some(id) => sqlx::query_scalar::<_, Value>(
                "SELECT to_jsonb(o) FROM raw.observations o WHERE id=$1 AND input_id=$2",
            )
            .bind(id)
            .bind(input)
            .fetch_optional(&mut **tx)
            .await?
            .unwrap_or(Value::Null),
            None => Value::Null,
        };
    }
    row["outcome_reference_evidence"] = json!({"success_known":success.is_some() || refs.as_ref().is_some_and(|r| r.success_known),"failure_known":failure.is_some() || refs.as_ref().is_some_and(|r| r.failure_known),"read_as":"Indexed last-outcome references for this observation; missing predecessor summaries remain unknown."});
    let mut history: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(o) FROM raw.observations o WHERE input_id=$1 AND ($2::bigint IS NULL OR seq<$2) ORDER BY seq DESC LIMIT $3")
                .bind(input).bind(request.before_seq).bind((HISTORY+1) as i64).fetch_all(&mut **tx).await?;
    let history_more = history.len() > HISTORY;
    history.truncate(HISTORY);
    let selected = request.observation.or_else(|| {
        row["last_run"]["id"]
            .as_str()
            .and_then(|s| Uuid::parse_str(s).ok())
    });
    let observed: Option<Value> = match selected {
        Some(id) => {
            sqlx::query_scalar(
                "SELECT to_jsonb(o) FROM raw.observations o WHERE input_id=$1 AND id=$2",
            )
            .bind(input)
            .bind(id)
            .fetch_optional(&mut **tx)
            .await?
        }
        None => None,
    };
    if request.observation.is_some() && observed.is_none() {
        return Err(Error::Invalid("observation does not belong to input"));
    }
    let known = observed
        .as_ref()
        .is_some_and(|o| o["metadata"]["_sinex_acquisition_v1"]["receipts"] == "capture_insert_v1");
    let mut receipts: Vec<Value> = if known {
        // Replay reparses the same material but sets created_by_operation. It
        // must never become an original acquisition's recovery receipt.
        sqlx::query_scalar("SELECT jsonb_build_object('event',e.id,'material',e.source_material_id,'start',e.offset_start,'end',e.offset_end,'record_state',e.record_state) FROM core.events e JOIN raw.source_material_registry m ON m.id=e.source_material_id WHERE m.input_id=$1 AND m.observation_id=$2 AND e.source IN ('terminal.atuin-history','browser.history.chromium') AND e.event_type IN ('command.executed','page.visited','download.observed') AND e.created_by_operation IS NULL AND ($3::uuid IS NULL OR e.id>$3) ORDER BY e.id LIMIT $4")
                    .bind(input).bind(selected).bind(request.after_receipt).bind((RECEIPTS+1) as i64).fetch_all(&mut **tx).await?
    } else {
        vec![]
    };
    let receipts_more = receipts.len() > RECEIPTS;
    receipts.truncate(RECEIPTS);
    let next_receipt = receipts_more
        .then(|| receipts.last().map(|r| r["event"].clone()))
        .flatten();
    let next_history = history_more
        .then(|| history.last().map(|r| r["seq"].clone()))
        .flatten();
    row["history"] = json!({"rows":history,"complete":!history_more,"next_before_seq":next_history,"page_size":HISTORY,"continuation":"sources --input INPUT --before-seq NEXT"});
    row["receipts"] = json!({"observation":observed,"rows":receipts,"evidence":if known{"capture_insert_v1"}else{"unknown_legacy_or_no_observation"},"complete":known && !receipts_more,"next_after_receipt":next_receipt,"page_size":RECEIPTS,"continuation":"sources --input INPUT --observation OBSERVATION --after-receipt NEXT","read_as":"New acquisition interpretations including removed records; current show/trace governs eligibility and protected content. No historical coverage claim."});
    Ok(())
}
