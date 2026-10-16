//! Fixed assembly over retained session and canonical rows, using the ordinary
//! role-bound reader, statement deadline, credential floor and RPC query route.
use crate::{Error, ReadPool, Result};
use serde_json::{Value, json};
use sqlx::{Connection, Row};
use uuid::Uuid;
// Only projection catch-up is eligible for the short settling window. Missing
// capability, owed session work and representation refusal stay ordinary errors.
enum RecallAttempt {
    Ready(Value),
    ProjectionPending,
}

pub fn is_recall(text: &str) -> bool {
    text.split_whitespace()
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("recall"))
}
pub fn validate_recall(text: &str) -> std::result::Result<Option<String>, &'static str> {
    if text == "recall last" {
        return Ok(None);
    }
    let rest=text.strip_prefix("recall last where project = ").ok_or("supported recall: recall last [where project = NAME]; time, mark and general recall are unavailable")?;
    let name = if rest.starts_with('"') {
        serde_json::from_str::<String>(rest)
            .map_err(|_| "project must be one name or JSON string")?
    } else {
        rest.to_string()
    };
    if name.is_empty()
        || name.len() > 255
        || (!rest.starts_with('"') && name.chars().any(char::is_whitespace))
    {
        return Err("project must be one name or JSON string");
    }
    Ok(Some(name))
}
impl ReadPool {
    pub async fn recall_with_timeout(
        &self,
        text: &str,
        cursor: Option<&str>,
        page: u32,
        budget: std::time::Duration,
    ) -> Result<Value> {
        let project = validate_recall(text).map_err(Error::Invalid)?;
        if cursor.is_some() || !(1..=100).contains(&page) {
            return Err(Error::Invalid(
                "recall last supports a single bounded assembly; no cursor; page must be 1..=100",
            ));
        }
        match tokio::time::timeout(budget, async {
            // The indexed owner uses this same bounded settling window. Normal
            // collector commit/projection races need no manual client intervention;
            // work still pending after the window remains explicitly unavailable.
            let deadline=tokio::time::Instant::now()+budget;
            let settle=tokio::time::Instant::now()+std::time::Duration::from_millis(500);
            loop {
                let remaining=deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero(){return Err(Error::Deadline);}
                match self.recall_last(text,project.clone(),page,remaining).await? {
                    RecallAttempt::Ready(value) => break Ok(value),
                    RecallAttempt::ProjectionPending if tokio::time::Instant::now()<settle => {
                        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                    }
                    RecallAttempt::ProjectionPending => break Err(Error::ProjectionUnavailable(
                        "shell sessions or indexed projection pending; inspect status; no settled recall answer is available",
                    )),
                }
            }
        }).await {
            Err(_) => Err(Error::Deadline),
            Ok(Err(Error::Database(sqlx::Error::Database(e))))
                if e.code().as_deref() == Some("57014") =>
            {
                Err(Error::Deadline)
            }
            Ok(result) => result,
        }
    }
    async fn recall_last(
        &self,
        text: &str,
        project: Option<String>,
        page: u32,
        budget: std::time::Duration,
    ) -> Result<RecallAttempt> {
        let mut lease = self.read_lease().await?;
        let mut tx = lease.connection().begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SELECT set_config('statement_timeout',$1,true),set_config('client_connection_check_interval','100ms',true)").bind(format!("{}ms",budget.as_millis().max(1))).execute(&mut *tx).await?;
        let seq: i64 = sqlx::query_scalar("SELECT seq FROM core.commit_counter WHERE one")
            .fetch_one(&mut *tx)
            .await?;
        let enabled:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core.producer_versions p JOIN core.runs r ON r.id=p.run_id WHERE p.name='session' AND r.status='running')").fetch_one(&mut *tx).await?;
        if !enabled {
            return Err(Error::ProjectionUnavailable(
                "shell session reconciliation is not configured",
            ));
        }
        let state=sqlx::query("SELECT generation,position,serving,failure FROM derivation.read_model_position WHERE name='shell-search'").fetch_optional(&mut *tx).await?.ok_or(Error::ProjectionUnavailable("indexed retrieval is not configured"))?;
        let pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM derivation.session_dirty) OR EXISTS(SELECT 1 FROM derivation.shell_dirty) OR EXISTS(SELECT 1 FROM derivation.session_progress WHERE failure IS NOT NULL)").fetch_one(&mut *tx).await?;
        if pending {
            return Err(Error::ProjectionUnavailable(
                "shell session reconciliation pending or failed; inspect status; no settled recall answer is available",
            ));
        }
        if state.try_get::<Option<String>, _>("failure")?.is_some() {
            return Err(Error::ProjectionUnavailable(
                "indexed projection maintenance unavailable; inspect status",
            ));
        }
        if !state.try_get::<bool, _>("serving")? || state.try_get::<i64, _>("position")? != seq {
            return Ok(RecallAttempt::ProjectionPending);
        }
        let generation: Uuid = state.try_get("generation")?;
        let session:Option<Value>=sqlx::query_scalar(&format!("SELECT {} FROM core.live_events e WHERE e.event_type='activity.session.boundary' AND ($1::text IS NULL OR EXISTS(SELECT 1 FROM core.event_project p WHERE p.generation=$2 AND p.project=$1 AND p.event_id=ANY(e.source_event_ids))) ORDER BY e.ts_orig DESC,e.ts_orig_tiebreak DESC,e.id LIMIT 1",crate::query::EVENT_VIEW)).bind(&project).bind(generation).fetch_optional(&mut *tx).await?;
        let mut rows: Vec<Value> = vec![];
        let mut total = 0i64;
        if let Some(session) = &session {
            let id = Uuid::parse_str(
                session["id"]
                    .as_str()
                    .ok_or(Error::Invalid("session identity unavailable"))?,
            )
            .map_err(|_| Error::Invalid("session identity unavailable"))?;
            total=sqlx::query_scalar("SELECT count(*) FROM core.live_events s CROSS JOIN unnest(s.source_event_ids) parent(id) JOIN core.live_events e ON e.id=parent.id WHERE s.id=$1 AND ($2::text IS NULL OR EXISTS(SELECT 1 FROM core.event_project p WHERE p.generation=$3 AND p.event_id=e.id AND p.project=$2))").bind(id).bind(&project).bind(generation).fetch_one(&mut *tx).await?;
            // Refuse an assembly larger than its explicit user-requested page.
            // A selected prefix never passes for a complete work session.
            if total > i64::from(page) {
                return Err(Error::ProjectionUnavailable(
                    "session command assembly exceeds requested page (maximum 100); whole-session recall unavailable; use retained query/show/trace",
                ));
            }
            rows=sqlx::query_scalar(&format!("SELECT {} FROM core.live_events s CROSS JOIN unnest(s.source_event_ids) parent(id) JOIN core.live_events e ON e.id=parent.id WHERE s.id=$1 AND ($2::text IS NULL OR EXISTS(SELECT 1 FROM core.event_project p WHERE p.generation=$3 AND p.event_id=e.id AND p.project=$2)) ORDER BY e.ts_orig,e.ts_orig_tiebreak,e.id LIMIT 100",crate::query::EVENT_VIEW)).bind(id).bind(&project).bind(generation).fetch_all(&mut *tx).await?;
        }
        let source_status = crate::source_coverage::read(&mut tx, seq).await?;
        let complete = !source_status.is_empty()
            && crate::source_coverage::interpreted_complete(&source_status);
        tx.commit().await?;
        lease.finish();
        Ok(RecallAttempt::Ready(
            json!({"notice":sinex_model::DATA_NOTICE,"unit":"recall","query":text,"session":session,"rows":rows,"total":total,"project":project,"complete":complete,"truncated":false,"source_status":source_status,"coverage":"unknown; retained shell policy grouping; focus, AFK, attention and underlying command outcomes are not inferred","resolved":{"commit_seq":seq,"projection_seq":seq,"zone":"Europe/Warsaw","logical_start":"04:00"},"read_as":"Latest retained shell session containing the selected project; command rows are project-scoped; the header and exact session parents describe the entire policy grouping."}),
        ))
    }
}
#[cfg(test)]
mod tests {
    #[test]
    fn only_supported_recall_is_bound() {
        assert_eq!(super::validate_recall("recall last").unwrap(), None);
        assert_eq!(
            super::validate_recall("recall last where project = tide").unwrap(),
            Some("tide".into())
        );
        for s in [
            "recall yesterday",
            "recall last where project = tide limit 1",
            "recall last where project = ",
        ] {
            assert!(super::validate_recall(s).is_err());
        }
    }
}
