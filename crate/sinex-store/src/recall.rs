//! Fixed assembly over retained session and canonical rows, using the ordinary
//! role-bound reader, statement deadline, credential floor and RPC query route.
use crate::{Error, ReadPool, Result};
use serde_json::{Value, json};
use sinex_model::retrieval::{
    Project, ProjectEvidence, SessionProjectAttribution, UnknownProjectMember, project_evidence,
};
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
        projects: &[Project],
        cursor: Option<&str>,
        page: u32,
        budget: std::time::Duration,
    ) -> Result<Value> {
        let project = validate_recall(text).map_err(Error::Invalid)?;
        if project
            .as_ref()
            .is_some_and(|name| !projects.iter().any(|p| &p.name == name))
        {
            return Err(Error::Invalid("recall project is not configured"));
        }
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
                match self.recall_last(text,project.clone(),projects,page,remaining).await? {
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
        projects: &[Project],
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
        let state=sqlx::query("SELECT generation,configuration,position,serving,failure FROM derivation.read_model_position WHERE name='shell-search'").fetch_optional(&mut *tx).await?.ok_or(Error::ProjectionUnavailable("indexed retrieval is not configured"))?;
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
        let configuration = crate::projection::configuration(projects);
        if state.try_get::<String, _>("configuration")? != configuration {
            return Err(Error::ProjectionUnavailable(
                "project configuration generation is unavailable",
            ));
        }
        // Inspect at most 128 groupings and 8192 direct members in this snapshot.
        // Reaching either boundary without a conclusive answer refuses the entire
        // assembly; no inspected prefix is presented as settled empty.
        let sessions: Vec<Uuid> = sqlx::query_scalar(
            "SELECT e.id FROM core.live_events e WHERE e.event_type='activity.session.boundary' ORDER BY e.ts_orig DESC,e.ts_orig_tiebreak DESC,e.id LIMIT 129")
            .fetch_all(&mut *tx).await?;
        let mut selected: Option<Value> = None;
        let mut rows: Vec<Value> = vec![];
        let mut attribution = None;
        let mut unresolved = vec![];
        let mut inspected_members = 0usize;
        let mut inspected_sessions = 0usize;
        for id in sessions {
            inspected_sessions += 1;
            if inspected_sessions > 128 {
                return Err(Error::ProjectionUnavailable(
                    "recall candidate inspection exceeds 128 sessions; bounded answer unavailable",
                ));
            }
            // A pending ledger or caught-up projection cannot replace the
            // read owner's current-parent fence. Refuse instead of losing a
            // missing member in a JOIN or falling through to an older session.
            let candidate = sqlx::query(&format!(
                "SELECT cardinality(e.source_event_ids) AS members,e.record_state='present' AND {} AS current_eligible FROM core.live_events e WHERE e.id=$1", crate::query::CURRENT_PARENTS))
                .bind(id).fetch_one(&mut *tx).await?;
            if !candidate.try_get::<bool, _>("current_eligible")? {
                return Err(Error::ProjectionUnavailable(
                    "recall candidate has ineligible current parents; whole-session answer unavailable; inspect retained show/trace",
                ));
            }
            let count: i32 = candidate.try_get("members")?;
            inspected_members += count as usize;
            if inspected_members > 8192 {
                return Err(Error::ProjectionUnavailable(
                    "recall candidate inspection exceeds 8192 direct members; bounded answer unavailable",
                ));
            }
            let member_evidence = sqlx::query("SELECT e.id,e.payload->>'cwd' AS cwd FROM core.live_events s CROSS JOIN unnest(s.source_event_ids) parent(id) JOIN core.live_events e ON e.id=parent.id WHERE s.id=$1 ORDER BY e.ts_orig,e.ts_orig_tiebreak,e.id")
                .bind(id).fetch_all(&mut *tx).await?;
            let mut known = std::collections::BTreeMap::<String, usize>::new();
            let mut unknown = vec![];
            let mut unconfigured = 0usize;
            let mut evidence_by_id = std::collections::BTreeMap::new();
            for member in &member_evidence {
                let event: Uuid = member.try_get("id")?;
                let cwd: Option<String> = member.try_get("cwd")?;
                let evidence = project_evidence(cwd.as_deref(), projects);
                match &evidence {
                    ProjectEvidence::Known { project } => {
                        *known.entry(project.clone()).or_default() += 1
                    }
                    ProjectEvidence::Unconfigured { .. } => unconfigured += 1,
                    ProjectEvidence::Unknown { .. } => unknown.push(UnknownProjectMember {
                        event: event.to_string(),
                        evidence: evidence.clone(),
                    }),
                }
                evidence_by_id.insert(event.to_string(), evidence);
            }
            let positive = project.as_ref().is_none_or(|name| known.contains_key(name));
            let has_unknown = !unknown.is_empty();
            let summary = SessionProjectAttribution {
                rule: "direct_member_own_cwd".into(),
                known_projects: known,
                unconfigured_members: unconfigured,
                complete: !has_unknown,
                unknown_members: unknown,
                total_members: member_evidence.len(),
            };
            if positive {
                if member_evidence.len() > page as usize {
                    return Err(Error::ProjectionUnavailable(
                        "session command assembly exceeds requested page (maximum 100); whole-session recall unavailable; use retained query/show/trace",
                    ));
                }
                selected = sqlx::query_scalar(&format!(
                    "SELECT {} FROM core.live_events e WHERE e.id=$1",
                    crate::query::EVENT_VIEW
                ))
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
                rows = sqlx::query_scalar(&format!("SELECT {} FROM core.live_events s CROSS JOIN unnest(s.source_event_ids) parent(id) JOIN core.live_events e ON e.id=parent.id WHERE s.id=$1 ORDER BY e.ts_orig,e.ts_orig_tiebreak,e.id", crate::query::EVENT_VIEW)).bind(id).fetch_all(&mut *tx).await?;
                for row in &mut rows {
                    row["project_evidence"] = serde_json::to_value(
                        &evidence_by_id[row["id"]
                            .as_str()
                            .ok_or(Error::Invalid("member identity unavailable"))?],
                    )
                    .expect("project evidence serialization");
                }
                attribution = Some(summary);
                break;
            }
            if has_unknown {
                let header: Value = sqlx::query_scalar(&format!(
                    "SELECT {} FROM core.live_events e WHERE e.id=$1",
                    crate::query::EVENT_VIEW
                ))
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
                unresolved.push(json!({"session":header,"attribution":summary,"reason":"unknown_direct_member_project_evidence"}));
            }
        }
        let total = rows.len();
        let source_status = crate::source_coverage::read(&mut tx, seq).await?;
        let complete = !source_status.is_empty()
            && crate::source_coverage::interpreted_complete(&source_status);
        tx.commit().await?;
        lease.finish();
        Ok(RecallAttempt::Ready(
            json!({"notice":sinex_model::DATA_NOTICE,"unit":"recall","query":text,"session":selected,"rows":rows,"attribution":attribution,"newer_unresolved_candidates":unresolved,"total":total,"project":project,"complete":complete,"truncated":false,"source_status":source_status,"coverage":"unknown; retained shell policy grouping; focus, AFK, attention and underlying command outcomes are not inferred","resolved":{"commit_seq":seq,"projection_seq":seq,"generation":generation,"configuration":configuration,"candidate_sessions_inspected":inspected_sessions,"candidate_members_inspected":inspected_members,"zone":"Europe/Warsaw","logical_start":"04:00"},"read_as":if project.is_some(){"Latest retained shell session with positive direct-member evidence for the selected project; every eligible member is context with its own cwd evidence. Newer unresolved candidates have unknown direct-member evidence, without inferred inheritance or task purpose."}else{"Latest retained shell session with every eligible member as context and its own cwd evidence; no project selection or inferred task purpose."}}),
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
