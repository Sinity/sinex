//! `recall last [where project = NAME]`: the latest shell session, optionally
//! one with a direct member in a project, with all its member commands.
use crate::query::{EVENT_VIEW, begin_snapshot, with_deadline};
use crate::{Error, ReadPool, Result};
use serde_json::{Value, json};
use sinex_model::retrieval::{
    Project, ProjectEvidence, SessionProjectAttribution, UnknownProjectMember, project_evidence,
};
use sqlx::{PgConnection, Row};
use std::collections::BTreeMap;
use uuid::Uuid;

const MAX_SESSIONS: usize = 128;
const MAX_MEMBERS: usize = 8192;
// Collectors commit before the projection catches up; recall waits briefly for
// it. Other failures, such as pending session work, are returned at once.
const CATCH_UP_WAIT: std::time::Duration = std::time::Duration::from_millis(500);
const CATCH_UP_POLL: std::time::Duration = std::time::Duration::from_millis(25);

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
    let rest = text.strip_prefix("recall last where project = ").ok_or(
        "supported recall: recall last [where project = NAME]; time, mark and general recall are unavailable",
    )?;
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

/// One session's direct members with their project evidence.
struct Members {
    evidence: BTreeMap<String, ProjectEvidence>,
    attribution: SessionProjectAttribution,
}
async fn session_members(
    tx: &mut PgConnection,
    session: Uuid,
    projects: &[Project],
) -> Result<Members> {
    let members = sqlx::query(
        "SELECT e.id,e.payload->>'cwd' AS cwd FROM core.live_events s \
         CROSS JOIN unnest(s.source_event_ids) parent(id) JOIN core.live_events e ON e.id=parent.id \
         WHERE s.id=$1 ORDER BY e.ts_orig,e.ts_orig_tiebreak,e.id",
    )
    .bind(session)
    .fetch_all(&mut *tx)
    .await?;
    let mut known = BTreeMap::<String, usize>::new();
    let mut unknown = vec![];
    let mut unconfigured = 0usize;
    let mut evidence_by_id = BTreeMap::new();
    for member in &members {
        let event: Uuid = member.try_get("id")?;
        let cwd: Option<String> = member.try_get("cwd")?;
        let evidence = project_evidence(cwd.as_deref(), projects);
        match &evidence {
            ProjectEvidence::Known { project } => *known.entry(project.clone()).or_default() += 1,
            ProjectEvidence::Unconfigured { .. } => unconfigured += 1,
            ProjectEvidence::Unknown { .. } => unknown.push(UnknownProjectMember {
                event: event.to_string(),
                evidence: evidence.clone(),
            }),
        }
        evidence_by_id.insert(event.to_string(), evidence);
    }
    Ok(Members {
        evidence: evidence_by_id,
        attribution: SessionProjectAttribution {
            rule: "direct_member_own_cwd".into(),
            known_projects: known,
            unconfigured_members: unconfigured,
            complete: unknown.is_empty(),
            unknown_members: unknown,
            total_members: members.len(),
        },
    })
}
async fn render_event(tx: &mut PgConnection, id: Uuid) -> Result<Value> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT {EVENT_VIEW} FROM core.live_events e WHERE e.id=$1"
    ))
    .bind(id)
    .fetch_one(&mut *tx)
    .await?)
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
        with_deadline(budget, async {
            let deadline = tokio::time::Instant::now() + budget;
            let give_up = tokio::time::Instant::now() + CATCH_UP_WAIT;
            loop {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return Err(Error::Deadline);
                }
                match self
                    .recall_last(text, project.as_deref(), projects, page, remaining)
                    .await?
                {
                    RecallAttempt::Ready(value) => break Ok(value),
                    RecallAttempt::ProjectionPending if tokio::time::Instant::now() < give_up => {
                        tokio::time::sleep(CATCH_UP_POLL).await;
                    }
                    RecallAttempt::ProjectionPending => {
                        break Err(Error::ProjectionUnavailable(
                            "shell sessions or indexed projection still pending; inspect status",
                        ));
                    }
                }
            }
        })
        .await
    }
    async fn recall_last(
        &self,
        text: &str,
        project: Option<&str>,
        projects: &[Project],
        page: u32,
        budget: std::time::Duration,
    ) -> Result<RecallAttempt> {
        let mut lease = self.read_lease().await?;
        let (mut tx, seq) = begin_snapshot(lease.connection(), budget).await?;
        let enabled: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM core.producer_versions p JOIN core.runs r ON r.id=p.run_id \
             WHERE p.name='session' AND r.status='running')",
        )
        .fetch_one(&mut *tx)
        .await?;
        if !enabled {
            return Err(Error::ProjectionUnavailable(
                "shell session reconciliation is not configured",
            ));
        }
        let state = sqlx::query(
            "SELECT generation,configuration,position,serving,failure FROM derivation.read_model_position WHERE name='shell-search'",
        )
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::ProjectionUnavailable("indexed retrieval is not configured"))?;
        let pending: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM derivation.session_dirty) OR EXISTS(SELECT 1 FROM derivation.shell_dirty) \
             OR EXISTS(SELECT 1 FROM derivation.session_progress WHERE failure IS NOT NULL)",
        )
        .fetch_one(&mut *tx)
        .await?;
        if pending {
            return Err(Error::ProjectionUnavailable(
                "shell session reconciliation pending or failed; inspect status",
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
        // Inspect at most MAX_SESSIONS sessions and MAX_MEMBERS direct members.
        // Reaching either limit without an answer fails the whole recall rather
        // than reporting an empty result.
        let sessions: Vec<Uuid> = sqlx::query_scalar(&format!(
            "SELECT e.id FROM core.live_events e WHERE e.event_type='activity.session.boundary' \
             ORDER BY e.ts_orig DESC,e.ts_orig_tiebreak DESC,e.id LIMIT {}",
            MAX_SESSIONS + 1
        ))
        .fetch_all(&mut *tx)
        .await?;
        let mut selected: Option<Value> = None;
        let mut rows: Vec<Value> = vec![];
        let mut attribution = None;
        let mut unresolved = vec![];
        let mut inspected_members = 0usize;
        let mut inspected_sessions = 0usize;
        for id in sessions {
            inspected_sessions += 1;
            if inspected_sessions > MAX_SESSIONS {
                return Err(Error::ProjectionUnavailable(
                    "recall candidate inspection exceeds 128 sessions; bounded answer unavailable",
                ));
            }
            // A caught-up projection does not prove the session's parents are
            // still present. Fail rather than drop a member in a JOIN or fall
            // back to an older session.
            let session = sqlx::query(&format!(
                "SELECT cardinality(e.source_event_ids) AS members,e.record_state='present' AND {} AS current_eligible \
                 FROM core.live_events e WHERE e.id=$1",
                crate::query::CURRENT_PARENTS
            ))
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
            if !session.try_get::<bool, _>("current_eligible")? {
                return Err(Error::ProjectionUnavailable(
                    "recall candidate has removed parents; use show or trace",
                ));
            }
            inspected_members += session.try_get::<i32, _>("members")? as usize;
            if inspected_members > MAX_MEMBERS {
                return Err(Error::ProjectionUnavailable(
                    "recall candidate inspection exceeds 8192 direct members; bounded answer unavailable",
                ));
            }
            let members = session_members(&mut tx, id, projects).await?;
            if project.is_none_or(|name| members.attribution.known_projects.contains_key(name)) {
                if members.attribution.total_members > page as usize {
                    return Err(Error::ProjectionUnavailable(
                        "session command assembly exceeds requested page (maximum 100); use query, show or trace",
                    ));
                }
                selected = Some(render_event(&mut tx, id).await?);
                rows = sqlx::query_scalar(&format!(
                    "SELECT {EVENT_VIEW} FROM core.live_events s CROSS JOIN unnest(s.source_event_ids) parent(id) \
                     JOIN core.live_events e ON e.id=parent.id WHERE s.id=$1 ORDER BY e.ts_orig,e.ts_orig_tiebreak,e.id"
                ))
                .bind(id)
                .fetch_all(&mut *tx)
                .await?;
                for row in &mut rows {
                    let member = row["id"]
                        .as_str()
                        .ok_or(Error::Invalid("member identity unavailable"))?;
                    row["project_evidence"] = serde_json::to_value(&members.evidence[member])
                        .expect("project evidence serialization");
                }
                attribution = Some(members.attribution);
                break;
            }
            if !members.attribution.complete {
                unresolved.push(json!({
                    "session": render_event(&mut tx, id).await?,
                    "attribution": members.attribution,
                    "reason": "unknown_direct_member_project_evidence",
                }));
            }
        }
        let source_status = crate::source_coverage::read(&mut tx, seq).await?;
        let complete = !source_status.is_empty()
            && crate::source_coverage::interpreted_complete(&source_status);
        tx.commit().await?;
        lease.finish();
        let read_as = if project.is_some() {
            "Latest shell session with a direct member in the project, with every member and its own cwd evidence. Newer sessions with members of unknown project are listed as unresolved."
        } else {
            "Latest shell session, with every member and its own cwd evidence."
        };
        Ok(RecallAttempt::Ready(json!({
            "notice": sinex_model::DATA_NOTICE,
            "unit": "recall",
            "query": text,
            "session": selected,
            "rows": rows,
            "attribution": attribution,
            "newer_unresolved_candidates": unresolved,
            "total": rows.len(),
            "project": project,
            "complete": complete,
            "truncated": false,
            "source_status": source_status,
            "coverage": "unknown; grouped by shell session rules; focus and attention are not inferred",
            "resolved": {
                "commit_seq": seq,
                "projection_seq": seq,
                "generation": generation,
                "configuration": configuration,
                "candidate_sessions_inspected": inspected_sessions,
                "candidate_members_inspected": inspected_members,
                "zone": "Europe/Warsaw",
                "logical_start": "04:00",
            },
            "read_as": read_as,
        })))
    }
}
#[cfg(test)]
mod tests {
    #[test]
    fn recall_accepts_only_last_with_an_optional_project() {
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
