//! Exclude or undo the exclusion of one Atuin occurrence (its equivalence key).
//! An exclusion hides the occurrence's events and keeps later readings of the
//! same row from being admitted. Each action is an operation whose request,
//! outcome and effects are stored records, so a repeated action ID returns its
//! first outcome.
use crate::{COUNTER, Error, Result, WriterPool};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_model::exclusion::{Action, Occurrence, Outcome, Request, State};
use sqlx::{Connection, Row, postgres::PgRow};
use uuid::Uuid;

pub(crate) const EXCLUDED: &str = "SELECT EXISTS(SELECT 1 FROM core.deleted_occurrences
    WHERE store=0 AND source='terminal.atuin-history' AND event_type='command.executed'
      AND equivalence_key=$1)";
const REQUEST: &str = "sinex.delete.request.v1";
const OUTCOME: &str = "sinex.delete.outcome.v1";
const EFFECTS: &str = "sinex.delete.effects.v1";
/// The stored request record. Field names are stored keys.
#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct StoredRequest {
    format: String,
    principal: String,
    method: String,
    target: Uuid,
    expected_key: Option<String>,
}
/// The stored effects record. Field names are stored keys.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Effects {
    format: String,
    operation_id: Uuid,
    archived: Vec<Uuid>,
    exclusion_added: Option<Uuid>,
    exclusion_removed: Option<Uuid>,
}
/// Where the target event's stored row is.
struct Coordinates {
    material: Option<Uuid>,
    anchor: Option<i64>,
}
impl WriterPool {
    pub(crate) async fn exclude(&mut self, action: Action, request: Request) -> Result<Value> {
        if self.replay_rules.is_none() {
            return Err(Error::ReplayRefused(
                "explicit operator profile is not enabled",
            ));
        }
        let target = request.event_id().map_err(Error::Invalid)?;
        let stored_request = StoredRequest {
            format: REQUEST.into(),
            principal: crate::replay::PRINCIPAL.into(),
            method: action.method().into(),
            target,
            expected_key: request.expected_key.clone(),
        };
        if let Some(outcome) = self
            .repeated_exclusion(action, &request, target, &stored_request)
            .await?
        {
            return Ok(outcome);
        }
        let id = Uuid::now_v7();
        let mut outcome = Outcome {
            operation_id: id,
            action_id: request.action_id,
            method: action.method().into(),
            state: State::Completed,
            target,
            occurrence: None,
            exclusion_operation: None,
            removed_exclusion_operation: None,
            archived: vec![],
            reason: None,
            raw_ref: None,
        };
        let mut effects = Effects {
            format: EFFECTS.into(),
            operation_id: id,
            archived: vec![],
            exclusion_added: None,
            exclusion_removed: None,
        };
        // Every stored event ID is v7. Other well-formed UUIDs are absent, and
        // Timescale's UUID time partition rejects them before a lookup can run.
        let row = if target.get_version_num() == 7 {
            sqlx::query(
                "SELECT source,event_type,equivalence_key,source_material_id,anchor_byte,
                   source_event_ids
                 FROM core.events WHERE id=$1",
            )
            .bind(target)
            .fetch_optional(&mut self.connection)
            .await?
        } else {
            None
        };
        let Some(row) = row else {
            outcome.state = State::NotFound;
            outcome.reason = Some("raw interpretation reference not found".into());
            let refs = self
                .store_exclusion_records(&stored_request, &effects, &outcome)
                .await?;
            let mut tx = self.connection.begin().await?;
            Self::record_exclusion(&mut tx, &outcome, refs).await?;
            tx.commit().await?;
            return Ok(json!(outcome));
        };
        let current = self
            .decide_exclusion(action, &row, &stored_request, &mut outcome, &mut effects)
            .await?;
        let coordinates = Coordinates {
            material: row.try_get("source_material_id")?,
            anchor: row.try_get("anchor_byte")?,
        };
        if effects.exclusion_added.is_some()
            && (coordinates.material.is_none() || coordinates.anchor.is_none())
        {
            return Err(Error::ReplayRefused(
                "raw occurrence has no stored row coordinates",
            ));
        }
        outcome.archived = effects.archived.clone();
        outcome.removed_exclusion_operation = effects.exclusion_removed;
        self.apply_exclusion(&stored_request, &outcome, &effects, coordinates, current)
            .await?;
        Ok(json!(outcome))
    }

    /// For an action ID already recorded, check that the request matches the
    /// stored one and return the stored first outcome, even after a later undo
    /// or replay.
    async fn repeated_exclusion(
        &mut self,
        action: Action,
        request: &Request,
        target: Uuid,
        stored_request: &StoredRequest,
    ) -> Result<Option<Value>> {
        let Some(old) = sqlx::query(
            "SELECT id,request_format,request_ref,outcome_format,outcome_ref,progress
             FROM core.operations_log WHERE action_principal=$1 AND action_id=$2",
        )
        .bind(crate::replay::PRINCIPAL)
        .bind(request.action_id)
        .fetch_optional(&mut self.connection)
        .await?
        else {
            return Ok(None);
        };
        if old.try_get::<String, _>("request_format")? != REQUEST {
            return Err(Error::ReplayRefused(
                "action ID is already used by a different method or request format",
            ));
        }
        let prior: StoredRequest = serde_json::from_value(
            self.reader
                .action_record(old.try_get("request_ref")?, REQUEST)
                .await?,
        )
        .map_err(|_| Error::ReplayRefused("unsupported stored exclusion request"))?;
        if &prior != stored_request {
            return Err(Error::ReplayRefused(
                "action ID is already used by a different request",
            ));
        }
        if old
            .try_get::<Option<String>, _>("outcome_format")?
            .as_deref()
            != Some(OUTCOME)
        {
            return Err(Error::ReplayRefused(
                "committed exclusion outcome unavailable",
            ));
        }
        let value = self
            .reader
            .action_record(old.try_get("outcome_ref")?, OUTCOME)
            .await?;
        let outcome: Outcome = serde_json::from_value(value["outcome"].clone())
            .map_err(|_| Error::ReplayRefused("stored exclusion outcome invalid"))?;
        let id: Uuid = old.try_get("id")?;
        if outcome.operation_id != id
            || outcome.action_id != request.action_id
            || outcome.target != target
            || outcome.method != action.method()
            || json!(outcome) != old.try_get::<Value, _>("progress")?
        {
            return Err(Error::ReplayRefused(
                "stored exclusion outcome differs from its operation",
            ));
        }
        let effects: Vec<Uuid> = sqlx::query_scalar(
            "SELECT effects_ref FROM core.operation_effects
             WHERE operation_id=$1 AND step_no=0 AND effect_format='sinex.delete.effects.v1'",
        )
        .bind(id)
        .fetch_all(&mut self.connection)
        .await?;
        if effects.len() != 1 {
            return Err(Error::ReplayRefused(
                "committed exclusion effects unavailable",
            ));
        }
        let effect: Effects =
            serde_json::from_value(self.reader.action_record(effects[0], EFFECTS).await?)
                .map_err(|_| Error::ReplayRefused("stored exclusion effects invalid"))?;
        let added = if outcome.state == State::Completed && action == Action::Exclude {
            Some(id)
        } else {
            None
        };
        if effect.operation_id != id
            || effect.archived != outcome.archived
            || effect.exclusion_added != added
            || effect.exclusion_removed != outcome.removed_exclusion_operation
        {
            return Err(Error::ReplayRefused(
                "stored exclusion effects differ from outcome",
            ));
        }
        Ok(Some(json!(outcome)))
    }

    /// Decide the outcome for the target event `row` and fill in the effects.
    /// Returns the live event an exclusion hides, if any.
    async fn decide_exclusion(
        &mut self,
        action: Action,
        row: &PgRow,
        request: &StoredRequest,
        outcome: &mut Outcome,
        effects: &mut Effects,
    ) -> Result<Option<Uuid>> {
        let source: String = row.try_get("source")?;
        let kind: String = row.try_get("event_type")?;
        if source != crate::ATUIN_SOURCE || kind != "command.executed" {
            outcome.state = State::Refused;
            outcome.reason = Some(
                "delete requires an exact raw Atuin occurrence; canonical references are not followed automatically"
                    .into(),
            );
            if kind == "command.canonical" {
                let parents: Option<Vec<Uuid>> = row.try_get("source_event_ids")?;
                if let Some(parents) = parents
                    && parents.len() == 1
                {
                    outcome.raw_ref = Some(format!("sinex:ev/{}", parents[0]));
                }
            }
            return Ok(None);
        }
        let key: String = row.try_get("equivalence_key")?;
        outcome.occurrence = Some(Occurrence {
            source,
            event_type: kind,
            equivalence_key: key.clone(),
        });
        if key.len() > 2048
            || request
                .expected_key
                .as_ref()
                .is_some_and(|expected| expected != &key)
        {
            outcome.state = State::Refused;
            outcome.reason = Some(
                "stable occurrence key differs from expected_key or exceeds the supported key budget"
                    .into(),
            );
            return Ok(None);
        }
        let existing: Option<(String, Uuid)> = sqlx::query_as(
            "SELECT kind,operation_id FROM core.deleted_occurrences
             WHERE store=0 AND source=$1 AND event_type='command.executed' AND equivalence_key=$2",
        )
        .bind(crate::ATUIN_SOURCE)
        .bind(&key)
        .fetch_optional(&mut self.connection)
        .await?;
        match (existing, action) {
            (Some((kind, _)), Action::Undo) if kind != "exclude" => {
                outcome.state = State::Refused;
                outcome.reason =
                    Some("undo removes only exclude; excise and purge are never undoable".into());
                Ok(None)
            }
            (Some((_, operation)), Action::Undo) => {
                effects.exclusion_removed = Some(operation);
                Ok(None)
            }
            (None, Action::Undo) => {
                outcome.state = State::Noop;
                Ok(None)
            }
            (Some((_, operation)), Action::Exclude) => {
                outcome.state = State::Noop;
                outcome.exclusion_operation = Some(operation);
                Ok(None)
            }
            (None, Action::Exclude) => {
                // An old event reference resolves to the current live event for the key.
                let current: Option<Uuid> = sqlx::query_scalar(
                    "SELECT id FROM core.events e
                     WHERE source=$1 AND event_type='command.executed' AND equivalence_key=$2
                       AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id)",
                )
                .bind(crate::ATUIN_SOURCE)
                .bind(&key)
                .fetch_optional(&mut self.connection)
                .await?;
                if let Some(raw) = current {
                    effects.archived.push(raw);
                    let children: Vec<Uuid> = sqlx::query_scalar(
                        "SELECT id FROM core.live_events
                         WHERE event_type='command.canonical' AND $1=ANY(source_event_ids)",
                    )
                    .bind(raw)
                    .fetch_all(&mut self.connection)
                    .await?;
                    effects.archived.extend(children);
                }
                effects.exclusion_added = Some(outcome.operation_id);
                outcome.exclusion_operation = Some(outcome.operation_id);
                Ok(current)
            }
        }
    }

    /// Commit the operation, the exclusion row change and the archiving of
    /// hidden events in one transaction.
    async fn apply_exclusion(
        &mut self,
        request: &StoredRequest,
        outcome: &Outcome,
        effects: &Effects,
        coordinates: Coordinates,
        current: Option<Uuid>,
    ) -> Result<()> {
        let refs = self
            .store_exclusion_records(request, effects, outcome)
            .await?;
        let mut tx = self.connection.begin().await?;
        Self::record_exclusion(&mut tx, outcome, refs).await?;
        if let Some(occurrence) = &outcome.occurrence {
            if effects.exclusion_added.is_some() {
                sqlx::query(
                    "INSERT INTO core.deleted_occurrences(store,source,event_type,equivalence_key,
                       material_id,anchor,kind,operation_id)
                     VALUES(0,$1,$2,$3,$4,$5,'exclude',$6)",
                )
                .bind(&occurrence.source)
                .bind(&occurrence.event_type)
                .bind(&occurrence.equivalence_key)
                .bind(coordinates.material)
                .bind(coordinates.anchor)
                .bind(outcome.operation_id)
                .execute(&mut *tx)
                .await?;
            } else if effects.exclusion_removed.is_some() {
                let removed = sqlx::query(
                    "DELETE FROM core.deleted_occurrences
                     WHERE store=0 AND source=$1 AND event_type=$2 AND equivalence_key=$3
                       AND kind='exclude' AND operation_id=$4",
                )
                .bind(&occurrence.source)
                .bind(&occurrence.event_type)
                .bind(&occurrence.equivalence_key)
                .bind(effects.exclusion_removed)
                .execute(&mut *tx)
                .await?;
                if removed.rows_affected() != 1 {
                    return Err(Error::ReplayRefused("exclusion precondition changed"));
                }
            }
        }
        for event in &effects.archived {
            let seq: i64 = sqlx::query_scalar(COUNTER).fetch_one(&mut *tx).await?;
            crate::session::dirty(&mut tx, *event).await?;
            sqlx::query(
                "INSERT INTO audit.archived(id,archived_seq,reason,operation_id)
                 VALUES($1,$2,'delete',$3)",
            )
            .bind(event)
            .bind(seq)
            .bind(outcome.operation_id)
            .execute(&mut *tx)
            .await?;
            let logged = sqlx::query(
                "INSERT INTO core.commit_log(commit_seq,event_id,change,store,event_type_id)
                 SELECT $1,e.id,1,0,t.id FROM core.events e
                 JOIN core.event_types t ON t.name=e.event_type WHERE e.id=$2",
            )
            .bind(seq)
            .bind(event)
            .execute(&mut *tx)
            .await?;
            if logged.rows_affected() != 1 {
                return Err(Error::Schema(
                    "archive commit log target or registered event type unavailable".into(),
                ));
            }
            sqlx::query("DELETE FROM core.live_coordinates WHERE store=0 AND event_id=$1")
                .bind(event)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM core.live_equivalence WHERE store=0 AND event_id=$1")
                .bind(event)
                .execute(&mut *tx)
                .await?;
        }
        if let Some(current) = current {
            crate::shell::dirty(&mut tx, current).await?;
        }
        let pause = json!({"outcome": outcome, "effects": effects});
        crate::TestPause::reach(&mut self.pause, "delete-staged", pause.clone()).await?;
        tx.commit().await?;
        // The action is committed; a failed test pause must not turn it into an error.
        let _ = crate::TestPause::reach(&mut self.pause, "delete-committed", pause).await;
        Ok(())
    }

    /// Store the request, effects and outcome records; returns their references.
    async fn store_exclusion_records(
        &mut self,
        request: &StoredRequest,
        effects: &Effects,
        outcome: &Outcome,
    ) -> Result<RecordRefs> {
        Ok(RecordRefs {
            request: self.store_record(&json!(request)).await?,
            effects: self.store_record(&json!(effects)).await?,
            outcome: self
                .store_record(&json!({"format": OUTCOME, "outcome": outcome}))
                .await?,
        })
    }

    async fn record_exclusion(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        outcome: &Outcome,
        refs: RecordRefs,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO core.operations_log(id,operation_type,actor,action_principal,action_id,
               request_format,request_ref,scope,admission_state,outcome_format,outcome_ref,progress)
             VALUES($1,$2,$3,$4,$5,'sinex.delete.request.v1',$6,'{}'::jsonb,$7,
               'sinex.delete.outcome.v1',$8,$9)",
        )
        .bind(outcome.operation_id)
        .bind(if outcome.method == "delete.run" {
            "delete"
        } else {
            "delete.undo"
        })
        .bind(json!({
            "kind": "operator",
            "principal": crate::replay::PRINCIPAL,
            "profile": "explicit_operator_token",
        }))
        .bind(crate::replay::PRINCIPAL)
        .bind(outcome.action_id)
        .bind(refs.request)
        .bind(
            if matches!(outcome.state, State::Refused | State::NotFound) {
                "refused"
            } else {
                "admitted"
            },
        )
        .bind(refs.outcome)
        .bind(json!(outcome))
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "INSERT INTO core.operation_effects(operation_id,step_no,effect_format,effects_ref)
             VALUES($1,0,'sinex.delete.effects.v1',$2)",
        )
        .bind(outcome.operation_id)
        .bind(refs.effects)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

struct RecordRefs {
    request: Uuid,
    effects: Uuid,
    outcome: Uuid,
}

#[cfg(test)]
#[path = "exclusion_tests.rs"]
mod tests;
