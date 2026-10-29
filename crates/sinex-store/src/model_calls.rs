//! Model-effect accounting inside the writer's admission transaction. No
//! provider is constructed here; recorded costs survive replay and deletion.
use crate::{Blobs, Error, Result};
use sinex_model::event_types::model_call::{
    ModelPolicy, ModelReceipt, ModelRequest, RecordedEffect, RecordedResponse,
};
use sinex_model::model_calls::{
    Action, Capabilities, Decision, Outcome, OutputHash, Refusal, State,
};
use sinex_model::{Class, Event, EventId, EventPayload, Timestamp, json_identity};
use sqlx::PgConnection;

mod accounting;
mod targets;

/// An internal ref-only lookup for a capture suppressed as a native repeat.
/// Public projected keys may be masked; private bodies never leave the writer.
async fn recorded_event(
    conn: &mut PgConnection,
    source: &str,
    kind: &sinex_model::KindRef,
    key: &str,
    current: bool,
) -> Result<Outcome> {
    let supported = [
        (ModelPolicy::EVENT_TYPE, ModelPolicy::VERSION),
        (ModelRequest::EVENT_TYPE, ModelRequest::VERSION),
        (RecordedResponse::EVENT_TYPE, RecordedResponse::VERSION),
        (ModelReceipt::EVENT_TYPE, ModelReceipt::VERSION),
    ];
    if !supported.contains(&(kind.name.as_str(), kind.version)) {
        return Err(Error::Invalid(
            "model accounting lookup requires a canonical model kind".into(),
        ));
    }
    let version = i64::try_from(kind.version)
        .map_err(|_| Error::Invalid("unsupported model record version".into()))?;
    let event=sqlx::query_scalar!(r#"WITH candidates AS (
      SELECT e.id,e.commit_seq,(NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id) AND
        (EXISTS(SELECT 1 FROM core.live_equivalence l WHERE l.event_id=e.id) OR EXISTS(SELECT 1 FROM core.live_locations l WHERE l.event_id=e.id))) AS live
      FROM core.events e WHERE e.source=$1 AND e.event_type=$2 AND e.kind_version=$3 AND e.equivalence_key=$4)
      SELECT id AS "id!" FROM candidates WHERE NOT $5 OR live
      ORDER BY live DESC,commit_seq,id LIMIT 1"#,source,kind.name,version,key,current).fetch_optional(conn).await?;
    Ok(Outcome::RecordLookup {
        event: event.map(EventId),
    })
}

/// Classification metadata can remain known after a target is archived or
/// tombstoned. Missing metadata is unavailable, never an invented minimum.
pub async fn recording_class(
    conn: &mut PgConnection,
    ids: &[EventId],
) -> Result<Option<(Class, bool)>> {
    let mut class = Class::Open;
    let mut current = true;
    for id in ids {
        let row=sqlx::query!(r#"SELECT class AS "class?",current AS "current!" FROM (
          SELECT e.class,(NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id) AND
            (EXISTS(SELECT 1 FROM core.live_equivalence l WHERE l.event_id=e.id) OR EXISTS(SELECT 1 FROM core.live_locations l WHERE l.event_id=e.id))) AS current
            FROM core.visible_events e WHERE e.id=$1
          UNION ALL SELECT t.class,false FROM core.event_tombstones t WHERE t.event_id=$1
            AND NOT EXISTS(SELECT 1 FROM core.events e WHERE e.id=t.event_id)) statuses"#,id.0).fetch_optional(&mut *conn).await?;
        let Some(row) = row else { return Ok(None) };
        let Some(known) = row.class else {
            return Ok(None);
        };
        class = class.max(
            serde_json::from_value::<Class>(serde_json::Value::String(known))
                .map_err(|_| Error::Invalid("stored model input class is invalid".into()))?,
        );
        current &= row.current;
    }
    if ids.is_empty() {
        return Ok(None);
    }
    Ok(Some((class, current)))
}

/// Validate selected content before a model request becomes admitted evidence.
/// Reuse checks this again; a reading ID alone never establishes equivalence.
pub async fn validate_requests(
    conn: &mut PgConnection,
    blobs: Option<&Blobs>,
    events: &[Event],
) -> Result<()> {
    for event in events
        .iter()
        .filter(|event| event.event_type == ModelRequest::EVENT_TYPE)
    {
        if event.kind_version != ModelRequest::VERSION {
            return Err(Error::Invalid(
                "unsupported model request kind version".into(),
            ));
        }
        let request: ModelRequest = serde_json::from_value(event.payload.clone())
            .map_err(|_| Error::Invalid("invalid model request".into()))?;
        request.validate().map_err(Error::Invalid)?;
        targets::verify(conn, blobs, &request.request).await?;
        if let Some(response) = request.recorded_response {
            let policy = policy(conn, request.policy, true).await?.map_err(|_| {
                Error::Invalid("model effect binding needs its current authorized policy".into())
            })?;
            if !request.request.matches(
                policy
                    .policy
                    .route(&request.request.operation)
                    .map_err(Error::Invalid)?,
            ) {
                return Err(Error::Invalid(
                    "model effect binding differs from its current configured route".into(),
                ));
            }
            let mut read =
                crate::derivations::ExactInputs::new(vec![response], 1, crate::PAGE_BYTES)?;
            let Some(page) = read.next(conn).await? else {
                return Err(Error::Invalid(
                    "model effect receipt is not current or available".into(),
                ));
            };
            let response = &page[0].event;
            if response.event_type != RecordedResponse::EVENT_TYPE
                || response.kind_version != RecordedResponse::VERSION
            {
                return Err(Error::Invalid(
                    "model effect binding is not a recorded response".into(),
                ));
            }
            let requirements = [sinex_model::InputRequirement::new(
                response.kind_ref(),
                ["/request_id", "/request", "/output", "/output_hash"],
            )];
            if !crate::privacy::estimability::unavailable(
                conn,
                std::slice::from_ref(response),
                Some(&requirements),
            )
            .await?
            .is_empty()
            {
                return Err(Error::Invalid(
                    "model effect receipt has unavailable required fields".into(),
                ));
            }
            let effect: RecordedEffect =
                serde_json::from_value(response.payload.clone()).map_err(|_| {
                    Error::Invalid("model effect receipt projection is unavailable".into())
                })?;
            effect.validate().map_err(Error::Invalid)?;
            if effect.request_id != request.request_id {
                return Err(Error::Invalid(
                    "model effect receipt belongs to different input or instructions".into(),
                ));
            }
        }
    }
    Ok(())
}

/// The only caller is admission after claiming the single writer. Internal
/// authorization actions have no encoding on the outside producer protocol.
pub(crate) async fn apply(
    conn: &mut PgConnection,
    blobs: Option<&Blobs>,
    actor: &str,
    action: &Action,
    capabilities: Capabilities,
) -> Result<Outcome> {
    if matches!(
        action,
        Action::AuthorizePolicy { .. }
            | Action::FindRequest { .. }
            | Action::FindRecord { .. }
            | Action::RecordActual { .. }
    ) && actor != "operator"
    {
        return Ok(refused(Refusal::PolicyUnauthorized));
    }
    if matches!(action, Action::RecordActual { .. }) && !capabilities.actual_receipts {
        return Ok(refused(Refusal::ReceiptAccountingDisabled));
    }
    let result = match action {
        Action::FindRecord {
            source,
            kind,
            key,
            current,
        } => recorded_event(conn, source, kind, key, *current).await,
        Action::AuthorizePolicy { policy, at } => authorize(conn, *policy, *at).await,
        Action::FindRequest { policy, response } => find_request(conn, *policy, *response).await,
        Action::Reserve { request, at } => accounting::reserve(conn, blobs, *request, *at).await,
        Action::Reconcile {
            request,
            response,
            at,
        } => accounting::reconcile(conn, *request, *response, *at).await,
        Action::RecordActual { receipt, at } => {
            accounting::record_actual(conn, *receipt, *at).await
        }
        Action::Uncertain {
            request,
            reason,
            at,
        } => uncertain(conn, request, reason, *at).await,
    };
    match result {
        Ok(result) => Ok(result),
        Err(Error::Invalid(detail)) => Ok(Outcome::Refused {
            refusal: Refusal::Invalid { detail },
        }),
        Err(error) => Err(error),
    }
}

async fn find_request(conn: &mut PgConnection, id: EventId, response: EventId) -> Result<Outcome> {
    let policy = match policy(conn, id, false).await? {
        Ok(policy) => policy,
        Err(reason) => return Ok(refused(reason)),
    };
    let Some((response, _)) = load::<RecordedResponse>(conn, response, false).await? else {
        return Err(Error::Invalid(
            "recorded model response is unavailable".into(),
        ));
    };
    response.validate().map_err(Error::Invalid)?;
    if !response.request.matches(
        policy
            .policy
            .route(&response.request.operation)
            .map_err(Error::Invalid)?,
    ) {
        return Err(Error::Invalid(
            "recorded model response differs from the authorized policy route".into(),
        ));
    }
    let current = load::<ModelPolicy>(conn, id, true).await?.is_some();
    let policy_id = id.to_string();
    let definition = serde_json::to_value(&response.request).expect("request serializes");
    let version = i64::try_from(ModelRequest::VERSION).expect("bundled kind version fits bigint");
    let found = sqlx::query_scalar!(
        r#"SELECT e.id FROM core.events e WHERE e.event_type=$1 AND e.kind_version=$2
        AND e.payload->>'request_id'=$3 AND e.payload->>'policy'=$4 AND e.payload->'request'=$5
        ORDER BY e.commit_seq,e.id LIMIT 1"#,
        ModelRequest::EVENT_TYPE,
        version,
        response.request_id,
        policy_id,
        definition
    )
    .fetch_optional(&mut *conn)
    .await?;
    Ok(Outcome::RequestLookup {
        request: found.map(EventId),
        policy_current: current,
    })
}

async fn load<P: EventPayload>(
    conn: &mut PgConnection,
    id: EventId,
    live: bool,
) -> Result<Option<(P, serde_json::Value)>> {
    // These three retained model records are private authority/accounting
    // inputs. Targets are always read separately through the current projection.
    let row = sqlx::query!("SELECT e.payload AS \"payload!\", e.kind_version AS \"kind_version!\", EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id) AS \"archived!\", (EXISTS(SELECT 1 FROM core.live_equivalence l WHERE l.event_id=e.id) OR EXISTS(SELECT 1 FROM core.live_locations l WHERE l.event_id=e.id)) AS \"current!\" FROM core.events e WHERE e.id=$1 AND e.event_type=$2", id.0, P::EVENT_TYPE)
        .fetch_optional(&mut *conn).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    if (live && (row.archived || !row.current))
        || u64::try_from(row.kind_version).ok() != Some(P::VERSION)
    {
        return Ok(None);
    }
    let payload = serde_json::from_value(row.payload.clone()).map_err(|_| {
        Error::Invalid(format!(
            "stored {} is unavailable under current policy",
            P::EVENT_TYPE
        ))
    })?;
    Ok(Some((payload, row.payload)))
}

async fn authorize(conn: &mut PgConnection, id: EventId, at: Timestamp) -> Result<Outcome> {
    let Some((policy, payload)) = load::<ModelPolicy>(conn, id, true).await? else {
        return Ok(Outcome::Refused {
            refusal: Refusal::PolicyUnavailable,
        });
    };
    policy.validate().map_err(Error::Invalid)?;
    let hash = json_identity::hash(&payload);
    if let Some(previous) = sqlx::query_scalar!(
        "SELECT encode(policy_hash,'hex') FROM derivation.model_policies WHERE policy_event=$1",
        id.0
    )
    .fetch_optional(&mut *conn)
    .await?
    {
        if previous.as_deref() != Some(hash.as_str()) {
            return Err(Error::Invalid("authorized policy content changed".into()));
        }
        return Ok(Outcome::Authorized {
            policy: id,
            reused: true,
        });
    }
    sqlx::query!("INSERT INTO derivation.model_policies(policy_event,policy_hash,authorized_at) VALUES($1,decode($2,'hex'),$3)", id.0, hash, at.0).execute(&mut *conn).await?;
    Ok(Outcome::Authorized {
        policy: id,
        reused: false,
    })
}

async fn policy(
    conn: &mut PgConnection,
    id: EventId,
    live: bool,
) -> Result<std::result::Result<ModelPolicy, Refusal>> {
    let Some((policy, payload)) = load::<ModelPolicy>(conn, id, live).await? else {
        return Ok(Err(Refusal::PolicyUnavailable));
    };
    let mark = sqlx::query_scalar!(
        "SELECT encode(policy_hash,'hex') FROM derivation.model_policies WHERE policy_event=$1",
        id.0
    )
    .fetch_optional(&mut *conn)
    .await?;
    if mark.flatten().as_deref() != Some(json_identity::hash(&payload).as_str()) {
        return Ok(Err(Refusal::PolicyUnauthorized));
    }
    policy.validate().map_err(Error::Invalid)?;
    Ok(Ok(policy))
}

pub async fn decision(conn: &mut PgConnection, request: &str) -> Result<Option<Decision>> {
    sinex_model::model_calls::digest(request).map_err(Error::Invalid)?;
    let row=sqlx::query!(r#"SELECT policy_event,encode(bucket_hash,'hex') AS "bucket!",currency,unit,state,
      reserved_units::text AS reserved,actual_units::text AS actual,response_event,encode(output_hash,'hex') AS output_hash,
      overrun AS "overrun!"
      FROM derivation.model_calls c WHERE request_hash=decode($1,'hex')"#,request).fetch_optional(&mut *conn).await?;
    let Some(row) = row else { return Ok(None) };
    let quantity = |value: &str| {
        value
            .parse::<u64>()
            .map_err(|_| Error::Invalid("stored model accounting quantity is invalid".into()))
    };
    Ok(Some(Decision {
        request: request.into(),
        policy: EventId(row.policy_event),
        bucket: row.bucket,
        currency: row.currency,
        unit: row.unit,
        state: serde_json::from_value(serde_json::Value::String(row.state))
            .map_err(|_| Error::Invalid("stored model accounting state is invalid".into()))?,
        reused: false,
        reserved_units: row.reserved.as_deref().map(quantity).transpose()?,
        actual_units: row.actual.as_deref().map(quantity).transpose()?,
        response: row.response_event.map(EventId),
        output_hash: row.output_hash.map(OutputHash),
        overrun: row.overrun,
    }))
}

fn accepted(mut decision: Decision, reused: bool) -> Outcome {
    decision.reused = reused;
    Outcome::Accepted { decision }
}
fn refused(refusal: Refusal) -> Outcome {
    Outcome::Refused { refusal }
}

async fn uncertain(
    conn: &mut PgConnection,
    request: &str,
    reason: &str,
    at: Timestamp,
) -> Result<Outcome> {
    if reason.trim().is_empty() {
        return Err(Error::Invalid(
            "uncertain model effect needs its observed reason".into(),
        ));
    }
    let Some(previous) = decision(conn, request).await? else {
        return Err(Error::Invalid(
            "uncertain model effect has no durable reservation".into(),
        ));
    };
    if previous.state == State::Recorded {
        return Ok(accepted(previous, true));
    }
    let reused = previous.state == State::Uncertain;
    sqlx::query!("UPDATE derivation.model_calls SET state='uncertain',reason=$2,updated_at=$3 WHERE request_hash=decode($1,'hex')",request,reason,at.0).execute(&mut *conn).await?;
    Ok(accepted(
        decision(conn, request)
            .await?
            .expect("uncertain reservation"),
        reused,
    ))
}

#[cfg(test)]
mod tests;
