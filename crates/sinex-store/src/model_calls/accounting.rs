//! One private relation owns pending holds and actual charges. An unbound
//! charge has no request identity; verified binding replaces its matching hold.
use super::*;
use sinex_model::event_types::model_call::ModelReceipt;
use sinex_model::model_calls::{ActualDecision, BudgetBasis};
use uuid::Uuid;

struct Entry {
    id: Uuid,
    request: Option<String>,
    policy: EventId,
    definition: String,
    name: String,
    version: String,
    operation: String,
    basis: BudgetBasis,
    bucket: String,
    reserved: Option<u64>,
    actual: Option<u64>,
    state: State,
    output: Option<OutputHash>,
    receipt: Option<String>,
    evidence: Option<EventId>,
    overrun: bool,
}
fn quantity(value: &str) -> Result<u64> {
    value
        .parse()
        .map_err(|_| Error::Invalid("stored model accounting quantity is invalid".into()))
}
pub(super) fn timestamp(value: &str) -> Result<Timestamp> {
    value
        .parse::<i128>()
        .ok()
        .and_then(|value| Timestamp::from_unix_ns(value).ok())
        .ok_or_else(|| Error::Invalid("stored model accounting period is invalid".into()))
}
async fn find(conn: &mut PgConnection, hash: &str, request: bool) -> Result<Option<Entry>> {
    sinex_model::model_calls::digest(hash).map_err(Error::Invalid)?;
    let row=sqlx::query!(r#"SELECT entry_id,encode(request_hash,'hex') AS request,policy_event,
      encode(policy_definition_hash,'hex') AS "definition!",policy_name,policy_version,operation,account_scope,currency,unit,
      period_start_ns::text AS "period_start!",period_end_ns::text AS "period_end!",encode(bucket_hash,'hex') AS "bucket!",reserved_units::text AS reserved,actual_units::text AS actual,state,
      encode(output_hash,'hex') AS output,encode(receipt_hash,'hex') AS receipt,receipt_event,overrun
      FROM derivation.model_calls WHERE CASE WHEN $2 THEN request_hash=decode($1,'hex') ELSE receipt_hash=decode($1,'hex') END"#,hash,request).fetch_optional(conn).await?;
    let Some(row) = row else { return Ok(None) };
    Ok(Some(Entry {
        id: row.entry_id,
        request: row.request,
        policy: EventId(row.policy_event),
        definition: row.definition,
        name: row.policy_name,
        version: row.policy_version,
        operation: row.operation,
        basis: BudgetBasis {
            scope: row.account_scope,
            currency: row.currency,
            unit: row.unit,
            period_start: timestamp(&row.period_start)?,
            period_end: timestamp(&row.period_end)?,
        },
        bucket: row.bucket,
        reserved: row.reserved.as_deref().map(quantity).transpose()?,
        actual: row.actual.as_deref().map(quantity).transpose()?,
        state: serde_json::from_value(serde_json::Value::String(row.state))
            .map_err(|_| Error::Invalid("stored model accounting state is invalid".into()))?,
        output: row.output.map(OutputHash),
        receipt: row.receipt,
        evidence: row.receipt_event.map(EventId),
        overrun: row.overrun,
    }))
}
fn definition(policy: &ModelPolicy) -> String {
    json_identity::hash(&serde_json::to_value(&policy.policy).expect("model policy serializes"))
}
fn matches(entry: &Entry, policy: &ModelPolicy, operation: &str) -> bool {
    entry.definition == definition(policy)
        && entry.name == policy.policy.name
        && entry.version == policy.policy.version
        && entry.operation == operation
        && entry.basis == BudgetBasis::from(&policy.policy.budget)
        && entry.bucket == policy.policy.budget.bucket()
}
async fn pending(
    conn: &mut PgConnection,
    id: EventId,
    request: &ModelRequest,
    policy: &ModelPolicy,
    at: Timestamp,
) -> Result<Uuid> {
    let entry = Uuid::now_v7();
    let budget = &policy.policy.budget;
    let estimate = policy
        .policy
        .route(&request.request.operation)
        .map_err(Error::Invalid)?
        .prices
        .estimate(&request.request.usage().map_err(Error::Invalid)?)
        .map_err(Error::Invalid)?
        .to_string();
    let limit = budget.limit_units.to_string();
    let hash = definition(policy);
    let bucket = budget.bucket();
    sqlx::query!(r#"INSERT INTO derivation.model_calls(entry_id,request_hash,policy_event,policy_definition_hash,policy_name,policy_version,operation,account_scope,bucket_hash,currency,unit,period_start_ns,period_end_ns,limit_units,reserved_units,state,created_at,updated_at)
      VALUES($1,decode($2,'hex'),$3,decode($4,'hex'),$5,$6,$7,$8,decode($9,'hex'),$10,$11,$12::text::numeric,$13::text::numeric,$14::text::numeric,$15::text::numeric,'reserved',$16,$16)"#,
      entry,request.request_id,id.0,hash,policy.policy.name,policy.policy.version,request.request.operation,budget.scope,bucket,budget.currency,budget.unit,budget.period_start.unix_ns().to_string(),budget.period_end.unix_ns().to_string(),limit,estimate,at.0).execute(conn).await?;
    Ok(entry)
}
/// An incurred response may have no representable quote. Its actual amount,
/// source and frozen basis are still facts; NULL never invents a zero hold.
async fn recorded(
    conn: &mut PgConnection,
    request: &ModelRequest,
    policy: &ModelPolicy,
    response_event: EventId,
    response: &RecordedResponse,
    at: Timestamp,
) -> Result<Uuid> {
    let entry = Uuid::now_v7();
    let budget = &policy.policy.budget;
    let quote = request.request.usage().and_then(|usage| {
        policy
            .policy
            .route(&request.request.operation)?
            .prices
            .estimate(&usage)
    });
    let estimate = quote.as_ref().ok().map(ToString::to_string);
    let reason = quote.err();
    let hash = definition(policy);
    let bucket = budget.bucket();
    let limit = budget.limit_units.to_string();
    let actual = response.actual.units.to_string();
    let receipt = response.receipt_identity();
    sqlx::query!(r#"INSERT INTO derivation.model_calls(entry_id,request_hash,policy_event,policy_definition_hash,policy_name,policy_version,operation,account_scope,bucket_hash,currency,unit,period_start_ns,period_end_ns,limit_units,reserved_units,actual_units,state,response_event,output_hash,receipt_hash,reason,created_at,updated_at)
      VALUES($1,decode($2,'hex'),$3,decode($4,'hex'),$5,$6,$7,$8,decode($9,'hex'),$10,$11,$12::text::numeric,$13::text::numeric,$14::text::numeric,$15::text::numeric,$16::text::numeric,'recorded',$17,decode($18,'hex'),decode($19,'hex'),$20,$21,$21)"#,
      entry,request.request_id,request.policy.0,hash,policy.policy.name,policy.policy.version,request.request.operation,budget.scope,bucket,budget.currency,budget.unit,budget.period_start.unix_ns().to_string(),budget.period_end.unix_ns().to_string(),limit,estimate,actual,response_event.0,response.output_hash.0,receipt,reason,at.0).execute(conn).await?;
    Ok(entry)
}

async fn mark_overrun(conn: &mut PgConnection, id: Uuid) -> Result<()> {
    sqlx::query!(r#"UPDATE derivation.model_calls c SET overrun=overrun OR coalesce(actual_units>reserved_units,false) OR
      (SELECT coalesce(sum(coalesce(other.actual_units,other.reserved_units)),0) FROM derivation.model_calls other WHERE other.bucket_hash=c.bucket_hash)>limit_units WHERE entry_id=$1"#,id).execute(conn).await?;
    Ok(())
}
fn actual_decision(entry: Entry, reused: bool) -> Result<Outcome> {
    Ok(Outcome::ActualAccepted {
        decision: ActualDecision {
            receipt: entry
                .receipt
                .ok_or_else(|| Error::Invalid("actual receipt identity is missing".into()))?,
            evidence: entry
                .evidence
                .ok_or_else(|| Error::Invalid("actual receipt evidence is missing".into()))?,
            policy: entry.policy,
            bucket: entry.bucket,
            state: entry.state,
            bound_request: entry.request,
            reused,
            overrun: entry.overrun,
        },
    })
}

pub(super) async fn reserve(
    conn: &mut PgConnection,
    blobs: Option<&Blobs>,
    id: EventId,
    at: Timestamp,
) -> Result<Outcome> {
    let Some((request, _)) = load::<ModelRequest>(conn, id, true).await? else {
        return Err(Error::Invalid(
            "model request is missing, archived or unavailable".into(),
        ));
    };
    request.validate().map_err(Error::Invalid)?;
    targets::verify(conn, blobs, &request.request).await?;
    let policy = match policy(conn, request.policy, true).await? {
        Ok(policy) => policy,
        Err(reason) => return Ok(refused(reason)),
    };
    let route = policy
        .policy
        .route(&request.request.operation)
        .map_err(Error::Invalid)?;
    if !request.request.matches(route) {
        return Err(Error::Invalid(
            "model request differs from its configured route or instructions".into(),
        ));
    }
    if let Some(previous) = decision(conn, &request.request_id).await? {
        return Ok(accepted(previous, true));
    }
    let budget = &policy.policy.budget;
    if at < budget.period_start || at >= budget.period_end {
        return Err(Error::Invalid(
            "configured model accounting period does not contain this request".into(),
        ));
    }
    let estimate = route
        .prices
        .estimate(&request.request.usage().map_err(Error::Invalid)?)
        .map_err(Error::Invalid)?;
    let bucket = budget.bucket();
    let amount = estimate.to_string();
    let limit = budget.limit_units.to_string();
    let account=sqlx::query!(r#"SELECT (coalesce(sum(coalesce(actual_units,reserved_units)),0)+$2::text::numeric<=$3::text::numeric) AS "fits!",
      coalesce(sum(coalesce(actual_units,reserved_units)),0)::text AS "committed!" FROM derivation.model_calls WHERE bucket_hash=decode($1,'hex')"#,bucket,amount,limit).fetch_one(&mut *conn).await?;
    if !account.fits {
        return Ok(refused(Refusal::InsufficientConfiguredBudget {
            policy: request.policy,
            currency: budget.currency.clone(),
            unit: budget.unit.clone(),
            required_units: estimate,
            limit_units: budget.limit_units,
            committed_units: account.committed,
        }));
    }
    pending(conn, request.policy, &request, &policy, at).await?;
    Ok(accepted(
        decision(conn, &request.request_id)
            .await?
            .expect("inserted reservation"),
        false,
    ))
}

pub(super) async fn record_actual(
    conn: &mut PgConnection,
    id: EventId,
    at: Timestamp,
) -> Result<Outcome> {
    let Some((record, _)) = load::<ModelReceipt>(conn, id, false).await? else {
        return Err(Error::Invalid("actual model receipt is unavailable".into()));
    };
    record.validate().map_err(Error::Invalid)?;
    let policy = match policy(conn, record.policy, false).await? {
        Ok(policy) => policy,
        Err(reason) => return Ok(refused(reason)),
    };
    let receipt = &record.receipt;
    let hash = receipt.identity();
    if receipt.matches(&policy.policy).is_err() {
        return Ok(refused(Refusal::ContradictoryReceipt { receipt: hash }));
    }
    if let Some(previous) = find(conn, &hash, false).await? {
        if previous.actual != Some(receipt.actual.units)
            || !matches(&previous, &policy, &receipt.operation)
        {
            return Ok(refused(Refusal::ContradictoryReceipt { receipt: hash }));
        }
        sqlx::query!("UPDATE derivation.model_calls SET receipt_event=coalesce(receipt_event,$2),updated_at=$3 WHERE entry_id=$1",previous.id,id.0,at.0).execute(&mut *conn).await?;
        return actual_decision(
            find(conn, &hash, false)
                .await?
                .expect("retained actual receipt"),
            true,
        );
    }
    let entry = Uuid::now_v7();
    let basis = &receipt.basis;
    let bucket = basis.bucket();
    let definition = definition(&policy);
    let actual = receipt.actual.units.to_string();
    let limit = policy.policy.budget.limit_units.to_string();
    sqlx::query!(r#"INSERT INTO derivation.model_calls(entry_id,policy_event,policy_definition_hash,policy_name,policy_version,operation,account_scope,bucket_hash,currency,unit,period_start_ns,period_end_ns,limit_units,actual_units,state,receipt_hash,receipt_event,created_at,updated_at)
      VALUES($1,$2,decode($3,'hex'),$4,$5,$6,$7,decode($8,'hex'),$9,$10,$11::text::numeric,$12::text::numeric,$13::text::numeric,$14::text::numeric,'actual_unbound',decode($15,'hex'),$16,$17,$17)"#,
      entry,record.policy.0,definition,receipt.policy_name,receipt.policy_version,receipt.operation,basis.scope,bucket,basis.currency,basis.unit,basis.period_start.unix_ns().to_string(),basis.period_end.unix_ns().to_string(),limit,actual,hash,id.0,at.0).execute(&mut *conn).await?;
    mark_overrun(conn, entry).await?;
    actual_decision(
        find(conn, &hash, false)
            .await?
            .expect("inserted actual receipt"),
        false,
    )
}

pub(super) async fn reconcile(
    conn: &mut PgConnection,
    request_event: EventId,
    response_event: EventId,
    at: Timestamp,
) -> Result<Outcome> {
    let Some((request, _)) = load::<ModelRequest>(conn, request_event, false).await? else {
        return Err(Error::Invalid(
            "recorded model request is unavailable".into(),
        ));
    };
    let Some((response, _)) = load::<RecordedResponse>(conn, response_event, false).await? else {
        return Err(Error::Invalid(
            "recorded model response is unavailable".into(),
        ));
    };
    request.validate().map_err(Error::Invalid)?;
    response.validate().map_err(Error::Invalid)?;
    let contradiction = || {
        refused(Refusal::ContradictoryResponse {
            request: request.request_id.clone(),
        })
    };
    if request.request_id != response.request_id {
        return Ok(contradiction());
    }
    let receipt_hash = response.receipt_identity();
    let pending_entry = find(conn, &request.request_id, true).await?;
    if let Some(previous) = &pending_entry {
        if previous.basis.currency != response.actual.currency
            || previous.basis.unit != response.actual.unit
        {
            return Ok(contradiction());
        }
        if previous.state == State::Recorded {
            if previous.actual != Some(response.actual.units)
                || previous.output.as_ref() != Some(&response.output_hash)
                || previous.receipt != receipt_hash
            {
                return Ok(contradiction());
            }
            return Ok(accepted(
                decision(conn, &request.request_id)
                    .await?
                    .expect("retained effect"),
                true,
            ));
        }
    }
    let policy = match policy(conn, request.policy, false).await? {
        Ok(policy) => policy,
        Err(reason) => return Ok(refused(reason)),
    };
    let route = policy
        .policy
        .route(&request.request.operation)
        .map_err(Error::Invalid)?;
    if !request.request.matches(route)
        || response.actual.currency != policy.policy.budget.currency
        || response.actual.unit != policy.policy.budget.unit
    {
        return Ok(contradiction());
    }
    if pending_entry
        .as_ref()
        .is_some_and(|entry| !matches(entry, &policy, &request.request.operation))
    {
        return Ok(contradiction());
    }
    let quote = request
        .request
        .usage()
        .and_then(|usage| route.prices.estimate(&usage));
    let quote_reason = quote.as_ref().err().cloned();
    let actual_entry = match &receipt_hash {
        Some(hash) => find(conn, hash, false).await?,
        None => None,
    };
    let (entry, reused, reservation) = if let Some(actual) = actual_entry {
        if actual
            .request
            .as_ref()
            .is_some_and(|hash| hash != &request.request_id)
            || actual.actual != Some(response.actual.units)
            || !matches(&actual, &policy, &request.request.operation)
        {
            return Ok(refused(Refusal::ContradictoryReceipt {
                receipt: receipt_hash.expect("looked up receipt"),
            }));
        }
        if actual.state != State::ActualUnbound {
            return Ok(contradiction());
        }
        // Both entries are private metadata in this writer transaction. Preserve
        // the source request facts; eliminate its hold before attaching identity.
        let reserved = if let Some(pending) = pending_entry {
            sqlx::query!("DELETE FROM derivation.model_calls WHERE entry_id=$1 AND state IN ('reserved','uncertain')",pending.id).execute(&mut *conn).await?;
            pending.reserved
        } else {
            quote.ok()
        };
        (actual.id, true, reserved.map(|value| value.to_string()))
    } else if let Some(pending) = pending_entry {
        (pending.id, false, None)
    } else {
        let entry = recorded(conn, &request, &policy, response_event, &response, at).await?;
        mark_overrun(conn, entry).await?;
        return Ok(accepted(
            decision(conn, &request.request_id)
                .await?
                .expect("recorded incurred response"),
            false,
        ));
    };
    let units = response.actual.units.to_string();
    sqlx::query!("UPDATE derivation.model_calls SET actual_units=$2::text::numeric,state='recorded',response_event=$3,output_hash=decode($4,'hex'),receipt_hash=decode($5,'hex'),updated_at=$6,reserved_units=coalesce($7::text::numeric,reserved_units),request_hash=decode($8,'hex'),reason=coalesce(reason,$9) WHERE entry_id=$1",
      entry,units,response_event.0,response.output_hash.0,receipt_hash,at.0,reservation,request.request_id,quote_reason).execute(&mut *conn).await?;
    mark_overrun(conn, entry).await?;
    Ok(accepted(
        decision(conn, &request.request_id)
            .await?
            .expect("recorded effect"),
        reused,
    ))
}
