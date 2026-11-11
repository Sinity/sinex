//! Materialize a canonical request through admission, then read its maintained output.
use super::{Settings, stored::StoredProducts};
use crate::{Error, Result};
use serde_json::{Value, json};
use sinex_analysis::Eligibility;
use sinex_derivations::Keyed;
use sinex_derivations::figures::Metric;
use sinex_model::event_types::analysis::AnalysisRequest;
use sinex_model::event_types::work::ContextPack;
use sinex_model::{EventPayload, Timestamp, query::Context};
use sqlx::PgConnection;

pub(crate) async fn question(
    conn: &mut PgConnection,
    ctx: &Context,
    settings: &Settings,
    operation: &str,
    recipe: Option<&str>,
    metrics: &[&Metric],
    from: jiff::civil::Date,
    to: jiff::civil::Date,
    eligibility: Eligibility,
) -> Result<AnalysisRequest> {
    if from > to {
        return Err(Error::bad_request("analysis end precedes start"));
    }
    let mut names: Vec<_> = metrics.iter().map(|m| m.name.to_owned()).collect();
    names.sort();
    names.dedup();
    let grants = sinex_store::query::current_grants(conn).await?;
    let mut grants: Vec<String> = grants
        .split(',')
        .filter(|g| !g.is_empty() && *g != "sealed")
        .map(str::to_owned)
        .collect();
    grants.sort();
    grants.dedup();
    let mut request = AnalysisRequest {
        selection: None,
        request: String::new(),
        operation: operation.into(),
        recipe: recipe.map(str::to_owned),
        from: from.to_string(),
        to: to.to_string(),
        metrics: names,
        min_observed_fraction: eligibility
            .min_observed_fraction
            .map(|fraction| if fraction == 0.0 { 0.0 } else { fraction }),
        zone: ctx.zone_name(),
        day_start: ctx.day_start.to_string(),
        grants,
        started_at: settings.time_settings.day_start(from)?,
        ended_at: settings.time_settings.day_end(to)?,
        by: String::new(),
        requested_at: Timestamp::from_unix_ns(ctx.now.as_nanosecond())?,
    };
    let canonical = json!([
        request.operation,
        request.recipe,
        request.from,
        request.to,
        request.metrics,
        request.min_observed_fraction,
        request.zone,
        request.day_start,
        request.grants
    ]);
    request.request = blake3::hash(canonical.to_string().as_bytes())
        .to_hex()
        .to_string();
    Ok(request)
}

pub(super) async fn find(
    conn: &mut PgConnection,
    event_type: &str,
    request: &str,
) -> Result<Option<Value>> {
    Ok(
        sinex_store::query::live_payload(conn, event_type, "request", request)
            .await?
            .map(|(id, mut payload)| {
                if event_type != AnalysisRequest::EVENT_TYPE {
                    payload["_citation"] = json!(format!("sinex:ev/{id}"));
                }
                payload
            }),
    )
}

pub(crate) async fn materialize(
    conn: &mut PgConnection,
    writer: &crate::writer::WriterHandle,
    blobs: &sinex_store::Blobs,
    mut request: AnalysisRequest,
    actor: &str,
    host: &str,
) -> Result<String> {
    let time_settings = sinex_model::time_settings::TimeSettings::from_parts(
        jiff::tz::TimeZone::get(&request.zone)?,
        request.day_start.parse::<jiff::civil::Time>()?,
    );
    let id = request.request.clone();
    let existing = find(conn, "analysis.request", &id).await?;
    let key = match existing {
        Some(payload) => payload,
        None => {
            request.by = actor.into();
            let now = Timestamp::now();
            let acquired = super::request::request_material(request.clone(), actor, host, now);
            let done = crate::intake::request(
                conn,
                writer,
                blobs,
                std::sync::Arc::new(super::request::request_source()),
                &format!("analysis.request:{actor}@{host}"),
                host,
                acquired,
            )
            .await?;
            if done.outcome.failures > 0 {
                return Err(Error::bad_request("analysis request was not admitted"));
            }
            json!(request)
        }
    };
    let dirty = sinex_store::query::key_dirty(conn, StoredProducts::NAME, &key).await?;
    if dirty || find(conn, "analysis.panel", &id).await?.is_none() {
        crate::derivation_runner::compute_now(
            conn,
            writer,
            &StoredProducts,
            vec![key],
            host,
            time_settings,
        )
        .await?;
    }
    Ok(id)
}

pub(crate) struct StoredPack {
    pub citation: String,
    pub payload: Value,
}

pub(crate) async fn load_pack(
    conn: &mut PgConnection,
    reference: &str,
) -> Result<Option<StoredPack>> {
    if let Some(id) = reference.strip_prefix("sinex:ev/") {
        let id = uuid::Uuid::parse_str(id)
            .map_err(|error| Error::bad_value(format!("context pack reference: {error}")))?;
        return Ok(sinex_store::query::event_payload(
            conn,
            ContextPack::EVENT_TYPE,
            sinex_model::EventId(id),
        )
        .await?
        .map(|payload| StoredPack {
            citation: format!("sinex:ev/{id}"),
            payload,
        }));
    }
    Ok(
        sinex_store::query::live_payload(conn, ContextPack::EVENT_TYPE, "pack", reference)
            .await?
            .map(|(id, payload)| StoredPack {
                citation: format!("sinex:ev/{id}"),
                payload,
            }),
    )
}
