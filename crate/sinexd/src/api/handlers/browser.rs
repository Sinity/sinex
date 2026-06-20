//! Browser capture RPC handlers.

use std::collections::BTreeSet;

use sinex_primitives::rpc::browser::{
    BrowserCaptureBatchRequest, BrowserCaptureBatchResponse, BrowserCaptureObservation,
};
use sinex_primitives::{Result, SinexError};

const MAX_BROWSER_CAPTURE_BATCH_OBSERVATIONS: usize = 256;
const MAX_BROWSER_CAPTURE_FIELD_LEN: usize = 16 * 1024;

pub async fn handle_browser_capture_batch(
    _services: &crate::api::service_container::ServiceContainer,
    req: BrowserCaptureBatchRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<BrowserCaptureBatchResponse> {
    validate_non_empty("profile_id", &req.profile_id)?;
    validate_non_empty("producer_instance_id", &req.producer_instance_id)?;
    validate_non_empty("batch_id", &req.batch_id)?;

    if req.observations.is_empty() {
        return Err(SinexError::validation(
            "browser.capture_batch requires at least one observation",
        )
        .with_context("batch_id", req.batch_id));
    }
    if req.observations.len() > MAX_BROWSER_CAPTURE_BATCH_OBSERVATIONS {
        return Err(SinexError::resource_exhausted(format!(
            "browser.capture_batch accepts at most {MAX_BROWSER_CAPTURE_BATCH_OBSERVATIONS} observations"
        ))
        .with_context("batch_id", req.batch_id)
        .with_context("observation_count", req.observations.len().to_string()));
    }

    for observation in &req.observations {
        validate_observation(observation)?;
    }

    let contract_ids = req
        .observations
        .iter()
        .map(|observation| observation.contract_id().to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let last_accepted_sequence = req
        .sequence_start
        .checked_add(req.observations.len() as u64)
        .and_then(|exclusive_end| exclusive_end.checked_sub(1))
        .ok_or_else(|| {
            SinexError::validation("browser.capture_batch sequence range overflows u64")
                .with_context("batch_id", req.batch_id.clone())
        })?;

    Ok(BrowserCaptureBatchResponse {
        batch_id: req.batch_id,
        accepted_count: req.observations.len(),
        first_sequence: req.sequence_start,
        last_accepted_sequence,
        profile_id: req.profile_id,
        producer_instance_id: req.producer_instance_id,
        actor_id: auth.actor_id().to_string(),
        event_contract_ids: contract_ids,
    })
}

fn validate_observation(observation: &BrowserCaptureObservation) -> Result<()> {
    match observation {
        BrowserCaptureObservation::Navigation {
            url,
            title,
            transition,
            referrer,
            ..
        } => {
            validate_non_empty("url", url)?;
            validate_len("url", url)?;
            validate_optional_len("title", title.as_deref())?;
            validate_optional_len("transition", transition.as_deref())?;
            validate_optional_len("referrer", referrer.as_deref())?;
        }
        BrowserCaptureObservation::TabActivated { url, title, .. } => {
            validate_optional_len("url", url.as_deref())?;
            validate_optional_len("title", title.as_deref())?;
        }
        BrowserCaptureObservation::DownloadObserved {
            download_id,
            url,
            filename,
            state,
            ..
        } => {
            validate_non_empty("download_id", download_id)?;
            validate_non_empty("url", url)?;
            validate_len("download_id", download_id)?;
            validate_len("url", url)?;
            validate_optional_len("filename", filename.as_deref())?;
            validate_optional_len("state", state.as_deref())?;
        }
    }
    Ok(())
}

fn validate_non_empty(field: &'static str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(SinexError::validation(format!(
            "browser.capture_batch field '{field}' must be non-empty"
        )));
    }
    validate_len(field, value)
}

fn validate_optional_len(field: &'static str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        validate_len(field, value)?;
    }
    Ok(())
}

fn validate_len(field: &'static str, value: &str) -> Result<()> {
    if value.len() > MAX_BROWSER_CAPTURE_FIELD_LEN {
        return Err(SinexError::resource_exhausted(format!(
            "browser.capture_batch field '{field}' exceeds {MAX_BROWSER_CAPTURE_FIELD_LEN} bytes"
        )));
    }
    Ok(())
}
