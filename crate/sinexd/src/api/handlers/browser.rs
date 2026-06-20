//! Browser capture RPC handlers.

use std::collections::BTreeSet;

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_db::repositories::SourceMaterial as DbSourceMaterial;
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::events::payloads::{
    BrowserDownloadObservedPayload, BrowserNavigationObservedPayload, BrowserTabActivatedPayload,
};
use sinex_primitives::rpc::browser::{
    BrowserCaptureBatchRequest, BrowserCaptureBatchResponse, BrowserCaptureObservation,
};
use sinex_primitives::{Id, Result, SinexError, Uuid};

const MAX_BROWSER_CAPTURE_BATCH_OBSERVATIONS: usize = 256;
const MAX_BROWSER_CAPTURE_FIELD_LEN: usize = 16 * 1024;

pub async fn handle_browser_capture_batch(
    services: &crate::api::service_container::ServiceContainer,
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
    let material_id = register_browser_batch_material(services, &req, auth).await?;
    let event_ids = persist_browser_observations(services, &req, material_id).await?;

    Ok(BrowserCaptureBatchResponse {
        batch_id: req.batch_id,
        accepted_count: req.observations.len(),
        first_sequence: req.sequence_start,
        last_accepted_sequence,
        profile_id: req.profile_id,
        producer_instance_id: req.producer_instance_id,
        actor_id: auth.actor_id().to_string(),
        material_id: material_id.to_string(),
        event_ids,
        event_contract_ids: contract_ids,
    })
}

async fn register_browser_batch_material(
    services: &crate::api::service_container::ServiceContainer,
    req: &BrowserCaptureBatchRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<Uuid> {
    let material_id = Uuid::now_v7();
    let source_uri = format!(
        "native-messaging://browser/{}/{}/{}/{}",
        req.profile_id, req.producer_instance_id, req.batch_id, material_id
    );
    let preview = format!(
        "browser native-message batch {}; {} observation(s); sequence {}..{}",
        req.batch_id,
        req.observations.len(),
        req.sequence_start,
        req.sequence_start + req.observations.len() as u64 - 1
    );
    let material = DbSourceMaterial::blob_text(source_uri.clone())
        .with_content_preview(preview)
        .with_metadata(json!({
            "source_uri": source_uri,
            "package_id": "browser.web",
            "mode_id": "browser.webextension-native-live",
            "profile_id": req.profile_id,
            "producer_instance_id": req.producer_instance_id,
            "batch_id": req.batch_id,
            "sequence_start": req.sequence_start,
            "observation_count": req.observations.len(),
            "capture_surface": "native_messaging",
        }))
        .with_staged_by(auth.actor_id().to_string());
    let record = services
        .pool()
        .source_materials()
        .register_external_material(material_id, material)
        .await
        .map_err(|error| {
            SinexError::processing("failed to register browser native-message batch material")
                .with_context("batch_id", req.batch_id.clone())
                .with_context("profile_id", req.profile_id.clone())
                .with_std_error(&error)
        })?;
    Ok(record.id)
}

async fn persist_browser_observations(
    services: &crate::api::service_container::ServiceContainer,
    req: &BrowserCaptureBatchRequest,
    material_id: Uuid,
) -> Result<Vec<String>> {
    let material_ref = Id::<SourceMaterial>::from_uuid(material_id);
    let mut event_ids = Vec::with_capacity(req.observations.len());

    for (index, observation) in req.observations.iter().enumerate() {
        let sequence = req.sequence_start + index as u64;
        let event = match observation {
            BrowserCaptureObservation::Navigation {
                observed_at,
                url,
                title,
                tab_id,
                window_id,
                transition,
                referrer,
            } => BrowserNavigationObservedPayload {
                profile_id: req.profile_id.clone(),
                producer_instance_id: req.producer_instance_id.clone(),
                batch_id: req.batch_id.clone(),
                sequence,
                observed_at: *observed_at,
                url: url.clone(),
                title: title.clone(),
                tab_id: *tab_id,
                window_id: *window_id,
                transition: transition.clone(),
                referrer: referrer.clone(),
            }
            .from_material(material_ref)
            .at_time(*observed_at)
            .build()?
            .to_json_event()
            .map_err(|error| {
                SinexError::serialization("browser.capture_batch: failed to serialize navigation")
                    .with_std_error(&error)
            })?,
            BrowserCaptureObservation::TabActivated {
                observed_at,
                tab_id,
                window_id,
                url,
                title,
            } => BrowserTabActivatedPayload {
                profile_id: req.profile_id.clone(),
                producer_instance_id: req.producer_instance_id.clone(),
                batch_id: req.batch_id.clone(),
                sequence,
                observed_at: *observed_at,
                tab_id: *tab_id,
                window_id: *window_id,
                url: url.clone(),
                title: title.clone(),
            }
            .from_material(material_ref)
            .at_time(*observed_at)
            .build()?
            .to_json_event()
            .map_err(|error| {
                SinexError::serialization(
                    "browser.capture_batch: failed to serialize tab activation",
                )
                .with_std_error(&error)
            })?,
            BrowserCaptureObservation::DownloadObserved {
                observed_at,
                download_id,
                url,
                filename,
                state,
                total_bytes,
            } => BrowserDownloadObservedPayload {
                profile_id: req.profile_id.clone(),
                producer_instance_id: req.producer_instance_id.clone(),
                batch_id: req.batch_id.clone(),
                sequence,
                observed_at: *observed_at,
                download_id: download_id.clone(),
                url: url.clone(),
                filename: filename.clone(),
                state: state.clone(),
                total_bytes: *total_bytes,
            }
            .from_material(material_ref)
            .at_time(*observed_at)
            .build()?
            .to_json_event()
            .map_err(|error| {
                SinexError::serialization("browser.capture_batch: failed to serialize download")
                    .with_std_error(&error)
            })?,
        };
        let inserted = services.pool().events().insert(event).await?;
        let event_id = inserted.id.ok_or_else(|| {
            SinexError::invalid_state("browser.capture_batch persisted event missing id")
                .with_context("batch_id", req.batch_id.clone())
                .with_context("sequence", sequence.to_string())
        })?;
        event_ids.push(event_id.to_string());
    }

    Ok(event_ids)
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
