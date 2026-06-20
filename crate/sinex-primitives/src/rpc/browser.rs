//! Browser capture RPC contracts.

use serde::{Deserialize, Serialize};

use crate::event_contracts::{
    BROWSER_DOWNLOAD_OBSERVED_CONTRACT_ID, BROWSER_NAVIGATION_OBSERVED_CONTRACT_ID,
    BROWSER_TAB_ACTIVATED_CONTRACT_ID, EventContractId,
};
use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use crate::temporal::Timestamp;

pub const BROWSER_CAPTURE_BATCH_METHOD: RpcMethod<
    BrowserCaptureBatchRequest,
    BrowserCaptureBatchResponse,
> = RpcMethod::new(
    methods::BROWSER_CAPTURE_BATCH,
    RpcRole::Write,
    RpcDomain::Browser,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

/// Request: `browser.capture_batch`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserCaptureBatchRequest {
    /// Browser profile identity as known to the extension/native-host pair.
    pub profile_id: String,
    /// Producer instance identity for reconnect/drain diagnostics.
    pub producer_instance_id: String,
    /// Extension-generated batch id for idempotency and operator diagnostics.
    pub batch_id: String,
    /// Sequence number assigned to the first observation in this batch.
    pub sequence_start: u64,
    /// Observations collected by the extension/native host.
    #[serde(default)]
    pub observations: Vec<BrowserCaptureObservation>,
}

/// Browser observation emitted by the WebExtension/native-host path.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BrowserCaptureObservation {
    Navigation {
        observed_at: Timestamp,
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tab_id: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        window_id: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        transition: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        referrer: Option<String>,
    },
    TabActivated {
        observed_at: Timestamp,
        tab_id: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        window_id: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
    },
    DownloadObserved {
        observed_at: Timestamp,
        download_id: String,
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        state: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        total_bytes: Option<u64>,
    },
}

impl BrowserCaptureObservation {
    #[must_use]
    pub const fn contract_id(&self) -> EventContractId {
        match self {
            Self::Navigation { .. } => BROWSER_NAVIGATION_OBSERVED_CONTRACT_ID,
            Self::TabActivated { .. } => BROWSER_TAB_ACTIVATED_CONTRACT_ID,
            Self::DownloadObserved { .. } => BROWSER_DOWNLOAD_OBSERVED_CONTRACT_ID,
        }
    }

    #[must_use]
    pub fn observed_at(&self) -> Timestamp {
        match self {
            Self::Navigation { observed_at, .. }
            | Self::TabActivated { observed_at, .. }
            | Self::DownloadObserved { observed_at, .. } => *observed_at,
        }
    }
}

/// Response: `browser.capture_batch`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserCaptureBatchResponse {
    pub batch_id: String,
    pub accepted_count: usize,
    pub first_sequence: u64,
    pub last_accepted_sequence: u64,
    pub profile_id: String,
    pub producer_instance_id: String,
    pub actor_id: String,
    pub material_id: String,
    pub event_ids: Vec<String>,
    pub event_contract_ids: Vec<String>,
}
