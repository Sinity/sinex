use crate::Uuid;
use crate::temporal::Timestamp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct GitOpsListSourcesRequest {
    #[serde(default)]
    pub include_disabled: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GitOpsListSourcesResponse {
    pub sources: Vec<GitOpsSourceInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GitOpsSourceInfo {
    pub id: Uuid,
    pub repository_url: String,
    pub branch: String,
    pub path_pattern: String,
    pub sync_enabled: bool,
    pub last_sync_at: Option<Timestamp>,
    pub last_sync_commit: Option<String>,
    pub sync_frequency_minutes: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GitOpsCreateSourceRequest {
    pub repository_url: String,
    #[serde(default = "default_branch")]
    pub branch: String,
    #[serde(default = "default_path_pattern")]
    pub path_pattern: String,
    #[serde(default = "default_sync_frequency")]
    pub sync_frequency_minutes: i32,
}

fn default_branch() -> String {
    "main".to_string()
}

fn default_path_pattern() -> String {
    "schemas/**/*.json".to_string()
}

fn default_sync_frequency() -> i32 {
    60
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GitOpsCreateSourceResponse {
    pub id: Uuid,
    pub repository_url: String,
    pub branch: String,
    pub path_pattern: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GitOpsDeleteSourceRequest {
    pub id: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GitOpsDeleteSourceResponse {
    pub deleted: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GitOpsTriggerSyncRequest {
    pub id: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GitOpsTriggerSyncResponse {
    pub triggered: bool,
    pub message: String,
}
