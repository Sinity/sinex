use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct TemplateMeta {
    pub fingerprint: String,
    pub extensions: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PoolMeta {
    pub fingerprint: Option<String>,
    pub extensions: HashMap<String, String>,
    pub dirty: bool,
    pub updated_at_rfc3339: String,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TemplateInfo {
    pub name: String,
    pub extensions: HashMap<String, String>,
}
