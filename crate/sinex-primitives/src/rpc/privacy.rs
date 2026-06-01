use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{
    Uuid,
    privacy::{PrivateModeReasonClass, RuntimePrivateModeState},
};

use super::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};

pub const PRIVACY_PRIVATE_MODE_STATUS_METHOD: RpcMethod<
    PrivateModeStatusRequest,
    PrivateModeStateResponse,
> = RpcMethod::new(
    methods::PRIVACY_PRIVATE_MODE_STATUS,
    RpcRole::ReadOnly,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const PRIVACY_PRIVATE_MODE_ENABLE_METHOD: RpcMethod<
    PrivateModeEnableRequest,
    PrivateModeStateResponse,
> = RpcMethod::new(
    methods::PRIVACY_PRIVATE_MODE_ENABLE,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_PRIVATE_MODE_DISABLE_METHOD: RpcMethod<
    PrivateModeDisableRequest,
    PrivateModeStateResponse,
> = RpcMethod::new(
    methods::PRIVACY_PRIVATE_MODE_DISABLE,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_POLICY_LIST_METHOD: RpcMethod<
    PrivacyPolicyListRequest,
    PrivacyPolicyListResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const PRIVACY_POLICY_CREATE_BACKEND_METHOD: RpcMethod<
    PrivacyPolicyCreateBackendRequest,
    PrivacyPolicyIdResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_CREATE_BACKEND,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_POLICY_CREATE_KEY_METHOD: RpcMethod<
    PrivacyPolicyCreateKeyRequest,
    PrivacyPolicyIdResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_CREATE_KEY,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_POLICY_CREATE_DICTIONARY_METHOD: RpcMethod<
    PrivacyPolicyCreateDictionaryRequest,
    PrivacyPolicyIdResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_CREATE_DICTIONARY,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_POLICY_ADD_DICTIONARY_TERM_METHOD: RpcMethod<
    PrivacyPolicyAddDictionaryTermRequest,
    PrivacyPolicyIdResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_ADD_DICTIONARY_TERM,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_POLICY_CREATE_RULE_METHOD: RpcMethod<
    PrivacyPolicyCreateRuleRequest,
    PrivacyPolicyIdResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_CREATE_RULE,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_POLICY_BIND_RULE_METHOD: RpcMethod<
    PrivacyPolicyBindRuleRequest,
    PrivacyPolicyIdResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_BIND_RULE,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_POLICY_SEED_CATALOG_METHOD: RpcMethod<
    PrivacyPolicySeedCatalogRequest,
    PrivacyPolicySeedCatalogResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_SEED_CATALOG,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PrivateModeStatusRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivateModeEnableRequest {
    #[serde(default = "default_actor")]
    pub actor: String,

    #[serde(default)]
    pub reason_class: PrivateModeReasonClass,

    #[serde(default)]
    pub source_classes: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<crate::temporal::Timestamp>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PrivateModeDisableRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivateModeStateResponse {
    pub state: RuntimePrivateModeState,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PrivacyPolicyListRequest {
    #[serde(default)]
    pub include_disabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyListResponse {
    pub rules: Vec<PrivacyRule>,
    pub field_rules: Vec<PrivacyFieldRule>,
    pub recognizer_backends: Vec<PrivacyRecognizerBackend>,
    pub dictionaries: Vec<PrivacyDictionary>,
    pub dictionary_terms: Vec<PrivacyDictionaryTerm>,
    pub key_namespaces: Vec<PrivacyKeyNamespace>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyIdResponse {
    pub id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyCreateBackendRequest {
    pub name: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    #[serde(default)]
    pub config: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyCreateKeyRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyCreateDictionaryRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default = "default_dictionary_source_kind")]
    pub source_kind: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyAddDictionaryTermRequest {
    pub dictionary_id: Uuid,
    pub term: String,
    #[serde(default)]
    pub metadata: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyCreateRuleRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recognizer_backend_id: Option<Uuid>,
    #[serde(default = "default_recognizer_kind")]
    pub recognizer_kind: String,
    pub matcher_type: String,
    pub matcher_value: String,
    #[serde(default)]
    pub matcher_config: JsonValue,
    #[serde(default)]
    pub case_sensitive: bool,
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_label: Option<String>,
    #[serde(default = "default_key_namespace")]
    pub key_namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyBindRuleRequest {
    pub rule_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_path: Option<String>,
    #[serde(default)]
    pub priority: i32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PrivacyPolicySeedCatalogRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicySeedCatalogResponse {
    pub seeded_rules: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyRule {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub recognizer_backend_id: Option<Uuid>,
    pub recognizer_kind: String,
    pub matcher_type: String,
    pub matcher_value: String,
    pub matcher_config: JsonValue,
    pub case_sensitive: bool,
    pub action: String,
    pub action_label: Option<String>,
    pub key_namespace: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyFieldRule {
    pub id: Uuid,
    pub rule_id: Uuid,
    pub event_source: Option<String>,
    pub event_type: Option<String>,
    pub field_path: Option<String>,
    pub priority: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyRecognizerBackend {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub endpoint_url: Option<String>,
    pub config: JsonValue,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyDictionary {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub language: Option<String>,
    pub source_kind: String,
    pub tags: Vec<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyDictionaryTerm {
    pub id: Uuid,
    pub dictionary_id: Uuid,
    pub term: String,
    pub metadata: JsonValue,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyKeyNamespace {
    pub id: Uuid,
    pub name: String,
    pub description: String,
}

fn default_actor() -> String {
    "operator".to_string()
}

fn default_dictionary_source_kind() -> String {
    "user".to_string()
}

fn default_recognizer_kind() -> String {
    "local_pattern".to_string()
}

fn default_key_namespace() -> String {
    "default".to_string()
}

impl Default for PrivateModeEnableRequest {
    fn default() -> Self {
        Self {
            actor: default_actor(),
            reason_class: PrivateModeReasonClass::default(),
            source_classes: Vec::new(),
            expires_at: None,
        }
    }
}
