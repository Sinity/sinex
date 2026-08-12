use serde::{Deserialize, Serialize};

use crate::privacy::{PrivateModeReasonClass, RuntimePrivateModeState};
use crate::{JsonValue, Uuid};

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

/// Read-only bounded recognizer audit over persisted privacy surfaces.
pub const PRIVACY_SHADOW_AUDIT_METHOD: RpcMethod<
    PrivacyShadowAuditRequest,
    PrivacyShadowAuditResponse,
> = RpcMethod::new(
    methods::PRIVACY_SHADOW_AUDIT,
    RpcRole::ReadOnly,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const PRIVACY_POLICY_RULE_ADD_METHOD: RpcMethod<
    PrivacyPolicyRuleAddRequest,
    PrivacyPolicyMutationResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_RULE_ADD,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_POLICY_BACKEND_ADD_METHOD: RpcMethod<
    PrivacyPolicyBackendAddRequest,
    PrivacyPolicyMutationResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_BACKEND_ADD,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_POLICY_DICTIONARY_ADD_METHOD: RpcMethod<
    PrivacyPolicyDictionaryAddRequest,
    PrivacyPolicyMutationResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_DICTIONARY_ADD,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_POLICY_SEED_BUILTIN_METHOD: RpcMethod<
    PrivacyPolicySeedBuiltinRequest,
    PrivacyPolicySeedBuiltinResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_SEED_BUILTIN,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_POLICY_SCOPE_BIND_METHOD: RpcMethod<
    PrivacyPolicyScopeBindRequest,
    PrivacyPolicyMutationResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_SCOPE_BIND,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_POLICY_RULE_REMOVE_METHOD: RpcMethod<
    PrivacyPolicyRuleRemoveRequest,
    PrivacyPolicyRuleRemoveResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_RULE_REMOVE,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_POLICY_RULE_SET_ENABLED_METHOD: RpcMethod<
    PrivacyPolicyRuleSetEnabledRequest,
    PrivacyPolicyRuleSetEnabledResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_RULE_SET_ENABLED,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_POLICY_FIELD_BIND_METHOD: RpcMethod<
    PrivacyPolicyFieldBindRequest,
    PrivacyPolicyFieldBindResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_FIELD_BIND,
    RpcRole::Write,
    RpcDomain::Privacy,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const PRIVACY_POLICY_FIELD_UNBIND_METHOD: RpcMethod<
    PrivacyPolicyFieldUnbindRequest,
    PrivacyPolicyFieldUnbindResponse,
> = RpcMethod::new(
    methods::PRIVACY_POLICY_FIELD_UNBIND,
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
    pub rules: Vec<PrivacyPolicyRule>,
    pub field_scopes: Vec<PrivacyPolicyFieldScope>,
    pub key_namespaces: Vec<PrivacyPolicyKeyNamespace>,
    pub recognizer_backends: Vec<PrivacyPolicyRecognizerBackend>,
    pub dictionaries: Vec<PrivacyPolicyDictionary>,
}

/// Request: privacy.shadow_audit.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PrivacyShadowAuditRequest {
    /// Lower bound for event timestamps. Supports RFC3339 and duration syntax.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
    /// Upper bound for event timestamps. Supports RFC3339 and duration syntax.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    /// Maximum event rows inspected from each event lane.
    #[serde(default = "default_shadow_event_limit")]
    pub limit_events: i64,
    /// Maximum rows inspected from each non-event surface.
    #[serde(default = "default_shadow_surface_limit")]
    pub limit_rows_per_surface: i64,
}

/// Stable status vocabulary shared by report-only audits and purge reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivacyInvalidationStatus {
    Purged,
    StaleMarked,
    Residual,
    RetainedByDesign,
    Scanned,
    Failed,
}

/// One enumerated privacy/lifecycle surface.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyInvalidationSurface {
    pub surface: String,
    pub status: PrivacyInvalidationStatus,
    #[serde(default)]
    pub before_count: u64,
    #[serde(default)]
    pub after_count: u64,
    #[serde(default)]
    pub affected_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residual_horizon: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Persisted report for a destructive invalidation operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyInvalidationReport {
    pub schema_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    pub generated_at: String,
    pub surfaces: Vec<PrivacyInvalidationSurface>,
    #[serde(default)]
    pub caveats: Vec<String>,
}

/// One aggregate row from a report-only recognizer audit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyShadowAuditFinding {
    pub recognizer: String,
    pub category: crate::privacy::RuleCategory,
    pub surface: String,
    pub source: String,
    pub event_type: String,
    pub field_path: String,
    pub sampled_row_count: u64,
    pub matched_row_count: u64,
    pub match_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_seen: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<String>,
}

/// Response: privacy.shadow_audit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyShadowAuditResponse {
    pub schema_version: String,
    pub generated_at: String,
    pub read_only_proven: bool,
    pub scanned_events: u64,
    pub scanned_rows: u64,
    pub scope: PrivacyShadowAuditRequest,
    pub surfaces: Vec<PrivacyInvalidationSurface>,
    pub findings: Vec<PrivacyShadowAuditFinding>,
    #[serde(default)]
    pub caveats: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyBackendAddRequest {
    pub name: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    #[serde(default = "empty_json_object")]
    pub config: JsonValue,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyDictionaryAddRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default = "default_dictionary_source_kind")]
    pub source_kind: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub terms: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyRuleAddRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub matcher_type: String,
    pub matcher_value: String,
    #[serde(default = "empty_json_object")]
    pub matcher_config: JsonValue,
    /// Presidio context words: terms whose presence near a candidate span
    /// boosts the recognizer's confidence score. Folded into
    /// `matcher_config["context"]` by the handler so the analyzer request
    /// forwards them. Ignored by non-Presidio recognizers.
    #[serde(default)]
    pub context_words: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recognizer_backend_id: Option<Uuid>,
    #[serde(default = "default_recognizer_kind")]
    pub recognizer_kind: String,
    #[serde(default)]
    pub case_sensitive: bool,
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_label: Option<String>,
    #[serde(default = "default_key_namespace")]
    pub key_namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyScopeBindRequest {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicySeedBuiltinRequest {
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicySeedBuiltinResponse {
    pub inserted: usize,
    pub updated: usize,
    pub unchanged: usize,
    pub total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyMutationResponse {
    pub id: Uuid,
    pub kind: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyRuleRemoveRequest {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyRuleRemoveResponse {
    pub name: String,
    pub removed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyRuleSetEnabledRequest {
    pub name: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyRuleSetEnabledResponse {
    pub name: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyFieldBindRequest {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyFieldBindResponse {
    pub scope: PrivacyPolicyFieldScope,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyFieldUnbindRequest {
    pub scope_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyFieldUnbindResponse {
    pub scope_id: Uuid,
    pub removed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyRule {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub matcher_type: String,
    pub matcher_value: String,
    pub matcher_config: JsonValue,
    /// Presidio context words, projected from `matcher_config["context"]` for a
    /// typed view. Empty when none are configured.
    #[serde(default)]
    pub context_words: Vec<String>,
    pub recognizer_backend_id: Option<Uuid>,
    pub recognizer_kind: String,
    pub case_sensitive: bool,
    pub action: String,
    pub action_label: Option<String>,
    pub key_namespace: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyFieldScope {
    pub id: Uuid,
    pub rule_id: Uuid,
    pub event_source: Option<String>,
    pub event_type: Option<String>,
    pub field_path: Option<String>,
    pub priority: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyKeyNamespace {
    pub id: Uuid,
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyRecognizerBackend {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub endpoint_url: Option<String>,
    pub config: JsonValue,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPolicyDictionary {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub language: Option<String>,
    pub source_kind: String,
    pub tags: Vec<String>,
    pub enabled: bool,
    pub enabled_terms: usize,
}

fn default_actor() -> String {
    "operator".to_string()
}

fn empty_json_object() -> JsonValue {
    serde_json::json!({})
}

fn default_recognizer_kind() -> String {
    "local_pattern".to_string()
}

fn default_key_namespace() -> String {
    "default".to_string()
}

const fn default_true() -> bool {
    true
}

fn default_dictionary_source_kind() -> String {
    "user".to_string()
}

fn default_shadow_event_limit() -> i64 {
    10_000
}

fn default_shadow_surface_limit() -> i64 {
    500
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
