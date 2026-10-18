//! JSON-RPC request, response and error types shared by sinexd and sinexctl.
pub use sinex_model::retrieval::{
    ProjectEvidence, SessionProjectAttribution, UnknownProjectMember,
};
pub mod rpc {
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use sinex_model::{CommitSeq, ValidationError};

    pub use sinex_model::DATA_NOTICE;

    macro_rules! methods {
        ($($variant:ident => $name:literal, $description:literal;)*) => {
            #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
            pub enum Method {
                $(#[serde(rename = $name)] $variant),*
            }
            pub const SUPPORTED_METHODS: &[&str] = &[$($name),*];
            impl Method {
                pub const ALL: &[Self] = &[$(Self::$variant),*];
                pub const fn as_str(self) -> &'static str {
                    match self {
                        $(Self::$variant => $name),*
                    }
                }
                pub fn parse(value: &str) -> Option<Self> {
                    match value {
                        $($name => Some(Self::$variant),)*
                        _ => None,
                    }
                }
                pub const fn description(self) -> &'static str {
                    match self {
                        $(Self::$variant => $description),*
                    }
                }
            }
        };
    }
    methods! {
        Status => "status", "Schema version, writer state, and supported sources and methods";
        Version => "version", "Package and protocol version";
        ConfigShow => "config.show", "Configuration, with secrets shown as file paths";
        Catalog => "catalog", "This list of methods";
        Sources => "sources.list", "Source observations, read counts and unfinished reads";
        Query => "query.run", "Run a query: raw event pages, indexed shell queries or recall last";
        Search => "search", "Search shell commands by words or phrases, with ranked snippets and project and time filters";
        Show => "object.show", "Show a raw event, canonical command or shell session, including archived readings and whether it is excluded";
        Trace => "object.trace", "Follow canonical parents back to their source records and stored material, with text masked";
        ReplayPlan => "replay.plan", "Preview a replay of stored Atuin material";
        ReplayStart => "replay.start", "Start a replay; the writer processes it in chunks";
        ReplayStatus => "replay.status", "Show replay progress";
        ReplayCancel => "replay.cancel", "Stop a replay between chunks";
        Delete => "delete.run", "Exclude one raw Atuin occurrence from reads; stored data is kept";
        DeleteUndo => "delete.undo", "Remove an exclusion; the occurrence returns on its next read";
    }

    /// Request ID. Every request gets a response; notifications and batches are not supported.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(untagged)]
    pub enum RpcId {
        Number(i64),
        String(String),
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RpcRequest {
        pub jsonrpc: String,
        pub id: RpcId,
        pub method: String,
        pub params: Value,
    }
    impl RpcRequest {
        pub fn new(id: RpcId, method: Method, params: Value) -> Self {
            Self {
                jsonrpc: "2.0".into(),
                id,
                method: method.as_str().into(),
                params,
            }
        }
        pub fn validate(&self) -> Result<Method, RpcError> {
            if self.jsonrpc != "2.0" {
                return Err(RpcError::invalid_request("jsonrpc must be 2.0"));
            }
            let method = Method::parse(&self.method)
                .ok_or_else(|| RpcError::method_not_found(&self.method))?;
            self.validate_params(method)
                .map_err(|message| RpcError::new(ErrorKind::BadQuery, message, Value::Null))?;
            Ok(method)
        }
        fn params<T: serde::de::DeserializeOwned>(&self, error: &str) -> Result<T, String> {
            serde_json::from_value(self.params.clone()).map_err(|_| error.to_string())
        }
        fn validate_params(&self, method: Method) -> Result<(), String> {
            use sinex_model::{acquisition, exclusion, replay};
            match method {
                Method::Sources => self
                    .params::<acquisition::SourcesRequest>("invalid source inspection parameters")?
                    .validate()
                    .map_err(|e| e.to_string()),
                Method::Delete | Method::DeleteUndo => {
                    let params: exclusion::Request =
                        self.params("delete requires ref, action_id and optional expected_key")?;
                    params.event_id()?;
                    if params.expected_key.is_some_and(|key| key.len() > 2048) {
                        return Err("expected_key exceeds 2048 bytes".into());
                    }
                    Ok(())
                }
                Method::ReplayPlan => self
                    .params::<replay::Selection>("replay.plan requires query")
                    .map(drop),
                Method::ReplayStart => self
                    .params::<replay::Start>(
                        "replay.start requires query and action_id, and accepts expected_basis",
                    )
                    .map(drop),
                Method::ReplayStatus | Method::ReplayCancel => self
                    .params::<replay::Operation>("replay status and cancel require operation_id")
                    .map(drop),
                Method::Query | Method::Search => {
                    let params: QueryParams = self.params(
                        "query requires text, optional page cursor and optional integer timeout_ms",
                    )?;
                    if params
                        .timeout_ms
                        .is_some_and(|ms| !(1..=3_600_000).contains(&ms))
                    {
                        return Err("query timeout_ms must be 1..=3600000".into());
                    }
                    Ok(())
                }
                Method::Show | Method::Trace => self
                    .params::<ShowParams>("show and trace require ref and accept bytes")
                    .map(drop),
                Method::Status | Method::Version | Method::ConfigShow | Method::Catalog => {
                    if self.params.as_object().is_some_and(|p| p.is_empty()) {
                        Ok(())
                    } else {
                        Err("method requires empty object params".into())
                    }
                }
            }
        }
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct QueryParams {
        pub query: String,
        #[serde(default)]
        pub cursor: Option<String>,
        #[serde(default)]
        pub page: Option<u32>,
        /// Deadline for the whole read; the API defaults to 10 seconds.
        #[serde(default)]
        pub timeout_ms: Option<u64>,
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct ShowParams {
        #[serde(rename = "ref")]
        pub reference: String,
        #[serde(default)]
        pub bytes: bool,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum ErrorKind {
        BadQuery,
        NotFound,
        Refused,
        Partial,
        Budget,
        Internal,
    }
    impl ErrorKind {
        pub const fn exit_code(self) -> u8 {
            match self {
                Self::BadQuery => 2,
                Self::NotFound => 3,
                Self::Refused => 4,
                Self::Partial => 5,
                Self::Budget => 6,
                Self::Internal => 1,
            }
        }
        pub const fn rpc_code(self) -> i32 {
            -32000 - self.exit_code() as i32
        }
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RpcError {
        pub code: i32,
        pub kind: ErrorKind,
        pub message: String,
        pub data: Value,
    }
    impl RpcError {
        pub fn new(kind: ErrorKind, message: impl Into<String>, data: Value) -> Self {
            Self {
                code: kind.rpc_code(),
                kind,
                message: message.into(),
                data,
            }
        }
        pub fn invalid_request(message: impl Into<String>) -> Self {
            Self {
                code: -32600,
                kind: ErrorKind::BadQuery,
                message: message.into(),
                data: Value::Null,
            }
        }
        pub fn parse_error() -> Self {
            Self {
                code: -32700,
                kind: ErrorKind::BadQuery,
                message: "invalid JSON request".into(),
                data: Value::Null,
            }
        }
        pub fn method_not_found(method: &str) -> Self {
            Self {
                code: -32601,
                kind: ErrorKind::NotFound,
                message: "unsupported method".into(),
                data: serde_json::json!({ "method": method }),
            }
        }
        pub fn exit_code(&self) -> u8 {
            self.kind.exit_code()
        }
    }

    /// A JSON-RPC 2.0 response with exactly one of result or error.
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(try_from = "ResponseWire", into = "ResponseWire")]
    pub struct RpcResponse {
        id: Option<RpcId>,
        outcome: Result<Value, RpcError>,
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(untagged, deny_unknown_fields)]
    enum ResponseWire {
        Success {
            jsonrpc: String,
            id: RpcId,
            result: Value,
        },
        Failure {
            jsonrpc: String,
            id: Option<RpcId>,
            error: RpcError,
        },
    }
    impl RpcResponse {
        pub fn id(&self) -> Option<&RpcId> {
            self.id.as_ref()
        }
        pub fn outcome(&self) -> &Result<Value, RpcError> {
            &self.outcome
        }
        pub fn into_outcome(self) -> Result<Value, RpcError> {
            self.outcome
        }
        pub fn success(id: RpcId, result: Value) -> Self {
            Self {
                id: Some(id),
                outcome: Ok(result),
            }
        }
        pub fn failure(id: Option<RpcId>, error: RpcError) -> Self {
            Self {
                id,
                outcome: Err(error),
            }
        }
    }
    impl TryFrom<ResponseWire> for RpcResponse {
        type Error = ValidationError;
        fn try_from(wire: ResponseWire) -> Result<Self, Self::Error> {
            let (jsonrpc, id, outcome) = match wire {
                ResponseWire::Success {
                    jsonrpc,
                    id,
                    result,
                } => (jsonrpc, Some(id), Ok(result)),
                ResponseWire::Failure { jsonrpc, id, error } => (jsonrpc, id, Err(error)),
            };
            if jsonrpc != "2.0" {
                return Err(ValidationError("response jsonrpc must be 2.0"));
            }
            Ok(Self { id, outcome })
        }
    }
    impl From<RpcResponse> for ResponseWire {
        fn from(response: RpcResponse) -> Self {
            match response.outcome {
                Ok(result) => ResponseWire::Success {
                    jsonrpc: "2.0".into(),
                    id: response.id.expect("successful response has request ID"),
                    result,
                },
                Err(error) => ResponseWire::Failure {
                    jsonrpc: "2.0".into(),
                    id: response.id,
                    error,
                },
            }
        }
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct VersionInfo {
        pub version: String,
        pub protocol: String,
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct SchemaStatus {
        pub version: i64,
        pub expected_version: i64,
        pub compatible: bool,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pub pending_changes: Vec<i32>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pub unavailable_paths: Vec<String>,
    }
    #[derive(Clone, Debug, Serialize)]
    #[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
    pub enum WriterOwnership {
        Owned,
        Unavailable { reason: String },
    }
    impl<'de> Deserialize<'de> for WriterOwnership {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            #[derive(Deserialize)]
            #[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
            enum Wire {
                Owned {},
                Unavailable { reason: String },
            }
            Ok(match Wire::deserialize(deserializer)? {
                Wire::Owned {} => Self::Owned,
                Wire::Unavailable { reason } => Self::Unavailable { reason },
            })
        }
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Status {
        pub schema: SchemaStatus,
        pub writer: WriterOwnership,
        pub commit_seq: CommitSeq,
        /// Sources with an enabled collector.
        pub supported_sources: Vec<String>,
        pub supported_methods: Vec<String>,
        #[serde(default)]
        pub replays: Vec<sinex_model::replay::Outcome>,
        #[serde(default)]
        pub replay_authorized: bool,
        #[serde(default)]
        pub indexed_retrieval: Value,
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct CatalogEntry {
        pub method: Method,
        pub description: String,
    }
    pub fn catalog() -> Vec<CatalogEntry> {
        Method::ALL
            .iter()
            .map(|&method| CatalogEntry {
                method,
                description: method.description().into(),
            })
            .collect()
    }
}
pub use rpc::*;

pub use sinex_model::replay::{Operation as ReplayOperation, fresh_action_id};

pub use sinex_model::exclusion::{
    Outcome as ExclusionOutcome, Request as ExclusionRequest, State as ExclusionState,
};
