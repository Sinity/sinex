//! JSON-RPC boundary for the implemented foundation. Future product routes are not registered here.
pub mod rpc {
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use sinex_model::{CommitSeq, ValidationError};

    pub const SUPPORTED_METHODS: &[&str] = &["status", "version", "config.show", "catalog"];
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub enum Method {
        #[serde(rename = "status")]
        Status,
        #[serde(rename = "version")]
        Version,
        #[serde(rename = "config.show")]
        ConfigShow,
        #[serde(rename = "catalog")]
        Catalog,
    }
    impl Method {
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::Status => "status",
                Self::Version => "version",
                Self::ConfigShow => "config.show",
                Self::Catalog => "catalog",
            }
        }
        pub fn parse(value: &str) -> Option<Self> {
            match value {
                "status" => Some(Self::Status),
                "version" => Some(Self::Version),
                "config.show" => Some(Self::ConfigShow),
                "catalog" => Some(Self::Catalog),
                _ => None,
            }
        }
    }
    /// A foundation request always expects a response; notifications/batches are not supported.
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
            // All supported foundation methods currently take an empty object.
            if !self
                .params
                .as_object()
                .is_some_and(|params| params.is_empty())
            {
                return Err(RpcError::new(
                    ErrorKind::BadQuery,
                    "method requires empty object params",
                    Value::Null,
                ));
            }
            Ok(method)
        }
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

    /// Exactly one result/error is represented, and every response carries JSON-RPC 2.0.
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
        /// Enabled implementations, not the broader design catalog. Empty does not certify source coverage.
        pub supported_sources: Vec<String>,
        pub supported_methods: Vec<String>,
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct CatalogEntry {
        pub method: Method,
        pub description: String,
    }
    pub fn catalog() -> Vec<CatalogEntry> {
        [
            (
                Method::Status,
                "Database schema, writer ownership and supported scope",
            ),
            (Method::Version, "Package and protocol version"),
            (
                Method::ConfigShow,
                "Public configuration with secret file references",
            ),
            (Method::Catalog, "Implemented foundation methods"),
        ]
        .into_iter()
        .map(|(method, description)| CatalogEntry {
            method,
            description: description.into(),
        })
        .collect()
    }
}
pub use rpc::*;
