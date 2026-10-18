//! Wire types (architecture §2, §7.2): one command table for the CLI, the API
//! methods and the MCP tools, and the producer protocol for the NATS ingress.
//! Depends only on `sinex-model` and serde.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::{EventId, MaterialId};

/// One entry of the command table. The CLI, API and MCP surfaces are
/// generated from this list, so they cannot drift apart.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum Request {
    Status,
    Query {
        query: String,
    },
    Sql {
        sql: String,
    },
    Search {
        text: String,
    },
    Recall {
        around: String,
        window: Option<String>,
    },
    Show {
        reference: String,
    },
    Trace {
        reference: String,
    },
    Follow {
        query: String,
    },
    Stage {
        path: String,
        parser: Option<String>,
    },
    Replay {
        scope: Value,
    },
    Recompute {
        derivation: String,
        compare: bool,
    },
    Hide {
        target: EventId,
    },
    Unhide {
        target: EventId,
    },
    Excise {
        target: EventId,
    },
    Same {
        a: EventId,
        b: EventId,
    },
    Distinct {
        a: EventId,
        b: EventId,
    },
    Mark {
        text: String,
    },
}

impl Request {
    /// Whether the request writes. Writes need the agent or operator token;
    /// excision needs the operator's.
    pub fn writes(&self) -> bool {
        matches!(
            self,
            Request::Stage { .. }
                | Request::Replay { .. }
                | Request::Recompute { .. }
                | Request::Hide { .. }
                | Request::Unhide { .. }
                | Request::Excise { .. }
                | Request::Same { .. }
                | Request::Distinct { .. }
                | Request::Mark { .. }
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Reply {
    Ok {
        result: Value,
    },
    /// A refusal is never an empty success.
    Refused {
        reason: String,
    },
}

/// Producer protocol over the NATS ingress (architecture §4.3). A producer
/// sends a material frame and waits for the acknowledgment that it is
/// registered before sending events that cite it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Ingress {
    Material {
        id: MaterialId,
        blob: String,
        length: u64,
        manifest: Value,
    },
    Events {
        producer: String,
        events: Vec<Value>,
    },
}

/// Persisted and queryable: a producer can ask what Sinex holds (`mn8u`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Acknowledgment {
    pub material: Option<MaterialId>,
    pub events: Vec<EventId>,
    pub commit_seq: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_have_stable_wire_names() {
        let r = Request::Excise {
            target: EventId(uuid::Uuid::nil()),
        };
        assert_eq!(
            serde_json::to_value(&r).unwrap(),
            serde_json::json!({"method": "excise", "target": "00000000-0000-0000-0000-000000000000"})
        );
        assert!(r.writes());
        assert!(!Request::Status.writes());
    }
}
