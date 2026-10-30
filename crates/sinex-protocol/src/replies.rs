//! Ordered, authenticated-inbox JSON reply framing. No frame represents a receipt on its own.
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplyFrame {
    Data {
        stream: String,
        sequence: u64,
        offset: u64,
        data: String,
    },
    End {
        stream: String,
        sequence: u64,
        total: u64,
        hash: String,
    },
    Abort {
        stream: String,
        code: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FlowAck {
    Accepted { stream: String, sequence: u64 },
    Cancel { stream: String },
}
