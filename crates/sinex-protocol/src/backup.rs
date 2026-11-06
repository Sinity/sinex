//! Producer publication cuts. A token belongs to one running participant incarnation.
use serde::{Deserialize, Serialize};
use sinex_model::backup::{File, Hold};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Freeze {
        token: String,
    },
    Capture {
        token: String,
        boundary: Hold,
    },
    Read {
        token: String,
        file: usize,
        offset: u64,
    },
    Release {
        token: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Reply {
    Frozen {
        token: String,
        incarnation: String,
    },
    Captured {
        token: String,
        incarnation: String,
        boundary: Hold,
        files: Vec<File>,
    },
    Bytes {
        offset: u64,
        data: Vec<u8>,
    },
    Released,
    Unavailable {
        reason: String,
    },
}
