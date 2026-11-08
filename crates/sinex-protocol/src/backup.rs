//! Backup requests from the daemon to a producer. A token belongs to one
//! producer process run; a restarted producer refuses its predecessor's token.
use serde::{Deserialize, Serialize};
use sinex_model::backup::{File, Hold};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Pause {
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
    Paused {
        token: String,
        run_id: String,
    },
    Captured {
        token: String,
        run_id: String,
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
