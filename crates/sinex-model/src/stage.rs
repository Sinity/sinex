//! A stage's inventory, accounting and report, shared by planning and every output surface.
use crate::{Accounting, MaterialId, OperationId, lifecycle::OperationResult};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};
/// The container a file is, from its first bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Container {
    Stateless,
    Gzip,
    Zip,
    Tar,
    TarGzip,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "as", rename_all = "snake_case")]
pub enum FileReading {
    /// Read by these formats' bindings.
    Parsed { formats: Vec<String> },
    /// Stored as received with no parser. `candidates` names the formats that
    /// recognised it, or would each read a member of it, when more than one
    /// did.
    Unparsed { candidates: Vec<String> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryMember {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(flatten)]
    pub disposition: Disposition,
}

/// What became of one member. Every member is in exactly one of these.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "disposition", rename_all = "snake_case")]
pub enum Disposition {
    /// Read by these sources' bindings.
    Read { sources: Vec<String> },
    /// The binding meant to read it failed; its bytes stay stored.
    Failed {
        sources: Vec<String>,
        reason: String,
    },
    /// Kept in the stored original and read by nothing. `candidates` names
    /// the formats whose sources would each read it, when several would.
    StoredNotInterpreted { candidates: Vec<String> },
}

/// How far one product of one staged file has been read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductState {
    #[default]
    Queued,
    Read,
    Failed,
}

/// One product (a source) of one staged file, as reading goes.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Product {
    pub state: ProductState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub accounting: Accounting,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Report {
    pub operation: OperationId,
    pub result: Option<OperationResult>,
    pub inventory: Inventory,
    pub counts: Option<Counts>,
    #[serde(default)]
    pub sources: BTreeMap<String, SourceTotals>,
    #[serde(default)]
    pub failures: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Inventory {
    pub files: Vec<File>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct File {
    pub path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container: Option<Container>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    pub reading: FileReading,
    pub members: Vec<InventoryMember>,
    pub products: BTreeMap<String, Product>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub material: Option<MaterialId>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    pub files: usize,
    pub stored_without_parser: usize,
    pub accounting: Accounting,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceTotals {
    pub polls: usize,
    pub files_stored: u64,
    pub pass_decoded_bytes: u64,
    pub pass_record_bytes: u64,
    pub materials: usize,
    pub material_bytes: usize,
    pub batches: usize,
    pub accounting: Accounting,
    pub dirty_keys: usize,
    pub wall_s: f64,
    pub admission_s: f64,
    pub poll_s: f64,
    pub parse_s: f64,
    pub queue_s: f64,
    pub service_s: f64,
    pub admission_readings_per_s: f64,
    pub readings_per_s: f64,
    pub mb_per_s: f64,
}

/// Durable progress, including accounting committed with each binding's cursor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SavedProgress {
    #[serde(flatten)]
    pub inventory: Inventory,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub checkpoints: BTreeMap<String, Checkpoint>,
}
impl From<Inventory> for SavedProgress {
    fn from(inventory: Inventory) -> Self {
        Self {
            inventory,
            checkpoints: BTreeMap::new(),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub accounting: Accounting,
}

/// The stage list returned by the daemon.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Reports {
    pub stages: Vec<Report>,
}
