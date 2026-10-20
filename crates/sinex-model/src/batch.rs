//! What producers hand the writer (architecture §4.1). A source batch carries
//! its new cursor and carried parser state, so events and progress commit in
//! one transaction: a cursor advances only when what it covers is stored.
use crate::{Event, EventId, InputId, MaterialId, OperationId, RecordLocation, RunId, Timestamp};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug)]
pub struct Batch {
    pub run: RunId,
    pub kind: BatchKind,
    pub records: Vec<RecordOutcome>,
    pub archive: Vec<ArchiveRequest>,
}

impl Batch {
    pub fn events(&self) -> impl Iterator<Item = &Event> {
        self.records.iter().flat_map(|r| match r {
            RecordOutcome::Events { events, .. } => events.as_slice(),
            RecordOutcome::Failure { .. } => &[],
        })
    }

    pub fn event_count(&self) -> usize {
        self.events().count()
    }
}

#[derive(Clone, Debug)]
pub enum BatchKind {
    Source(Box<SourceProgress>),
    Derived {
        derivation: String,
        scope: Value,
    },
    Api {
        actor: String,
    },
    /// One frame from an outside producer over the NATS ingress
    /// (architecture §4.3). Admission records the frame and its outcome in the
    /// same transaction as its events, which is the producer's persisted
    /// acknowledgment.
    Ingress(Box<IngressFrame>),
    /// New readings of stored materials, from a replay; the material is
    /// already registered, so there is no source progress.
    Replay {
        operation: OperationId,
    },
}

#[derive(Clone, Debug)]
pub struct IngressFrame {
    pub producer: String,
    /// The producer's own name for the frame, unique per producer.
    pub frame: String,
    /// A material frame registers its material like a source batch does.
    pub material: Option<SourceProgress>,
    /// Set when the frame was refused as a whole: it is recorded, with no
    /// events, so asking again gets the same answer.
    pub refusal: Option<String>,
}

/// One framed record's result: its events, or why the parser could not read it.
#[derive(Clone, Debug)]
pub enum RecordOutcome {
    Events {
        location: Option<RecordLocation>,
        events: Vec<Event>,
    },
    /// The raw bytes stay in the material, so a retry is a replay of exactly
    /// those bytes.
    Failure {
        location: RecordLocation,
        reason: String,
    },
}

/// Progress of one source binding, committed with the batch's events.
#[derive(Clone, Debug)]
pub struct SourceProgress {
    pub binding: String,
    pub input: InputRef,
    pub observation: ObservationFacts,
    pub revision: RevisionChange,
    /// The material this batch's records were read from, already written to
    /// the content store. `None` when the observation found nothing new.
    pub material: Option<MaterialRef>,
    /// Where the next read starts, in the driver's terms.
    pub cursor: Value,
    /// The parser's carried state at the cursor.
    pub carry: Value,
}

/// The thing observed: a file path, a database, an API collection, a socket.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputRef {
    pub kind: String,
    pub locator: String,
    pub host: String,
}

/// File facts from one look at an input.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationFacts {
    pub observed_at: Option<Timestamp>,
    pub size: Option<u64>,
    pub mtime_ns: Option<i128>,
    pub ctime_ns: Option<i128>,
    pub inode: Option<u64>,
    pub device: Option<u64>,
    /// Whether this look found a change; unchanged looks only advance a counter.
    pub changed: bool,
    /// Set when the input could not be read; the reason is shown in status.
    pub unavailable: Option<String>,
}

/// How the input's content relates to its previous revision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RevisionChange {
    /// Same revision; this batch appends to it or reads nothing new.
    Same,
    Appended,
    Rewritten,
    Replaced,
    Truncated,
    Snapshot,
}

/// Whether retained material contributes to the original input byte stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MaterialRole {
    Input,
    OriginalArchive,
    Records,
}

impl MaterialRole {
    /// Existing drivers without an explicit role store original input bytes.
    pub fn of(manifest: &Value) -> Result<Self, serde_json::Error> {
        manifest
            .get("role")
            .map(|value| serde_json::from_value(value.clone()))
            .unwrap_or(Ok(Self::Input))
    }
}

/// A material whose bytes are already in the content store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterialRef {
    pub id: MaterialId,
    pub input: Option<InputId>,
    /// `blake3:<hex>` of the bytes.
    pub blob: String,
    pub length: u64,
    pub part_index: u32,
    pub previous_part: Option<MaterialId>,
    pub parser: String,
    pub parser_version: u32,
    /// Lossless description: path bytes, envelopes, media type, charset.
    pub manifest: Value,
}

/// Requests that archive live readings in the same transaction. Excision is
/// not one of them: it rewrites material bytes, so it is its own writer job
/// (`sinex_store::excision`).
#[derive(Clone, Debug)]
pub enum ArchiveRequest {
    Hide {
        target: EventId,
        operation: OperationId,
    },
    Unhide {
        target: EventId,
        operation: OperationId,
    },
    /// Archive the live readings a replay re-reads, before its new readings
    /// are admitted in the same transaction (architecture §5.5).
    Replay {
        scope: ReplayScope,
        operation: OperationId,
    },
    /// The source no longer holds the records at these locations (a rewritten
    /// file without record IDs dropped them). Each live reading there gets a
    /// newer reading in state removed, through the ordinary identity rules.
    RemovedAtSource { locations: Vec<RecordLocation> },
}

/// Which live readings a replay re-reads: those read from `materials`,
/// optionally narrowed to some event types and a `ts_orig` window, and every
/// derived event that descends from them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayScope {
    pub materials: Vec<MaterialId>,
    pub event_types: Option<Vec<String>>,
    pub from: Option<Timestamp>,
    pub until: Option<Timestamp>,
}

impl ReplayScope {
    /// Whether a new reading belongs to the scope (its material is checked by
    /// the caller, which only re-reads in-scope materials).
    pub fn admits(&self, event: &Event) -> bool {
        if let Some(types) = &self.event_types
            && !types.iter().any(|t| t == event.event_type)
        {
            return false;
        }
        match (event.ts_orig.at, self.from, self.until) {
            (None, None, None) => true,
            (None, _, _) => false,
            (Some(at), from, until) => from.is_none_or(|f| at >= f) && until.is_none_or(|u| at < u),
        }
    }
}
