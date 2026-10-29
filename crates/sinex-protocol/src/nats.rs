//! What travels over NATS: the ingress where outside producers send materials
//! and events, their acknowledgments, and the confirmed stream of committed
//! events in commit order.
//!
//! Subjects, all under one root (`sinex` when deployed):
//!
//! | Subject | Who publishes | What |
//! |---|---|---|
//! | `<root>.ingress.<producer>` | the producer | an [`Ingress`] frame, into the ingress stream |
//! | `<root>.ack.<producer>` | `sinexd` | the [`Acknowledgment`] of each frame |
//! | `<root>.recorded.<producer>` | the producer, as a request | [`RecordedQuery`]: what Sinex recorded for these frames |
//! | `<root>.materials.<producer>` | the producer, as a request | [`Materials`]: a page of the materials it registered |
//! | `<root>.excise.<producer>` | the producer, as a request | [`Excise`]: excise whole materials it registered |
//! | `<root>.confirmed.event.<class>.<event type>` | `sinexd` | a [`Confirmed::Event`] |
//! | `<root>.confirmed.lifecycle.<class>.<event type>` | `sinexd` | a [`Confirmed::Archived`], [`Confirmed::Unhidden`] or [`Confirmed::Excised`] message |
//!
//! Each producer's credentials allow only its own subjects
//! ([`Subjects::producer_permissions`]), so the producer named in a subject is
//! the producer that sent it.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::{Class, EventId, InputRef, MaterialId, RecordLocation, RunId, Timestamp};

/// Subject and stream names under one root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subjects {
    root: String,
}

impl Subjects {
    /// `root` is a subject token: `sinex` when deployed, a unique prefix in
    /// tests.
    pub fn new(root: impl Into<String>) -> Self {
        Self { root: root.into() }
    }

    pub fn sinex() -> Self {
        Self::new("sinex")
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    /// A stream name derived from the root: `SINEX_<NAME>`.
    fn stream(&self, name: &str) -> String {
        let root: String = self
            .root
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_uppercase()
                } else {
                    '_'
                }
            })
            .collect();
        format!("{root}_{name}")
    }

    pub fn confirmed_stream(&self) -> String {
        self.stream("CONFIRMED")
    }

    pub fn ingress_stream(&self) -> String {
        self.stream("INGRESS")
    }

    pub fn confirmed_event(&self, class: &str, event_type: &str) -> String {
        format!("{}.confirmed.event.{class}.{event_type}", self.root)
    }
    pub fn confirmed_lifecycle(&self, class: &str, event_type: &str) -> String {
        format!("{}.confirmed.lifecycle.{class}.{event_type}", self.root)
    }
    pub fn confirmed_control(&self) -> String {
        format!("{}.confirmed.control", self.root)
    }
    pub fn confirmed_consumer(&self, producer: &str) -> String {
        format!("{}.confirmed_consumer.{producer}", self.root)
    }
    pub fn confirmed_consumer_all(&self) -> String {
        format!("{}.confirmed_consumer.*", self.root)
    }
    pub fn consumer_name(&self, scope: &str) -> String {
        format!("sinex-{scope}")
    }
    pub fn inbox_prefix(&self, producer: &str) -> String {
        format!("_INBOX.{producer}")
    }

    /// Every confirmed-stream subject.
    pub fn confirmed_all(&self) -> String {
        format!("{}.confirmed.>", self.root)
    }

    pub fn ingress(&self, producer: &str) -> String {
        format!("{}.ingress.{producer}", self.root)
    }

    pub fn ingress_all(&self) -> String {
        format!("{}.ingress.*", self.root)
    }

    pub fn ack(&self, producer: &str) -> String {
        format!("{}.ack.{producer}", self.root)
    }

    pub fn recorded(&self, producer: &str) -> String {
        format!("{}.recorded.{producer}", self.root)
    }

    pub fn recorded_all(&self) -> String {
        format!("{}.recorded.*", self.root)
    }

    pub fn connect(&self, producer: &str) -> String {
        format!("{}.connect.{producer}", self.root)
    }

    pub fn connect_all(&self) -> String {
        format!("{}.connect.*", self.root)
    }

    pub fn materials(&self, producer: &str) -> String {
        format!("{}.materials.{producer}", self.root)
    }

    pub fn materials_all(&self) -> String {
        format!("{}.materials.*", self.root)
    }

    pub fn replay_scope(&self, producer: &str) -> String {
        format!("{}.replay_scope.{producer}", self.root)
    }

    pub fn replay_scope_all(&self) -> String {
        format!("{}.replay_scope.*", self.root)
    }

    pub fn excise(&self, producer: &str) -> String {
        format!("{}.excise.{producer}", self.root)
    }

    pub fn excise_all(&self) -> String {
        format!("{}.excise.*", self.root)
    }

    /// The producer an ingress, have, materials or excise subject names, if
    /// it is one of ours and a valid producer name.
    pub fn producer_of<'a>(&self, subject: &'a str) -> Option<&'a str> {
        let rest = subject
            .strip_prefix(self.root.as_str())?
            .strip_prefix('.')?;
        let producer = rest
            .strip_prefix("ingress.")
            .or_else(|| rest.strip_prefix("connect."))
            .or_else(|| rest.strip_prefix("recorded."))
            .or_else(|| rest.strip_prefix("materials."))
            .or_else(|| rest.strip_prefix("excise."))
            .or_else(|| rest.strip_prefix("replay_scope."))
            .or_else(|| rest.strip_prefix("fetch_event."))
            .or_else(|| rest.strip_prefix("fetch_material."))
            .or_else(|| rest.strip_prefix("confirmed_consumer."))?;
        valid_producer(producer).then_some(producer)
    }

    pub fn fetch_event(&self, producer: &str) -> String {
        format!("{}.fetch_event.{producer}", self.root)
    }
    pub fn fetch_event_all(&self) -> String {
        format!("{}.fetch_event.*", self.root)
    }

    pub fn fetch_material(&self, producer: &str) -> String {
        format!("{}.fetch_material.{producer}", self.root)
    }
    pub fn fetch_material_all(&self) -> String {
        format!("{}.fetch_material.*", self.root)
    }

    /// What a producer's NATS user may publish and subscribe to: its own
    /// ingress, have, materials and excise subjects, its acknowledgments, and
    /// reply inboxes.
    pub fn producer_permissions(
        &self,
        producer: &str,
        scope: Option<&str>,
    ) -> (Vec<String>, Vec<String>) {
        let mut publish = vec![
            self.ingress(producer),
            self.connect(producer),
            self.recorded(producer),
            self.materials(producer),
            self.excise(producer),
            self.replay_scope(producer),
            self.fetch_event(producer),
            self.fetch_material(producer),
            self.confirmed_consumer(producer),
            format!("{}.>", self.inbox_prefix(producer)),
        ];
        if let Some(scope) = scope {
            let stream = self.confirmed_stream();
            let consumer = self.consumer_name(scope);
            publish.extend([
                format!("$JS.API.STREAM.INFO.{stream}"),
                format!("$JS.API.CONSUMER.INFO.{stream}.{consumer}"),
                format!("$JS.API.CONSUMER.MSG.NEXT.{stream}.{consumer}"),
                format!("$JS.ACK.{stream}.{consumer}.>"),
            ]);
        }
        (
            publish,
            vec![
                self.ack(producer),
                format!("{}.>", self.inbox_prefix(producer)),
            ],
        )
    }

    /// A NATS server `authorization` block: the daemon user with full access,
    /// and one user per producer limited to its own subjects, each given as
    /// its name and the bcrypt hash of its password, which the server checks
    /// passwords against.
    pub fn authorization(
        &self,
        daemon: (&str, &str),
        producers: &[(&str, &str)],
        consumers: &[(&str, String)],
    ) -> String {
        let quote = |s: &str| serde_json::to_string(s).expect("strings serialize");
        let list = |items: &[String]| {
            items
                .iter()
                .map(|s| quote(s))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut users = vec![format!(
            "{{ user: {}, password: {} }}",
            quote(daemon.0),
            quote(daemon.1)
        )];
        for (producer, password) in producers {
            let (publish, subscribe) = self.producer_permissions(
                producer,
                consumers
                    .iter()
                    .find(|(identity, _)| identity == producer)
                    .map(|(_, scope)| scope.as_str()),
            );
            users.push(format!(
                "{{ user: {}, password: {}, permissions: {{ publish: {{ allow: [{}] }}, subscribe: {{ allow: [{}] }} }} }}",
                quote(producer),
                quote(password),
                list(&publish),
                list(&subscribe)
            ));
        }
        format!(
            "authorization {{ users = [\n  {}\n] }}",
            users.join(",\n  ")
        )
    }
}

/// Producer names are one subject token: lowercase letters, digits, `-`, `_`.
pub fn valid_producer(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

/// One frame a producer sends. `frame` is the producer's own name for it,
/// unique per producer: sending the same frame again returns the recorded
/// acknowledgment instead of admitting anything twice.
///
/// A frame is one NATS message, so it fits the server's `max_payload`. Bytes
/// of any size reach Sinex through the frame's directory: written there
/// by a producer on the same host, or sent in [`Ingress::Chunk`]s by one that
/// is not. A producer splits many events across frames, and sends a frame too
/// large for one message as an [`Ingress::LargeFrame`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Ingress {
    RegisterKinds {
        frame: String,
        declarations: Vec<sinex_model::KindDeclaration>,
    },
    ReplaySequence {
        frame: String,
        sequence: Box<ProducerReplay>,
    },
    /// Register a material. Its bytes are in the content store already, or
    /// in this frame's directory, from where Sinex stores them before
    /// admitting the frame. Send it, and wait for its acknowledgment, before
    /// events that cite it.
    Material {
        frame: String,
        material: Box<MaterialFrame>,
    },
    Events {
        frame: String,
        events: Vec<ProducerEvent>,
        /// Records the producer's parser could not read. Their bytes stay in
        /// the cited material, so a replay can read them again.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        failures: Vec<ProducerFailure>,
    },
    /// Part of the bytes named `blob`, base64 in `data`, starting at `offset`
    /// of `total`, for the frame named `for_frame`: how a producer on another
    /// host fills that frame's directory. Chunks are sent in order;
    /// Sinex keeps what it has by offset and checks the bytes against their
    /// name when the last arrives, so chunks sent again under new frame names
    /// (a new attempt) change nothing they already delivered.
    Chunk {
        frame: String,
        for_frame: String,
        blob: String,
        offset: u64,
        total: u64,
        data: String,
    },
    /// A material or events frame too large for one message, written to this
    /// frame's directory as the bytes named `blob` (its JSON). Sinex
    /// handles it as that frame and records it under this frame's name.
    LargeFrame {
        frame: String,
        blob: String,
        length: u64,
    },
}

impl Ingress {
    pub fn frame(&self) -> &str {
        match self {
            Ingress::RegisterKinds { frame, .. }
            | Ingress::ReplaySequence { frame, .. }
            | Ingress::Material { frame, .. }
            | Ingress::Events { frame, .. }
            | Ingress::Chunk { frame, .. }
            | Ingress::LargeFrame { frame, .. } => frame,
        }
    }
}

/// Exact declarations a producer used while parsing a replacement.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeclarationHash {
    pub kind: sinex_model::KindRef,
    pub hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReplayScopeQuery {
    #[serde(default)]
    pub after: Option<MaterialId>,
    pub materials: Vec<MaterialId>,
    pub event_types: Option<Vec<String>>,
    pub from: Option<Timestamp>,
    pub until: Option<Timestamp>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReplaySelection {
    pub root: MaterialId,
    pub materials: Vec<ReplayMaterial>,
    pub rows: u64,
    pub expected_live_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayMaterial {
    pub id: MaterialId,
    pub blob: String,
    pub length: u64,
    pub extents: Vec<Vec<u8>>,
}

/// One completely parsed replacement. Acquisition can spool while publication
/// for this sequence waits for its atomic acknowledgment.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProducerReplay {
    pub operation: sinex_model::OperationId,
    pub root: MaterialId,
    pub materials: Vec<ReplayMaterial>,
    pub event_types: Option<Vec<String>>,
    pub from: Option<Timestamp>,
    pub until: Option<Timestamp>,
    pub declarations: Vec<DeclarationHash>,
    pub parser_build: String,
    pub parser_version: String,
    pub expected_live_hash: String,
    pub batches: Vec<ReplayBatch>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReplayBatch {
    pub events: Vec<ProducerEvent>,
    pub failures: Vec<ProducerFailure>,
}

/// A record a producer's parser could not read.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProducerFailure {
    pub location: RecordLocation,
    pub reason: String,
}

/// A material an outside producer read: what it was read from and its bytes,
/// named by their BLAKE3 hash.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MaterialFrame {
    pub id: MaterialId,
    /// What the material was read from.
    pub input: InputRef,
    /// `blake3:<hex>` of the bytes.
    pub blob: String,
    pub length: u64,
    pub parser: String,
    pub parser_version: u32,
    #[serde(default)]
    pub part_index: u32,
    #[serde(default)]
    pub previous_part: Option<MaterialId>,
    /// What the bytes are and how to read them.
    pub manifest: sinex_model::Manifest,
}

/// An event as a producer sends it: a registered event type and its payload,
/// with where it came from. Sinex decodes the payload into the type's Rust
/// form, so the type's identity, time and removal rules apply as for every
/// other source.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProducerEvent {
    pub kind: sinex_model::KindRef,
    pub payload: Value,
    /// Where in a registered material the record starts. Absent for an event
    /// derived from `parents`.
    #[serde(default)]
    pub location: Option<RecordLocation>,
    /// Output number when one record yields several events.
    #[serde(default)]
    pub output_index: u64,
    #[serde(default)]
    pub parents: Vec<EventId>,
    /// An explicit source order, or `None` for a writer-assigned order for this stable record key.
    #[serde(default)]
    pub reading_order: Option<i64>,
    #[serde(default)]
    pub class: Class,
}

/// What Sinex recorded for one frame. Persisted with the frame's events and
/// returned again for the same frame.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Acknowledgment {
    #[serde(default)]
    pub number: Option<i64>,
    pub frame: String,
    /// The material a material frame registered.
    pub material: Option<MaterialId>,
    /// Committed encoded segment descriptors for the acknowledged material.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub material_extents: Vec<Vec<u8>>,
    /// Events admitted as new or newer readings.
    pub admitted: Vec<EventId>,
    /// Counts by what admission did: admitted, repeats, replaced, older,
    /// hidden, superseded.
    pub outcome: Value,
    /// Why the whole frame was refused; a refusal is never an empty success.
    pub refusal: Option<String>,
    /// The last commit position of the frame's events.
    pub commit_seq: Option<i64>,
}

impl Acknowledgment {
    pub fn refusal_kind(&self) -> Option<sinex_model::FrameRefusal> {
        serde_json::from_value(self.outcome["refusal_kind"].clone()).ok()
    }
}

/// Payload reads use the authenticated subject identity, never an identity from the body.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FetchEvent {
    pub event: EventId,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FetchRefusalKind {
    Denied,
    Missing,
    Locked,
    Invalid,
    Unavailable,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct FetchEventReply {
    pub event: Option<ConfirmedEvent>,
    pub refusal: Option<String>,
    pub refusal_kind: Option<FetchRefusalKind>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FetchMaterial {
    pub material: MaterialId,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct FetchMaterialReply {
    pub material: Option<MaterialFrame>,
    pub extents: Vec<Vec<u8>>,
    pub refusal: Option<String>,
    pub refusal_kind: Option<FetchRefusalKind>,
}
/// A broadcast is only a wakeup hint. Progress requires the complete verified receipt.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiptHint {
    pub number: i64,
    pub frame_hash: String,
}

pub const PRODUCER_VERSION: u32 = 2;

/// Checked before any material or events are sent. Event declarations can
/// be registered here before checking the names the producer will emit.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProducerHello {
    pub version: u32,
    pub event_types: Vec<String>,
    pub class: Class,
}

impl Default for ProducerHello {
    fn default() -> Self {
        Self {
            version: PRODUCER_VERSION,
            event_types: Vec::new(),
            class: Class::Open,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProducerWelcome {
    pub refusal: Option<String>,
    #[serde(default)]
    pub family: Option<String>,
    #[serde(default)]
    pub canonical_publisher: bool,
}

/// A request on `<root>.recorded.<producer>`: which of these frames Sinex has
/// recorded. The reply is a framed [`RecordedPage`]. Continue after its
/// frontier to retrieve every matching receipt; missing frames were not received.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RecordedQuery {
    #[serde(default)]
    pub frames: Vec<String>,
    #[serde(default)]
    pub numbers: Vec<i64>,
    #[serde(default)]
    pub after: i64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct RecordedPage {
    pub acknowledgments: Vec<Acknowledgment>,
    pub next: Option<i64>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct MaterialsPage {
    pub materials: Vec<ProducerMaterial>,
    pub next: Option<i64>,
}

/// A request on `<root>.materials.<producer>`: the materials this producer
/// registered and Sinex still holds, in the order Sinex recorded them,
/// starting after the frame numbered `after`. A producer that keeps nothing of
/// its own rebuilds what it sent from these pages. The framed [`MaterialsPage`]
/// contains at most `limit` materials and the next frontier.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Materials {
    #[serde(default)]
    pub after: i64,
    pub limit: u32,
}

/// One registered material, as its frame described it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProducerMaterial {
    /// The order Sinex recorded the frame in; the next page starts after it.
    pub number: i64,
    pub frame: String,
    pub material: MaterialFrame,
    /// Committed encoded segment descriptors; these authorize reads from the shared store.
    pub extents: Vec<Vec<u8>>,
}

/// A request on `<root>.excise.<producer>`: permanently delete whole
/// materials this producer registered, every event read from them and every
/// event derived from those, through Sinex's one excision with the producer
/// as the actor. Each material goes whole, its bytes and its rows; only its
/// digest is kept, so the same bytes are refused if they are sent again.
/// Refused, with nothing changed, when a material was registered by anyone
/// else, is sealed, or is followed by a later part not in the request. Sending it again finishes an interrupted one. The reply is an
/// [`ExciseReply`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Excise {
    pub materials: Vec<MaterialId>,
}

/// What an [`Excise`] request removed, or why it removed nothing.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ExciseReply {
    pub materials: u64,
    pub events: u64,
    pub refusal: Option<String>,
}

/// One message on the confirmed stream.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Confirmed {
    /// Every change through this position has been delivered. Only this boundary advances a scalar consumer.
    PositionComplete {
        commit_seq: i64,
    },
    /// Historical evidence cannot establish an exact position; consumers report a gap without advancing.
    Gap {
        reason: String,
    },
    Event(Box<ConfirmedEvent>),
    /// Metadata exceeding a transfer unit is fetched under the same scoped grants.
    EventReference {
        commit_seq: i64,
        event: EventId,
    },
    Archived(Archived),
    /// A previously hidden reading is live again at a new position.
    Unhidden(Unhidden),
    /// The event was excised: its message has been erased from the stream,
    /// and a consumer drops anything it kept of it.
    Excised {
        /// Historical tombstones may predate recorded commit boundaries.
        #[serde(default)]
        commit_seq: Option<i64>,
        /// The removed reading's original position, when retained historical evidence establishes it.
        #[serde(default)]
        original_commit_seq: Option<i64>,
        event: EventId,
        operation: sinex_model::OperationId,
    },
}

/// Committed metadata on a class-and-kind-scoped stream topic. Shared-stream
/// payloads are fetched under the authenticated consumer's grants.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConfirmedEvent {
    pub kind_version: u64,
    pub validation: sinex_model::Validation,
    pub commit_seq: i64,
    pub id: EventId,
    pub event_type: String,
    pub source: String,
    pub run: RunId,
    pub location: Option<RecordLocation>,
    pub output_index: u64,
    pub parents: Vec<EventId>,
    pub ts_orig: Option<Timestamp>,
    pub ts_quality: String,
    pub ts_coided: Option<Timestamp>,
    pub ts_persisted: Timestamp,
    pub record_state: String,
    pub class: Class,
    pub equivalence_key: Option<String>,
    /// Which of two readings of one record is newer.
    pub reading_order: i64,
    /// The BLAKE3 of the record as read, in hex: what tells a repeat from a
    /// changed reading, so a consumer can check its copy against Sinex's.
    pub record_hash: String,
    pub payload: Option<Value>,
    /// Why `payload` is absent, when it is. The event itself is stored whole;
    /// read it from Sinex by ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub withheld: Option<Withheld>,
}

/// Why a confirmed-stream message carries no payload.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum Withheld {
    /// Shared stream payloads are fetched under the authenticated consumer grants.
    Grants,
}

/// A reading taken out of the live set.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Archived {
    pub change_seq: i64,
    pub event: EventId,
    /// The commit position at which it was archived.
    pub archived_seq: i64,
    pub reason: sinex_model::ArchiveReason,
    pub replaced_by: Option<EventId>,
    pub operation: Option<sinex_model::OperationId>,
}

/// Restoration changes visibility; it does not admit a second Event.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Unhidden {
    pub change_seq: i64,
    pub event: EventId,
    pub unhidden_seq: i64,
    pub operation: Option<sinex_model::OperationId>,
}

#[cfg(test)]
mod tests;

/// The daemon selects and provisions the authenticated identity's scoped consumer.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConfirmedConsumer {
    pub after: i64,
    pub cache: String,
    #[serde(default)]
    pub status_only: bool,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfirmedConsumerReply {
    #[serde(default)]
    pub position: Option<i64>,
    pub stream: String,
    pub consumer: String,
    pub scope: String,
    pub refusal: Option<String>,
    pub gap: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ReplayScopePage {
    pub sequences: Vec<ReplaySelection>,
    pub next: Option<MaterialId>,
}
