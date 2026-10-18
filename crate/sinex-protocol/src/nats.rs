//! What travels over NATS (architecture §4.3): the ingress where outside
//! producers send materials and events, their acknowledgments, and the
//! confirmed stream of committed events in commit order.
//!
//! Subjects, all under one root (`sinex` when deployed):
//!
//! | Subject | Who publishes | What |
//! |---|---|---|
//! | `<root>.ingress.<producer>` | the producer | an [`Ingress`] frame, into the ingress stream |
//! | `<root>.ack.<producer>` | `sinexd` | the [`Acknowledgment`] of each frame |
//! | `<root>.have.<producer>` | the producer, as a request | [`Have`]: what Sinex recorded for these frames |
//! | `<root>.confirmed.event.<event type>` | `sinexd` | a [`Confirmed::Event`] |
//! | `<root>.confirmed.archive` | `sinexd` | a [`Confirmed::Archived`] or [`Confirmed::Excised`] notice |
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

    pub fn confirmed_event(&self, event_type: &str) -> String {
        format!("{}.confirmed.event.{event_type}", self.root)
    }

    pub fn confirmed_archive(&self) -> String {
        format!("{}.confirmed.archive", self.root)
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

    pub fn have(&self, producer: &str) -> String {
        format!("{}.have.{producer}", self.root)
    }

    pub fn have_all(&self) -> String {
        format!("{}.have.*", self.root)
    }

    /// The producer an ingress or have subject names, if it is one of ours
    /// and a valid producer name.
    pub fn producer_of<'a>(&self, subject: &'a str) -> Option<&'a str> {
        let rest = subject
            .strip_prefix(self.root.as_str())?
            .strip_prefix('.')?;
        let producer = rest
            .strip_prefix("ingress.")
            .or_else(|| rest.strip_prefix("have."))?;
        valid_producer(producer).then_some(producer)
    }

    /// What a producer's NATS user may publish and subscribe to: its own
    /// ingress and have subjects, its acknowledgments, and reply inboxes.
    pub fn producer_permissions(&self, producer: &str) -> (Vec<String>, Vec<String>) {
        (
            vec![self.ingress(producer), self.have(producer)],
            vec![self.ack(producer), "_INBOX.>".into()],
        )
    }

    /// A NATS server `authorization` block: the daemon user with full access,
    /// and one user per producer limited to its own subjects. Passwords come
    /// from the deployment's secrets.
    pub fn authorization(&self, daemon: (&str, &str), producers: &[(&str, &str)]) -> String {
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
            let (publish, subscribe) = self.producer_permissions(producer);
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
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

/// One frame a producer sends. `frame` is the producer's own name for it,
/// unique per producer: sending the same frame again returns the recorded
/// acknowledgment instead of admitting anything twice.
///
/// A frame is one NATS message, so it fits the server's `max_payload`. Bytes
/// of any size travel as a material placed in the content store; a producer
/// splits many events across frames.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Ingress {
    /// Register a material whose bytes the producer already placed in the
    /// content store. Send it, and wait for its acknowledgment, before
    /// events that cite it.
    Material {
        frame: String,
        material: Box<MaterialFrame>,
    },
    Events {
        frame: String,
        events: Vec<ProducerEvent>,
    },
}

impl Ingress {
    pub fn frame(&self) -> &str {
        match self {
            Ingress::Material { frame, .. } | Ingress::Events { frame, .. } => frame,
        }
    }
}

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
    /// Lossless description: path bytes, envelopes, media type, charset.
    pub manifest: Value,
}

/// An event as a producer sends it: a registered event type and its payload,
/// with where it came from. Sinex decodes the payload into the type's Rust
/// form, so the type's identity, time and removal rules apply as for every
/// other source.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProducerEvent {
    pub event_type: String,
    pub payload: Value,
    /// Where in a registered material the record starts. Absent for an event
    /// derived from `parents`.
    #[serde(default)]
    pub location: Option<RecordLocation>,
    /// Output number when one record yields several events.
    #[serde(default)]
    pub ordinal: u16,
    #[serde(default)]
    pub parents: Vec<EventId>,
    /// Which of two readings of one record is newer; only a newer reading
    /// replaces a live one.
    #[serde(default)]
    pub reading_order: i64,
    #[serde(default)]
    pub class: Class,
}

/// What Sinex recorded for one frame. Persisted with the frame's events and
/// returned again for the same frame.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Acknowledgment {
    pub frame: String,
    /// The material a material frame registered.
    pub material: Option<MaterialId>,
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

/// A request on `<root>.have.<producer>`: which of these frames Sinex has
/// recorded. The reply is a JSON array of [`Acknowledgment`]s, one per
/// recorded frame; a frame missing from it was not received.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Have {
    pub frames: Vec<String>,
}

/// One message on the confirmed stream.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Confirmed {
    Event(Box<ConfirmedEvent>),
    Archived(ArchiveNotice),
    /// The event was excised: its message has been erased from the stream,
    /// and a consumer drops anything it kept of it.
    Excised {
        event: EventId,
        operation: sinex_model::OperationId,
    },
}

/// A committed event. The payload is withheld (`None`) unless the event's
/// class is `open`: consumers of the stream hold no class grants.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConfirmedEvent {
    pub commit_seq: i64,
    pub id: EventId,
    pub event_type: String,
    pub source: String,
    pub run: RunId,
    pub location: Option<RecordLocation>,
    pub ordinal: u16,
    pub parents: Vec<EventId>,
    pub ts_orig: Option<Timestamp>,
    pub ts_quality: String,
    pub ts_coided: Option<Timestamp>,
    pub ts_persisted: Timestamp,
    pub record_state: String,
    pub class: Class,
    pub equivalence_key: Option<String>,
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
    /// The event's class is `guarded` or `sealed`.
    Class,
    /// The message with the payload would exceed the NATS server's message
    /// size limit (`max_payload`).
    Size { payload_bytes: u64, limit: u64 },
}

/// A reading taken out of the live set.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ArchiveNotice {
    pub notice_seq: i64,
    pub event: EventId,
    /// The commit position at which it was archived.
    pub archived_seq: i64,
    /// `newer_reading`, `hide`, `excise`, `replay` or `version_change`.
    pub reason: String,
    pub replaced_by: Option<EventId>,
    pub operation: Option<sinex_model::OperationId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subjects_and_streams_follow_the_root() {
        let s = Subjects::sinex();
        assert_eq!(s.ingress("kitty"), "sinex.ingress.kitty");
        assert_eq!(
            s.confirmed_event("command.executed"),
            "sinex.confirmed.event.command.executed"
        );
        assert_eq!(s.confirmed_stream(), "SINEX_CONFIRMED");
        let t = Subjects::new("t123x9f");
        assert_eq!(t.ingress_stream(), "T123X9F_INGRESS");
        assert_eq!(t.producer_of("t123x9f.ingress.kitty"), Some("kitty"));
        assert_eq!(t.producer_of("t123x9f.have.kitty"), Some("kitty"));
        assert_eq!(t.producer_of("t123x9f.ingress.Kitty"), None);
        assert_eq!(t.producer_of("other.ingress.kitty"), None);
        assert_eq!(t.producer_of("t123x9f.ack.kitty"), None);
    }

    #[test]
    fn producer_permissions_name_only_its_own_subjects() {
        let s = Subjects::sinex();
        let (publish, subscribe) = s.producer_permissions("phone");
        assert_eq!(publish, ["sinex.ingress.phone", "sinex.have.phone"]);
        assert_eq!(subscribe, ["sinex.ack.phone", "_INBOX.>"]);
        let config = s.authorization(("sinexd", "pw"), &[("phone", "p\"w")]);
        assert!(
            config.contains(r#"user: "phone", password: "p\"w""#),
            "{config}"
        );
        assert!(
            config.contains(r#"publish: { allow: ["sinex.ingress.phone", "sinex.have.phone"] }"#)
        );
    }

    #[test]
    fn frames_round_trip_with_defaults() {
        let json = serde_json::json!({
            "kind": "events",
            "frame": "f1",
            "events": [{"event_type": "command.imported", "payload": {"command": "ls"}}]
        });
        let frame: Ingress = serde_json::from_value(json).unwrap();
        let Ingress::Events { frame, events } = frame else {
            panic!("events frame")
        };
        assert_eq!(frame, "f1");
        assert_eq!(events[0].ordinal, 0);
        assert_eq!(events[0].class, Class::Open);
        assert!(events[0].location.is_none());
    }
}
