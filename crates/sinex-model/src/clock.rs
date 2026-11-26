//! The three clocks. `ts_orig` is when the thing happened as the source
//! records it, with a quality; unknown stays unknown ("never falsify a clock").
//! `ts_coided` comes from the event ID; `ts_persisted` is set by the database
//! at commit.
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// How a payload records the end of an interval starting at `ts_orig`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IntervalEnd {
    At(&'static str),
    /// A missing end leaves only the recorded instant, with unknown duration.
    EndOrInstant(&'static str),
    AfterNs(&'static str),
}

impl IntervalEnd {
    /// A missing timestamp is an open interval; a missing duration is a moment.
    pub fn at(self, start: Timestamp, payload: &serde_json::Value) -> Option<Timestamp> {
        match self {
            Self::At(field) => payload
                .get(field)
                .and_then(|v| serde_json::from_value(v.clone()).ok()),
            Self::EndOrInstant(field) => Some(Self::At(field).at(start, payload).unwrap_or(start)),
            Self::AfterNs(field) => Timestamp::from_unix_ns(
                start.unix_ns()
                    + i128::from(
                        payload
                            .get(field)
                            .and_then(serde_json::Value::as_i64)
                            .unwrap_or(0)
                            .max(0),
                    ),
            )
            .ok(),
        }
    }

    /// The payload field the end is read from.
    pub fn field(self) -> &'static str {
        match self {
            Self::At(field) | Self::EndOrInstant(field) | Self::AfterNs(field) => field,
        }
    }

    /// The rule's name in the store, whose `core.interval_end` computes the
    /// end it keeps for each live interval.
    pub fn rule(self) -> &'static str {
        match self {
            Self::At(_) => "at",
            Self::EndOrInstant(_) => "end_or_instant",
            Self::AfterNs(_) => "after_ns",
        }
    }
}

/// An event type whose events last from `ts_orig` to an end their payload
/// records. A window names the interval kinds it reads by overlap; the store
/// keeps how far each live interval reaches so that read is indexed.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IntervalKind {
    pub event_type: &'static str,
    pub end: IntervalEnd,
    /// The payload field whose text value readers narrow this type by. The
    /// store indexes it, so a read of one value never visits the others.
    pub narrowed_by: Option<&'static str>,
    /// The payload field holding the time that bounds an end left unknown:
    /// the state cannot have held after it (a later boot of its host), so
    /// the interval reaches no window that starts later.
    pub bounded_by: Option<&'static str>,
}

/// Every interval kind. The store keeps how far these reach and no others; a
/// kind added here, or given a bound, needs a schema upgrade step that
/// brings the rows of its stored events up to date.
pub static INTERVAL_KINDS: &[IntervalKind] = &[
    IntervalKind::until("activity.session", "ended_at"),
    IntervalKind::until("activity.span", "ended_at"),
    IntervalKind::until("activity.window", "ended_at"),
    IntervalKind::lasting("command.executed", "duration_ns"),
    IntervalKind::lasting("command.fused", "duration_ns"),
    IntervalKind::lasting("command.reported", "duration_ns"),
    IntervalKind {
        narrowed_by: Some("category"),
        ..IntervalKind::until("health.record", "end")
    },
    IntervalKind::until("input.coverage", "ended_at"),
    IntervalKind::until("music.played", "ended_at"),
    IntervalKind::until("presence.interval", "ended_at"),
    IntervalKind::until("presence.span", "ended_at"),
    IntervalKind::until("shell.session", "ended_at"),
    IntervalKind {
        narrowed_by: Some("state_kind"),
        bounded_by: Some("fenced_at"),
        ..IntervalKind::until("state.interval", "ended_at")
    },
    IntervalKind::until("window.span", "ended_at"),
    IntervalKind {
        end: IntervalEnd::EndOrInstant("ended_at"),
        ..IntervalKind::until("work.act", "ended_at")
    },
];

impl IntervalKind {
    const fn until(event_type: &'static str, field: &'static str) -> Self {
        Self {
            event_type,
            end: IntervalEnd::At(field),
            narrowed_by: None,
            bounded_by: None,
        }
    }

    const fn lasting(event_type: &'static str, field: &'static str) -> Self {
        Self {
            end: IntervalEnd::AfterNs(field),
            ..Self::until(event_type, field)
        }
    }

    /// The declaration of `event_type`, when it is an interval kind.
    pub fn of(event_type: &str) -> Option<&'static Self> {
        INTERVAL_KINDS
            .iter()
            .find(|kind| kind.event_type == event_type)
    }

    /// The last instant an interval starting at `start` can reach: its
    /// recorded end, else the bound of an end left unknown. None while the
    /// interval is open, when it reaches every later window.
    pub fn reaches_until(
        &self,
        start: Timestamp,
        payload: &serde_json::Value,
    ) -> Option<Timestamp> {
        self.end
            .at(start, payload)
            .or_else(|| serde_json::from_value(payload.get(self.bounded_by?)?.clone()).ok())
    }
}

/// An instant with nanosecond precision, serialized as RFC 3339 in UTC.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub OffsetDateTime);

impl Timestamp {
    pub fn now() -> Self {
        Self(OffsetDateTime::now_utc())
    }

    pub fn from_unix_ns(ns: i128) -> Result<Self, time::error::ComponentRange> {
        OffsetDateTime::from_unix_timestamp_nanos(ns).map(Self)
    }

    pub fn from_unix_secs(secs: i64) -> Result<Self, time::error::ComponentRange> {
        OffsetDateTime::from_unix_timestamp(secs).map(Self)
    }

    pub fn unix_ns(&self) -> i128 {
        self.0.unix_timestamp_nanos()
    }
}

impl Serialize for Timestamp {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let text = self
            .0
            .to_offset(time::UtcOffset::UTC)
            .format(&Rfc3339)
            .map_err(serde::ser::Error::custom)?;
        s.serialize_str(&text)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        OffsetDateTime::parse(&text, &Rfc3339)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

impl schemars::JsonSchema for Timestamp {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Timestamp".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({"type": "string", "format": "date-time"})
    }
}

/// How `ts_orig` was obtained.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    schemars::JsonSchema,
    crate::DbCheck,
)]
#[serde(rename_all = "snake_case")]
pub enum TimeQuality {
    /// The source recorded the time of the thing itself.
    Intrinsic,
    /// Sinex saw it happen live and took the time at capture.
    Captured,
    /// Only the file's modification time is known.
    FileMtime,
    /// The operator stated it.
    Operator,
    /// Nothing says when it happened.
    Unknown,
}

/// When the thing happened. An unknown time has no instant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TsOrig {
    pub at: Option<Timestamp>,
    pub quality: TimeQuality,
}

impl TsOrig {
    pub fn known(at: Timestamp, quality: TimeQuality) -> Self {
        debug_assert!(quality != TimeQuality::Unknown);
        Self {
            at: Some(at),
            quality,
        }
    }

    pub fn unknown() -> Self {
        Self {
            at: None,
            quality: TimeQuality::Unknown,
        }
    }

    /// `known` when there is a time, `unknown` otherwise.
    pub fn maybe(at: Option<Timestamp>, quality: TimeQuality) -> Self {
        match at {
            Some(at) => Self::known(at, quality),
            None => Self::unknown(),
        }
    }
}

#[cfg(test)]
mod tests;
