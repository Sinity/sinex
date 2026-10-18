//! The three clocks (architecture §3.2). `ts_orig` is when the thing happened by
//! the source's own evidence, with a quality; unknown stays unknown ("never
//! falsify a clock"). `ts_coided` comes from the event ID; `ts_persisted` is set
//! by the database at commit.
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, crate::DbCheck)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
mod tests {
    use super::*;

    #[test]
    fn timestamp_round_trips_nanoseconds() {
        let t = Timestamp::from_unix_ns(1_792_339_243_093_191_950).unwrap();
        let text = serde_json::to_string(&t).unwrap();
        assert_eq!(text, "\"2026-10-18T16:00:43.09319195Z\"");
        let back: Timestamp = serde_json::from_str(&text).unwrap();
        assert_eq!(back, t);
    }

    #[test]
    fn unknown_has_no_instant() {
        assert_eq!(
            TsOrig::maybe(None, TimeQuality::Intrinsic),
            TsOrig::unknown()
        );
    }
}
