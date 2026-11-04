//! Identifiers. Every stored identity is a UUIDv7, minted when the thing is
//! created; event IDs are never derived from content, so a replay mints new
//! ones.
use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! ids {
    ($($(#[$doc:meta])* $name:ident),* $(,)?) => {$(
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, schemars::JsonSchema)]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl $name {
            /// A new random UUIDv7 stamped with the current time.
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }
    )*};
}

ids! {
    /// One reading of a record, or one derived result.
    EventId,
    /// One execution of a producer.
    RunId,
    /// Immutable stored bytes that events cite.
    MaterialId,
    /// Something Sinex observes repeatedly.
    InputId,
    /// One look at an input.
    ObservationId,
    /// An input's content as of one observation that found a change.
    RevisionId,
    /// Something the operator or an agent did to Sinex.
    OperationId,
}

impl EventId {
    /// When Sinex made this reading (`ts_coided`): the time inside the UUIDv7.
    /// Every `EventId` is minted as a v7, so this never invents a time.
    pub fn ts_coided(&self) -> Option<crate::Timestamp> {
        let (secs, nanos) = self.0.get_timestamp()?.to_unix();
        let ns = i128::from(secs) * 1_000_000_000 + i128::from(nanos);
        crate::Timestamp::from_unix_ns(ns).ok()
    }
}

/// The millisecond a UUIDv7 in text form was minted in, in Unix
/// milliseconds; none for text that is not a UUIDv7.
pub fn uuid7_millisecond(text: &str) -> Option<i64> {
    let id = Uuid::parse_str(text).ok()?;
    if id.get_version_num() != 7 {
        return None;
    }
    let (secs, nanos) = id.get_timestamp()?.to_unix();
    i64::try_from(secs)
        .ok()?
        .checked_mul(1000)?
        .checked_add(i64::from(nanos / 1_000_000))
}

/// The span a start-minted UUIDv7 token is alive for, from a record that
/// holds it: from the token's mint millisecond until the record's time, or
/// until the mint time plus the record's `length` field (in nanoseconds)
/// when that is later. A record holding the same token lies inside this
/// span, widened by `slack_ns` on both sides; a read for a token's partners
/// stops at its bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TokenLifetime {
    /// The top-level payload field holding how long the occurrence lasted.
    pub length: Option<&'static str>,
    pub slack_ns: i128,
}

impl TokenLifetime {
    /// The earliest and latest Unix nanosecond a partner of `token`, held by
    /// a record at `at_ns` with this payload, can lie at.
    pub fn bounds(&self, token: &str, at_ns: i128, payload: &serde_json::Value) -> (i128, i128) {
        let start = uuid7_millisecond(token)
            .map_or(at_ns, |ms| i128::from(ms) * 1_000_000)
            .min(at_ns);
        let length = self
            .length
            .and_then(|field| payload.get(field)?.as_i64())
            .map_or(0, |ns| i128::from(ns.max(0)));
        let end = at_ns.max(start + length);
        (start - self.slack_ns, end + self.slack_ns)
    }
}

#[cfg(test)]
mod tests;
