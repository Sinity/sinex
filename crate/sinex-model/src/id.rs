//! Identifiers. Every stored identity is a UUIDv7, minted when the thing is
//! created; event IDs are never derived from content (identity rule 1), so a
//! replay mints new ones.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ts_coided_is_the_minting_time() {
        let before = time::OffsetDateTime::now_utc();
        let id = EventId::new();
        let after = time::OffsetDateTime::now_utc();
        let coided = id.ts_coided().unwrap().0;
        // UUIDv7 keeps milliseconds.
        assert!(coided >= before - time::Duration::milliseconds(1));
        assert!(coided <= after);
    }

    #[test]
    fn ids_serialize_as_bare_uuids() {
        let id = RunId::new();
        assert_eq!(
            serde_json::to_value(id).unwrap(),
            serde_json::json!(id.0.to_string())
        );
    }
}
