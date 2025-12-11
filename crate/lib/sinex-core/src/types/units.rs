use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result as FmtResult};

/// Byte-count newtype that prevents unit mixups.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(transparent)]
pub struct Bytes(u64);

impl Bytes {
    /// Construct the newtype from a raw byte count.
    pub const fn from_bytes(value: u64) -> Self {
        Self(value)
    }

    /// Construct from mebibytes (MiB).
    pub const fn from_mebibytes(mib: u64) -> Self {
        Self(mib * 1024 * 1024)
    }

    /// Retrieve the underlying count in bytes.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Retrieve the count as `usize` (lossy on 32-bit platforms when > `usize::MAX`).
    pub fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl Display for Bytes {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{} bytes", self.0)
    }
}

impl From<u64> for Bytes {
    fn from(value: u64) -> Self {
        Self::from_bytes(value)
    }
}

impl From<Bytes> for u64 {
    fn from(value: Bytes) -> Self {
        value.0
    }
}

impl From<Bytes> for usize {
    fn from(value: Bytes) -> Self {
        value.as_usize()
    }
}

/// Second-count newtype that encodes duration semantics explicitly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(transparent)]
pub struct Seconds(u64);

impl Seconds {
    /// Construct from a raw number of seconds.
    pub const fn from_secs(value: u64) -> Self {
        Self(value)
    }

    /// Retrieve the underlying second count.
    pub const fn as_secs(self) -> u64 {
        self.0
    }
}

impl Display for Seconds {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}s", self.0)
    }
}

impl From<u64> for Seconds {
    fn from(value: u64) -> Self {
        Self::from_secs(value)
    }
}

impl From<Seconds> for u64 {
    fn from(value: Seconds) -> Self {
        value.0
    }
}
