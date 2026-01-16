use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::str::FromStr;

use crate::types::validation::ValidationError;

/// Byte-count newtype that prevents unit mixups.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(transparent)]
pub struct Bytes(u64);

impl Bytes {
    /// Maximum allowed value: 1 GiB
    pub const MAX: Bytes = Bytes(1024 * 1024 * 1024);

    /// Construct the newtype from a raw byte count.
    pub const fn from_bytes(value: u64) -> Self {
        Self(value)
    }

    /// Construct from mebibytes (MiB).
    pub const fn from_mebibytes(mib: u64) -> Self {
        Self(mib * 1024 * 1024)
    }

    /// Construct from kibibytes (KiB).
    pub const fn from_kibibytes(kib: u64) -> Self {
        Self(kib * 1024)
    }

    /// Construct from gibibytes (GiB).
    pub const fn from_gibibytes(gib: u64) -> Self {
        Self(gib * 1024 * 1024 * 1024)
    }

    /// Retrieve the underlying count in bytes.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Retrieve the count as `usize` (lossy on 32-bit platforms when > `usize::MAX`).
    pub fn as_usize(self) -> usize {
        self.0 as usize
    }

    /// Validate that value is within acceptable range.
    ///
    /// Returns an error if the value exceeds the maximum of 1 GiB.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.0 > Self::MAX.0 {
            return Err(ValidationError::General(format!(
                "Bytes value {} exceeds maximum of {} (1 GiB)",
                self.0, Self::MAX.0
            )));
        }
        Ok(())
    }

    /// Create from bytes with validation.
    ///
    /// Returns an error if the value exceeds the maximum of 1 GiB.
    pub fn from_bytes_validated(bytes: u64) -> Result<Self, ValidationError> {
        let b = Self::from_bytes(bytes);
        b.validate()?;
        Ok(b)
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

impl FromStr for Bytes {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>().map(Bytes::from_bytes)
    }
}

/// Second-count newtype that encodes duration semantics explicitly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(transparent)]
pub struct Seconds(u64);

impl Seconds {
    /// Maximum allowed value: 24 hours (86400 seconds)
    pub const MAX: Seconds = Seconds(86400);

    /// Construct from a raw number of seconds.
    pub const fn from_secs(value: u64) -> Self {
        Self(value)
    }

    /// Construct from milliseconds.
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis / 1000)
    }

    /// Construct from minutes.
    pub const fn from_minutes(minutes: u64) -> Self {
        Self(minutes * 60)
    }

    /// Construct from hours.
    pub const fn from_hours(hours: u64) -> Self {
        Self(hours * 3600)
    }

    /// Retrieve the underlying second count.
    pub const fn as_secs(self) -> u64 {
        self.0
    }

    /// Convert to standard Duration
    pub const fn as_duration(self) -> std::time::Duration {
        std::time::Duration::from_secs(self.0)
    }

    /// Validate that value is within acceptable range.
    ///
    /// Returns an error if the value exceeds the maximum of 86400 seconds (24 hours).
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.0 > Self::MAX.0 {
            return Err(ValidationError::General(format!(
                "Seconds value {} exceeds maximum of {} (24 hours)",
                self.0, Self::MAX.0
            )));
        }
        Ok(())
    }

    /// Create from seconds with validation.
    ///
    /// Returns an error if the value exceeds the maximum of 86400 seconds (24 hours).
    pub fn from_secs_validated(secs: u64) -> Result<Self, ValidationError> {
        let s = Self::from_secs(secs);
        s.validate()?;
        Ok(s)
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

impl FromStr for Seconds {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>().map(Seconds::from_secs)
    }
}

/// Millisecond-count newtype that encodes duration semantics explicitly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(transparent)]
pub struct Milliseconds(u64);

impl Milliseconds {
    /// Construct from a raw number of milliseconds.
    pub const fn from_millis(value: u64) -> Self {
        Self(value)
    }

    /// Retrieve the underlying millisecond count.
    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// Convert to standard Duration
    pub const fn as_duration(self) -> std::time::Duration {
        std::time::Duration::from_millis(self.0)
    }
}

impl Display for Milliseconds {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}ms", self.0)
    }
}

impl From<u64> for Milliseconds {
    fn from(value: u64) -> Self {
        Self::from_millis(value)
    }
}

impl From<Milliseconds> for u64 {
    fn from(value: Milliseconds) -> Self {
        value.0
    }
}

impl FromStr for Milliseconds {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>().map(Milliseconds::from_millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seconds_validation_valid() {
        // Valid values within range
        assert!(Seconds::from_secs(0).validate().is_ok());
        assert!(Seconds::from_secs(30).validate().is_ok());
        assert!(Seconds::from_secs(3600).validate().is_ok());
        assert!(Seconds::from_secs(86400).validate().is_ok()); // Exactly 24 hours
    }

    #[test]
    fn test_seconds_validation_invalid() {
        // Invalid values exceeding maximum
        assert!(Seconds::from_secs(86401).validate().is_err());
        assert!(Seconds::from_secs(100000).validate().is_err());
        assert!(Seconds::from_secs(1000000).validate().is_err());
    }

    #[test]
    fn test_seconds_from_validated() {
        // Valid construction
        assert!(Seconds::from_secs_validated(30).is_ok());
        assert!(Seconds::from_secs_validated(86400).is_ok());

        // Invalid construction
        assert!(Seconds::from_secs_validated(86401).is_err());
        assert!(Seconds::from_secs_validated(1000000).is_err());
    }

    #[test]
    fn test_seconds_helper_constructors() {
        assert_eq!(Seconds::from_millis(5000).as_secs(), 5);
        assert_eq!(Seconds::from_minutes(5).as_secs(), 300);
        assert_eq!(Seconds::from_hours(2).as_secs(), 7200);
    }

    #[test]
    fn test_bytes_validation_valid() {
        // Valid values within range
        assert!(Bytes::from_bytes(0).validate().is_ok());
        assert!(Bytes::from_bytes(1024).validate().is_ok());
        assert!(Bytes::from_mebibytes(100).validate().is_ok());
        assert!(Bytes::from_mebibytes(1024).validate().is_ok()); // Exactly 1 GiB
        assert!(Bytes::from_gibibytes(1).validate().is_ok()); // Exactly 1 GiB
    }

    #[test]
    fn test_bytes_validation_invalid() {
        // Invalid values exceeding maximum (> 1 GiB)
        assert!(Bytes::from_mebibytes(1025).validate().is_err());
        assert!(Bytes::from_gibibytes(2).validate().is_err());
        assert!(Bytes::from_bytes(2 * 1024 * 1024 * 1024).validate().is_err());
    }

    #[test]
    fn test_bytes_from_validated() {
        // Valid construction
        assert!(Bytes::from_bytes_validated(1024).is_ok());
        assert!(Bytes::from_bytes_validated(1024 * 1024 * 1024).is_ok()); // 1 GiB

        // Invalid construction
        let over_limit = (1024 * 1024 * 1024) + 1;
        assert!(Bytes::from_bytes_validated(over_limit).is_err());
    }

    #[test]
    fn test_bytes_helper_constructors() {
        assert_eq!(Bytes::from_kibibytes(1).as_u64(), 1024);
        assert_eq!(Bytes::from_mebibytes(1).as_u64(), 1024 * 1024);
        assert_eq!(Bytes::from_gibibytes(1).as_u64(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_validation_error_messages() {
        // Verify error messages are descriptive
        let err = Seconds::from_secs(100000).validate().unwrap_err();
        assert!(matches!(err, ValidationError::General(_)));
        if let ValidationError::General(msg) = err {
            assert!(msg.contains("100000"));
            assert!(msg.contains("86400"));
            assert!(msg.contains("24 hours"));
        }

        let err = Bytes::from_mebibytes(2000).validate().unwrap_err();
        assert!(matches!(err, ValidationError::General(_)));
        if let ValidationError::General(msg) = err {
            assert!(msg.contains("1 GiB"));
        }
    }

    #[test]
    fn test_const_max_values() {
        // Verify MAX constants are correct
        assert_eq!(Seconds::MAX.as_secs(), 86400);
        assert_eq!(Bytes::MAX.as_u64(), 1024 * 1024 * 1024);
    }
}
