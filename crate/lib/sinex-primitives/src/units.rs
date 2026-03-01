use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::str::FromStr;

use crate::error::{Result, SinexError};

/// Byte-count newtype that prevents unit mixups.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, JsonSchema,
)]
#[serde(transparent)]
pub struct Bytes(u64);

impl Bytes {
    /// Maximum allowed value: 1 GiB
    pub const MAX: Bytes = Bytes(1024 * 1024 * 1024);

    /// Construct the newtype from a raw byte count.
    #[must_use]
    pub const fn from_bytes(value: u64) -> Self {
        Self(value)
    }

    /// Construct from mebibytes (MiB).
    #[must_use]
    pub const fn from_mebibytes(mib: u64) -> Self {
        Self(mib * 1024 * 1024)
    }

    /// Construct from kibibytes (KiB).
    #[must_use]
    pub const fn from_kibibytes(kib: u64) -> Self {
        Self(kib * 1024)
    }

    /// Construct from gibibytes (GiB).
    #[must_use]
    pub const fn from_gibibytes(gib: u64) -> Self {
        Self(gib * 1024 * 1024 * 1024)
    }

    /// Retrieve the underlying count in bytes.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Retrieve the count as `usize` (lossy on 32-bit platforms when > `usize::MAX`).
    #[must_use]
    pub fn as_usize(self) -> usize {
        self.0 as usize
    }

    /// Validate that value is within acceptable range.
    ///
    /// # Errors
    ///
    /// Returns an error if the value exceeds the maximum of 1 GiB.
    pub fn validate(&self) -> Result<()> {
        if self.0 > Self::MAX.0 {
            return Err(SinexError::validation(format!(
                "Bytes value {} exceeds maximum of {} (1 GiB)",
                self.0,
                Self::MAX.0
            )));
        }
        Ok(())
    }

    /// Create from bytes with validation.
    ///
    /// # Errors
    ///
    /// Returns an error if the value exceeds the maximum of 1 GiB.
    pub fn from_bytes_validated(bytes: u64) -> Result<Self> {
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

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        s.parse::<u64>().map(Bytes::from_bytes)
    }
}

/// Second-count newtype that encodes duration semantics explicitly.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, JsonSchema,
)]
#[serde(transparent)]
pub struct Seconds(u64);

impl Seconds {
    /// Maximum allowed value: 24 hours (86400 seconds)
    pub const MAX: Seconds = Seconds(86400);

    /// Construct from a raw number of seconds.
    #[must_use]
    pub const fn from_secs(value: u64) -> Self {
        Self(value)
    }

    /// Construct from milliseconds.
    #[must_use]
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis / 1000)
    }

    /// Construct from minutes.
    #[must_use]
    pub const fn from_minutes(minutes: u64) -> Self {
        Self(minutes * 60)
    }

    /// Construct from hours.
    #[must_use]
    pub const fn from_hours(hours: u64) -> Self {
        Self(hours * 3600)
    }

    /// Retrieve the underlying second count.
    #[must_use]
    pub const fn as_secs(self) -> u64 {
        self.0
    }

    /// Convert to standard Duration
    #[must_use]
    pub const fn as_duration(self) -> std::time::Duration {
        std::time::Duration::from_secs(self.0)
    }

    /// Validate that value is within acceptable range.
    ///
    /// # Errors
    ///
    /// Returns an error if the value exceeds the maximum of 86400 seconds (24 hours).
    pub fn validate(&self) -> Result<()> {
        if self.0 > Self::MAX.0 {
            return Err(SinexError::validation(format!(
                "Seconds value {} exceeds maximum of {} (24 hours)",
                self.0,
                Self::MAX.0
            )));
        }
        Ok(())
    }

    /// Create from seconds with validation.
    ///
    /// # Errors
    ///
    /// Returns an error if the value exceeds the maximum of 86400 seconds (24 hours).
    pub fn from_secs_validated(secs: u64) -> Result<Self> {
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

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        s.parse::<u64>().map(Seconds::from_secs)
    }
}

/// Millisecond-count newtype that encodes duration semantics explicitly.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, JsonSchema,
)]
#[serde(transparent)]
pub struct Milliseconds(u64);

impl Milliseconds {
    /// Construct from a raw number of milliseconds.
    #[must_use]
    pub const fn from_millis(value: u64) -> Self {
        Self(value)
    }

    /// Retrieve the underlying millisecond count.
    #[must_use]
    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// Convert to standard Duration
    #[must_use]
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

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        s.parse::<u64>().map(Milliseconds::from_millis)
    }
}

/// Microsecond-count newtype for high-precision timing.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, JsonSchema,
)]
#[serde(transparent)]
pub struct Microseconds(i64);

impl Microseconds {
    /// Construct from a raw number of microseconds.
    #[must_use]
    pub const fn from_micros(value: i64) -> Self {
        Self(value)
    }

    /// Retrieve the underlying microsecond count.
    #[must_use]
    pub const fn as_micros(self) -> i64 {
        self.0
    }

    /// Convert to milliseconds (lossy).
    #[must_use]
    pub const fn as_millis(self) -> i64 {
        self.0 / 1000
    }

    /// Convert to seconds (lossy).
    #[must_use]
    pub const fn as_secs(self) -> i64 {
        self.0 / 1_000_000
    }
}

impl Display for Microseconds {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}µs", self.0)
    }
}

impl From<i64> for Microseconds {
    fn from(value: i64) -> Self {
        Self::from_micros(value)
    }
}

impl From<Microseconds> for i64 {
    fn from(value: Microseconds) -> Self {
        value.0
    }
}

/// Nanosecond-count newtype for very high-precision timing.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, JsonSchema,
)]
#[serde(transparent)]
pub struct Nanoseconds(i64);

impl Nanoseconds {
    /// Construct from a raw number of nanoseconds.
    #[must_use]
    pub const fn from_nanos(value: i64) -> Self {
        Self(value)
    }

    /// Retrieve the underlying nanosecond count.
    #[must_use]
    pub const fn as_nanos(self) -> i64 {
        self.0
    }

    /// Convert to microseconds (lossy).
    #[must_use]
    pub const fn as_micros(self) -> i64 {
        self.0 / 1000
    }

    /// Convert to milliseconds (lossy).
    #[must_use]
    pub const fn as_millis(self) -> i64 {
        self.0 / 1_000_000
    }
}

impl Display for Nanoseconds {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}ns", self.0)
    }
}

impl From<i64> for Nanoseconds {
    fn from(value: i64) -> Self {
        Self::from_nanos(value)
    }
}

impl From<Nanoseconds> for i64 {
    fn from(value: Nanoseconds) -> Self {
        value.0
    }
}

// ─────────────────────────────────────────────────────────────
// Process and System Types
// ─────────────────────────────────────────────────────────────

/// Process exit code newtype.
///
/// Distinguishes exit codes from other i32 values. Unix convention:
/// - 0: success
/// - 1-125: application-defined errors
/// - 126: command not executable
/// - 127: command not found
/// - 128+N: killed by signal N
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, JsonSchema,
)]
#[serde(transparent)]
pub struct ExitCode(i32);

impl ExitCode {
    /// Success exit code (0).
    pub const SUCCESS: ExitCode = ExitCode(0);

    /// Construct from a raw exit code.
    #[must_use]
    pub const fn from_raw(value: i32) -> Self {
        Self(value)
    }

    /// Retrieve the underlying exit code.
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        self.0
    }

    /// Check if this represents success (exit code 0).
    #[must_use]
    pub const fn is_success(self) -> bool {
        self.0 == 0
    }

    /// Check if this represents a signal termination (128+).
    #[must_use]
    pub const fn is_signal(self) -> bool {
        self.0 >= 128
    }

    /// Get the signal number if terminated by signal.
    #[must_use]
    pub const fn signal_number(self) -> Option<i32> {
        if self.0 >= 128 {
            Some(self.0 - 128)
        } else {
            None
        }
    }
}

impl Display for ExitCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        if self.is_success() {
            write!(f, "0 (success)")
        } else if let Some(sig) = self.signal_number() {
            write!(f, "{} (signal {})", self.0, sig)
        } else {
            write!(f, "{}", self.0)
        }
    }
}

impl From<i32> for ExitCode {
    fn from(value: i32) -> Self {
        Self::from_raw(value)
    }
}

impl From<ExitCode> for i32 {
    fn from(value: ExitCode) -> Self {
        value.0
    }
}

impl Default for ExitCode {
    fn default() -> Self {
        Self::SUCCESS
    }
}

/// Unix process ID newtype.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, JsonSchema,
)]
#[serde(transparent)]
pub struct ProcessId(u32);

impl ProcessId {
    /// Construct from a raw PID.
    #[must_use]
    pub const fn from_raw(value: u32) -> Self {
        Self(value)
    }

    /// Retrieve the underlying PID.
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl Display for ProcessId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "PID {}", self.0)
    }
}

impl From<u32> for ProcessId {
    fn from(value: u32) -> Self {
        Self::from_raw(value)
    }
}

impl From<ProcessId> for u32 {
    fn from(value: ProcessId) -> Self {
        value.0
    }
}

/// Unix user ID newtype.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, JsonSchema,
)]
#[serde(transparent)]
pub struct UnixUid(u32);

impl UnixUid {
    /// Root user ID.
    pub const ROOT: UnixUid = UnixUid(0);

    /// Construct from a raw UID.
    #[must_use]
    pub const fn from_raw(value: u32) -> Self {
        Self(value)
    }

    /// Retrieve the underlying UID.
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    /// Check if this is root (UID 0).
    #[must_use]
    pub const fn is_root(self) -> bool {
        self.0 == 0
    }
}

impl Display for UnixUid {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "UID {}", self.0)
    }
}

impl From<u32> for UnixUid {
    fn from(value: u32) -> Self {
        Self::from_raw(value)
    }
}

impl From<UnixUid> for u32 {
    fn from(value: UnixUid) -> Self {
        value.0
    }
}

/// Unix group ID newtype.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, JsonSchema,
)]
#[serde(transparent)]
pub struct UnixGid(u32);

impl UnixGid {
    /// Root group ID.
    pub const ROOT: UnixGid = UnixGid(0);

    /// Construct from a raw GID.
    #[must_use]
    pub const fn from_raw(value: u32) -> Self {
        Self(value)
    }

    /// Retrieve the underlying GID.
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl Display for UnixGid {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "GID {}", self.0)
    }
}

impl From<u32> for UnixGid {
    fn from(value: u32) -> Self {
        Self::from_raw(value)
    }
}

impl From<UnixGid> for u32 {
    fn from(value: UnixGid) -> Self {
        value.0
    }
}

// ─────────────────────────────────────────────────────────────
// Count Types
// ─────────────────────────────────────────────────────────────

/// Event count newtype for statistics and metrics.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Hash,
    JsonSchema,
)]
#[serde(transparent)]
pub struct EventCount(u64);

impl EventCount {
    /// Zero count.
    pub const ZERO: EventCount = EventCount(0);

    /// Construct from a raw count.
    #[must_use]
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Retrieve the underlying count.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Increment the count by one.
    pub fn increment(&mut self) {
        self.0 = self.0.saturating_add(1);
    }

    /// Add to the count.
    pub fn add(&mut self, n: u64) {
        self.0 = self.0.saturating_add(n);
    }
}

impl Display for EventCount {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{} events", self.0)
    }
}

impl From<u64> for EventCount {
    fn from(value: u64) -> Self {
        Self::from_raw(value)
    }
}

impl From<EventCount> for u64 {
    fn from(value: EventCount) -> Self {
        value.0
    }
}

impl From<usize> for EventCount {
    fn from(value: usize) -> Self {
        Self::from_raw(value as u64)
    }
}

impl std::ops::Add for EventCount {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0.saturating_add(rhs.0))
    }
}

impl std::ops::AddAssign for EventCount {
    fn add_assign(&mut self, rhs: Self) {
        self.0 = self.0.saturating_add(rhs.0);
    }
}

/// Line count newtype for text processing.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Hash,
    JsonSchema,
)]
#[serde(transparent)]
pub struct LineCount(u32);

impl LineCount {
    /// Zero lines.
    pub const ZERO: LineCount = LineCount(0);

    /// Construct from a raw count.
    #[must_use]
    pub const fn from_raw(value: u32) -> Self {
        Self(value)
    }

    /// Retrieve the underlying count.
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl Display for LineCount {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        if self.0 == 1 {
            write!(f, "1 line")
        } else {
            write!(f, "{} lines", self.0)
        }
    }
}

impl From<u32> for LineCount {
    fn from(value: u32) -> Self {
        Self::from_raw(value)
    }
}

impl From<LineCount> for u32 {
    fn from(value: LineCount) -> Self {
        value.0
    }
}

/// Syslog priority level (0-7).
///
/// - 0: Emergency
/// - 1: Alert
/// - 2: Critical
/// - 3: Error
/// - 4: Warning
/// - 5: Notice
/// - 6: Informational
/// - 7: Debug
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, JsonSchema,
)]
#[serde(transparent)]
pub struct SyslogPriority(u8);

impl SyslogPriority {
    pub const EMERGENCY: SyslogPriority = SyslogPriority(0);
    pub const ALERT: SyslogPriority = SyslogPriority(1);
    pub const CRITICAL: SyslogPriority = SyslogPriority(2);
    pub const ERROR: SyslogPriority = SyslogPriority(3);
    pub const WARNING: SyslogPriority = SyslogPriority(4);
    pub const NOTICE: SyslogPriority = SyslogPriority(5);
    pub const INFO: SyslogPriority = SyslogPriority(6);
    pub const DEBUG: SyslogPriority = SyslogPriority(7);

    /// Construct from a raw priority (clamped to 0-7).
    #[must_use]
    pub const fn from_raw(value: u8) -> Self {
        Self(if value > 7 { 7 } else { value })
    }

    /// Retrieve the underlying priority.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self.0
    }

    /// Get the priority name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self.0 {
            0 => "emergency",
            1 => "alert",
            2 => "critical",
            3 => "error",
            4 => "warning",
            5 => "notice",
            6 => "info",
            7 => "debug",
            _ => "unknown",
        }
    }

    /// Check if this is an error-level or higher priority (0-3).
    #[must_use]
    pub const fn is_error(self) -> bool {
        self.0 <= 3
    }
}

impl Display for SyslogPriority {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{} ({})", self.name(), self.0)
    }
}

impl From<u8> for SyslogPriority {
    fn from(value: u8) -> Self {
        Self::from_raw(value)
    }
}

impl From<SyslogPriority> for u8 {
    fn from(value: SyslogPriority) -> Self {
        value.0
    }
}

impl Default for SyslogPriority {
    fn default() -> Self {
        Self::INFO
    }
}

/// Sequence number newtype for ordering.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Hash,
    JsonSchema,
)]
#[serde(transparent)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    /// Construct from a raw sequence number.
    #[must_use]
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Retrieve the underlying sequence number.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Get the next sequence number.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl Display for SequenceNumber {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "#{}", self.0)
    }
}

impl From<u64> for SequenceNumber {
    fn from(value: u64) -> Self {
        Self::from_raw(value)
    }
}

impl From<SequenceNumber> for u64 {
    fn from(value: SequenceNumber) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_seconds_validation_valid() -> TestResult<()> {
        // Valid values within range
        assert!(Seconds::from_secs(0).validate().is_ok());
        assert!(Seconds::from_secs(30).validate().is_ok());
        assert!(Seconds::from_secs(3600).validate().is_ok());
        assert!(Seconds::from_secs(86400).validate().is_ok()); // Exactly 24 hours
        Ok(())
    }

    #[sinex_test]
    async fn test_seconds_validation_invalid() -> TestResult<()> {
        // Invalid values exceeding maximum
        assert!(Seconds::from_secs(86401).validate().is_err());
        assert!(Seconds::from_secs(100000).validate().is_err());
        assert!(Seconds::from_secs(1000000).validate().is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_seconds_from_validated() -> TestResult<()> {
        // Valid construction
        assert!(Seconds::from_secs_validated(30).is_ok());
        assert!(Seconds::from_secs_validated(86400).is_ok());

        // Invalid construction
        assert!(Seconds::from_secs_validated(86401).is_err());
        assert!(Seconds::from_secs_validated(1000000).is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_seconds_helper_constructors() -> TestResult<()> {
        assert_eq!(Seconds::from_millis(5000).as_secs(), 5);
        assert_eq!(Seconds::from_minutes(5).as_secs(), 300);
        assert_eq!(Seconds::from_hours(2).as_secs(), 7200);
        Ok(())
    }

    #[sinex_test]
    async fn test_bytes_validation_valid() -> TestResult<()> {
        // Valid values within range
        assert!(Bytes::from_bytes(0).validate().is_ok());
        assert!(Bytes::from_bytes(1024).validate().is_ok());
        assert!(Bytes::from_mebibytes(100).validate().is_ok());
        assert!(Bytes::from_mebibytes(1024).validate().is_ok()); // Exactly 1 GiB
        assert!(Bytes::from_gibibytes(1).validate().is_ok()); // Exactly 1 GiB
        Ok(())
    }

    #[sinex_test]
    async fn test_bytes_validation_invalid() -> TestResult<()> {
        // Invalid values exceeding maximum (> 1 GiB)
        assert!(Bytes::from_mebibytes(1025).validate().is_err());
        assert!(Bytes::from_gibibytes(2).validate().is_err());
        assert!(
            Bytes::from_bytes(2 * 1024 * 1024 * 1024)
                .validate()
                .is_err()
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_bytes_from_validated() -> TestResult<()> {
        // Valid construction
        assert!(Bytes::from_bytes_validated(1024).is_ok());
        assert!(Bytes::from_bytes_validated(1024 * 1024 * 1024).is_ok()); // 1 GiB

        // Invalid construction
        let over_limit = (1024 * 1024 * 1024) + 1;
        assert!(Bytes::from_bytes_validated(over_limit).is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_bytes_helper_constructors() -> TestResult<()> {
        assert_eq!(Bytes::from_kibibytes(1).as_u64(), 1024);
        assert_eq!(Bytes::from_mebibytes(1).as_u64(), 1024 * 1024);
        assert_eq!(Bytes::from_gibibytes(1).as_u64(), 1024 * 1024 * 1024);
        Ok(())
    }

    #[sinex_test]
    async fn test_validation_error_messages() -> TestResult<()> {
        // Verify error messages are descriptive
        let err = Seconds::from_secs(100000).validate().unwrap_err();
        assert!(matches!(err, SinexError::Validation(_)));
        let msg = err.message();
        assert!(msg.contains("100000"));
        assert!(msg.contains("86400"));
        assert!(msg.contains("24 hours"));

        let err = Bytes::from_mebibytes(2000).validate().unwrap_err();
        assert!(matches!(err, SinexError::Validation(_)));
        let msg = err.message();
        assert!(msg.contains("1 GiB"));
        Ok(())
    }

    #[sinex_test]
    async fn test_const_max_values() -> TestResult<()> {
        // Verify MAX constants are correct
        assert_eq!(Seconds::MAX.as_secs(), 86400);
        assert_eq!(Bytes::MAX.as_u64(), 1024 * 1024 * 1024);
        Ok(())
    }
}
