//! Deterministic interruption points for durability and recovery tests.
//!
//! Tests install rules on the object they exercise. An external crash harness
//! can configure the same points with `SINEX_FAULT_INJECTION=point=abort`.
//! Returning an error models a redelivery/rollback boundary; aborting models a
//! process disappearing at that boundary.

use crate::runtime::{RuntimeResult, SinexError};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FaultPoint {
    CasStagedFile,
    CasLease,
    CasPublish,
    CasQuarantine,
    CasPendingDelete,
    CasReconciliation,
    MaterialStagedFile,
    MaterialWal,
    /// Test-only response loss after the material transaction and CAS lease
    /// cleanup have both completed. This is deliberately not part of the
    /// production fault-injection surface.
    #[cfg(test)]
    MaterialCommitPostCommitResponse,
    /// Test-only termination of the PostgreSQL backend immediately before
    /// commit. This exercises the real `Transaction::commit` error path.
    #[cfg(test)]
    MaterialCommitConnectionTermination,
}

impl FaultPoint {
    fn as_str(self) -> &'static str {
        match self {
            Self::CasStagedFile => "cas-staged-file",
            Self::CasLease => "cas-lease",
            Self::CasPublish => "cas-publish",
            Self::CasQuarantine => "cas-quarantine",
            Self::CasPendingDelete => "cas-pending-delete",
            Self::CasReconciliation => "cas-reconciliation",
            Self::MaterialStagedFile => "material-staged-file",
            Self::MaterialWal => "material-wal",
            #[cfg(test)]
            Self::MaterialCommitPostCommitResponse => "material-commit-post-commit-response",
            #[cfg(test)]
            Self::MaterialCommitConnectionTermination => "material-commit-connection-termination",
        }
    }
}

impl fmt::Display for FaultPoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for FaultPoint {
    type Err = SinexError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "cas-staged-file" => Ok(Self::CasStagedFile),
            "cas-lease" => Ok(Self::CasLease),
            "cas-publish" => Ok(Self::CasPublish),
            "cas-quarantine" => Ok(Self::CasQuarantine),
            "cas-pending-delete" => Ok(Self::CasPendingDelete),
            "cas-reconciliation" => Ok(Self::CasReconciliation),
            "material-staged-file" => Ok(Self::MaterialStagedFile),
            "material-wal" => Ok(Self::MaterialWal),
            #[cfg(test)]
            "material-commit-post-commit-response" => Ok(Self::MaterialCommitPostCommitResponse),
            #[cfg(test)]
            "material-commit-connection-termination" => {
                Ok(Self::MaterialCommitConnectionTermination)
            }
            other => Err(SinexError::validation(format!(
                "unknown deterministic fault point: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultMode {
    ReturnError,
    AbortProcess,
}

#[derive(Debug, Clone, Copy)]
struct FaultRule {
    remaining: usize,
    mode: FaultMode,
}

/// A deterministic, cloneable fault plan. Each rule is consumed exactly once
/// per configured count, making recovery tests independent of timing.
#[derive(Debug, Clone, Default)]
pub struct FaultInjector {
    rules: Arc<Mutex<BTreeMap<FaultPoint, FaultRule>>>,
}

impl FaultInjector {
    #[must_use]
    pub fn from_env() -> Self {
        let injector = Self::default();
        let Ok(specification) = std::env::var("SINEX_FAULT_INJECTION") else {
            return injector;
        };
        for rule in specification
            .split(',')
            .filter(|rule| !rule.trim().is_empty())
        {
            let mut fields = rule.split('=').map(str::trim);
            let Some(point) = fields.next() else { continue };
            let Some(mode) = fields.next() else { continue };
            let Ok(point) = point.parse::<FaultPoint>() else {
                continue;
            };
            let (count, mode) = match mode {
                "abort" => (1, FaultMode::AbortProcess),
                "error" => (1, FaultMode::ReturnError),
                value => value
                    .parse::<usize>()
                    .ok()
                    .map_or((0, FaultMode::ReturnError), |count| {
                        (count, FaultMode::ReturnError)
                    }),
            };
            if count > 0 {
                injector.set(point, count, mode);
            }
        }
        injector
    }

    pub fn set(&self, point: FaultPoint, count: usize, mode: FaultMode) {
        if count == 0 {
            return;
        }
        if let Ok(mut rules) = self.rules.lock() {
            rules.insert(
                point,
                FaultRule {
                    remaining: count,
                    mode,
                },
            );
        }
    }

    pub fn fail_once(&self, point: FaultPoint) {
        self.set(point, 1, FaultMode::ReturnError);
    }

    /// Trigger the next matching fault. Abort mode intentionally never returns.
    pub fn inject(&self, point: FaultPoint) -> RuntimeResult<()> {
        let mode = self.rules.lock().ok().and_then(|mut rules| {
            let rule = rules.get_mut(&point)?;
            rule.remaining = rule.remaining.saturating_sub(1);
            let mode = rule.mode;
            if rule.remaining == 0 {
                rules.remove(&point);
            }
            Some(mode)
        });
        match mode {
            Some(FaultMode::ReturnError) => Err(SinexError::io(format!(
                "deterministic fault injected at {point}"
            ))),
            Some(FaultMode::AbortProcess) => std::process::abort(),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FaultInjector, FaultMode, FaultPoint};

    #[test]
    fn rules_are_consumed_deterministically() {
        let injector = FaultInjector::default();
        injector.set(FaultPoint::CasPublish, 2, FaultMode::ReturnError);
        assert!(injector.inject(FaultPoint::CasPublish).is_err());
        assert!(injector.inject(FaultPoint::CasPublish).is_err());
        assert!(injector.inject(FaultPoint::CasPublish).is_ok());
    }

    #[test]
    fn points_round_trip_through_operator_names() {
        for point in [
            FaultPoint::CasStagedFile,
            FaultPoint::CasLease,
            FaultPoint::CasPublish,
            FaultPoint::CasQuarantine,
            FaultPoint::CasPendingDelete,
            FaultPoint::CasReconciliation,
            FaultPoint::MaterialStagedFile,
            FaultPoint::MaterialWal,
            FaultPoint::MaterialCommitPostCommitResponse,
            FaultPoint::MaterialCommitConnectionTermination,
        ] {
            assert_eq!(point.to_string().parse::<FaultPoint>().unwrap(), point);
        }
    }
}
