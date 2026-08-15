//! Source registry — validates and enumerates source contracts from the
//! compile-time [`SourceContract`] inventory.
//!
//! The registry wraps [`sinex_primitives::source_contracts::all_source_contracts`] to provide
//! a stable lookup surface. At link time, every crate that calls
//! [`register_source_contract!`] contributes its descriptors to the inventory.

use sinex_primitives::parser::SourceId;
use sinex_primitives::source_contracts::{self, SourceContract};
use sinex_primitives::sources::source_role;

/// Registry of source contracts loaded from the compile-time inventory.
///
/// This is a lightweight wrapper over the global [`inventory`]-based descriptor
/// collection. It is cheap to construct and does not allocate.
#[derive(Debug, Default)]
pub struct SourceContractRegistry;

impl SourceContractRegistry {
    /// Create a registry from the global compile-time descriptor inventory.
    #[must_use]
    pub fn from_inventory() -> Self {
        Self
    }

    /// Find a source contract by its `id`.
    #[must_use]
    pub fn find(&self, id: &SourceId) -> Option<&'static SourceContract> {
        source_contracts::find_source_contract(id)
    }

    /// Validate that a source id is registered.
    ///
    /// Returns the contract on success, or an error message listing available
    /// source contracts on failure.
    ///
    /// # Errors
    ///
    /// Returns an error string if `id` is not found in the inventory.
    pub fn validate(&self, id: &SourceId) -> Result<&'static SourceContract, String> {
        self.find(id).ok_or_else(|| {
            let available = self.list_ids();
            if available.is_empty() {
                format!(
                    "source '{id}' not found in inventory. \
                     No source contracts are registered in this binary."
                )
            } else {
                format!(
                    "source '{id}' not found in inventory. \
                     Available: {}",
                    available.join(", ")
                )
            }
        })
    }

    /// Validate that each registered event source resolves to its contract's
    /// explicitly declared product lane.
    ///
    /// The persistence layer selects `core.events` or `reflection.events` with
    /// [`source_role`]. Keeping the intended role on the same inventory entry
    /// that declares each event source makes namespace typos fail before they
    /// can silently pollute an activity-facing surface.
    pub fn validate_event_source_roles(&self) -> Result<(), String> {
        let mismatches = self
            .list()
            .into_iter()
            .flat_map(role_mismatches)
            .collect::<Vec<_>>();

        if mismatches.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "{} registered event source role mismatch(es): {}",
                mismatches.len(),
                mismatches.join("; ")
            ))
        }
    }

    /// List all registered source contracts.
    #[must_use]
    pub fn list(&self) -> Vec<&'static SourceContract> {
        source_contracts::all_source_contracts().collect()
    }

    /// List the ids of all registered source contracts.
    #[must_use]
    pub fn list_ids(&self) -> Vec<&'static str> {
        source_contracts::all_source_contracts()
            .map(|descriptor| descriptor.id)
            .collect()
    }
}

fn role_mismatches(contract: &SourceContract) -> impl Iterator<Item = String> + '_ {
    contract
        .event_types
        .iter()
        .filter_map(move |(source, event_type)| {
            let resolved = source_role(source);
            (resolved != contract.source_role).then(|| {
                format!(
                    "contract `{}` declares {}/{} as {}, but routing resolves it as {}",
                    contract.id,
                    source,
                    event_type,
                    contract.source_role.as_str(),
                    resolved.as_str(),
                )
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::source_contracts::{
        AccessScope, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy,
    };
    use sinex_primitives::sources::SourceRole;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn registered_contract_roles_match_event_persistence_routing() -> TestResult<()> {
        SourceContractRegistry::from_inventory()
            .validate_event_source_roles()
            .map_err(|error| color_eyre::eyre::eyre!(error))
    }

    #[sinex_test]
    async fn role_guard_rejects_mismatched_contract_declaration() -> TestResult<()> {
        let fixture = SourceContract {
            id: "fixture.role-mismatch",
            namespace: "fixture",
            event_types: &[("fixture.activity", "fixture.event")],
            source_role: SourceRole::Reflection,
            privacy_tier: PrivacyTier::Public,
            horizons: &[Horizon::Continuous],
            retention: RetentionPolicy::Forever,
            occurrence_identity: OccurrenceIdentity::Natural,
            access_scope: AccessScope::Internal,
        };

        // Anti-vacuity: this calls the production comparison that the
        // exhaustive inventory test uses. Removing `source_role` from the
        // declaration or this resolved-role comparison would let the wrong
        // activity source pass as Reflection.
        let mismatch = role_mismatches(&fixture).collect::<Vec<_>>();

        assert_eq!(mismatch.len(), 1, "mismatched role must be rejected");
        assert!(mismatch[0].contains("fixture.role-mismatch"));
        assert!(mismatch[0].contains("reflection"));
        assert!(mismatch[0].contains("activity"));
        Ok(())
    }
}
