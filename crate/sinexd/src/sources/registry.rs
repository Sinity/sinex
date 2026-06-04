//! Source registry — validates and enumerates source contracts from the
//! compile-time [`SourceContract`] inventory.
//!
//! The registry wraps [`sinex_primitives::proof::all_source_contracts`] to provide
//! a stable lookup surface. At link time, every crate that calls
//! [`register_source_contract!`] contributes its descriptors to the inventory.

use sinex_primitives::parser::SourceId;
use sinex_primitives::proof::{self, SourceContract};

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
        proof::find_source_contract(id)
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

    /// List all registered source contracts.
    #[must_use]
    pub fn list(&self) -> Vec<&'static SourceContract> {
        proof::all_source_contracts().collect()
    }

    /// List the ids of all registered source contracts.
    #[must_use]
    pub fn list_ids(&self) -> Vec<&'static str> {
        proof::all_source_contracts()
            .map(|descriptor| descriptor.id)
            .collect()
    }
}
