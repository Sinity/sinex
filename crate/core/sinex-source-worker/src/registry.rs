//! Source-unit registry — validates and enumerates source units from the
//! compile-time [`SourceUnitDescriptor`] inventory.
//!
//! The registry wraps [`sinex_primitives::proof::all_source_units`] to provide
//! a stable lookup surface. At link time, every crate that calls
//! [`register_source_unit!`] contributes its descriptors to the inventory.

use sinex_primitives::parser::SourceUnitId;
use sinex_primitives::proof::{self, SourceUnitDescriptor};

/// Registry of source-unit descriptors loaded from the compile-time inventory.
///
/// This is a lightweight wrapper over the global [`inventory`]-based descriptor
/// collection. It is cheap to construct and does not allocate.
#[derive(Debug, Default)]
pub struct SourceUnitRegistry;

impl SourceUnitRegistry {
    /// Create a registry from the global compile-time descriptor inventory.
    #[must_use]
    pub fn from_inventory() -> Self {
        Self
    }

    /// Find a source-unit descriptor by its `id`.
    #[must_use]
    pub fn find(&self, id: &SourceUnitId) -> Option<&'static SourceUnitDescriptor> {
        proof::find_source_unit(id)
    }

    /// Validate that a source-unit id is registered.
    ///
    /// Returns the descriptor on success, or an error message listing available
    /// source units on failure.
    ///
    /// # Errors
    ///
    /// Returns an error string if `id` is not found in the inventory.
    pub fn validate(
        &self,
        id: &SourceUnitId,
    ) -> Result<&'static SourceUnitDescriptor, String> {
        self.find(id).ok_or_else(|| {
            let available = self.list_ids();
            if available.is_empty() {
                format!(
                    "source unit '{id}' not found in inventory. \
                     No source units are registered in this binary."
                )
            } else {
                format!(
                    "source unit '{id}' not found in inventory. \
                     Available: {}",
                    available.join(", ")
                )
            }
        })
    }

    /// List all registered source-unit descriptors.
    #[must_use]
    pub fn list(&self) -> Vec<&'static SourceUnitDescriptor> {
        proof::all_source_units().collect()
    }

    /// List the ids of all registered source units.
    #[must_use]
    pub fn list_ids(&self) -> Vec<&'static str> {
        proof::all_source_units().map(|descriptor| descriptor.id).collect()
    }
}
