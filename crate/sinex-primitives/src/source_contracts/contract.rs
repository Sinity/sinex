use serde::Serialize;

use crate::source_contracts::{
    AccessScope, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy,
};

/// The typed declaration every source fills in.
///
/// This is strictly a *semantic* descriptor: identity, emitted event-type
/// pairs, privacy tier, time horizons, retention, occurrence identity, and
/// access scope. Deployment-shape fields (`runner_pack`, `resource_profile`,
/// `checkpoint_family`, `runtime_shape`, `build_impact`) live on the matching
/// [`SourceRuntimeBinding`]. See issue #1175.
///
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SourceContract {
    pub id: &'static str,
    pub namespace: &'static str,
    pub event_types: &'static [(&'static str, &'static str)],
    pub privacy_tier: PrivacyTier,
    pub horizons: &'static [Horizon],
    pub retention: RetentionPolicy,
    pub occurrence_identity: OccurrenceIdentity,
    /// Resource locator this source reads (de-conflated from data category).
    pub access_scope: AccessScope,
}

inventory::collect!(SourceContract);

/// Iterate over every registered source contract in the binary.
pub fn all_source_contracts() -> impl Iterator<Item = &'static SourceContract> {
    inventory::iter::<SourceContract>()
}

/// Find a source contract by `id`.
#[must_use]
pub fn find_source_contract(id: &crate::parser::SourceId) -> Option<&'static SourceContract> {
    let id_str = id.as_str();
    all_source_contracts().find(|descriptor| descriptor.id == id_str)
}

/// Re-exported `inventory` for consumers of [`register_source_contract!`].
#[doc(hidden)]
pub mod __register {
    pub use inventory;
}

/// Register a source contract with the binary's inventory.
///
/// ```rust,ignore
/// register_source_contract!(
///     descriptor: MY_DESCRIPTOR,
/// );
/// ```
#[macro_export]
macro_rules! register_source_contract {
    // Plain form — descriptor only.
    ($descriptor:expr $(,)?) => {
        $crate::source_contracts::__register::inventory::submit! { $descriptor }
    };
    // Named form.
    (descriptor: $descriptor:expr $(,)?) => {
        $crate::source_contracts::__register::inventory::submit! { $descriptor }
    };
}

/// Register a [`SourceRuntimeBinding`] with the binary's inventory.
///
/// Companion to [`register_source_contract!`]: contracts describe the *semantic*
/// shape of a source, bindings describe the deployed adapter that runs
/// it. Both are mechanically discoverable through the `inventory` crate.
#[macro_export]
macro_rules! register_source_runtime_binding {
    ($binding:expr $(,)?) => {
        $crate::source_contracts::__register::inventory::submit! { $binding }
    };
}
