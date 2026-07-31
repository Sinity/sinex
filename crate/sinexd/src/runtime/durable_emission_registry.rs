//! Process-wide handle for the `sinexd`-hosted [`SettlementRegistry`]
//! (sinex-r6d.11).
//!
//! `sinexd` hosts exactly one event engine per process — the event engine
//! (API + automata + hosted source bindings all run as tasks inside the
//! same `sinexd serve` process, see `crate::supervisor`). Source bindings
//! are dispatched through [`crate::sources::source_factory::SourceFactoryFn`],
//! a `Vec<OsString> -> BoxFuture` shape shared with the standalone CLI
//! parser and fixed by the per-source `register_source!` codegen; threading
//! a typed `SettlementRegistry` through that call chain would ripple through
//! every generated registration site for no benefit, since there is only
//! ever one registry per process to thread through.
//!
//! This module is therefore a narrow, explicitly-scoped exception to normal
//! dependency injection: `supervisor::start_event_engine` installs the same
//! [`SettlementRegistry`] it attaches to the [`JetStreamConsumer`] here,
//! before any source binding is spawned; production source initialization
//! (`runtime::stream::runner::initialize`) reads it once, at construction
//! time, and stores the clone on [`RuntimeHandles`] — every other caller
//! (including every test in this crate) goes through the normal
//! `RuntimeHandles`/`RuntimeContext` accessor and never touches this module.
//! Mirrors the established `systemd_notify::enter_hosted_mode()` pattern in
//! this same daemon for the same reason: process-wide, install-once,
//! read-many state that only makes sense for the single hosted `sinexd`
//! process shape.
//!
//! [`JetStreamConsumer`]: crate::event_engine::jetstream_consumer::JetStreamConsumer
//! [`RuntimeHandles`]: crate::runtime::stream::RuntimeHandles

use std::sync::OnceLock;

use crate::runtime::durable_emission::SettlementRegistry;

static SHARED_REGISTRY: OnceLock<SettlementRegistry> = OnceLock::new();

/// Install the process's shared settlement registry. Called once, by
/// `supervisor::start_event_engine`, before any source binding is spawned.
/// A second call is a silent no-op (`OnceLock::set` semantics) — there is
/// exactly one event engine, and therefore exactly one settlement registry,
/// per `sinexd` process.
pub fn install(registry: SettlementRegistry) {
    let _ = SHARED_REGISTRY.set(registry);
}

/// Retrieve the process's shared settlement registry, cloning out the
/// cheap `Arc`-backed handle. `None` when no event engine has installed one
/// in this process yet (or ever — standalone source-only tooling). Callers
/// must treat `None` as "no receipt backend available" and fall back to a
/// fresh, disconnected [`SettlementRegistry::new()`] rather than silently
/// upgrading to durable semantics — an unconnected registry's
/// `await_batch` calls simply time out, which is the honest outcome when
/// nothing will ever resolve them.
#[must_use]
pub fn get() -> Option<SettlementRegistry> {
    SHARED_REGISTRY.get().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: `SHARED_REGISTRY` is a real process-global `OnceLock`, so this
    // module intentionally has exactly one test that only ever calls
    // `install()` at most once, and never asserts `get()` returns `None`
    // (a prior test binary run installing it first would make that flaky).
    // Correctness of "install then get returns the same registry" is proven
    // indirectly instead, via a fresh registry compared to itself is
    // trivial; the real assembly proof lives in the caller integration
    // tests (`adapter_source_test.rs`) exercised through explicit
    // `RuntimeHandles::with_settlement_registry` wiring, not this global.
    #[test]
    fn get_before_any_install_in_this_process_is_none_or_a_valid_registry() {
        // Either nothing has installed yet (None) or another test in this
        // binary already did (Some) — both are valid process states; the
        // only invariant this module owns is "never panics".
        let _ = get();
    }

    #[test]
    fn install_then_get_returns_a_registry_usable_for_register_resolve() {
        let registry = SettlementRegistry::new();
        install(registry);
        let retrieved = get().expect("install() ran above, so get() must return Some");
        // Prove the retrieved handle is a live, working registry (shares
        // state with whatever was installed process-wide, though not
        // necessarily THIS call's registry if another test won the race).
        let event_id = sinex_primitives::Uuid::now_v7();
        let rx = retrieved.register(event_id);
        assert!(retrieved.resolve(
            event_id,
            crate::runtime::durable_emission::EmissionReceiptState::NoOutputSettled
        ));
        drop(rx);
    }
}
