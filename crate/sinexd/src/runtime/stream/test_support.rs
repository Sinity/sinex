//! Shared test helpers for `RuntimeDrainController`.
//!
//! Both `source_driver` and `automaton::adapter` had identical inline tests
//! exercising the runtime drain controller (#1012, #1175 follow-up). The bodies
//! were copy-pasted modulo the node label. Both call sites now route through
//! these helpers so the laws are pinned in one place.
//!
//! Keep these helpers test-only — production code talks to
//! `RuntimeDrainController` directly through the public re-export in
//! `crate::runtime::stream`.
use super::handles::RuntimeDrainController;
use xtask::sandbox::prelude::TestResult;

/// Assert that `request_drain_and_warn` delivers the drain edge to a
/// fresh subscriber.
///
/// The assertion shape is: a brand-new controller starts un-drained, a single
/// `request_drain_and_warn` call returns `true` (signal accepted), the
/// subscriber observes the watch-channel transition, and the resulting borrow
/// reads `true`.
pub async fn assert_request_drain_delivers_to_receiver(node_label: &str) -> TestResult<()> {
    let drain = RuntimeDrainController::new();
    let mut rx = drain.subscribe();

    assert!(drain.request_drain_and_warn(node_label));
    rx.changed().await?;
    assert!(*rx.borrow());
    Ok(())
}

/// Assert that `request_drain_and_warn` is idempotent: the second call still
/// reports success (drain already requested) and the controller stays in the
/// requested state.
pub fn assert_request_drain_is_idempotent(node_label: &str) {
    let drain = RuntimeDrainController::new();

    assert!(drain.request_drain_and_warn(node_label));
    assert!(drain.request_drain_and_warn(node_label));
    assert!(drain.is_requested());
}
