//! Compile-time type-isolation invariant tests for `Id<T>`.
//!
//! Uses `trybuild` to verify that assigning `Id<A>` to `Id<B>` is a compile
//! error — the type-safety guarantee that prevents event IDs from being
//! accidentally used as checkpoint IDs, blob IDs, etc.
//!
//! Exception: Raw `#[test]` is allowlisted here because `trybuild` requires
//! it — `#[sinex_test]` wraps an async runtime that trybuild cannot use.
//! These tests do not access the database or any async infrastructure.
//!
//! # Cache locality
//!
//! Earlier versions of this file isolated trybuild's `CARGO_TARGET_DIR` to
//! `.sinex/trybuild-target` to avoid claimed nextest deadlocks. Empirical
//! data from xtask history (2026-05-11): zero deadlock/lock failures across
//! 96 runs of this test + the sibling `proof_descriptor_compile_failures`
//! test. The isolation forced a cold rebuild of sinex-primitives' dep graph
//! every run (mean 59.5s; min 1.4s only when trybuild's stderr cache hit).
//! Letting trybuild share the workspace target dir gets warm-cache hits and
//! relies on cargo's own per-target locking — the same pattern
//! `proof_compile_fail_test.rs` uses without issue.

// Ignored by default because trybuild compile-failure tests spawn their own
// rustc and dominate sinex-primitives wallclock (90–280 s each). Run via
// `xtask test --heavy -p sinex-primitives`; CI workspace lane runs the
// `--heavy` slice for this package explicitly so coverage isn't lost (#1215).
#[test]
#[ignore = "heavy: trybuild compile-failure (run via --heavy)"]
fn id_type_mismatch_is_compile_error() {
    // Compiles `compile_errors/id_type_mismatch.rs` and asserts it produces
    // a type-mismatch error. The `.stderr` file (auto-generated on first run
    // with TRYBUILD=overwrite) captures the expected error message.
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_errors/id_type_mismatch.rs");
}
