//! Compile-time type-isolation invariant tests for `Id<T>`.
//!
//! Uses `trybuild` to verify that assigning `Id<A>` to `Id<B>` is a compile
//! error — the type-safety guarantee that prevents event IDs from being
//! accidentally used as checkpoint IDs, blob IDs, etc.
//!
//! Exception: Raw `#[test]` is allowlisted here because `trybuild` requires
//! it — `#[sinex_test]` wraps an async runtime that trybuild cannot use.
//! These tests do not access the database or any async infrastructure.

#[test]
fn id_type_mismatch_is_compile_error() {
    // Compiles `compile_errors/id_type_mismatch.rs` and asserts it produces
    // a type-mismatch error. The `.stderr` file (auto-generated on first run
    // with TRYBUILD=overwrite) captures the expected error message.
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_errors/id_type_mismatch.rs");
}
