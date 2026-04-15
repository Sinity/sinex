//! Compile-time type-isolation invariant tests for `Id<T>`.
//!
//! Uses `trybuild` to verify that assigning `Id<A>` to `Id<B>` is a compile
//! error — the type-safety guarantee that prevents event IDs from being
//! accidentally used as checkpoint IDs, blob IDs, etc.
//!
//! Exception: Raw `#[test]` is allowlisted here because `trybuild` requires
//! it — `#[sinex_test]` wraps an async runtime that trybuild cannot use.
//! These tests do not access the database or any async infrastructure.

fn trybuild_target_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(".sinex")
        .join("trybuild-target")
}

#[test]
fn id_type_mismatch_is_compile_error() {
    // Keep this package's trybuild artifacts off the shared workspace target
    // so it doesn't deadlock with other compile-fail tests under nextest.
    let _target_guard =
        xtask::sandbox::EnvGuard::set_single("CARGO_TARGET_DIR", trybuild_target_dir());

    // Compiles `compile_errors/id_type_mismatch.rs` and asserts it produces
    // a type-mismatch error. The `.stderr` file (auto-generated on first run
    // with TRYBUILD=overwrite) captures the expected error message.
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_errors/id_type_mismatch.rs");
}
