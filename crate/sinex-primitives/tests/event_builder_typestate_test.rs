//! Compile-time coverage for the `EventBuilder` provenance typestate.
//!
//! Raw `#[test]` is intentional: trybuild owns its own compiler process and
//! does not need the async `#[sinex_test]` harness.

#[path = "support/trybuild.rs"]
mod trybuild_support;

#[test]
#[ignore = "heavy: trybuild compile-failure (run via --heavy)"]
fn event_builder_requires_provenance_before_build() {
    let t = trybuild_support::cases();
    t.compile_fail("tests/event_builder_typestate/no_provenance_build.rs");
    t.pass("tests/event_builder_typestate/valid_provenance_paths.rs");
}
