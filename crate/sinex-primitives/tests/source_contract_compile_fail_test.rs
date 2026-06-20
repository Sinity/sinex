#[path = "support/trybuild.rs"]
mod trybuild_support;

#[test]
#[ignore = "heavy: trybuild compile-failure (run via --heavy)"]
fn source_contract_compile_failures() {
    trybuild_support::cases().compile_fail("tests/source_contract_compile_fail/*.rs");
}
