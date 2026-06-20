#[path = "support/trybuild.rs"]
mod trybuild_support;

// Ignored by default because trybuild compile-failure tests spawn rustc
// subprocesses and dominate ordinary sinex-macros test loops. The CI workspace
// lane runs this package's heavy slice automatically, so the compile-fail proof
// remains mandatory without charging every local default test run.
#[test]
#[ignore = "heavy: trybuild compile-failure (run via automatic CI heavy slice)"]
fn compile_fail_tests() {
    trybuild_support::cases().compile_fail("tests/compile_fail/*.rs");
}
