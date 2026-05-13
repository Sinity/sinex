// Ignored by default because trybuild compile-failure tests spawn their own
// rustc and dominate sinex-primitives wallclock (90–280 s each). Run via
// `xtask test --heavy -p sinex-primitives`; CI workspace lane runs the
// `--heavy` slice for this package explicitly so coverage isn't lost (#1215).
#[test]
#[ignore = "heavy: trybuild compile-failure (run via --heavy)"]
fn proof_descriptor_compile_failures() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/proof_compile_fail/*.rs");
}
