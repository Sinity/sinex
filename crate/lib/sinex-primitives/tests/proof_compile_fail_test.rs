#[test]
fn proof_descriptor_compile_failures() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/proof_compile_fail/*.rs");
}
