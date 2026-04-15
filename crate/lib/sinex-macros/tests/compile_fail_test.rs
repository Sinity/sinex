fn trybuild_target_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(".sinex")
        .join("trybuild-target")
}

#[test]
fn compile_fail_tests() {
    // Keep macro compile-fail artifacts off the shared workspace target so
    // concurrent trybuild suites don't serialize behind one build lock.
    let _target_guard =
        xtask::sandbox::EnvGuard::set_single("CARGO_TARGET_DIR", trybuild_target_dir());

    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
}
