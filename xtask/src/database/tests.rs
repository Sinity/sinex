use std::path::Path;

#[devkit::test]
fn checked_queries_rebuild_for_schema_changes_and_manual_database_changes() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir(root.join("src")).unwrap();
    std::fs::write(
        root.join("Cargo.toml"),
        "[package]\nname = \"checked-schema-fixture\"\nversion = \"0.0.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .unwrap();
    std::fs::write(root.join("src/lib.rs"), "pub fn checked() {}\n").unwrap();
    let build = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("crates/sinex-store/build.rs");
    std::fs::write(
        root.join("build.rs"),
        format!(
            r#"mod owner {{
    include!({build:?});
    pub fn run() {{ main(); }}
}}
fn main() {{
    owner::run();
    use std::io::Write;
    let mut log = std::fs::OpenOptions::new().create(true).append(true)
        .open(std::env::var_os("BUILD_RUN_LOG").unwrap()).unwrap();
    writeln!(log, "checked").unwrap();
}}
"#,
        ),
    )
    .unwrap();
    let log = root.join("runs");
    let run = |identity: Option<&str>, url: &str, expected_runs: usize| {
        let mut command = devkit::cargo_command();
        command
            .args(["check", "--offline", "--quiet"])
            .current_dir(root)
            .env("CARGO_TARGET_DIR", root.join("target"))
            .env("BUILD_RUN_LOG", &log)
            .env("DATABASE_URL", url)
            .env_remove("SINEX_SCHEMA_IDENTITY");
        if let Some(identity) = identity {
            command.env("SINEX_SCHEMA_IDENTITY", identity);
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(&log).unwrap().lines().count(),
            expected_runs,
            "identity={identity:?}, url={url}"
        );
    };
    run(Some("sinex_schema_a"), "postgresql://socket-a/schema-a", 1);
    run(Some("sinex_schema_a"), "postgresql://socket-b/schema-a", 1);
    run(Some("sinex_schema_b"), "postgresql://socket-b/schema-b", 2);
    run(None, "postgresql://socket-b/schema-b", 3);
    run(None, "postgresql://socket-c/manual", 4);
    run(None, "postgresql://socket-c/manual", 4);
    run(Some("sinex_schema_b"), "postgresql://socket-c/schema-b", 5);
}
