use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_dependency_surface_extracts_flags() -> ::xtask::sandbox::TestResult<()> {
    let value = r#"{ path = "../crate/sinex-db", optional = true }"#.parse::<toml::Value>()?;
    let dependency = dependency_surface("sinex-db", &value);

    assert_eq!(dependency.name, "sinex-db");
    assert_eq!(dependency.path.as_deref(), Some("../crate/sinex-db"));
    assert!(dependency.optional);
    assert!(!dependency.workspace);
    Ok(())
}

#[sinex_test]
async fn test_module_buckets_group_top_level_paths() -> ::xtask::sandbox::TestResult<()> {
    let source_root = Path::new("/repo/xtask/src");
    let buckets = module_buckets(
        source_root,
        &[
            SourceFileSurface {
                path: "/repo/xtask/src/lib.rs".to_string(),
                bytes: 10,
            },
            SourceFileSurface {
                path: "/repo/xtask/src/history/db.rs".to_string(),
                bytes: 20,
            },
            SourceFileSurface {
                path: "/repo/xtask/src/history/query.rs".to_string(),
                bytes: 30,
            },
        ],
    );

    let history = buckets
        .iter()
        .find(|bucket| bucket.bucket == "history")
        .unwrap();
    assert_eq!(history.files, 2);
    assert_eq!(history.bytes, 50);
    Ok(())
}
