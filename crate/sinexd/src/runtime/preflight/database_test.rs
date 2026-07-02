use super::{discover_schema_sources, validate_schema_source};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn schema_source_manifest_is_embedded() -> TestResult<()> {
    let schema_sources = discover_schema_sources();
    assert!(!schema_sources.is_empty());
    assert!(schema_sources.iter().all(|source| source.embedded));
    assert!(schema_sources.iter().all(|source| source.bytes > 0));
    assert!(
        schema_sources
            .iter()
            .all(|source| source.path.starts_with("crate/sinex-schema/src/"))
    );
    Ok(())
}

#[sinex_test]
async fn embedded_schema_sources_validate_without_filesystem_access() -> TestResult<()> {
    for source in discover_schema_sources() {
        validate_schema_source(&source).expect("embedded schema source should validate");
    }
    Ok(())
}
