use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn granted_schema_names_cover_public_and_runtime_schemas() -> TestResult<()> {
    let schemas = granted_schema_names();
    assert_eq!(schemas.first().copied(), Some("public"));
    assert!(schemas.contains(&"core"));
    assert!(schemas.contains(&"raw"));
    assert!(schemas.contains(&"sinex_schemas"));
    assert!(schemas.contains(&"audit"));
    Ok(())
}
