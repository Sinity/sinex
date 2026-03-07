use sinex_schema::schema_registry::{SINEX_SCHEMAS, schema_names};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn all_schemas_have_names() -> TestResult<()> {
    for schema in SINEX_SCHEMAS {
        assert!(!schema.name.is_empty(), "Schema name cannot be empty");
        assert!(
            !schema.description.is_empty(),
            "Schema {} missing description",
            schema.name
        );
    }
    Ok(())
}

#[sinex_test]
async fn public_schema_is_first() -> TestResult<()> {
    assert_eq!(
        SINEX_SCHEMAS[0].name, "public",
        "public schema should be listed first"
    );
    Ok(())
}

#[sinex_test]
async fn all_schemas_require_grants() -> TestResult<()> {
    for schema in SINEX_SCHEMAS {
        assert!(
            schema.requires_grants,
            "Schema {} should require grants",
            schema.name
        );
    }
    Ok(())
}

#[sinex_test]
async fn schema_names_iterator_works() -> TestResult<()> {
    let names: Vec<&str> = schema_names().collect();
    assert_eq!(names.len(), SINEX_SCHEMAS.len());
    assert!(names.contains(&"core"));
    assert!(names.contains(&"public"));
    Ok(())
}
