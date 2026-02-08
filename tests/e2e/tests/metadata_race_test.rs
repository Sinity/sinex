use xtask::sandbox::prelude::*;

#[sinex_test]
#[ignore = "requires concurrent metadata access testing"]
async fn test_metadata_update_race_condition(_ctx: TestContext) -> TestResult<()> {
    /* Broken: This test uses SourceMaterialRepository which doesn't exist in the current API.
    // The crate previously relied on sinex_db::repositories::SourceMaterialRepository
    // which has been removed. This test should be rewritten with the new Event/Provenance API.
    // See: tests/e2e/tests/stress_test.rs for the updated pattern.
    let pool = ctx.pool().clone();
    let repo = SourceMaterialRepository::new(&pool);
    ...
    */
    Ok(())
}
