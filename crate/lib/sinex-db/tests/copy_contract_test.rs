use sinex_db::postgres_copy::verify_event_copy_contract;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn copy_column_contract_matches_schema(_ctx: TestContext) -> TestResult<()> {
    verify_event_copy_contract();
    Ok(())
}
