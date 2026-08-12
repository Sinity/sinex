//! sinex-gnsy (finding #3): `EventRepository::get_cascade_ids` (non-Tx,
//! `persistence.rs:2120`) builds raw SQL by interpolating `table_name`
//! directly into a `format!` string, without calling
//! `validate_cascade_table_name` first -- unlike its Tx sibling
//! (`persistence.rs:2901`), which validates before interpolating.
//! Inconsistent defense-in-depth for the same logical operation.

use sinex_db::DbPoolExt;
use xtask::sandbox::prelude::*;

#[sinex_test]
#[ignore = "sinex-gnsy open: get_cascade_ids (non-Tx) skips validate_cascade_table_name that its \
            Tx sibling enforces -- a malformed table_name reaches raw SQL interpolation \
            unvalidated instead of being rejected up front"]
async fn get_cascade_ids_validates_table_name_like_its_tx_sibling(
    ctx: TestContext,
) -> TestResult<()> {
    let result = ctx
        .pool()
        .events()
        .get_cascade_ids("not a valid cascade table name")
        .await;

    match result {
        Err(e) => {
            assert!(
                format!("{e}").contains("invalid cascade table name"),
                "get_cascade_ids should reject a malformed table_name via \
                 validate_cascade_table_name (matching its Tx sibling at persistence.rs:2901), \
                 not let it reach raw SQL interpolation and fail for an unrelated reason: {e}"
            );
        }
        Ok(_) => panic!("expected get_cascade_ids to reject a malformed table_name"),
    }

    Ok(())
}
