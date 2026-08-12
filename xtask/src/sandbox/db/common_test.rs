//! Regression coverage for sinex-mb9u: `get_nonempty_tables` hardcodes
//! `raw.source_material_registry` out of dirty-state detection entirely, so
//! residual rows there can never fail `verify_clean_state` — even when it's
//! the only table left dirty.

use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
#[ignore = "sinex-mb9u open: is_reportable_nonempty_table hardcodes \
            raw.source_material_registry out of dirty-state detection, so a real \
            residual row there is never reported"]
async fn source_material_registry_residual_rows_are_reportable() -> TestResult<()> {
    assert!(
        is_reportable_nonempty_table("raw.source_material_registry", true),
        "a non-empty raw.source_material_registry must be reportable as residual \
         state, not silently excluded"
    );
    Ok(())
}
