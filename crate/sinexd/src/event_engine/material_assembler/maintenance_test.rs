use super::super::RESTORED_SELF_OBSERVATION_ORPHAN_TIMEOUT_SECS;

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::{Id, MaterialStatus, Timestamp, Uuid};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn orphan_reconcile_does_not_short_timeout_live_self_observation_registry_rows(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (assembler, _content_store_dir, _state_dir) =
        super::super::test_support::TestAssemblerBuilder::new("maintenance-test")
            .slice_timeout_secs(3_600)
            .build(&ctx)
            .await?;

    let short_stale_start = Timestamp::now()
        - time::Duration::seconds(RESTORED_SELF_OBSERVATION_ORPHAN_TIMEOUT_SECS + 1);
    let self_observation_id = Uuid::now_v7();
    let ordinary_id = Uuid::now_v7();

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            self_observation_id,
            "self_observation",
            Some(&format!(
                "sinex.self-observation.test#material={self_observation_id}"
            )),
            json!({}),
            short_stale_start,
        )
        .await?;
    ctx.pool
        .source_materials()
        .register_external_in_flight(
            ordinary_id,
            "test",
            Some(&format!("test://ordinary-orphan/{ordinary_id}")),
            json!({}),
            short_stale_start,
        )
        .await?;

    assembler.reconcile_orphaned_sensing_materials().await?;

    let self_observation = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(self_observation_id))
        .await?
        .expect("self-observation material should exist");
    let ordinary = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(ordinary_id))
        .await?
        .expect("ordinary material should exist");

    assert_eq!(self_observation.status, MaterialStatus::Sensing);
    assert_eq!(ordinary.status, MaterialStatus::Sensing);
    Ok(())
}
