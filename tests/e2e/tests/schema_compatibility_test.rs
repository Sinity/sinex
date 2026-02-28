use sinex_db::DbPoolExt;
use sinex_ingestd::schema_sync::synchronize_schemas;
use sinex_primitives::query::{EventQuery, EventQueryResult, SortDirection};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn schema_and_services_remain_compatible(ctx: TestContext) -> Result<()> {
    let sync = synchronize_schemas(&ctx.pool).await?;
    assert!(sync.discovered > 0, "schema registry should not be empty");

    // Ensure the composable query engine can execute against the fresh schema.
    let query = EventQuery {
        direction: SortDirection::Desc,
        ..Default::default()
    };
    let result = ctx.pool.events().query(query).await?;
    match result {
        EventQueryResult::Events { events, .. } => {
            assert!(events.is_empty(), "fresh database should have no events");
        }
        _ => panic!("expected Events result variant"),
    }
    Ok(())
}
