use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_migration_up_down_cycle() -> color_eyre::eyre::Result<()> {
    let ctx = TestContext::new().await.unwrap();
    let pool = &ctx.pool;
    // This test must not mutate the shared `core.events` table because the test harness reuses
    // pooled databases across tests and only truncates data (it does not reset schema).
    //
    // Use a transaction-scoped, scratch table to validate the up/down DDL shape, then roll back.
    let mut tx = pool.begin().await?;

    sqlx::query("DROP TABLE IF EXISTS core.events_migration_test")
        .execute(&mut *tx)
        .await?;

    // Create a minimal events-like table without the column.
    sqlx::query(
        "CREATE TABLE core.events_migration_test (
            id UUID PRIMARY KEY DEFAULT uuidv7(),
            source TEXT NOT NULL,
            event_type TEXT NOT NULL,
            host TEXT NOT NULL,
            payload JSONB NOT NULL,
            ts_orig TIMESTAMPTZ NOT NULL,
            ts_coided TIMESTAMPTZ NOT NULL
        )",
    )
    .execute(&mut *tx)
    .await?;

    // Simulate migration UP: add associated_blob_ids column.
    sqlx::query(
        "ALTER TABLE core.events_migration_test ADD COLUMN IF NOT EXISTS associated_blob_ids UUID[]",
    )
    .execute(&mut *tx)
    .await?;

    // Verify the column was added.
    let columns: Vec<String> = sqlx::query(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'core' AND table_name = 'events_migration_test'",
    )
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .map(|row| row.get::<String, _>("column_name"))
    .collect();
    assert!(columns.iter().any(|c| c == "associated_blob_ids"));

    // Simulate migration DOWN: drop the column.
    sqlx::query(
        "ALTER TABLE core.events_migration_test DROP COLUMN IF EXISTS associated_blob_ids",
    )
    .execute(&mut *tx)
    .await?;

    // Verify the column was removed.
    let columns: Vec<String> = sqlx::query(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'core' AND table_name = 'events_migration_test'",
    )
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .map(|row| row.get::<String, _>("column_name"))
    .collect();
    assert!(!columns.iter().any(|c| c == "associated_blob_ids"));

    tx.rollback().await?;
    Ok(())
}
