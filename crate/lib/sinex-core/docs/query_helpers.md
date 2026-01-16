# Database Query Helpers

Helper functions for common database operations with consistent error handling and retry logic.

## Function Helpers

```rust,no_run
use sinex_core::db::query_helpers::{query_one, ulid_to_uuid};
use sinex_core::types::ulid::Ulid;

# async fn example(pool: &sinex_core::DbPool) -> sinex_core::types::SinexResult<()> {
let event_id = Ulid::new();
let event = query_one(
    pool,
    "SELECT * FROM core.events WHERE id = $1",
    ulid_to_uuid(event_id),
    "get event by id",
)
.await?;
# Ok(())
# }
```

## Transaction Helpers

```ignore
use sinex_core::db::query_helpers::{
    with_retry_transaction_idempotent, with_transaction, db_error, IdempotentTransaction,
};

# async fn example(pool: &sinex_core::DbPool) -> sinex_core::types::SinexResult<()> {
let result = with_transaction(pool, |tx| Box::pin(async move {
    sqlx::query("INSERT INTO table VALUES ($1)")
        .bind("value")
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error(e, "insert operation"))?;
    Ok(())
})).await?;

let retry_config = sinex_core::types::retry::RetryConfig::default();
let result = with_retry_transaction_idempotent(
    pool,
    retry_config,
    IdempotentTransaction::new(),
    |tx| Box::pin(async move {
        sqlx::query("UPDATE table SET value = $1 WHERE id = $2")
            .bind("new_value")
            .bind(123)
            .execute(&mut **tx)
            .await
            .map_err(|e| db_error(e, "update operation"))?;
        Ok(())
    }),
)
.await?;
# Ok(())
# }
```

## ULID Conversion Helpers

```rust,no_run
use sinex_core::db::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use sinex_core::types::ulid::Ulid;
use sinex_core::db::query_helpers::UlidArrayExt;

# fn example() {
let ulid = Ulid::new();
let uuid = ulid_to_uuid(ulid);
let ulid_back = uuid_to_ulid(uuid);

let ulids = vec![Ulid::new(), Ulid::new()];
let uuids = ulids.to_uuid_vec();
# }
```
