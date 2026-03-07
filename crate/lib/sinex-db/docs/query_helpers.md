# Database Query Helpers

Helper functions for common database operations with consistent error handling and retry logic.

## Function Helpers

```rust,no_run
use sinex_db::query_helpers::with_retry_transaction_idempotent;
use sinex_db::{DbPoolExt, IdempotentTransaction, RetryConfig};
use uuid::Uuid;

# async fn example(pool: &sqlx::PgPool) -> sinex_primitives::Result<()> {
let event_id = Uuid::now_v7();
let event = pool.events().get_by_id(event_id.into()).await?;
# let _ = event;
# Ok(())
# }
```

## Transaction Helpers

```ignore
use sinex_db::query_helpers::{with_retry_transaction_idempotent, db_error, IdempotentTransaction, RetryConfig};

# async fn example(pool: &sqlx::PgPool) -> sinex_primitives::Result<()> {
let retry_config = RetryConfig::default();
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
# let _ = result;
# Ok(())
# }
```

## UUID Helpers

```rust,no_run
use uuid::Uuid;

# fn example() {
let event_id = Uuid::now_v7();
let uuids = vec![Uuid::now_v7(), Uuid::now_v7()];
let _ = (event_id, uuids);
# }
```
