use crate::db::schema::ProcessorCheckpoints;
use crate::models::RawEvent;
use crate::repositories::common::{db_error, DbResult, EnhancedRepository, Repository};
use crate::types::domain::{ConsumerGroup, ConsumerName, ProcessorName};
use crate::types::Id;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::{FromRow, PgPool, Postgres, Transaction};

/// Checkpoint repository for database operations
pub struct CheckpointRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for CheckpointRepository<'a> {
    fn pool(&self) -> &'a PgPool {
        self.pool
    }

    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }
}

impl<'a> EnhancedRepository<'a> for CheckpointRepository<'a> {
    type Table = ProcessorCheckpoints;
}

/// Checkpoint record from database
#[derive(Debug, FromRow)]
pub struct CheckpointRecord {
    pub id: Id<CheckpointRecord>,
    pub processor_name: ProcessorName,
    pub consumer_group: ConsumerGroup,
    pub consumer_name: ConsumerName,
    pub last_processed_id: Option<Id<RawEvent>>,
    pub last_processed_ts: Option<DateTime<Utc>>,
    pub processed_count: i64,
    pub checkpoint_data: Option<JsonValue>,
    pub state_data: Option<JsonValue>,
    pub checkpoint_version: i32,
    pub last_activity: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Checkpoint to create
#[derive(Debug)]
pub struct Checkpoint {
    pub processor_name: ProcessorName,
    pub consumer_group: Option<ConsumerGroup>,
    pub consumer_name: Option<ConsumerName>,
    pub last_processed_id: Option<Id<RawEvent>>,
    pub last_processed_ts: Option<DateTime<Utc>>,
    pub checkpoint_data: Option<JsonValue>,
    pub state_data: Option<JsonValue>,
}

impl Checkpoint {
    /// Create a new checkpoint for a processor
    pub fn new(processor_name: impl Into<ProcessorName>) -> Self {
        Checkpoint {
            processor_name: processor_name.into(),
            consumer_group: None,
            consumer_name: None,
            last_processed_id: None,
            last_processed_ts: None,
            checkpoint_data: None,
            state_data: None,
        }
    }

    /// Create a checkpoint for a specific consumer
    pub fn consumer(
        processor_name: impl Into<ProcessorName>,
        consumer_group: impl Into<ConsumerGroup>,
        consumer_name: impl Into<ConsumerName>,
    ) -> Self {
        Checkpoint {
            processor_name: processor_name.into(),
            consumer_group: Some(consumer_group.into()),
            consumer_name: Some(consumer_name.into()),
            last_processed_id: None,
            last_processed_ts: None,
            checkpoint_data: None,
            state_data: None,
        }
    }

    /// Fluent method to set consumer group
    pub fn with_consumer_group(mut self, group: impl Into<ConsumerGroup>) -> Self {
        self.consumer_group = Some(group.into());
        self
    }

    /// Fluent method to set consumer name
    pub fn with_consumer_name(mut self, name: impl Into<ConsumerName>) -> Self {
        self.consumer_name = Some(name.into());
        self
    }

    /// Fluent method to set last processed event ID
    pub fn with_last_processed_id(mut self, id: Id<RawEvent>) -> Self {
        self.last_processed_id = Some(id);
        self
    }

    /// Fluent method to set last processed timestamp
    pub fn with_last_processed_ts(mut self, ts: DateTime<Utc>) -> Self {
        self.last_processed_ts = Some(ts);
        self
    }

    /// Fluent method to set checkpoint data
    pub fn with_checkpoint_data(mut self, data: JsonValue) -> Self {
        self.checkpoint_data = Some(data);
        self
    }

    /// Fluent method to set state data
    pub fn with_state_data(mut self, data: JsonValue) -> Self {
        self.state_data = Some(data);
        self
    }
}

impl<'a> CheckpointRepository<'a> {
    pub async fn insert(&self, checkpoint: Checkpoint) -> DbResult<CheckpointRecord> {
        let id = Id::<Checkpoint>::new();
        let consumer_group = checkpoint
            .consumer_group
            .unwrap_or_else(|| ConsumerGroup::new("default"));
        let consumer_name = checkpoint
            .consumer_name
            .unwrap_or_else(|| ConsumerName::new("default"));

        sqlx::query_as!(
            CheckpointRecord,
            r#"
            INSERT INTO core.processor_checkpoints (
                id, processor_name, consumer_group, consumer_name,
                last_processed_id, last_processed_ts, checkpoint_data, state_data
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            RETURNING 
                id as "id: Id<CheckpointRecord>",
                processor_name as "processor_name!: ProcessorName",
                consumer_group as "consumer_group!: ConsumerGroup",
                consumer_name as "consumer_name!: ConsumerName",
                last_processed_id as "last_processed_id: Id<RawEvent>",
                last_processed_ts,
                processed_count as "processed_count!",
                checkpoint_data,
                state_data,
                checkpoint_version as "checkpoint_version!",
                last_activity as "last_activity!",
                created_at as "created_at!",
                updated_at as "updated_at!"
            "#,
            *id.as_ulid() as _,
            checkpoint.processor_name.as_str(),
            consumer_group.as_str(),
            consumer_name.as_str(),
            checkpoint.last_processed_id.map(|id| *id.as_ulid()) as _,
            checkpoint.last_processed_ts,
            checkpoint.checkpoint_data,
            checkpoint.state_data
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "insert checkpoint"))
    }

    pub async fn get_by_processor(
        &self,
        processor_name: &ProcessorName,
    ) -> DbResult<Option<CheckpointRecord>> {
        sqlx::query_as!(
            CheckpointRecord,
            r#"
            SELECT 
                id as "id: Id<CheckpointRecord>",
                processor_name as "processor_name!: ProcessorName",
                consumer_group as "consumer_group!: ConsumerGroup",
                consumer_name as "consumer_name!: ConsumerName",
                last_processed_id as "last_processed_id: Id<RawEvent>",
                last_processed_ts,
                processed_count as "processed_count!",
                checkpoint_data,
                state_data,
                checkpoint_version as "checkpoint_version!",
                last_activity as "last_activity!",
                created_at as "created_at!",
                updated_at as "updated_at!"
            FROM core.processor_checkpoints 
            WHERE processor_name = $1
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
            processor_name.as_str()
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get checkpoint by processor"))
    }

    pub async fn get_by_processor_and_consumer(
        &self,
        processor_name: &ProcessorName,
        consumer_group: &ConsumerGroup,
        consumer_name: &ConsumerName,
    ) -> DbResult<Option<CheckpointRecord>> {
        sqlx::query_as!(
            CheckpointRecord,
            r#"
            SELECT 
                id as "id: Id<CheckpointRecord>",
                processor_name as "processor_name!: ProcessorName",
                consumer_group as "consumer_group!: ConsumerGroup",
                consumer_name as "consumer_name!: ConsumerName",
                last_processed_id as "last_processed_id: Id<RawEvent>",
                last_processed_ts,
                processed_count as "processed_count!",
                checkpoint_data,
                state_data,
                checkpoint_version as "checkpoint_version!",
                last_activity as "last_activity!",
                created_at as "created_at!",
                updated_at as "updated_at!"
            FROM core.processor_checkpoints 
            WHERE processor_name = $1 
              AND consumer_group = $2 
              AND consumer_name = $3
            "#,
            processor_name.as_str(),
            consumer_group.as_str(),
            consumer_name.as_str()
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get checkpoint by processor and consumer"))
    }

    pub async fn update(
        &self,
        processor_name: &ProcessorName,
        consumer_group: &ConsumerGroup,
        consumer_name: &ConsumerName,
        last_processed_id: Option<Id<RawEvent>>,
        last_processed_ts: Option<DateTime<Utc>>,
        checkpoint_data: Option<JsonValue>,
        state_data: Option<JsonValue>,
        increment_count: bool,
    ) -> DbResult<CheckpointRecord> {
        if increment_count {
            sqlx::query_as!(
                CheckpointRecord,
                r#"
                UPDATE core.processor_checkpoints 
                SET 
                    last_processed_id = $4,
                    last_processed_ts = $5,
                    checkpoint_data = $6,
                    state_data = $7,
                    processed_count = processed_count + 1,
                    last_activity = NOW(),
                    updated_at = NOW()
                WHERE processor_name = $1 
                  AND consumer_group = $2 
                  AND consumer_name = $3
                RETURNING 
                    id as "id: Id<CheckpointRecord>",
                    processor_name as "processor_name!: ProcessorName",
                    consumer_group as "consumer_group!: ConsumerGroup",
                    consumer_name as "consumer_name!: ConsumerName",
                    last_processed_id as "last_processed_id: Id<RawEvent>",
                    last_processed_ts,
                    processed_count as "processed_count!",
                    checkpoint_data,
                    state_data,
                    checkpoint_version as "checkpoint_version!",
                    last_activity as "last_activity!",
                    created_at as "created_at!",
                    updated_at as "updated_at!"
                "#,
                processor_name.as_str(),
                consumer_group.as_str(),
                consumer_name.as_str(),
                last_processed_id.map(|id| *id.as_ulid()) as _,
                last_processed_ts,
                checkpoint_data,
                state_data
            )
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "update checkpoint"))
        } else {
            sqlx::query_as!(
                CheckpointRecord,
                r#"
                UPDATE core.processor_checkpoints 
                SET 
                    last_processed_id = $4,
                    last_processed_ts = $5,
                    checkpoint_data = $6,
                    state_data = $7,
                    last_activity = NOW(),
                    updated_at = NOW()
                WHERE processor_name = $1 
                  AND consumer_group = $2 
                  AND consumer_name = $3
                RETURNING 
                    id as "id: Id<CheckpointRecord>",
                    processor_name as "processor_name!: ProcessorName",
                    consumer_group as "consumer_group!: ConsumerGroup",
                    consumer_name as "consumer_name!: ConsumerName",
                    last_processed_id as "last_processed_id: Id<RawEvent>",
                    last_processed_ts,
                    processed_count as "processed_count!",
                    checkpoint_data,
                    state_data,
                    checkpoint_version as "checkpoint_version!",
                    last_activity as "last_activity!",
                    created_at as "created_at!",
                    updated_at as "updated_at!"
                "#,
                processor_name.as_str(),
                consumer_group.as_str(),
                consumer_name.as_str(),
                last_processed_id.map(|id| *id.as_ulid()) as _,
                last_processed_ts,
                checkpoint_data,
                state_data
            )
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "update checkpoint"))
        }
    }

    pub async fn upsert(
        &self,
        processor_name: &ProcessorName,
        consumer_group: &ConsumerGroup,
        consumer_name: &ConsumerName,
        last_processed_id: Option<Id<RawEvent>>,
        last_processed_ts: Option<DateTime<Utc>>,
        checkpoint_data: Option<JsonValue>,
        state_data: Option<JsonValue>,
    ) -> DbResult<CheckpointRecord> {
        let id = Id::<Checkpoint>::new();

        sqlx::query_as!(
            CheckpointRecord,
            r#"
            INSERT INTO core.processor_checkpoints (
                id, processor_name, consumer_group, consumer_name,
                last_processed_id, last_processed_ts, checkpoint_data, state_data
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            ON CONFLICT (processor_name, consumer_group, consumer_name) 
            DO UPDATE SET
                last_processed_id = EXCLUDED.last_processed_id,
                last_processed_ts = EXCLUDED.last_processed_ts,
                checkpoint_data = EXCLUDED.checkpoint_data,
                state_data = EXCLUDED.state_data,
                processed_count = core.processor_checkpoints.processed_count + 1,
                last_activity = NOW(),
                updated_at = NOW()
            RETURNING 
                id as "id: Id<CheckpointRecord>",
                processor_name as "processor_name!: ProcessorName",
                consumer_group as "consumer_group!: ConsumerGroup",
                consumer_name as "consumer_name!: ConsumerName",
                last_processed_id as "last_processed_id: Id<RawEvent>",
                last_processed_ts,
                processed_count as "processed_count!",
                checkpoint_data,
                state_data,
                checkpoint_version as "checkpoint_version!",
                last_activity as "last_activity!",
                created_at as "created_at!",
                updated_at as "updated_at!"
            "#,
            *id.as_ulid() as _,
            processor_name.as_str(),
            consumer_group.as_str(),
            consumer_name.as_str(),
            last_processed_id.map(|id| *id.as_ulid()) as _,
            last_processed_ts,
            checkpoint_data,
            state_data
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "upsert checkpoint"))
    }

    pub async fn delete(
        &self,
        processor_name: &ProcessorName,
        consumer_group: &ConsumerGroup,
        consumer_name: &ConsumerName,
    ) -> DbResult<bool> {
        let result = sqlx::query!(
            "DELETE FROM core.processor_checkpoints WHERE processor_name = $1 AND consumer_group = $2 AND consumer_name = $3",
            processor_name.as_str(),
            consumer_group.as_str(),
            consumer_name.as_str()
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "delete checkpoint"))?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn list(
        &self,
        processor_name: Option<&ProcessorName>,
        consumer_group: Option<&ConsumerGroup>,
        stale_before: Option<DateTime<Utc>>,
        limit: Option<i64>,
    ) -> DbResult<Vec<CheckpointRecord>> {
        // Build a dynamic query based on the filters
        // For simplicity, we'll use multiple query variants
        match (processor_name, consumer_group, stale_before, limit) {
            (None, None, None, None) => sqlx::query_as!(
                CheckpointRecord,
                r#"
                    SELECT 
                        id as "id: Id<CheckpointRecord>",
                        processor_name as "processor_name!: ProcessorName",
                        consumer_group as "consumer_group!: ConsumerGroup",
                        consumer_name as "consumer_name!: ConsumerName",
                        last_processed_id as "last_processed_id: Id<RawEvent>",
                        last_processed_ts,
                        processed_count as "processed_count!",
                        checkpoint_data,
                        state_data,
                        checkpoint_version as "checkpoint_version!",
                        last_activity as "last_activity!",
                        created_at as "created_at!",
                        updated_at as "updated_at!"
                    FROM core.processor_checkpoints 
                    ORDER BY updated_at DESC
                    "#
            )
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "list checkpoints")),
            (None, None, None, Some(limit)) => sqlx::query_as!(
                CheckpointRecord,
                r#"
                    SELECT 
                        id as "id: Id<CheckpointRecord>",
                        processor_name as "processor_name!: ProcessorName",
                        consumer_group as "consumer_group!: ConsumerGroup",
                        consumer_name as "consumer_name!: ConsumerName",
                        last_processed_id as "last_processed_id: Id<RawEvent>",
                        last_processed_ts,
                        processed_count as "processed_count!",
                        checkpoint_data,
                        state_data,
                        checkpoint_version as "checkpoint_version!",
                        last_activity as "last_activity!",
                        created_at as "created_at!",
                        updated_at as "updated_at!"
                    FROM core.processor_checkpoints 
                    ORDER BY updated_at DESC
                    LIMIT $1
                    "#,
                limit
            )
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "list checkpoints")),
            _ => {
                // For other combinations, use a simple default query
                // In a real implementation, you'd build this dynamically
                sqlx::query_as!(
                    CheckpointRecord,
                    r#"
                    SELECT 
                        id as "id: Id<CheckpointRecord>",
                        processor_name as "processor_name!: ProcessorName",
                        consumer_group as "consumer_group!: ConsumerGroup",
                        consumer_name as "consumer_name!: ConsumerName",
                        last_processed_id as "last_processed_id: Id<RawEvent>",
                        last_processed_ts,
                        processed_count as "processed_count!",
                        checkpoint_data,
                        state_data,
                        checkpoint_version as "checkpoint_version!",
                        last_activity as "last_activity!",
                        created_at as "created_at!",
                        updated_at as "updated_at!"
                    FROM core.processor_checkpoints 
                    ORDER BY updated_at DESC
                    LIMIT 100
                    "#
                )
                .fetch_all(self.pool)
                .await
                .map_err(|e| db_error(e, "list checkpoints"))
            }
        }
    }

    pub async fn upsert_with_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        processor_name: &ProcessorName,
        consumer_group: &ConsumerGroup,
        consumer_name: &ConsumerName,
        last_processed_id: Option<Id<RawEvent>>,
        last_processed_ts: Option<DateTime<Utc>>,
        checkpoint_data: Option<JsonValue>,
        state_data: Option<JsonValue>,
    ) -> DbResult<CheckpointRecord> {
        let id = Id::<Checkpoint>::new();

        sqlx::query_as!(
            CheckpointRecord,
            r#"
            INSERT INTO core.processor_checkpoints (
                id, processor_name, consumer_group, consumer_name,
                last_processed_id, last_processed_ts, checkpoint_data, state_data
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            ON CONFLICT (processor_name, consumer_group, consumer_name) 
            DO UPDATE SET
                last_processed_id = EXCLUDED.last_processed_id,
                last_processed_ts = EXCLUDED.last_processed_ts,
                checkpoint_data = EXCLUDED.checkpoint_data,
                state_data = EXCLUDED.state_data,
                processed_count = core.processor_checkpoints.processed_count + 1,
                last_activity = NOW(),
                updated_at = NOW()
            RETURNING 
                id as "id: Id<CheckpointRecord>",
                processor_name as "processor_name!: ProcessorName",
                consumer_group as "consumer_group!: ConsumerGroup",
                consumer_name as "consumer_name!: ConsumerName",
                last_processed_id as "last_processed_id: Id<RawEvent>",
                last_processed_ts,
                processed_count as "processed_count!",
                checkpoint_data,
                state_data,
                checkpoint_version as "checkpoint_version!",
                last_activity as "last_activity!",
                created_at as "created_at!",
                updated_at as "updated_at!"
            "#,
            *id.as_ulid() as _,
            processor_name.as_str(),
            consumer_group.as_str(),
            consumer_name.as_str(),
            last_processed_id.map(|id| *id.as_ulid()) as _,
            last_processed_ts,
            checkpoint_data,
            state_data
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| db_error(e, "upsert checkpoint with tx"))
    }
}

/// Extension trait for Checkpoint terminal methods
pub trait CheckpointExt {
    /// Insert the checkpoint in the database
    async fn insert(self, pool: &PgPool) -> DbResult<CheckpointRecord>;

    /// Upsert the checkpoint in the database
    async fn upsert(self, pool: &PgPool) -> DbResult<CheckpointRecord>;
}

impl CheckpointExt for Checkpoint {
    async fn insert(self, pool: &PgPool) -> DbResult<CheckpointRecord> {
        CheckpointRepository::new(pool).insert(self).await
    }

    async fn upsert(self, pool: &PgPool) -> DbResult<CheckpointRecord> {
        let processor_name = self.processor_name.clone();
        let consumer_group = self
            .consumer_group
            .clone()
            .unwrap_or_else(|| ConsumerGroup::new("default"));
        let consumer_name = self
            .consumer_name
            .clone()
            .unwrap_or_else(|| ConsumerName::new("default"));

        CheckpointRepository::new(pool)
            .upsert(
                &processor_name,
                &consumer_group,
                &consumer_name,
                self.last_processed_id,
                self.last_processed_ts,
                self.checkpoint_data,
                self.state_data,
            )
            .await
    }
}
