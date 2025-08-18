//! The Canonical Database Schema for the Transactional Outbox.
//!
//! This module defines the `core.transactional_outbox` table, a critical component
//! for implementing the "Post-Commit Publish" invariant. This pattern guarantees
//! that an event is published to the message bus (NATS) if, and only if, the
//! transaction that persisted it to `core.events` was successfully committed.

use crate::schema::{Events, TableDef};
use crate::ulid::Ulid;
use chrono::{DateTime, Utc};
use sea_orm_migration::prelude::*;
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// The `core.transactional_outbox` Table
// =============================================================================

/// **Table: `core.transactional_outbox`**
///
/// This table serves as a durable, temporary queue for events that have been
/// committed to the database but not yet published to the NATS message bus.
///
/// **The Workflow (as implemented by `ingestd`):**
/// 1. `BEGIN` a database transaction.
/// 2. `INSERT` a batch of events into `core.events`.
/// 3. `INSERT` a corresponding entry for each of those events into this outbox table.
/// 4. `COMMIT` the transaction.
/// 5. A separate, asynchronous poller process queries this table for `pending` messages.
/// 6. The poller publishes the messages to NATS.
/// 7. Upon successful acknowledgement from NATS, the poller `DELETE`s the row from this table.
///
/// **Architectural Value:** This pattern solves the "dual write" problem. It makes the
/// action of "saving an event and publishing it" atomic. If the system crashes after
/// the database commit but before the NATS publish, the outbox record persists.
/// On restart, the poller will find the unprocessed message and ensure it is delivered.
#[derive(Iden, Copy, Clone)]
pub enum TransactionalOutbox {
    Table,
    Id,
    EventId,
    Destination,
    Payload,
    Headers,
    Status,
    RetryCount,
    LastAttemptAt,
    ErrorMessage,
    CreatedAt,
    ProcessedAt,
}

impl TableDef for TransactionalOutbox {
    fn table_name() -> &'static str {
        "transactional_outbox"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

/// The Rust struct representation of a row from `core.transactional_outbox`.
#[derive(Debug, FromRow)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OutboxRecord {
    pub id: i64, // Using BIGSERIAL for simple, ordered polling.
    pub event_id: Ulid,
    pub destination: String, // e.g., the NATS subject.
    pub payload: Vec<u8>,    // Storing as raw bytes is more efficient for a bus message.
    pub headers: JsonValue,
    pub status: String,
    pub retry_count: i32,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
}

impl TransactionalOutbox {
    /// Generates the `CREATE TABLE` statement for `core.transactional_outbox`.
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            // Using BIGSERIAL is a good choice here. It's not a domain ID but a simple,
            // ordered queue pointer, which is exactly what a serial integer is for.
            .col(
                ColumnDef::new(TransactionalOutbox::Id)
                    .big_integer()
                    .primary_key()
                    .auto_increment(),
            )
            .col(
                ColumnDef::new(TransactionalOutbox::EventId)
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(TransactionalOutbox::Destination)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(TransactionalOutbox::Payload)
                    .binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(TransactionalOutbox::Headers)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(TransactionalOutbox::Status)
                    .text()
                    .not_null()
                    .default("'pending'")
                    .check(Expr::cust(
                        "status IN ('pending', 'processing', 'sent', 'failed')",
                    )),
            )
            .col(
                ColumnDef::new(TransactionalOutbox::RetryCount)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(ColumnDef::new(TransactionalOutbox::LastAttemptAt).timestamp_with_time_zone())
            .col(ColumnDef::new(TransactionalOutbox::ErrorMessage).text())
            .col(
                ColumnDef::new(TransactionalOutbox::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(TransactionalOutbox::ProcessedAt).timestamp_with_time_zone())
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), TransactionalOutbox::EventId)
                    .to(Events::table_iden(), Alias::new("id"))
                    .on_delete(ForeignKeyAction::Cascade), // If the source event is deleted (archived), the outbox message for it should also be removed.
            )
            .to_owned()
    }

    /// Generates indexes for `core.transactional_outbox`.
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // This partial index is the MOST IMPORTANT one. It allows the poller to
            // find pending messages with near-zero cost, without scanning rows
            // that are already processing or have been sent.
            Index::create()
                .name("ix_outbox_pending_messages")
                .table(Self::table_iden())
                .col(TransactionalOutbox::CreatedAt)
                .cond_where(Expr::col(TransactionalOutbox::Status).eq("pending"))
                .to_owned(),
            // Index to efficiently find and clean up failed messages that may require
            // manual intervention or a dead-letter queue.
            Index::create()
                .name("ix_outbox_failed_messages")
                .table(Self::table_iden())
                .col(TransactionalOutbox::LastAttemptAt)
                .cond_where(Expr::col(TransactionalOutbox::Status).eq("failed"))
                .to_owned(),
        ]
    }
}
