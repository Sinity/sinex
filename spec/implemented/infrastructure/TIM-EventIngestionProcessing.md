# TIM-EventIngestionProcessing: Queues, Workers, Notifications

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 75% (PostgreSQL queue and worker patterns working, FastCDC and advanced features missing)
**Dependencies**: PostgreSQL, promotion_queue table, worker processes, agent manifests
**Blocks**: Event processing pipeline, promotion workflows, downstream analysis

## MVP Specification
- PostgreSQL-based promotion queue with transactional processing
- Worker polling with FOR UPDATE SKIP LOCKED
- Exponential backoff retry mechanism
- Dead letter queue for failed items
- Content deduplication with FastCDC and BLAKE3

## Enhanced Features
- Redis streams for high-throughput scenarios
- LISTEN/NOTIFY wake-up optimization
- Distributed worker coordination
- Advanced retry policies
- Performance monitoring and metrics

## Implementation Checklist
- [x] promotion_queue table schema
- [x] Worker polling and claiming logic
- [x] Exponential backoff with jitter
- [x] Dead letter queue handling
- [ ] Content-defined chunking (FastCDC)
- [x] BLAKE3 hashing for deduplication
- [x] Performance indexes and constraints
- [ ] LISTEN/NOTIFY wake-up signals
- [ ] Redis streams integration
- [ ] Distributed worker coordination

*   **Relevant ADR:** `[ADR-002-EventProcessingNotificationMechanism.md](docs/adr/ADR-002-EventProcessingNotificationMechanism.md)`
*   **Original UG Context:** Section 3 (specifically 3.1, 3.2, 3.3)

This TIM details the mechanisms for ingesting raw events, processing them via queues and worker patterns, and the trade-offs of different event notification systems. The primary mechanism for processing is a PostgreSQL-based queue with worker polling, as per ADR-002.

## 1. Content Hashing & Deduplication on Ingest (for specific large/repetitive payloads) [UG Sec 3.1]

While most `raw.events.payload` objects are expected to be small metadata, for specific event types that might carry substantial or highly repetitive textual/binary data *directly within their payload* (rather than as linked `core_blobs`), an ingestor can employ content-defined chunking and hashing *before* database insertion.

*   **Algorithm [CR3]:** FastCDC for content-defined chunking, BLAKE3 for hashing chunks.
*   **Performance [CR3]:** FastCDC+BLAKE3 benchmarked at 2.1 GB/s, 96.4% deduplication on a specific test dataset.
*   **Scenario:**
    1.  Ingestor receives/generates a large inline payload.
    2.  Applies FastCDC to chunk it.
    3.  Hashes each chunk with BLAKE3.
    4.  Checks if chunks (by hash) exist (e.g., in `core_blobs` or a dedicated chunk store).
    5.  Stores new unique chunks.
    6.  The `raw.events.payload` then stores metadata or a manifest of chunk hashes, not the full inline data.
*   **Relevance:** Primarily for event sources producing large inline data. For most large content (PKM notes, web pages, media), the primary strategy is direct storage in `git-annex` via `core_blobs`, with `raw.events` referencing these.

## 2. Event Processing Queue: `sinex_schemas.promotion_queue` [UG Sec 3.2.1]

This table acts as a persistent, transactional work queue for agents to process raw events.

*   **DDL (from UG Sec 3.2.1, refined):**
    ```sql
    CREATE TABLE IF NOT EXISTS sinex_schemas.promotion_queue (
      queue_id                ULID PRIMARY KEY DEFAULT gen_ulid(), -- Using pgx_ulid
      raw_event_id            ULID NOT NULL REFERENCES raw.events(id) ON DELETE CASCADE,
      target_agent_name       TEXT NOT NULL REFERENCES sinex_schemas.agent_manifests(agent_name) ON DELETE CASCADE,
      status                  TEXT NOT NULL DEFAULT 'pending', -- Values: 'pending', 'processing', 'failed_retryable'
      attempts                INT NOT NULL DEFAULT 0,
      max_attempts            INT NOT NULL DEFAULT 5, -- Default, can be overridden by agent config/logic
      last_attempt_ts         TIMESTAMPTZ NULLABLE,
      next_retry_ts           TIMESTAMPTZ NULLABLE, -- For exponential backoff
      error_message_last      TEXT NULLABLE,
      created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
      processing_worker_id    TEXT NULLABLE, -- Identifier of worker instance currently processing
      CONSTRAINT uq_promotion_queue_event_agent UNIQUE (raw_event_id, target_agent_name)
    );

    COMMENT ON TABLE sinex_schemas.promotion_queue IS 'Work queue for agents to process raw events for promotion, enrichment, etc.';
    COMMENT ON COLUMN sinex_schemas.promotion_queue.status IS 'Current status: pending, processing, failed_retryable.';
    COMMENT ON COLUMN sinex_schemas.promotion_queue.next_retry_ts IS 'If status is failed_retryable, when next attempt should be made.';

    -- Index for workers to efficiently pick up pending tasks
    CREATE INDEX IF NOT EXISTS idx_promo_queue_pending_tasks ON sinex_schemas.promotion_queue (status, target_agent_name, next_retry_ts ASC NULLS FIRST, created_at ASC)
    WHERE status = 'pending' OR status = 'failed_retryable';

    -- Index for monitoring tasks that have repeatedly failed
    CREATE INDEX IF NOT EXISTS idx_promo_queue_failed_tasks ON sinex_schemas.promotion_queue (target_agent_name, status, attempts)
    WHERE status = 'failed_retryable';
    ```
*   **Population:** A router mechanism (e.g., PostgreSQL trigger on `raw.events` calling `sinex_router.route_raw_event_to_promotion_queue()`, see `TIM-PostgreSQL-AdvancedFeatures.md` and `TIM-AgentManifestManagement.md`) populates this queue based on `agent_manifests.subscribes_to_event_types`.

## 3. Worker Pattern for Promotion Queue [UG Sec 3.2.2]

Worker processes (e.g., Rust `sinex-promo-worker`) poll the `promotion_queue`.

*   **SQL for Polling and Claiming Items:**
    ```sql
    -- $1 = target_agent_name (e.g., 'PkmNoteEmbedderAgent_Python_v0.1.0')
    -- $2 = batch_size (e.g., 10)
    -- $3 = worker_id (e.g., 'hostname-worker-1')
    UPDATE sinex_schemas.promotion_queue
    SET status = 'processing', last_attempt_ts = now(), processing_worker_id = $3
    WHERE queue_id IN (
        SELECT queue_id
        FROM sinex_schemas.promotion_queue
        WHERE
            status IN ('pending', 'failed_retryable')
            AND target_agent_name = $1
            AND (next_retry_ts IS NULL OR next_retry_ts <= now())
        ORDER BY
            CASE status WHEN 'failed_retryable' THEN 0 ELSE 1 END, -- Prioritize retries
            next_retry_ts ASC NULLS FIRST,
            created_at ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED -- Crucial for concurrent workers
    )
    RETURNING queue_id, raw_event_id, target_agent_name, attempts, max_attempts;
    ```
    *This combined `UPDATE ... RETURNING` is often more efficient than separate `SELECT FOR UPDATE` then `UPDATE` for claiming items.*

*   **Rust Worker Logic (`sinex-promo-worker` - Conceptual Core from UG Sec 3.2.2):**
    ```rust
     use sqlx::{PgPool, Row, types::chrono::{DateTime, Utc}};
     use ulid::Ulid;
     use std::time::Duration;
     use rand::Rng; // For jitter in backoff

     #[derive(sqlx::FromRow, Debug)]
     struct PromotionQueueItem {
         queue_id: Ulid,
         raw_event_id: Ulid,
         target_agent_name: String,
         attempts: i32,
         max_attempts: i32,
     }

     // Placeholder for actual agent processing logic
     async fn dispatch_to_agent_processor(db_pool: &PgPool, item: &PromotionQueueItem) -> Result<(), anyhow::Error> {
         // Simulate work; actual logic fetches raw_event.payload, transforms, inserts to domain tables etc.
         println!("Processing item: {:?}", item);
         tokio::time::sleep(Duration::from_millis(100)).await;
         // Simulate occasional failure
         if rand::thread_rng().gen_bool(0.1) {
             return Err(anyhow::anyhow!("Simulated processing failure for event {}", item.raw_event_id));
         }
         Ok(())
     }

    // pub async fn promotion_worker_loop(db_pool: PgPool, worker_id: String, agent_filter_name: String, batch_size: i32) {
         loop {
             let items_to_process: Vec<PromotionQueueItem> = match sqlx::query_as(
                 "UPDATE sinex_schemas.promotion_queue \
                  SET status = 'processing', last_attempt_ts = now(), processing_worker_id = $3 \
                  WHERE queue_id IN ( \
                      SELECT queue_id FROM sinex_schemas.promotion_queue \
                      WHERE status IN ('pending', 'failed_retryable') AND target_agent_name = $1 \
                        AND (next_retry_ts IS NULL OR next_retry_ts <= now()) \
                      ORDER BY CASE status WHEN 'failed_retryable' THEN 0 ELSE 1 END, next_retry_ts ASC NULLS FIRST, created_at ASC \
                      LIMIT $2 FOR UPDATE SKIP LOCKED \
                  ) \
                  RETURNING queue_id, raw_event_id, target_agent_name, attempts, max_attempts"
             )
             .bind(&agent_filter_name)
             .bind(batch_size)
             .bind(&worker_id)
             .fetch_all(&db_pool)
             .await
             {
                 Ok(items) => items,
                 Err(e) => {
                     eprintln!("[{}] Error claiming items from queue: {}. Retrying in 5s.", worker_id, e);
                     tokio::time::sleep(Duration::from_secs(5)).await;
                     continue;
                 }
             };

             if items_to_process.is_empty() {
                 tokio::time::sleep(Duration::from_secs(1)).await; // Poll interval if no items
                 continue;
             }

             println!("[{}] Claimed {} items for processing.", worker_id, items_to_process.len());

             for item in items_to_process {
                 let processing_result = dispatch_to_agent_processor(&db_pool, &item).await;

                 if processing_result.is_ok() {
                     // Successfully processed, delete from queue
                     if let Err(e) = sqlx::query!("DELETE FROM sinex_schemas.promotion_queue WHERE queue_id = $1", item.queue_id)
                         .execute(&db_pool).await {
                         eprintln!("[{}] Processed item {} but failed to delete from queue: {}", worker_id, item.queue_id, e);
                     }
                 } else {
                     // Processing failed
                     let err_msg = format!("{:?}", processing_result.err().unwrap());
                     let new_attempts = item.attempts + 1;

                     if new_attempts >= item.max_attempts {
                         // Move to DLQ (see TIM-DeadLetterQueueImplementation.md)
                         eprintln!("[{}] Item {} failed {} times, moving to DLQ. Error: {}", worker_id, item.queue_id, new_attempts, err_msg);
                         // ... (DLQ insertion logic) ...
                         // Then delete from promotion_queue
                         sqlx::query!("DELETE FROM sinex_schemas.promotion_queue WHERE queue_id = $1", item.queue_id)
                             .execute(&db_pool).await.ok();
                     } else {
                         // Schedule retry with exponential backoff
                         let base_delay_secs = 60.0;
                         let delay_secs = base_delay_secs * (2.0_f64.powi(item.attempts)); // attempts is 0-indexed
                         let jitter_factor = rand::thread_rng().gen_range(0.8..=1.2);
                         let final_delay_secs = (delay_secs * jitter_factor).max(1.0).min(24.0 * 3600.0); // Cap min 1s, max 24h
                         let next_retry_at: DateTime<Utc> = Utc::now() + chrono::Duration::seconds(final_delay_secs as i64);

                         if let Err(e) = sqlx::query!(
                             "UPDATE sinex_schemas.promotion_queue \
                              SET attempts = $2, status = 'failed_retryable', error_message_last = $3, \
                                  next_retry_ts = $4, processing_worker_id = NULL \
                              WHERE queue_id = $1",
                             item.queue_id, new_attempts, err_msg, next_retry_at
                         )
                         .execute(&db_pool).await {
                             eprintln!("[{}] Item {} failed, error updating queue for retry: {}", worker_id, item.queue_id, e);
                         }
                     }
                 }
             }
         }
     }
    ```
*   **Performance [CR3]:** Polling workers with `SKIP LOCKED` can achieve high throughput (e.g., 2,500+ ops/sec in CR3's benchmark, though highly dependent on actual processing complexity).

## 4. Event Notification Mechanisms and Trade-offs [UG Sec 3.3]

As per ADR-002, the primary mechanism is polling the `promotion_queue`. `LISTEN/NOTIFY` is an optional enhancement for specific workers.

### 4.1. PostgreSQL `LISTEN/NOTIFY` (Optional Wake-Up Signal)

*   **Purpose:** To reduce polling latency by having workers `LISTEN` for a simple "new work available for agent X" notification, then immediately poll the queue.
*   **Technical Specifications [CR2, SR1, SA1]:**
    *   Payload Limit: ~8000 bytes (notification payload itself is minimal, just a signal).
    *   Queue Size: Internal PostgreSQL queue.
    *   Delivery: At-least-once (if listener connected), transactional.
    *   Deduplication: Identical channel+payload notifications within the same transaction are coalesced.
*   **Performance & Scalability Concerns (Why it's not primary) [SR1, SA1, CR3]:**
    *   Sender-Side Bottleneck: `NOTIFY` is synchronous to all listeners; many listeners (>100) can slow down the notifying transaction's commit.
    *   Dropped Notifications: If no client is listening, notification is lost. Slow listeners can also miss notifications if their internal PG connection queue overflows.
    *   Connection Pooler Compatibility (PgBouncer): `LISTEN` does not work reliably with transaction-level pooling. Requires session pooling or direct connections for listeners.
*   **Implementation as Wake-Up:**
    *   The trigger that inserts into `promotion_queue` (e.g., `raw.route_new_event_to_promo_queue_trigger_func()`) could also issue:
        `PERFORM pg_notify('promo_queue_update_' || NEW.target_agent_name, NEW.raw_event_id::text);`
    *   The worker agent would establish a dedicated listening connection (or use session pooling) to `LISTEN promo_queue_update_<its_agent_name>;`. On notification, it triggers an immediate poll of the queue table.

### 4.2. Redis Streams (Alternative for Future Scaling)

*   **Benefits [SR1]:** Higher throughput (30k-100k msgs/sec), persistence, consumer groups for load balancing, message acknowledgements.
*   **Considerations:** Adds external dependency (Redis), increases complexity (dual write problem or outbox pattern for atomicity with PG).
*   **Status:** Deferred as per ADR-002. If `promotion_queue` with optional `NOTIFY` becomes a proven bottleneck, Redis Streams would be the next consideration.

