# TIM-DeadLetterQueueImplementation: Central DLQ and Error Handling

## Status Dashboard
**Maturity Level**: L2 - Ready for Implementation
**Implementation**: 75% (Database schema and basic infrastructure complete, CLI and automation pending)
**Dependencies**: PostgreSQL, promotion queue system, worker infrastructure
**Blocks**: Error recovery, system reliability, debugging workflows, operational visibility

## MVP Specification
- Central dead letter queue table in PostgreSQL
- Automatic failed message collection from promotion queue
- Basic retry and replay mechanisms
- Error categorization and tracking
- Simple management interface

## Enhanced Features
- Advanced error pattern analysis
- Automated recovery strategies
- Comprehensive operational dashboard
- Integration with monitoring systems
- Historical error trend analysis
- Custom replay policies per error type

## Implementation Checklist
- [x] Dead letter queue table schema
- [x] Integration with promotion queue failures
- [ ] Retry policy configuration
- [x] Error classification system
- [ ] Manual replay mechanisms
- [ ] Monitoring and alerting
- [ ] Operational tooling
- [ ] Performance optimization
- [ ] Historical analysis features

*   **Relevant ADR:** (N/A directly, core infrastructure for robustness)
*   **Original UG Context:** Section 3.4

This TIM details the implementation of the central Dead Letter Queue (DLQ) in PostgreSQL, along with retry policies and tools for managing terminally failed messages.

## 1. Central PostgreSQL DLQ: `core.dead_letter_queue` [UG Sec 3.4.1]

A single, rich central DLQ table stores messages that failed processing after exhausting retries in their primary queues (e.g., `sinex_schemas.work_queue`).

*   **Schema (from UG Sec 3.4.1, CR5, `openai_sinex_6.md` Sec 2):**
    ```sql
    CREATE SCHEMA IF NOT EXISTS core;

    CREATE TABLE IF NOT EXISTS core.dead_letter_queue (
        dlq_id                  ULID PRIMARY KEY DEFAULT gen_ulid(), -- Using pgx_ulid
        original_event_id       ULID NULLABLE REFERENCES raw.events(id) ON DELETE SET NULL,
        correlation_id          UUID NULLABLE, -- Copied from original_event_id's _provenance if available
        message_payload         JSONB NOT NULL, -- The actual payload of the item that failed
        error_type              VARCHAR(255) NOT NULL,
        error_message           TEXT,
        error_stack_trace       TEXT NULLABLE, -- If applicable and captured
        retry_count_before_dlq  INTEGER DEFAULT 0,
        source_queue_name       VARCHAR(255) NOT NULL, -- e.g., "work_queue", "agent_X_internal_queue"
        processing_agent_name   TEXT NULLABLE REFERENCES sinex_schemas.agent_manifests(agent_name),
        additional_metadata     JSONB, -- e.g., agent version, host, relevant config, parameters
        ts_added_to_dlq         TIMESTAMPTZ NOT NULL DEFAULT now(),
        status                  TEXT NOT NULL DEFAULT 'pending_review', -- 'pending_review', 'investigating', 'replaying', 'resolved_manual', 'ignored_permanent', 'archived_ttl', 'archived_manual'
        resolution_notes        TEXT,
        resolved_at             TIMESTAMPTZ NULLABLE,
        resolved_by_actor       TEXT NULLABLE, -- 'user_sinex', 'agent_DLQReplayer_v1'
        -- expires_at (TTL) can be managed by a periodic cleanup job rather than a generated column if policies are complex.
        -- If using generated column for simple TTL:
        -- expires_at              TIMESTAMPTZ GENERATED ALWAYS AS (ts_added_to_dlq + interval '90 days') STORED,
        last_replayed_at        TIMESTAMPTZ NULLABLE,
        replay_attempts         INTEGER DEFAULT 0
    );

    COMMENT ON TABLE core.dead_letter_queue IS 'Central Dead Letter Queue for terminally failed messages.';
    COMMENT ON COLUMN core.dead_letter_queue.original_event_id IS 'If DLQ item originated from a raw.event, its ID.';
    COMMENT ON COLUMN core.dead_letter_queue.message_payload IS 'The specific message payload that failed processing.';
    COMMENT ON COLUMN core.dead_letter_queue.retry_count_before_dlq IS 'Retries in source queue before DLQ.';
    COMMENT ON COLUMN core.dead_letter_queue.status IS 'Current status of the DLQ item.';

    CREATE INDEX IF NOT EXISTS idx_dlq_status_ts_added ON core.dead_letter_queue (status, ts_added_to_dlq DESC);
    CREATE INDEX IF NOT EXISTS idx_dlq_source_agent_ts ON core.dead_letter_queue (source_queue_name, processing_agent_name, ts_added_to_dlq DESC);
    CREATE INDEX IF NOT EXISTS idx_dlq_original_event_id ON core.dead_letter_queue (original_event_id) WHERE original_event_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_dlq_correlation_id ON core.dead_letter_queue (correlation_id) WHERE correlation_id IS NOT NULL;
    ```

## 2. Redis-Based DLQ (Optional, for Short-Term Retries) [UG Sec 3.4.2]

*   **Mechanism [CR5]:** Uses Redis Sorted Sets (scored by `next_retry_at`) for entry IDs and Hashes for message metadata. Implements exponential backoff.
*   **Use Case:** Intermediate DLQ for frequent, short-lived retries (e.g., transient network errors). If items repeatedly fail here, they are moved to the PostgreSQL `core.dead_letter_queue`.
*   **Status:** This is an optional component; primary DLQ is PostgreSQL-based. If implemented, it requires Redis as an additional dependency.

## 3. Retry Policies: Exponential Backoff with Jitter [UG Sec 3.4.3]

This logic is applied by workers processing items from primary queues (like `sinex_schemas.work_queue`) or by DLQ replay workers.

*   **Implementation (Conceptual, based on UG Sec 3.2.2 Rust worker):**
    1.  On processing failure, increment `item.attempts`.
    2.  If `new_attempts < item.max_attempts`:
        *   `base_delay_seconds = configurable_per_agent_or_queue_type` (e.g., 30s, 60s).
        *   `delay_seconds = base_delay_seconds * (factor.powi(item.attempts))` (factor e.g., 2).
        *   `jitter = delay_seconds * random_float_between(-0.2, 0.2)` (e.g., +/- 20% jitter).
        *   `actual_delay_seconds = (delay_seconds + jitter).max(min_delay).min(max_delay)`.
        *   Update `next_retry_ts = now() + actual_delay_interval` in the queue item.
        *   Set status to `'failed_retryable'`.
    3.  If `new_attempts >= item.max_attempts`:
        *   Move the item to `core.dead_letter_queue`.
        *   Set `retry_count_before_dlq` in `core.dead_letter_queue` to `new_attempts`.
        *   Delete item from original queue.
*   **Parameters:** `base_delay`, `factor`, `jitter_percentage`, `max_attempts`, `min_delay`, `max_delay` should be configurable.

## 4. Replay Tools and UI Dashboard for `core.dead_letter_queue` [UG Sec 3.4.4]

A mechanism (CLI via `exo dlq ...` or a future web UI) is needed for managing `core.dead_letter_queue`.

*   **Core `exo dlq` CLI Commands:**
    *   `exo dlq list [--status <status>] [--agent <agent_name>] [--limit N] [--since <timespec>]`: Lists DLQ items.
    *   `exo dlq get <dlq_id>`: Shows full details of a DLQ item.
    *   `exo dlq replay <dlq_id | --all --status pending_review --agent X>`: Attempts to re-enqueue selected DLQ item(s) back into their `source_queue_name` (or a special replay topic).
        *   Updates `core.dead_letter_queue.status` to `'replaying'`, increments `replay_attempts`, sets `last_replayed_at`.
        *   If replay successful (item processed by agent and deleted from source queue), target agent should emit an event that allows updating DLQ item to `'resolved_replay'`.
        *   If replay fails again, item might return to DLQ or its `status` in DLQ updated to `'replay_failed_again'`.
    *   `exo dlq update-status <dlq_id> --new-status <resolved_manual|ignored_permanent|archived_manual> [--notes "Resolution notes..."]`: Manually changes status.
*   **Flask/Python Replay Service Example (CR5):** `DLQReplayService` class from CR5 serves as a conceptual model for a backend service powering such tools, including rate limiting for replays and outcome handling.
*   **Performance Impact Monitoring of DLQ Operations [UG Sec 3.4.5, CR5]:** The replay service or `exo dlq` commands should log metrics (e.g., to Prometheus via a sidecar or direct instrumentation) for replay attempts, successes, failures. Python example in CR5 for `PerformanceMonitor` class shows tracking latency and error rates for operations.

## 5. DLQ Item Expiry/Archival/Purge [UG Sec 3.4.1, `openai_sinex_6.md` Sec 2]

*   **TTL Policy:** Define a Time-To-Live (TTL) for DLQ items, especially those that are resolved or ignored.
*   **Mechanism:** A systemd timer (`sinex-dlq-purge.timer`) runs a periodic job (e.g., daily).
*   **Service Unit (`sinex-dlq-purge.service`):** Executes a `psql` command.
    ```sql
    -- Example SQL for purge job:
    DELETE FROM core.dead_letter_queue
    WHERE
        (status IN ('resolved_manual', 'ignored_permanent', 'resolved_replay', 'archived_ttl', 'archived_manual')
         AND resolved_at < now() - interval '30 days') -- Purge resolved items older than 30 days
    OR
        (status = 'pending_review' AND ts_added_to_dlq < now() - interval '180 days'); -- Purge very old pending items
    -- Adjust intervals based on retention policy.
    ```
    *   Alternatively, if using a generated `expires_at` column: `DELETE FROM core.dead_letter_queue WHERE expires_at < now() AND status IN (...);`

## 6. Ingestor-Specific File-Based DLQs (Initial Failure Catch) [UG Sec 3.4.6]

*   **Purpose:** For ingestors that cannot immediately write to `raw.events` (e.g., database completely down, network issue to DB server from a remote ingestor). This is a local, last-resort fallback before data is lost.
*   **Mechanism (Primary Document Part III.2.1):**
    1.  Ingestor attempts to write event to `raw.events` via its normal DB connection.
    2.  If write fails after configured local retries (e.g., 3 attempts with short backoff):
        a.  Serialize the full event (including intended `id`, `source`, `event_type`, `ts_orig`, `payload`, etc.) to a local file in a dedicated per-ingestor DLQ directory (e.g., `/var/lib/sinex/ingestor_dlqs/<agent_name>/<ulid_of_event>.json`).
        b.  The ingestor then attempts to emit a single, simple `sinex.agent.dlq_event_written` meta-event to `raw.events`. Payload: `{ "dlq_ingestor_name": "<agent_name>", "original_event_id_attempted": "<ulid_of_event>", "dlq_filepath": "/path/to/dlq_file.json", "reason": "DB_WRITE_FAILURE_MAX_RETRIES" }`.
    3.  If this meta-event (`dlq_event_written`) *also* fails to write to DB, this critical "meta-failure" is logged to the ingestor's `stdout/stderr` (captured by journald) AND to a specific local append-only text file for that ingestor (e.g., `/var/log/sinex/<agent_name>/critical_meta_failures.log`). This file is for absolute last-resort manual recovery.
*   **Reprocessing File-Based DLQs:**
    *   A dedicated system agent (`sinex-agent-file-dlq-reprocessor`) periodically scans all `/var/lib/sinex/ingestor_dlqs/*/` directories for new files.
    *   It attempts to read these files, deserialize the original event, and insert it into `raw.events`.
    *   On successful insertion, the local DLQ file is deleted or moved to an "archived" subdirectory.
    *   This agent also monitors the `critical_meta_failures.log` files and attempts to re-emit the `sinex.agent.dlq_event_written` meta-events.

