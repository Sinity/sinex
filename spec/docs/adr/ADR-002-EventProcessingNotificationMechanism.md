# ADR-002: Event Processing Notification/Triggering Mechanism

*   **Status:** Accepted
*   **Date:** 2024-03-11
*   **Context & Problem Statement:**
    After new events are ingested into the `raw.events` table, subsequent processing (promotion to domain tables, enrichment, analysis) needs to be triggered for various agents. The mechanism for notifying these agents about new work must be reliable, performant, and integrate well with the Exocortex architecture. Key considerations include:
    1.  **Decoupling:** Ingestion should be decoupled from processing to handle bursts and allow agents to work at their own pace.
    2.  **Reliability:** Ensure events are not lost if a processing agent is temporarily down.
    3.  **Scalability:** Handle potentially high volumes of events and a growing number of diverse processing agents.
    4.  **Transactional Integrity:** Ideally, processing triggers should align with the transactional commit of the raw event.
    5.  **Latency:** Minimize delay between event ingestion and processing initiation for time-sensitive tasks.
    6.  **Complexity:** Favor solutions that minimize external dependencies and operational overhead for the single-host Exocortex MVP.

*   **Discussed Options:**

    1.  **PostgreSQL `LISTEN/NOTIFY`:**
        *   **Description:** A trigger on `raw.events` `AFTER INSERT` issues a `NOTIFY channel_name, payload` command. Agents connect to PostgreSQL and `LISTEN` on specific channels.
        *   **Pros:**
            *   Built into PostgreSQL, no external dependencies.
            *   Transactional: Notifications are sent only upon successful commit of the inserting transaction.
            *   Relatively low latency for waking up listeners.
            *   Simple for basic "new work available" signals.
        *   **Cons:**
            *   **Payload Limit:** ~8000 bytes per notification; larger data needs to be fetched separately.
            *   **Queue Size & Reliability:** Internal PostgreSQL notification queue can fill under high load or with slow listeners, potentially blocking notifiers or losing notifications for specific slow listeners. Notifications are lost if no client is listening when `NOTIFY` is committed.
            *   **Scalability (Number of Listeners):** `NOTIFY` is synchronous to all connected listeners. Performance can degrade significantly with many active listeners (e.g., >100), slowing down the commit of the *notifying* transaction [SA1, SR1].
            *   **Connection Pooler Compatibility:** `LISTEN` does not work reliably with transaction-level poolers like PgBouncer (most common mode), as the listening registration is tied to a specific physical connection which may not be reused for subsequent transactions by the listener. Requires session pooling or direct connections for listeners.
            *   **Fan-out Complexity:** If multiple different agents need to process the same event, managing notifications to distinct channels or having agents filter a common channel adds complexity.

    2.  **Dedicated PostgreSQL Queue Table with Worker Polling (e.g., `sinex_schemas.promotion_queue`):**
        *   **Description:** A trigger or router populates a dedicated queue table (e.g., `promotion_queue`) with references to new `raw_event_id`s and the `target_agent_name` that should process it. Worker agents (e.g., Rust services) periodically poll this table for pending items using `SELECT ... FOR UPDATE SKIP LOCKED` to claim batches of work.
        *   **Pros:**
            *   **Robust & Reliable:** Uses PostgreSQL's transactional guarantees. Items persist in the queue until successfully processed and explicitly deleted by a worker.
            *   **Decoupled & Scalable:** Ingestion is fully decoupled. Multiple worker instances (even for the same agent type) can poll concurrently.
            *   **Rich Queue Item Metadata:** Queue items can store status (`pending`, `processing`, `failed_retryable`), attempt counts, `next_retry_ts` for exponential backoff, error messages, etc.
            *   **Targeted Dispatch:** Easily routes specific events to specific agents.
            *   **No External Dependencies:** Leverages existing PostgreSQL infrastructure.
            *   **Connection Pooler Friendly:** Standard `SELECT/UPDATE/DELETE` operations work fine with connection poolers.
        *   **Cons:**
            *   **Polling Latency:** Introduces some latency between item queuing and worker pickup, depending on polling interval. Can be mitigated with very short polls or combined with `LISTEN/NOTIFY` as a "wake-up" signal (see Option 4).
            *   **Database Load:** Frequent polling can add some load to the database, though efficient queries with `SKIP LOCKED` minimize this.

    3.  **External Message Queue (e.g., Redis Streams, RabbitMQ, Kafka):**
        *   **Description:** New raw events (or notifications about them) are published to an external message queue. Agents subscribe as consumers.
        *   **Pros:**
            *   **High Throughput & Scalability:** Purpose-built for high-volume messaging. Redis Streams can handle 30k-100k msgs/sec [SR1].
            *   **Advanced Queuing Features:** Often provide features like consumer groups (for load balancing), persistent streams, message acknowledgements, dead-letter topics.
            *   **Language/Platform Agnostic:** Clear separation, agents can be in any language.
        *   **Cons:**
            *   **Adds External Dependency:** Requires deploying, managing, monitoring, and backing up another system (Redis, RabbitMQ, Kafka). Increases operational complexity for a single-host MVP.
            *   **Transactional Complexity (Dual Write Problem):** Ensuring atomicity between writing the raw event to PostgreSQL and publishing to the external queue can be complex (e.g., requires two-phase commit, or an outbox pattern with a separate agent relaying from DB outbox table to message queue).
            *   **Increased Infrastructure Cost/Overhead.**

    4.  **Hybrid: PostgreSQL Queue Table + `LISTEN/NOTIFY` for "Wake-Up":**
        *   **Description:** Uses the PostgreSQL queue table (Option 2) as the primary reliable mechanism. Additionally, the trigger that inserts into the queue table *also* issues a `NOTIFY` on a channel specific to the `target_agent_name` or a general "new_promo_item" channel.
        *   **Pros:**
            *   Combines reliability and rich features of the DB queue table with potentially lower latency of `LISTEN/NOTIFY`.
            *   Workers can `LISTEN` for a notification. On receiving one, they immediately poll the queue table. If no notification, they fall back to periodic polling.
            *   Reduces "empty polls" if `LISTEN/NOTIFY` is reliable enough for the wake-up.
        *   **Cons:**
            *   Still subject to `LISTEN/NOTIFY` limitations (payload size, listener scalability, pooler issues if listener uses transaction pooling). The `NOTIFY` payload would just be a simple "new work available" signal, not the event data itself.
            *   Adds some complexity compared to pure polling. The queue table remains the source of truth for work items.

*   **Decision:**
    The initial and primary mechanism for triggering agent processing of new `raw.events` will be **Option 2: Worker polling of a dedicated PostgreSQL queue table (`sinex_schemas.promotion_queue`)**.
    `LISTEN/NOTIFY` (as per Option 4) may be used as an *optional optimization* by specific worker agents to reduce polling latency. If used, the `NOTIFY` payload will be a simple "wake-up" signal (e.g., just the `target_agent_name`), and the queue table remains the authoritative source for work items. The decision to add this `NOTIFY`-based wake-up will be made on a per-agent basis if polling latency becomes a demonstrable issue for its specific workload and the `LISTEN/NOTIFY` limitations are acceptable for that agent.
    External message queues (Option 3) are deferred as a future scaling optimization if the PostgreSQL-based queue and optional `NOTIFY` wake-up prove insufficient.

*   **Rationale for Decision:**
    1.  **Robustness and Reliability (Primary Driver):** The PostgreSQL queue table provides strong transactional guarantees. Work items (references to raw events) are durably stored until explicitly processed and removed. This is critical for ensuring no events are lost in the processing pipeline.
    2.  **Simplicity for MVP & Single-Host:** Leverages the existing PostgreSQL database, avoiding the introduction and management of an additional external dependency (like Redis or Kafka) in the core architecture of a single-host system. This aligns with keeping the initial operational footprint lean.
    3.  **Decoupling and Scalability:** Effectively decouples ingestion from processing. Multiple worker instances can pull from the queue, allowing for horizontal scaling of processing logic if needed. `FOR UPDATE SKIP LOCKED` ensures efficient concurrent access.
    4.  **Rich Feature Support:** The queue table model naturally supports tracking processing status, retry counts, error messages, and scheduled retry times (for exponential backoff) per item, directly within the database.
    5.  **Transactional Coherence:** If a trigger on `raw.events` populates the `promotion_queue` within the same transaction, the queuing of work is atomic with the ingestion of the event.
    6.  **Flexibility for `LISTEN/NOTIFY` Augmentation:** The polling model can be enhanced with `LISTEN/NOTIFY` as a "nudge" without fundamentally changing the reliability model, as the queue table remains the source of truth. This allows targeted optimization where latency is critical and `NOTIFY` constraints are manageable.

*   **Consequences:**
    *   Worker agents must implement polling logic for the `promotion_queue` table, using `SELECT ... FOR UPDATE SKIP LOCKED`.
    *   The design of the `promotion_queue` table (columns, indexes) is critical for performance (see `TIM-EventIngestionProcessing.md`).
    *   There will be a baseline polling latency. The frequency of polling is a tunable parameter, balancing responsiveness against database load.
    *   If `LISTEN/NOTIFY` is added for specific agents, those agents will need to manage a PostgreSQL connection suitable for `LISTEN` (e.g., direct connection or session pooling).
    *   The `sinex_router.route_raw_event_to_promotion_queue` function (or similar logic) is responsible for populating the queue based on agent subscriptions defined in `sinex_schemas.agent_manifests`.

