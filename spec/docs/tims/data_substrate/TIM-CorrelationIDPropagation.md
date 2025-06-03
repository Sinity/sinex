# TIM-CorrelationIDPropagation: Implementing Traceability  [NOT PART OF THE CURRENT VISION AT ALL]

*   **Relevant ADR:** (N/A directly, implements Vision Doc I.3 Principle 4 - WHICH IS OBSOLETE THO).
*   **Original UG Context:** Section 3.5
*   **Vision Document Reference:** Part I.3, Principle 4 (Context is Continuous - emphasis on `correlation_id`). *Note: Per recent discussion, mandatory propagation of a single top-level `correlation_id` in `raw.events` is revised. Correlation is now primarily via derived composite action events or `event_relations`. This TIM reflects the technical mechanisms if/when such correlation IDs are used by agents or specific workflows.*

This TIM details technical mechanisms for propagating correlation identifiers (like W3C Trace Context or custom IDs) across different services, processes, and asynchronous boundaries within the Exocortex, IF a specific workflow or agent decides to use them for linking a series of operations.

## 1. Standards for Correlation/Trace IDs [UG Sec 3.5.1]

*   **W3C Trace Context (Recommended for Interoperable Distributed Tracing):**
    *   **`traceparent` header:** `version-traceid-parentid-flags` (e.g., `00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01`).
        *   `traceid`: 16-byte ID for the entire trace.
        *   `parentid` (or `spanid`): 8-byte ID of parent/current span.
        *   `flags`: Includes `sampled` flag.
    *   **`tracestate` header:** Optional, vendor-specific key-value pairs.
*   **Custom Correlation ID Header (Simpler Internal Use):**
    *   Example: `X-Correlation-ID: <ULID_or_UUID_as_string>`.
    *   If used within Exocortex event payloads: `payload._provenance.my_workflow_correlation_id = "..."`.

## 2. Protocol-Specific Header/Metadata Handling [UG Sec 3.5.3]

*   **HTTP:** Standard HTTP headers (`traceparent`, `tracestate`, `X-Correlation-ID`). Middleware in web frameworks often handles propagation.
*   **gRPC:** gRPC metadata (`io.grpc.Metadata`). OpenTelemetry gRPC instrumentation libraries handle this.
*   **AMQP (RabbitMQ, etc.):** Message properties/headers.
*   **WebSockets:** Embed correlation/trace IDs within the WebSocket message payload (e.g., in a JSON envelope), as WebSockets lack a standard header mechanism.
*   **Database Calls (PostgreSQL):**
    *   **Pass as Parameter:** Include in function/procedure arguments or `WHERE` clauses.
    *   **Dedicated Columns:** Add `correlation_id UUID` or `TEXT` columns to relevant tables (e.g., `agent_processing_dlq.correlation_id`).
    *   **Session Context Variable (`current_setting`) [CR4]:**
        *   Application sets: `SET my_app.current_correlation_id = 'your-id';` (requires custom GUC `my_app.current_correlation_id` to be defined, or use `application_name` as a workaround: `SET application_name = '... correlation_id=your-id';`).
        *   Triggers/functions retrieve: `current_setting('my_app.current_correlation_id', true)`.
        *   *Caution with Connection Poolers (Transaction Mode):* Session settings may not persist. Must be set on every connection obtained from pool.

## 3. Handling Asynchronous Boundaries in Different Languages [UG Sec 3.5.4, CR4]

Standard thread-local storage doesn't work across `await` or thread handoffs.

*   **Python (AsyncIO):**
    *   **`contextvars` (Python 3.7+):** Standard library for context-local state that propagates through `asyncio` tasks.
        ```python
        # import contextvars
        # correlation_id_var = contextvars.ContextVar("correlation_id_for_task")
        # # Set in an entry point:
        # # token = correlation_id_var.set("some-unique-id-for-this-request")
        # # Get in downstream async function:
        // # current_id = correlation_id_var.get(None) # Get with default if not set
        // # Reset when task scope ends:
        // # correlation_id_var.reset(token)
        ```
    *   **`asgi-correlation-id`:** Middleware for ASGI frameworks (FastAPI, Starlette).
*   **Go:**
    *   **`context.Context`:** Pass `ctx` as the first argument to functions. Store ID as a value: `ctx = context.WithValue(parentCtx, correlationIDKey{}, "your-id")`.
*   **JavaScript (Node.js):**
    *   **`async_hooks` and `AsyncLocalStorage` (Node.js v13.10+ / v12.17+):**
        ```javascript
        // const { AsyncLocalStorage } = require('node:async_hooks');
        // const als = new AsyncLocalStorage();
        // // In an entry point (e.g., HTTP request handler):
        // als.run(new Map([['correlationId', generateNewId()]]), () => {
        //   // All async operations from here can access it via als.getStore()
        //   anotherAsyncFunction();
        // });
        // // In anotherAsyncFunction():
        // // const store = als.getStore();
        // // const correlationId = store?.get('correlationId');
        ```
        *Overhead note [CR4]: `AsyncLocalStorage` can add 5-8% overhead in high-throughput async Node.js apps.*
*   **Rust (Async - Tokio/async-std):**
    *   **Task-Local Storage (`tokio::task_local!{}`):**
        ```rust
        // tokio::task_local! {
        //     static ASYNC_CORRELATION_ID: String;
        // }
        // // In an async function entry point:
        // // ASYNC_CORRELATION_ID.scope(String::from("your-id-for-this-task"), async {
        // //   // Code here can access via ASYNC_CORRELATION_ID.try_with(|id| ...)
        // //   // or ASYNC_CORRELATION_ID.with(|id| ...) if sure it's set.
        // //   await_some_other_function().await;
        // // }).await;
        ```
    *   **`tracing` Crate Ecosystem:** The `tracing` crate, often with `tracing-opentelemetry`, handles context propagation implicitly when spans are created and entered. It uses task-local mechanisms internally. This is the preferred method if full distributed tracing is being implemented.

## 4. Performance Overhead and Optimization (Sampling) [UG Sec 3.5.2, 3.5.5, CR4]

*   **Typical Overhead:** Generating, serializing, deserializing, and propagating IDs usually adds minimal overhead (1-3%) for most scenarios.
*   **Sampling (for full Distributed Tracing):** If implementing comprehensive distributed tracing (e.g., with OpenTelemetry, see `TIM-ObservabilityStackSetup.md`) and trace volume/overhead is high:
    *   **Head-Based Sampling:** Decide at trace start (e.g., sample 10% of requests).
    *   **Tail-Based Sampling:** Collect all trace data, decide at trace end whether to keep/export (e.g., keep all traces with errors, or slow traces).
    *   The `sampled` flag in W3C `traceparent` header communicates this decision.
*   **For Exocortex:**
    *   If a specific agent workflow generates a `workflow_correlation_id`, this is usually always propagated for that workflow.
    *   Sampling applies more to verbose OpenTelemetry-style distributed tracing across many micro-services, which is less of a concern for the initial single-host Exocortex architecture but relevant if it becomes more distributed or uses many OTel-instrumented external API calls.

