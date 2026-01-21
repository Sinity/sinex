# Rapid Assembly Estimates

> Extracted from architecture analysis. These estimates assume the core pipeline is validated.

## Browser Extension (~1-2 days)

| Component | Lines | Notes |
|-----------|-------|-------|
| Event payloads (page.visited, page.archived, tab.*, etc.) | ~200 | `#[derive(EventPayload)]` |
| Native messaging host integration | ~100 | Infrastructure exists in gateway |
| Manifest V3 extension | ~500 | Standard Chrome extension boilerplate |
| WARC archiving | ~300 | Well-understood format, existing Rust crates |
| NixOS node config | ~30 | Templated from existing |

**Total: ~1,130 lines**

The gateway already has native messaging infrastructure (`native_messaging.rs` with `NativeMessagingTransport` trait).

## LLM Integration (~1 day)

| Component | Lines | Notes |
|-----------|-------|-------|
| Ollama client wrapper | ~150 | HTTP calls to local API |
| LLM automaton (following Analytics pattern) | ~300 | Consumes events, produces summaries |
| Synthesis payloads (daily.summary, topic.extracted, etc.) | ~100 | `#[derive(EventPayload)]` |
| NixOS Ollama config | ~50 | Service configuration |

**Total: ~600 lines**

The `AnalyticsAutomaton` is a complete template. It implements `StatefulStreamProcessor`, consumes from `core.events`, produces synthesis events with `Provenance::Synthesis { source_event_ids }`.

## Embedding Generation (~half day)

| Component | Lines | Notes |
|-----------|-------|-------|
| Embedding automaton | ~200 | Process events, generate embeddings |
| Vector storage (domain table or events) | ~50 | pgvector already integrated |
| Semantic search RPC method | ~100 | Query `ORDER BY embedding <=> $1` |

**Total: ~350 lines**

## Key Patterns for Rapid Development

### New Event Type
```rust
#[derive(EventPayload)]
#[event_payload(source = "new_source", event_type = "action.happened")]
pub struct ActionPayload { /* fields */ }
```

### New Automaton
1. Copy existing automaton structure
2. Implement `StatefulStreamProcessor` trait
3. Subscribe to relevant event types
4. Emit synthesis events with provenance

### New RPC Method
1. Add handler function
2. Add match arm in dispatch
3. Expose via CLI if needed

## Prerequisites

These estimates assume:
- Core pipeline validated (NATS → ingestd → Postgres → gateway)
- SDK patterns working in practice
- Development environment stable

---

*Source: Architecture extensibility analysis.*
