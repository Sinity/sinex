# Embedding Runtime

Status: design contract for the embedding execution layer. Implementation
tracking lives in #1021 (embedding repository and hybrid search SQL) and #400
(Tier-1 design). Recorded model effects are a separate dormant
schema/repository surface above this runtime layer; they are not part of the
embedding runtime until a live derivation or proposal/judgment consumer is
wired.

This record owns the *runtime* concerns of embedding generation: which model
runs where, how the client throttles and retries, how backfill is paced, and
how the runtime is wired into the rest of the system. It does not own the
question of *what* gets embedded — that is the document/event layer's
responsibility (see `crate/sinex-schema/docs/document_layer.md` for embeddable
content surfaces and rules).

## What This Owns

- Embedding model selection and the provider/model identity recorded in the DB.
- The HTTP client to the local inference backend (Ollama today).
- Rate limiting, retry, and concurrency policy for embedding calls.
- Backfill pacing for historical events.
- Runtime configuration surface (env, NixOS module) for the embedding service.

## What This Does Not Own

- Text extraction strategy per event type. Lives in `crate/sinex-schema/docs/document_layer.md`
  and in the per-event embeddability rules table.
- The pgvector schema, HNSW index DDL, and hybrid-search SQL function. Those
  are #1021's substrate (`sinex-db` repository module).
- Semantic search ranking, hybrid scoring weights, or query-time prompt
  shape. Those are the search/retrieval layer's concerns.
- Model-effect tracking and replay. `core.model_effects` exists for recorded
  non-deterministic effects, but current embedding generation should use the
  embedding model registry and embedding repository. Do not route embeddings
  through the dormant model-effect table unless a future replay/derivation
  design explicitly does so.

## Model Identity

The default model is `bge-base-en-v1.5` served by local Ollama:

| Field | Value |
| --- | --- |
| Provider | `ollama` |
| Model name | `bge-base-en-v1.5` |
| Dimensions | `768` |
| Quantization | float32 at insert time (no halfvec yet) |
| Fallback | `nomic-embed-text` (also 768d) if BGE unavailable |

The `(provider, model_name)` tuple is the identity recorded in the embedding
model registry table. Inserting an embedding for an unregistered model is an
error; `ensure_model(...)` is the only legal entry point. Switching models is
a registration plus a backfill — old embeddings stay in place, tagged with
their model id, so search can request a specific generation when needed.

### Why BGE-base, not larger

Personal-scale math: 1M vectors × 768 dims × 4 bytes ≈ 3 GB raw, which fits
comfortably in pgvector with HNSW. Larger models (1024d, 1.5B params) buy
marginal retrieval quality for substantial CPU cost on a 13700K without GPU.

INT8 quantization is rejected initially — at < 5M vectors, full float32 is
the simpler default. Revisit when total vector count crosses a multi-million
threshold.

## Ollama Runtime Status

As of this record, Ollama is **not present** on the workstation:

- Not on `$PATH`
- Not configured in sinnix NixOS modules
- Not running as system or user service

Bringing the embedding pipeline online is therefore a sequence:

1. Add Ollama to sinnix (system service preferred, user service acceptable).
2. `ollama pull bge-base-en-v1.5` on first run, idempotent.
3. Verify `POST http://localhost:11434/api/embed` returns the expected
   batch-embed shape.
4. Wire the embedding automaton's `on_initialize` to `ensure_model(...)` and
   spawn the backfill loop.

The Ollama embed API (v0.5+) accepts `input: string | string[]` and returns
`embeddings: number[][]`. The runtime always uses the batch form.

## OllamaClient Contract

```rust
pub struct OllamaClient {
    http: reqwest::Client,
    base_url: String,          // "http://localhost:11434"
    model: String,             // "bge-base-en-v1.5"
    rate_limiter: Arc<RateLimiter>,
}

impl OllamaClient {
    pub async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, OllamaError>;
}
```

### Retry Policy

| Condition | Action |
| --- | --- |
| Connection error, 503 (model loading), 500 | Retry, exponential backoff, 100ms base, max 3 attempts |
| 400 (bad request) | No retry; surface error |
| 404 (model not found) | No retry; runtime is misconfigured |

### Rate Limiting

Token-bucket throttle. Defaults:

| Mode | Rate | Burst | Concurrency |
| --- | --- | --- | --- |
| Foreground (live events) | 20 req/s | 5 | bounded by NATS consumer |
| Backfill (historical) | 1 in-flight | n/a | 1 |

The rate-limit numbers come from empirical CPU shape on a 13700K (no GPU):
single embeddings at 20–50ms, batches of 32 at 200–400ms, sustainable
~80–160 vectors/sec. Foreground throughput is bounded by event ingest rate,
not by the embedder; backfill is throttled to avoid starving other automata.

The batch size for the Ollama call is independent of the automaton's input batch.
The current default is 32, configurable.

## Backfill Pacing

Backfill runs as a background task spawned in `on_initialize`. The query
surface lives in `EmbeddingRepository::events_without_embeddings(model_id,
event_types, limit)`, ordered oldest-first via UUIDv7 anchoring.

Loop shape:

1. Pull up to 100 missing targets per pass.
2. Single in-flight Ollama batch call (CPU-saturating concurrency rejected).
3. Insert via `insert_event_embeddings(...)` with ON CONFLICT DO NOTHING
   (idempotent).
4. Empty result → sleep 300s before re-checking. Backfill never "completes"
   — it idles until new events arrive.

Backfill must survive process restart: the query is stateless, and ON
CONFLICT DO NOTHING handles double-insertion safely.

## Runtime Configuration

The embedding runtime is wired via NixOS module (`services.sinex.embedding`)
with at least these knobs:

| Knob | Meaning |
| --- | --- |
| `enable` | Whether the automaton runs at all |
| `model` | Provider + model name |
| `ollama.base_url` | Override for non-default Ollama deployments |
| `rate_limit_per_sec` | Foreground token-bucket rate |
| `batch_size` | Ollama batch size |
| `backfill.enabled` | Whether to backfill historical events |
| `backfill.event_types` | Which event types to backfill |

The model identity inside the DB is the source of truth; config changes
that don't match the registered model are a registration error, not a silent
override.

## Privacy

Embedding runs locally; no text leaves the workstation. Embedded text still
enters through the DB/user policy admission layer before it is eligible for
embedding (see `crate/sinex-schema/docs/document_layer.md`). The embedding runtime does not run its
own redaction pass; text arriving at the runtime is already admission-checked.

## Open Questions

- Whether to add a GPU profile (`use_gpu = true`) and a CUDA backend
  alongside Ollama. Realistic only if a GPU is added to the workstation;
  out of scope today.
- Whether recorded model effects and embedding rows should ever share a common
  effect abstraction. They serve different replay semantics today; unifying is
  premature unless a concrete derivation/replay consumer needs it.
- How to handle a model upgrade (new dimensions): the registry already
  isolates models, but search needs a "preferred model" pointer and a
  migration path. Owning record: search/retrieval layer.

## Boundaries

- Do not embed via cloud APIs. Local-only is invariant.
- Do not concurrently call Ollama beyond the rate limit. CPU saturation
  starves other automata.
- Do not mix model identities in a single search call. One query, one
  model id.
- Do not skip the model registry. All inserts go through `ensure_model`.

**Related:** `crate/sinex-schema/docs/document_layer.md`,
`crate/sinexd/docs/runtime_qos.md`,
issues #400, #1021, #1076, #1118 (inference decision metadata), #1116
(prompt router + budget ledger), and #1963 (cleanup ownership for dormant
model-effect surfaces).
