# Blob Relay Pattern

## Decision: Gateway → NATS → Satellite Architecture

**Status**: Documented (Not Implemented Yet)

## Context

The gateway needs to support relaying large binary content (blobs) from external sources (e.g., CLI, web clients) to nodes for processing and storage.

## Design Decision

**Pattern**: Gateway publishes blob chunks to NATS subjects, nodes consume and persist

### Flow

```
Client/CLI
    ↓ (HTTP/RPC)
Gateway (sinex-gateway)
    ↓ (NATS publish to content.ingress.{blob_id})
NATS JetStream
    ↓ (NATS subscribe)
Satellite (content processor)
    ↓ (persist)
Annex Storage + PostgreSQL
```

### Rationale

1. **Decoupling**: Gateway doesn't need to know about storage backends
2. **Scalability**: NATS provides natural buffering and backpressure
3. **Consistency**: Uses same event-driven pattern as other data flows
4. **Replay-able**: Blob ingestion becomes part of the event stream
5. **Multi-consumer**: Multiple nodes could process blobs if needed

### Alternative Considered (Rejected)

**Direct Gateway → Annex**: Gateway directly writes to git-annex

- **Pros**: Simpler, fewer hops
- **Cons**:
  - Violates separation of concerns (gateway knows about annex)
  - No automatic provenance tracking
  - Harder to audit/replay
  - Requires file system access from gateway

## Implementation Plan (B5)

### Phase 1: Gateway Endpoint (1 day)

**File**: `crate/core/sinex-gateway/src/handlers/blob.rs`

```rust
/// Handle POST /blob/publish
pub async fn handle_blob_publish(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
) -> Result<Value>
```

**Parameters**:
- `content_base64`: Base64-encoded blob content
- `filename`: Original filename
- `content_type`: MIME type
- `chunk_size`: Optional chunk size (default 1MB)

**Response**:
- `blob_id`: Generated ULID for tracking
- `chunks_published`: Number of chunks published to NATS
- `total_bytes`: Total size of blob

### Phase 2: NATS Subject Schema

**Subject Pattern**: `{env}.content.ingress.{blob_id}.{chunk_index}`

Example: `dev.content.ingress.01HX5VNQM8K7N9GQRZ4PTXYZ12.000`

**Message Payload**:
```json
{
  "blob_id": "01HX5VNQM8K7N9GQRZ4PTXYZ12",
  "chunk_index": 0,
  "total_chunks": 3,
  "chunk_data_base64": "...",
  "metadata": {
    "filename": "document.pdf",
    "content_type": "application/pdf",
    "total_bytes": 2457600,
    "source": "sinex-cli"
  }
}
```

### Phase 3: Satellite Consumer (Future Work)

**New Satellite**: `sinex-content-processor`

Responsibilities:
- Subscribe to `content.ingress.*`
- Reassemble chunks
- Validate checksums
- Persist to git-annex via BlobManager
- Emit event to `events.content.ingested`

## RPC Method: `blob.publish`

### Request

```json
{
  "jsonrpc": "2.0",
  "method": "blob.publish",
  "params": {
    "content_base64": "SGVsbG8gV29ybGQh",
    "filename": "hello.txt",
    "content_type": "text/plain"
  },
  "id": 1
}
```

### Response

```json
{
  "jsonrpc": "2.0",
  "result": {
    "blob_id": "01HX5VNQM8K7N9GQRZ4PTXYZ12",
    "chunks_published": 1,
    "total_bytes": 12,
    "status": "published"
  },
  "id": 1
}
```

### Error Cases

- **413 Payload Too Large**: Blob exceeds `SINEX_GATEWAY_MAX_BLOB_BYTES`
- **500 Internal Error**: NATS publish failure
- **400 Bad Request**: Invalid base64 or missing required fields

## Testing Strategy

1. **Unit Tests**: Chunking logic, base64 encoding/decoding
2. **Integration Tests**: Gateway → NATS publish verification
3. **E2E Tests**: Full flow with mock node consumer

## Monitoring

Metrics to track:
- `blob_publish_total`: Counter of blobs published
- `blob_publish_bytes`: Histogram of blob sizes
- `blob_publish_duration`: Histogram of publish time
- `blob_chunk_failures`: Counter of failed chunk publishes

## Security Considerations

1. **Size Limits**: Enforce `SINEX_GATEWAY_MAX_BLOB_BYTES` (default 5MB)
2. **Rate Limiting**: Use existing gateway concurrency limits
3. **Content Validation**: Nodes should validate MIME types
4. **Provenance**: Include `source` in metadata for audit trail

## Migration Path

1. Existing `content.store_blob` RPC method continues to work (direct annex write)
2. New `blob.publish` method for relay pattern
3. CLI updated to use `blob.publish` by default
4. Deprecate direct annex writes in future release

## Status

- ✅ Pattern documented
- ⏸️ Implementation deferred (not required for Phase 1.2)
- 📋 Tracked in backlog for future sprint

---

**Last Updated**: 2026-01-15
**Author**: Claude Code (Stream B)
