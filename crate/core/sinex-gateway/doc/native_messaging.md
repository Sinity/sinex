# Native Messaging Protocol

Implements Chrome/Firefox native messaging so browser extensions can communicate with the Sinex
gateway.

## Protocol Overview

Native messaging uses stdin/stdout for bidirectional communication:

1. Message length (4-byte little-endian `u32`) precedes the JSON payload.
2. Maximum message size is capped at 1 MB to prevent resource exhaustion.
3. Message types are `request` for calls and `response` (or `error`) for replies.

## Message Format

Request example:

```json
{
  "type": "request",
  "method": "query_events",
  "params": { "...": "..." },
  "id": "unique_request_id"
}
```

Response example:

```json
{
  "type": "response",
  "result": { "...": "..." },
  "id": "matching_request_id"
}
```

## Browser Extension Integration

- Extensions register the gateway as a native messaging host in their manifests.
- The gateway process is launched on demand by the browser.
- Bidirectional communication enables real-time data exchange.
- The browser cleans up the process when the extension disconnects.

## Security Considerations

- Message size limits prevent DoS attacks.
- All message fields are validated before dispatching.
- Error messages are sanitized to avoid leaking sensitive details.
- Trusted extensions must be declared via `SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS`.
  - Format: comma-separated entries such as `chrome-extension://abc123#shared-secret`.
  - Entries without `#secret` only check the extension ID; entries with secrets require matching `extension_secret` fields on every message.
- Authentication decisions are logged as structured tracing events (`native_messaging.auth`) so operators can audit which extension IDs succeeded or failed, including reasons such as `missing_extension_id`, `not_trusted`, or `invalid_secret`.
