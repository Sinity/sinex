# Native Messaging Protocol

Implements Chrome/Firefox native messaging so browser extensions can communicate with the Sinex
gateway.

## Protocol Overview

Native messaging uses stdin/stdout for bidirectional communication:

1. Message length (4-byte little-endian `u32`) precedes the JSON payload.
2. Maximum message size is capped at 1 MB to prevent resource exhaustion.
3. Message types are `request` for calls and `response` (or `error`) for replies.

## Security Architecture

### Authentication
- **Fail-Closed**: Requires explicit allowlist via `SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS`. Empty allowlist rejects all requests.
- **Constant-Time Comparison**: Optional shared secrets are verified using `subtle::ConstantTimeEq` to prevent timing attacks.
- **Host Validation**: Trusted hosts can be restricted via `SINEX_NATIVE_MESSAGING_TRUSTED_HOSTS`.
- **Protocol Versioning**: `SINEX_NATIVE_MESSAGING_PROTOCOL_VERSION` enforcement prevents protocol downgrade attacks.

### Threat Model
- **Malicious Extension**: Prevented by fail-closed allowlist.
- **Compromised Extension**: Audit logs record all dangerous operations (though attributed to "system" context).
- **DoS (Size)**: 1MB message limit prevents memory exhaustion.
- **DoS (Flooding)**: Currently relied on single-threaded event loop backpressure.

### Input Validation
- **JSON Parsing**: Strongly typed deserialization rejects malformed payloads before processing.
- **Size Limits**: Checked before buffer allocation to prevent OOM.
- **Async I/O**: Non-blocking reads ensure the gateway remains responsive to shutdown signals.

## Message Format

Request example:

```json
{
  "type": "request",
  "method": "query_events",
  "params": { "...": "..." },
  "id": "unique_request_id",
  "extension_id": "chrome-extension://trusted-sinex",
  "host": "sinex-host",
  "protocol_version": "1"
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

## Configuration

- `SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS`: Comma-separated list of `extension-id[#secret]`.
- `SINEX_NATIVE_MESSAGING_TRUSTED_HOSTS`: Comma-separated list of allowed hosts.
- `SINEX_NATIVE_MESSAGING_PROTOCOL_VERSION`: Expected protocol version string.
- `SINEX_NATIVE_MESSAGING_MAX_SIZE_BYTES`: Max message size (default 1MB).
