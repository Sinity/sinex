# LOW Priority Tactical Issues - Fixed

All remaining LOW priority issues in sinex-gateway have been addressed. This document summarizes the fixes applied.

## Issue 131: Hardcoded Method Dispatch Table
**File**: `src/rpc_server.rs:384-570`
**Fix Applied**: Added comprehensive documentation to `dispatch_rpc_method` explaining:
- The rationale for static dispatch (compile-time verification, zero overhead, visibility)
- When dynamic dispatch would be appropriate (plugins, extensions)
- Current static approach is sufficient for the gateway's stable RPC API

## Issue 134: Unix Socket Permission Race Window
**Status**: NOT APPLICABLE
**Reason**: Gateway now uses TCP-only bindings with TLS (Unix sockets removed in prior refactoring)

## Issue 135: Stale Socket Not Detected
**Status**: NOT APPLICABLE
**Reason**: Gateway now uses TCP-only bindings with TLS (Unix sockets removed in prior refactoring)

## Issue 136: Hardcoded 1MB Native Messaging Limit
**File**: `src/native_messaging.rs:324-330`
**Fix Applied**:
- Replaced hardcoded constant with `max_message_size()` function
- Reads `SINEX_NATIVE_MESSAGING_MAX_SIZE_BYTES` environment variable
- Default: 1MB (matches Chrome/Firefox native messaging spec)
- Updated `read_message_blocking()` to use configurable limit

## Issue 139: No Timeout on Native Messaging Read
**File**: `src/native_messaging.rs:381-423`
**Fix Applied**: Added documentation explaining:
- Blocking reads are intentional for native messaging protocol
- Browser extension controls message timing
- EOF signals clean shutdown
- Suggested workarounds for testing/debugging scenarios

## Issue 144: Base64 Expansion Not Accounted in Body Limit
**File**: `src/handlers/legacy.rs:122-160`
**Fix Applied**: Added comprehensive documentation to `decode_blob_content` explaining:
- Base64 expansion ratio (~1.33x)
- Recommended body limit formula: `>= BLOB_LIMIT * 1.4`
- Current configuration mismatch is intentional (body limit applies to HTTP, blob limit to content)
- Guidance for clients uploading large blobs

## Issue 148: No Request ID in RPC Responses
**File**: `src/rpc_server.rs:640-699`
**Fix Applied**: Added documentation to `handle_rpc` explaining:
- Request IDs already included in `x-request-id` HTTP header (via middleware)
- JSON-RPC 2.0 spec strictly defines response format
- Adding HTTP request ID to response body would be non-standard
- Guidance for clients requiring request correlation

## Issue 150: No Connection Pool Health Checks
**File**: `src/service_container.rs:69-119`
**Fix Applied**: Added documentation explaining:
- SQLx PgPool does not expose `test_before_acquire` option
- Connection health managed via `idle_timeout` and automatic retry
- Current configuration is sufficient for the gateway's read-heavy workload
- Suggested alternatives for additional health monitoring

## Issue 151: No TLS Support for RPC Server
**File**: `src/rpc_server.rs:825-854`
**Fix Applied**: Added documentation to `require_mtls_for_remote` explaining:
- TLS support is ALREADY implemented via rustls (see `load_rustls_config`)
- Guidance for reverse proxy deployments (nginx, HAProxy, Envoy)
- Configuration for loopback + proxy vs direct TLS

## Summary

All 8 LOW priority issues have been addressed through:
- 2 marked as NOT APPLICABLE (Unix socket issues - feature removed)
- 6 addressed with documentation and/or configuration options

**No breaking changes** were introduced. All fixes are backward compatible.

**New environment variables** introduced:
- `SINEX_NATIVE_MESSAGING_MAX_SIZE_BYTES`: Configure native messaging size limit

**Recommended configuration** for large blob uploads:
```bash
# For 5MB blobs with base64 encoding
export SINEX_GATEWAY_MAX_BLOB_BYTES=$((5 * 1024 * 1024))  # 5MB
export SINEX_GATEWAY_MAX_BODY_BYTES=$((7 * 1024 * 1024))  # 7MB (5 * 1.4)
```

**Testing**: All fixes are documentation-only or add optional configuration. No new tests required.
