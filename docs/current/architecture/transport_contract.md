# Transport Security Contract

## Overview

This document defines the security boundaries and transport requirements for the Sinex ecosystem. It serves as the source of truth for "Correctness" regarding TLS, mTLS, and ad-hoc device access.

## 1. Trust Boundaries

### 1.1 The Gateway (Control Plane)

The Gateway exposes RPC endpoints for user interaction and system control.

- **Localhost**: **MUST** use TLS (`https://...`); trust the local CA via `SINEX_RPC_CA_CERT` when using self-signed certs.
- **Network Exposed**: **MUST** be encrypted. Any TCP binding to a non-loopback interface requires TLS + mTLS (`SINEX_GATEWAY_TLS_CLIENT_CA`).
- **Authentication**:
  - **Bearer Token**: Required for all connections (default).
  - **mTLS**: Optional high-security mode. If enabled, client certificates serve as strong identity and may bypass or augment token auth.

### 1.2 The Event Bus (Data Plane)

NATS JetStream is the central nervous system.

- **Production/CI**: **MUST** be encrypted (`tls://`). Plaintext `nats://` is forbidden in non-dev environments.
- **nodes**:
  - **Trusted**: An entity with a valid cryptographic identity (TLS Client Cert or NATS Creds) authorized to publish/subscribe.
  - **Ad-hoc**: Temporary devices must enroll (exchange a bootstrap token for a cert/cred) before joining the mesh. We do not support "anonymous" or "open" nodes.

## 2. Enforcement Mechanisms

### 2.1 Ingestd (The Hub)

- **Config**: Must support `require_tls: boolean`.
- **Validation**: If `require_tls` is true, startup fails if `nats_url` scheme is not `tls://`.

### 2.2 Gateway

- **TCP**: TLS is mandatory; the gateway refuses to start without `SINEX_GATEWAY_TLS_CERT` and `SINEX_GATEWAY_TLS_KEY`.

## 3. Transition Plan

1. **Phase 0 (Policy)**: This document.
2. **Phase 1 (TLS Baseline)**: Enforce scheme checks and verify handshake capability.
3. **Phase 2 (Strong Identity)**: Implement NATS Creds / mTLS for nodes.
