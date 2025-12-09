# Actionable Improvements

Extracted from deep-dive exploration of sinex codebase. Items are prioritized and include file references.

---

## Critical (Security/Correctness)

### 1. Apply systemd security hardening to all production services
**Files**: `nixos/modules/satellite-services.nix`, `nixos/modules/ingestd.nix`, `nixos/modules/gateway.nix`

Currently only `preflight-verification.nix` has hardening. All other services run with zero protection.

```nix
serviceConfig = {
    ProtectSystem = "strict";
    ProtectHome = true;
    PrivateTmp = true;
    NoNewPrivileges = true;
    ProtectKernelTunables = true;
    ProtectKernelModules = true;
    ProtectControlGroups = true;
    RestrictAddressFamilies = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
    RestrictNamespaces = true;
    RestrictRealtime = true;
    RestrictSUIDSGID = true;
    MemoryDenyWriteExecute = true;
    LockPersonality = true;
};
```

### 2. Add SIGTERM handler to ingestd
**File**: `crate/core/sinex-ingestd/src/main.rs`

systemd sends SIGTERM on stop, but ingestd only catches SIGINT. This causes unclean shutdowns.

```rust
// Replace ctrl_c() with:
let mut sigterm = signal(SignalKind::terminate())?;
let mut sigint = signal(SignalKind::interrupt())?;
tokio::select! {
    _ = sigterm.recv() => {}
    _ = sigint.recv() => {}
}
```

---

## High (Functionality Gaps)

### 3. Implement reset_checkpoint()
**File**: `crate/lib/sinex-satellite-sdk/src/checkpoint.rs:458-474`

Currently logs a warning and does nothing. Needed for operational recovery.

```rust
pub async fn reset_checkpoint(&self) -> SatelliteResult<()> {
    self.pool.checkpoints().delete(
        &ProcessorName::new(&self.processor_name),
        &ConsumerGroup::new(&self.consumer_group),
        &ConsumerName::new(&self.consumer_name),
    ).await?;
    info!(processor = %self.processor_name, "Checkpoint reset");
    Ok(())
}
```

### 4. Implement get_checkpoint_stats()
**File**: `crate/lib/sinex-satellite-sdk/src/checkpoint.rs:477-486`

Returns empty/zero stats. Needed for observability.

### 5. Make max_ack_pending configurable
**File**: `crate/core/sinex-ingestd/src/jetstream_consumer.rs`

Currently hardcoded to 100. Should be in IngestdConfig for tuning.

---

## Medium (Consistency/Quality)

### 6. Replace shutdown polling with channel-based detection
**File**: `crate/lib/sinex-satellite-sdk/src/runtime/stream/mod.rs:778-782`

100ms polling loop wastes CPU and adds latency. Use `tokio::sync::watch` channel instead.

### 7. Standardize environment variable prefixes
**Files**: All config modules

Current inconsistency:
- `SINEX_*` (gateway)
- `INGESTD_*` (ingestd)
- `SATELLITE_*` (satellites)

Proposed: `SINEX_<SERVICE>_*` (e.g., `SINEX_INGESTD_BATCH_SIZE`)

### 8. Add individual satellite enable/disable flags
**File**: `nixos/modules/satellite-services.nix`

Currently all-or-nothing. Should allow per-satellite control.

### 9. Expose shutdown_timeout in NixOS options
**Files**: `nixos/modules/ingestd.nix`, `nixos/modules/satellite-services.nix`

Not currently configurable via NixOS options.

---

## Low (Polish)

### 10. Fix batch_size config mismatch
**File**: `crate/core/sinex-ingestd/src/jetstream_consumer.rs`

Config allows `batch_size: 1000` but consumer hardcodes pull of 100. Either use config value or document the limit.

### 11. Add get_checkpoint_history() implementation
**File**: `crate/lib/sinex-satellite-sdk/src/checkpoint.rs:438-455`

Currently returns empty vector. Useful for debugging.

### 12. Document default self-referential provenance behavior
**File**: `crate/core/sinex-ingestd/src/jetstream_consumer.rs:515-520`

When provenance is missing, system defaults to self-referential. This fallback should be documented as it may mask satellite bugs.

---

## Tracking

| # | Item | Status | PR |
|---|------|--------|-----|
| 1 | Security hardening | TODO | |
| 2 | SIGTERM handler | TODO | |
| 3 | reset_checkpoint() | TODO | |
| 4 | get_checkpoint_stats() | TODO | |
| 5 | max_ack_pending config | TODO | |
| 6 | Channel-based shutdown | TODO | |
| 7 | Env var prefixes | TODO | |
| 8 | Satellite enable flags | TODO | |
| 9 | shutdown_timeout option | TODO | |
| 10 | batch_size mismatch | TODO | |
| 11 | checkpoint history | TODO | |
| 12 | Provenance docs | TODO | |
