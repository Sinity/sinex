# Loop 002 - Checkpoint Persistence and Cleanup Wiring

Scope
- Checkpoint file path selection.
- Cleanup configuration wiring and background task scheduling.
- Tooling vs runtime env var contracts.

Checkpoint File Path Flow
- Default file checkpoint path uses `SINEX_RUNTIME_DIR` or falls back to `/tmp`.
  - `crate/lib/sinex-node-sdk/src/shutdown.rs` `default_checkpoint_path()` reads `SINEX_RUNTIME_DIR` and builds `<processor>.checkpoint.json`.
- `ShutdownConfig` uses that default unless a custom path is provided programmatically.
  - `crate/lib/sinex-node-sdk/src/shutdown.rs` `ShutdownConfig::checkpoint_path()`.
- SimpleProcessor loads and saves file checkpoints via `ShutdownConfig`.
  - `crate/lib/sinex-node-sdk/src/simple_processor.rs` `load_state()` and `save_state_to_file()`.

Env Var Contract Mismatch
- The dev tool sets `SINEX_CHECKPOINT_FILE` for child processes.
  - `crate/tools/sx/src/dev.rs` sets `SINEX_CHECKPOINT_FILE` when `--checkpoint` is provided.
- The runtime does not read `SINEX_CHECKPOINT_FILE` anywhere.
  - A repo-wide search only finds this env var in the dev tool.
- Net effect: the dev CLI advertises an env-based override, but the processor runtime ignores it.

Checkpoint Cleanup Wiring
- Cleanup configuration exists and can be loaded from env vars.
  - `crate/lib/sinex-node-sdk/src/checkpoint.rs` `CheckpointCleanupConfig::from_env()` reads `SINEX_CHECKPOINT_CLEANUP_*`.
- Cleanup task spawner exists.
  - `crate/lib/sinex-node-sdk/src/checkpoint.rs` `spawn_checkpoint_cleanup_task()`.
- Neither the config nor the cleanup task is referenced elsewhere.
  - Repo search shows no call sites for `spawn_checkpoint_cleanup_task()`.
- Net effect: cleanup env vars are inert; cleanup never runs unless a caller is added.

Findings
- File checkpoint path override is only available via `ShutdownConfig`; env-based override in `sx dev` is not wired.
- Cleanup configuration is defined but unused; cleanup tasks are never started.

Risks
- Hot reload workflows may silently ignore user-specified checkpoint paths via `sx dev --checkpoint`.
- Stale checkpoint data accumulates in NATS KV when cleanup is expected to run via env vars.

Opportunities
- Add `SINEX_CHECKPOINT_FILE` handling in `ShutdownConfig` (or document the correct env var).
- Wire `CheckpointCleanupConfig::from_env()` and `spawn_checkpoint_cleanup_task()` in the node runtime initialization when NATS KV is available.
