# Loop 002 - Concrete Issues

1) `SINEX_CHECKPOINT_FILE` env var is unused by the runtime
- Evidence: only set in `crate/tools/sx/src/dev.rs`; no read sites found in the runtime.
- Impact: `sx dev --checkpoint` has no effect on processor checkpoint path; hot reload state is saved to the default path instead.

2) Checkpoint cleanup is never started
- Evidence: `CheckpointCleanupConfig::from_env()` and `spawn_checkpoint_cleanup_task()` exist in `crate/lib/sinex-node-sdk/src/checkpoint.rs`, but there are no call sites.
- Impact: `SINEX_CHECKPOINT_CLEANUP_*` env vars are inert and stale checkpoint entries will not be cleaned automatically.
