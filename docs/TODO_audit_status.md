# TODO Coverage Audit (2025-11-12)

The following unchecked TODO items currently lack fail-first test coverage. Impact reflects the expected blast radius if the gap ships.

| # | Title | Impact | Coverage Gap |
|---|-------|--------|--------------|
| 15 | BlobManager integration suite resurrection | High | `blob_manager_detects_corruption_on_retrieve` now fails because `retrieve_content` serves corrupted annex data without verification |
| 17 | Schema property/integration tests | Medium | `schema_registry_should_drive_json_validation` shows registered schemas still aren’t enforced by `validate_json` |
| 22 | Gateway performance isolation | Medium | `analytics_queries_block_each_other_with_single_connection` shows two analytics calls still serialize on a single DB connection |
| 28 | Remove sensd stubs from satellites | Low | `material_slice_stub_should_be_removed` fails because the sensd `MaterialSlice` shim still exists |
| 29 | Replay automation lifecycle coverage | High | `replay_execution_records_outcome` shows execute completes without recording any outcome/summary |
| 30 | Gateway secret management via agenix | Medium | `gateway_requires_admin_token_secret` fails because `SINEX_GATEWAY_ADMIN_TOKEN_FILE` is unset |
| 32 | Testing roadmap update | Low | Documentation-only |
| 42 | Watcher shutdown signals | High | `processor_runner_triggers_processor_shutdown` shows runner never calls `StatefulStreamProcessor::shutdown`, so watcher tasks remain alive |
| 49 | Stage-as-You-Go requires DB pool | High | `stage_as_you_go_context_should_not_require_live_database` shows contexts still demand a live Postgres pool |
