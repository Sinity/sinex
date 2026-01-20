# Blocking-in-Async Detection

Scope
- Find synchronous IO or blocking operations used inside async flows that may stall the runtime.

Method
- rg for std::fs and async fn overlaps; manual inspection of high-traffic loops.

Findings
- systemd change polling is async but calls sync cgroup/proc reads under the hood. SystemdChangeMonitor::poll_changes is async and calls SystemdMonitor::list_service_units/get_unit_status, which perform std::fs read_dir and read_to_string (crate/nodes/sinex-system-ingestor/src/systemd_integration.rs:79-182, 351-378).
- Heartbeat emission runs in an async interval loop, but uses std::fs::read_to_string to parse /proc/self/status on each tick. This is sync IO inside the async loop (crate/lib/sinex-node-sdk/src/heartbeat.rs:205-381).
- SimpleProcessor::load_state is async and uses CheckpointState::load_from_file, which performs std::fs read_to_string and other sync file IO (crate/lib/sinex-node-sdk/src/simple_processor.rs:309-333, crate/lib/sinex-node-sdk/src/checkpoint.rs:157-212).

Impact
- Any of the above can block the Tokio runtime thread under load, especially when multiple nodes run on the same executor.

Suggested follow-ups
- Move cgroup/proc reads into spawn_blocking or migrate to tokio::fs where feasible.
- For checkpoint IO, consider async file operations or explicitly move load/save into spawn_blocking when called from async contexts.
- If systemd monitoring must remain sync, isolate it on a dedicated blocking thread and communicate over channels.
