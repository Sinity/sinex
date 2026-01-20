# Loop 001 - Concrete Issues

1) Event processor shutdown is never triggered
- Evidence: `StreamProcessorRunner` creates `processor_shutdown_sender` and stores it in `event_processor_shutdown`, but there are no uses of the sender after assignment. See `crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs`.
- Impact: `EventProcessor::run()` waits on its shutdown receiver to flush pending events and exit, but the signal is never sent. See `crate/lib/sinex-node-sdk/src/event_processor.rs`.
- Risk: event processor task may continue running after runner shutdown, leaving a dangling task and potential resource usage.

2) Event processor task is never awaited or aborted
- Evidence: `StreamProcessorRunner` stores `event_processor_handle` but does not await or abort it during shutdown; there are no uses after assignment. See `crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs`.
- Impact: the task can continue running even after the runner finishes shutdown, which makes shutdown ordering unpredictable and can hide task failures.
