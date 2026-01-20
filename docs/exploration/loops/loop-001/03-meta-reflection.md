# Loop 001 - Meta-Reflection

What went well
- Traced shutdown wiring end-to-end for ingestd and gateway using explicit signal handling and service code.
- Verified event processor shutdown expectations by reading the event processor loop and the runner wiring.

What is missing or uncertain
- Did not inspect every node implementation for custom shutdown hooks; focus stayed on the shared runtime.
- Did not validate behavior under cancellation (e.g., drop semantics of JoinHandle) with an execution trace.
- Did not review systemd unit files or external supervisors that may affect shutdown behavior.

Biases and assumptions
- Assumed process exit is a valid shutdown path for gateway; may be insufficient for graceful drain requirements.
- Treated missing shutdown signal wiring as a defect; might be intentional if runtime lifetime always matches process lifetime.

Next steps if continuing
- Audit node-specific `shutdown()` implementations for consistency and in-flight flush behavior.
- Add tests around event processor shutdown ordering (flush then exit) once the signal is wired.
