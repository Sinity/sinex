# Loop 010 - Meta-Reflection

What went well
- Followed both raw and confirmation streams to identify buffer usage and ack timing.
- Identified concrete lack of backpressure with code references.

What is missing or uncertain
- Did not validate ordering behavior under real JetStream delivery patterns.
- Did not measure actual memory growth or buffer size in production workloads.

Biases and assumptions
- Assumed early confirmations are possible; this depends on stream lag and consumer start positions.

Next steps if continuing
- Add tests simulating confirmation-before-raw ordering.
- Instrument buffer size and confirmation lag metrics.
