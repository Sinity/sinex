# Material Stream

`material_stream.rs` manages the in-memory queues that carry source material
from sensors into the rotation and ingestion stages.

- Provides backpressure and flow control.
- Tracks metrics for throughput and error rates.
- Emits provenance information alongside material payloads.
