# Performance Test Suite (JetStream Era)

This note documents the JetStream-backed performance benches that replace the
legacy Redis suite. Each test lives under `tests/performance/`.

## Publish & Consume
- `jetstream_performance_test::jetstream_publish_throughput`
- `jetstream_performance_test::jetstream_consumer_latency`
- `jetstream_performance_test::jetstream_concurrent_consumer_distribution`
- `jetstream_performance_test::jetstream_redelivery_on_expired_ack`

## Checkpoint Handling
- `checkpoint_performance_test::jetstream_checkpoint_roundtrip`
- `checkpoint_performance_test::jetstream_checkpoint_recovery_behaviour`

## Bottleneck Identification
- `bottleneck_identification_test::jetstream_ack_backlog_detection`
- `bottleneck_identification_test::jetstream_detect_publish_pressure`

## Resource Exhaustion
- `resource_exhaustion_test::jetstream_backpressure_limits`
- `resource_exhaustion_test::jetstream_consumer_recovery`

## Large Payloads
- `large_payload_test::jetstream_large_payload_roundtrip`
- `large_payload_test::jetstream_large_batch_drain`

## Regression Detection
- `regression_detection_test` now uses the in-suite `BaselineTracker` and does
  not rely on external Redis fixtures.

Run the JetStream benches with:

```bash
cargo +nightly bench --test performance -- --bench
```

(Or use `just bench-performance` if you have an alias configured.)
