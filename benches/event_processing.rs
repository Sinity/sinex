use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::json;
use sinex_db::queries::EventQueries;
use sinex_events::{EventFactory, RawEvent};
use sinex_ulid::Ulid;
use tokio::runtime::Runtime;

/// Benchmark single event creation and validation
fn bench_event_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_creation");

    // Benchmark different payload sizes
    for size in [10, 100, 1000, 10000].iter() {
        let payload = generate_payload(*size);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("payload_size", size),
            &payload,
            |b, payload| {
                b.iter(|| {
                    let event = EventFactory::new("benchmark")
                        .create_event("test.event", black_box(payload.clone()));
                    black_box(event);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark batch event processing
fn bench_batch_processing(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let mut group = c.benchmark_group("batch_processing");

    // Test different batch sizes
    for batch_size in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("batch_size", batch_size),
            batch_size,
            |b, &batch_size| {
                b.to_async(&runtime).iter(|| async {
                    let events: Vec<RawEvent> = (0..batch_size)
                        .map(|i| {
                            EventFactory::new("benchmark")
                                .create_event("batch.test", json!({"index": i}))
                        })
                        .collect();

                    // Simulate batch validation
                    for event in &events {
                        validate_event(black_box(event));
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark ULID generation performance
fn bench_ulid_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("ulid_generation");

    group.bench_function("single_ulid", |b| {
        b.iter(|| {
            let ulid = Ulid::new();
            black_box(ulid);
        });
    });

    group.bench_function("ulid_to_uuid", |b| {
        let ulid = Ulid::new();
        b.iter(|| {
            let uuid = black_box(&ulid).to_uuid();
            black_box(uuid);
        });
    });

    group.bench_function("ulid_ordering", |b| {
        let ulids: Vec<Ulid> = (0..100).map(|_| Ulid::new()).collect();
        b.iter(|| {
            let mut sorted = ulids.clone();
            sorted.sort();
            black_box(sorted);
        });
    });

    group.finish();
}

/// Benchmark JSON validation performance
fn bench_json_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_validation");

    let schema = json!({
        "type": "object",
        "properties": {
            "name": { "type": "string" },
            "age": { "type": "integer" },
            "data": { "type": "object" }
        },
        "required": ["name", "age"]
    });

    for complexity in ["simple", "nested", "large"].iter() {
        let payload = match *complexity {
            "simple" => json!({"name": "test", "age": 25}),
            "nested" => json!({
                "name": "test",
                "age": 25,
                "data": {
                    "nested": {
                        "deeply": {
                            "value": "test"
                        }
                    }
                }
            }),
            "large" => generate_large_payload(),
            _ => unreachable!(),
        };

        group.bench_with_input(
            BenchmarkId::new("payload_complexity", complexity),
            &payload,
            |b, payload| {
                b.iter(|| {
                    validate_json_schema(black_box(payload), black_box(&schema));
                });
            },
        );
    }

    group.finish();
}

/// Benchmark checkpoint operations
fn bench_checkpoint_operations(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let mut group = c.benchmark_group("checkpoint_operations");

    group.bench_function("checkpoint_serialization", |b| {
        let checkpoint_data = json!({
            "processed_count": 10000,
            "last_id": Ulid::new().to_string(),
            "state": {
                "buffer": vec![1, 2, 3, 4, 5],
                "metrics": {
                    "total": 10000,
                    "errors": 0
                }
            }
        });

        b.iter(|| {
            let serialized = serde_json::to_vec(black_box(&checkpoint_data)).unwrap();
            black_box(serialized);
        });
    });

    group.finish();
}

// Helper functions

fn generate_payload(fields: usize) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for i in 0..fields {
        map.insert(format!("field_{}", i), json!(format!("value_{}", i)));
    }
    json!(map)
}

fn generate_large_payload() -> serde_json::Value {
    json!({
        "data": (0..1000).map(|i| {
            json!({
                "id": i,
                "value": format!("item_{}", i),
                "nested": {
                    "field": "value"
                }
            })
        }).collect::<Vec<_>>()
    })
}

fn validate_event(event: &RawEvent) {
    // Simulate validation
    assert!(!event.id.to_string().is_empty());
    assert!(!event.source.is_empty());
    assert!(!event.event_type.is_empty());
}

fn validate_json_schema(payload: &serde_json::Value, schema: &serde_json::Value) {
    // Simulate schema validation
    // In real implementation, would use jsonschema crate
    let _ = (payload, schema);
}

criterion_group!(
    benches,
    bench_event_creation,
    bench_batch_processing,
    bench_ulid_generation,
    bench_json_validation,
    bench_checkpoint_operations
);
criterion_main!(benches);
