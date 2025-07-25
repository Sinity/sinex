# Test Infrastructure Conceptual Examples

This document contains conceptual examples and advanced testing patterns that supplement the test infrastructure implemented in this crate. These examples show how you could implement various testing patterns, though the actual implementation in Sinex uses the `TestContext` approach.

## Synthetic Event Generation

### High-Volume Event Generation for Load Testing

```rust
use rand::{Rng, distributions::Alphanumeric, seq::SliceRandom};
use sqlx::PgPool;
use serde_json::json;
use ulid::Ulid;
use tokio::time::{sleep, Duration};
use chrono::Utc;

async fn generate_and_insert_event_batch(db_pool: &PgPool, batch_size: usize) -> Result<(), sqlx::Error> {
    let mut rng = rand::thread_rng();
    let sources = ["ingestor_A", "ingestor_B", "synthetic_input"];
    let event_types = ["type_1", "type_2", "type_3", "type_4", "type_5"];
    let hosts = ["host_main", "host_laptop", "host_mobile"];

    let mut records_to_insert: Vec<(Ulid, String, String, chrono::DateTime<Utc>, String, serde_json::Value)> = Vec::with_capacity(batch_size);

    for _ in 0..batch_size {
        let event_id_ulid = Ulid::new();
        let source = sources.choose(&mut rng).unwrap().to_string();
        let event_type = event_types.choose(&mut rng).unwrap().to_string();
        let ts_orig_pg: chrono::DateTime<chrono::Utc> = Utc::now() - chrono::Duration::seconds(rng.gen_range(0..86400)); // Within last day
        let host = hosts.choose(&mut rng).unwrap().to_string();
        let payload_data = json!({
            "message": rng.sample_iter(&Alphanumeric).take(rng.gen_range(50..150)).map(char::from).collect::<String>(),
            "value": rng.gen_range(1..10000),
            "is_processed": rng.gen_bool(0.5),
            "nested_obj": {
                "keyA": rng.gen_range(0.0..1.0),
                "keyB": ["option1", "option2", "option3"].choose(&mut rng).unwrap().to_string()
            },
            "_provenance": { "generator_run_id": Ulid::new().to_string() }
        });
        records_to_insert.push((event_id_ulid, source, event_type, ts_orig_pg, host, payload_data));
    }
    
    // Batch insert using unnest for potentially better performance with sqlx
    let mut ulids: Vec<Ulid> = Vec::new();
    let mut sources_vec: Vec<String> = Vec::new();
    let mut event_types_vec: Vec<String> = Vec::new();
    let mut ts_origs_vec: Vec<chrono::DateTime<Utc>> = Vec::new();
    let mut hosts_vec: Vec<String> = Vec::new();
    let mut payloads_vec: Vec<serde_json::Value> = Vec::new();

    for r in records_to_insert {
        ulids.push(r.0);
        sources_vec.push(r.1);
        event_types_vec.push(r.2);
        ts_origs_vec.push(r.3);
        hosts_vec.push(r.4);
        payloads_vec.push(r.5);
    }

    sqlx::query!(
        "INSERT INTO core.events (event_id, source, event_type, ts_orig, host, payload) \
         SELECT * FROM UNNEST($1::ulid[], $2::text[], $3::text[], $4::timestamptz[], $5::text[], $6::jsonb[])",
        &ulids, &sources_vec, &event_types_vec, &ts_origs_vec, &hosts_vec, &payloads_vec
    )
    .execute(db_pool)
    .await?;
    
    Ok(())
}
```

## Load Testing Tools Integration

### k6 Load Testing Example

```javascript
import http from 'k6/http';
import { check } from 'k6';

export let options = {
  stages: [
    { duration: '30s', target: 100 },   // Ramp up to 100 users
    { duration: '1m', target: 100 },    // Stay at 100 users
    { duration: '30s', target: 0 },     // Ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<200'],   // 95% of requests must complete below 200ms
    http_req_failed: ['rate<0.1'],      // Error rate must be below 10%
  },
};

export default function() {
  let response = http.post('http://localhost:8080/events', JSON.stringify({
    event_type: 'test.load',
    source: 'k6',
    payload: { test: true }
  }));
  
  check(response, {
    'status is 200': (r) => r.status === 200,
    'response time < 200ms': (r) => r.timings.duration < 200,
  });
}
```

## Chaos Engineering Scripts

### Service Disruption Testing

```bash
#!/bin/bash
# Chaos script to randomly kill services

SERVICES=("sinex-ingestd" "sinex-gateway" "sinex-fs-watcher")
SLEEP_MIN=5
SLEEP_MAX=30

while true; do
    # Pick random service
    SERVICE=${SERVICES[$RANDOM % ${#SERVICES[@]}]}
    
    echo "🔥 Chaos: Killing $SERVICE"
    sudo systemctl kill -s KILL $SERVICE
    
    # Random sleep
    SLEEP=$((SLEEP_MIN + RANDOM % (SLEEP_MAX - SLEEP_MIN)))
    echo "😴 Sleeping for $SLEEP seconds"
    sleep $SLEEP
    
    echo "🚀 Restarting $SERVICE"
    sudo systemctl start $SERVICE
done
```

### Network Fault Injection

```bash
#!/bin/bash
# Inject network latency and packet loss

# Add 100ms latency with 25ms variation, 5% packet loss
sudo tc qdisc add dev lo root netem delay 100ms 25ms loss 5%

# Run tests
cargo test --test integration_resilience

# Clean up
sudo tc qdisc del dev lo root netem
```

## Testcontainers Integration

### PostgreSQL with TimescaleDB

```rust
#[cfg(test)]
mod integration_tests_with_testcontainers {
    use testcontainers::{clients::Cli, images::postgres::Postgres, Container};
    use sqlx::{PgPool, postgres::PgConnectOptions};
    use std::str::FromStr;

    struct TestDb<'a> {
        _container: Container<'a, Postgres>,
        pool: PgPool,
    }

    async fn setup_test_db(docker: &Cli) -> Result<TestDb<'_>, anyhow::Error> {
        let image = Postgres::default()
            .with_version("16")
            .with_user("test")
            .with_password("test")
            .with_db_name("sinex_test");
        
        let container = docker.run(image);
        let port = container.get_host_port_ipv4(5432);
        
        let url = format!("postgres://test:test@localhost:{}/sinex_test", port);
        let pool = PgPool::connect(&url).await?;
        
        // Run migrations
        sqlx::migrate!("./migrations").run(&pool).await?;
        
        Ok(TestDb { _container: container, pool })
    }

    #[tokio::test]
    async fn test_event_insertion() -> Result<(), anyhow::Error> {
        let docker = Cli::default();
        let test_db = setup_test_db(&docker).await?;
        
        // Your test code here using test_db.pool
        
        Ok(())
    }
}
```

## Distributed Tracing in Tests

### OpenTelemetry Integration

```rust
use opentelemetry::{global, sdk::trace as sdktrace, trace::TraceError};
use opentelemetry_otlp::WithExportConfig;

fn init_tracer() -> Result<sdktrace::Tracer, TraceError> {
    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint("http://localhost:4317")
        )
        .with_trace_config(
            sdktrace::config()
                .with_sampler(sdktrace::Sampler::AlwaysOn)
                .with_id_generator(sdktrace::RandomIdGenerator::default())
        )
        .install_batch(opentelemetry::runtime::Tokio)
}

#[tokio::test]
async fn test_with_tracing() -> Result<(), Box<dyn std::error::Error>> {
    let _tracer = init_tracer()?;
    
    let tracer = global::tracer("test");
    let span = tracer.start("test_operation");
    
    // Your test code here
    
    span.end();
    
    // Shutdown tracer to flush spans
    global::shutdown_tracer_provider();
    
    Ok(())
}
```

## Performance Benchmarking

### Criterion.rs Integration

```rust
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};

fn benchmark_event_insertion(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = rt.block_on(setup_test_pool()).unwrap();
    
    let mut group = c.benchmark_group("event_insertion");
    
    for batch_size in [100, 1000, 10000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    generate_and_insert_event_batch(&pool, size).await.unwrap()
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(benches, benchmark_event_insertion);
criterion_main!(benches);
```

## Synthetic Data with Faker

### Realistic Event Payload Generation

```rust
use fake::{Fake, Faker};
use fake::faker::internet::en::*;
use fake::faker::filesystem::en::*;
use fake::faker::chrono::en::*;

#[derive(Debug)]
struct SyntheticFileEvent {
    path: String,
    operation: String,
    size: u64,
    modified: DateTime<Utc>,
    user: String,
}

impl SyntheticFileEvent {
    fn generate() -> Self {
        Self {
            path: FilePath().fake(),
            operation: ["create", "modify", "delete"].choose(&mut rand::thread_rng()).unwrap().to_string(),
            size: (100..1_000_000).fake(),
            modified: DateTimeBetween(
                Utc::now() - Duration::days(7),
                Utc::now()
            ).fake(),
            user: Username().fake(),
        }
    }
}
```

## Notes

These examples demonstrate advanced testing patterns. The actual Sinex test infrastructure uses the `TestContext` approach documented in the main library documentation, which provides a cleaner, more integrated testing experience.

For production use, always prefer the established `#[sinex_test]` macro and `TestContext` patterns over these conceptual examples.