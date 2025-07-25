# Test Infrastructure Examples

This document provides practical examples of using the sinex-test-utils infrastructure for various testing scenarios. All examples use the unified `TestContext` approach that is the standard for Sinex testing.

## Synthetic Event Generation

### High-Volume Event Generation with TestContext

```rust
use sinex_test_utils::prelude::*;
use rand::{Rng, distributions::Alphanumeric, seq::SliceRandom};

#[sinex_test(timeout = 60)]
async fn test_high_volume_event_generation(ctx: TestContext) -> TestResult<()> {
    let sources = ["sensor_a", "sensor_b", "sensor_c"];
    let event_types = ["metric.cpu", "metric.memory", "metric.disk", "metric.network"];
    
    // Generate 10,000 events in batches
    let batch_size = 100;
    let total_events = 10_000;
    
    let (_, duration) = ctx.measure(async {
        for batch in 0..(total_events / batch_size) {
            let mut batch_events = Vec::with_capacity(batch_size);
            
            for i in 0..batch_size {
                let mut rng = rand::thread_rng();
                let event = ctx.event()
                    .source(sources.choose(&mut rng).unwrap())
                    .type_(event_types.choose(&mut rng).unwrap())
                    .field("value", rng.gen_range(0.0..100.0))
                    .field("batch", batch)
                    .field("index", i)
                    .field("message", rng.sample_iter(&Alphanumeric)
                        .take(50)
                        .map(char::from)
                        .collect::<String>())
                    .build()?;
                batch_events.push(event);
            }
            
            // Batch insert
            ctx.insert_events(&batch_events).await?;
        }
        Ok::<_, CoreError>(())
    }).await?;
    
    // Verify and measure performance
    let count = ctx.events().count().await?;
    assert_eq!(count, total_events as i64);
    
    let events_per_second = total_events as f64 / duration.as_secs_f64();
    println!("Generated {} events in {:?} ({:.0} events/sec)", 
             total_events, duration, events_per_second);
    
    ctx.assert("performance")
        .that(events_per_second > 1000.0, 
              &format!("Should generate >1000 events/sec, got {:.0}", events_per_second))?;
    
    Ok(())
}
```

### Realistic Event Patterns

```rust
#[sinex_test]
async fn test_realistic_filesystem_activity(ctx: TestContext) -> TestResult<()> {
    use fake::{Fake, Faker};
    use fake::faker::filesystem::en::*;
    
    // Simulate realistic file operations
    let operations = ["created", "modified", "deleted", "renamed"];
    let extensions = ["txt", "pdf", "jpg", "log", "json", "rs"];
    
    for _ in 0..100 {
        let mut rng = rand::thread_rng();
        let path: String = FilePath().fake();
        let size: u64 = (100..10_000_000).fake();
        
        let event = ctx.event()
            .filesystem()
            .path(&path)
            .size(size)
            .field("extension", extensions.choose(&mut rng).unwrap())
            .field("operation", operations.choose(&mut rng).unwrap())
            .modified()
            .insert()
            .await?;
    }
    
    // Query patterns
    let large_files = ctx.events()
        .by_source("fs")
        .fetch()
        .await?
        .into_iter()
        .filter(|e| e.payload["size"].as_u64().unwrap_or(0) > 1_000_000)
        .count();
    
    println!("Found {} large files (>1MB)", large_files);
    
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

## Integration with External Tools

### Using TestContext with Property Testing

```rust
#[sinex_test]
async fn test_property_based_event_validation(ctx: TestContext) -> TestResult<()> {
    use proptest::prelude::*;
    
    // Define property test within async context
    parameterized!([
        ("alphanumeric", "[a-zA-Z0-9]{5,20}", 20),
        ("with-special", "[a-zA-Z0-9._-]{5,30}", 20),
        ("unicode", ".*{1,50}", 10),
    ], |(name, pattern, iterations)| {
        // Limited iterations for database tests
        for i in 0..iterations {
            let source = format!("prop-test-{}-{}", name, i);
            
            let event = ctx.event()
                .source(&source)
                .type_("property.test")
                .field("pattern", pattern)
                .field("iteration", i)
                .insert()
                .await?;
            
            // Verify source follows expected pattern
            assert!(event.source.contains("prop-test"));
            assert_eq!(event.payload["iteration"], json!(i));
        }
        Ok(())
    });
    
    Ok(())
}
```

### Performance Profiling in Tests

```rust
#[sinex_test(timeout = 120)]
async fn test_with_performance_profiling(ctx: TestContext) -> TestResult<()> {
    use std::time::Instant;
    
    let mut timings = vec![];
    
    // Profile different operations
    for operation in ["insert", "query", "update"] {
        let start = Instant::now();
        
        match operation {
            "insert" => {
                for i in 0..1000 {
                    ctx.event()
                        .source("perf-test")
                        .type_("benchmark")
                        .field("op", operation)
                        .field("index", i)
                        .insert()
                        .await?;
                }
            }
            "query" => {
                for _ in 0..100 {
                    let _ = ctx.events()
                        .by_source("perf-test")
                        .limit(10)
                        .fetch()
                        .await?;
                }
            }
            "update" => {
                // Simulate updates by creating derived events
                let events = ctx.events()
                    .by_source("perf-test")
                    .limit(100)
                    .fetch()
                    .await?;
                
                for event in events {
                    ctx.event()
                        .source("perf-updated")
                        .type_("benchmark.updated")
                        .field("original_id", event.id)
                        .insert()
                        .await?;
                }
            }
            _ => unreachable!()
        }
        
        let elapsed = start.elapsed();
        timings.push((operation, elapsed));
        println!("Operation '{}' took {:?}", operation, elapsed);
    }
    
    // Generate performance report
    println!("\nPerformance Summary:");
    for (op, duration) in &timings {
        println!("  {}: {:?}", op, duration);
    }
    
    Ok(())
}
```

## Advanced Mock Scenarios

### Simulating Complex System Behavior

```rust
#[sinex_test]
async fn test_cascading_failures(ctx: TestContext) -> TestResult<()> {
    // Setup mock infrastructure
    let fs = ctx.mocks().filesystem();
    let db = ctx.mocks().database()
        .with_failure_rate(0.0); // Start healthy
    
    // Create initial state
    fs.create_file("/app/config.json", br#"{"healthy": true}"#).await?;
    
    // Simulate normal operation
    for i in 0..10 {
        let config = fs.read_file("/app/config.json").await?;
        ctx.event()
            .source("app")
            .type_("health.check")
            .field("iteration", i)
            .field("config_size", config.len())
            .insert()
            .await?;
    }
    
    // Inject filesystem failure
    fs.inject_error("/app/config.json", std::io::ErrorKind::PermissionDenied);
    
    // Observe cascade effect
    let mut failure_count = 0;
    for i in 10..20 {
        match fs.read_file("/app/config.json").await {
            Ok(_) => {
                ctx.event()
                    .source("app")
                    .type_("health.recovered")
                    .field("iteration", i)
                    .insert()
                    .await?;
            }
            Err(e) => {
                failure_count += 1;
                ctx.event()
                    .source("app")
                    .type_("health.failed")
                    .field("iteration", i)
                    .field("error", e.to_string())
                    .insert()
                    .await?;
            }
        }
    }
    
    // Verify failure propagation
    ctx.assert("cascade detection")
        .that(failure_count > 5, "Should see cascading failures")?;
    
    let failure_events = ctx.events()
        .by_type("health.failed")
        .count()
        .await?;
    
    assert_eq!(failure_events, failure_count as i64);
    
    Ok(())
}
```

## Testing Best Practices

1. **Use TestContext for Everything**: All test operations should flow through the TestContext
2. **Leverage Fixtures**: Reuse common test scenarios via `ctx.scenarios()`
3. **Test in Isolation**: Each test gets its own database - use this isolation
4. **Measure Performance**: Use `ctx.measure()` to track operation timing
5. **Rich Assertions**: Use `ctx.assert()` for better error messages
6. **Mock External Dependencies**: Use mocks for filesystem, network, etc.
7. **Property Test Wisely**: Limit iterations when using database operations

For more examples, see the TESTING.md guide and the test files throughout the Sinex codebase.