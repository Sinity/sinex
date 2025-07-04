# TIM-TestFrameworkInfrastructure: Test Framework and Infrastructure

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 98% (Comprehensive test infrastructure with robust database pooling and FK constraint handling)
**Dependencies**: Rust test framework, PostgreSQL test databases, synthetic data generation, ULID foreign keys
**Blocks**: Quality assurance, performance validation, regression testing, CDD workflows
**Recent Improvements**: Database pool optimization, foreign key constraint handling, timing-sensitive test fixes

## MVP Specification
- Synthetic event generators for load testing
- Isolated test database environments
- Custom test fixtures and factories
- Performance benchmarking framework
- Basic chaos engineering capabilities

## Enhanced Features
- Advanced chaos engineering scenarios
- Distributed testing across multiple hosts
- AI-driven test case generation
- Real-time performance monitoring
- Comprehensive regression test suites

## Implementation Checklist
- [x] Synthetic event generation (Rust/Python)
- [x] High-throughput event insertion
- [x] Faker integration for realistic data
- [x] Isolated test database setup
- [x] Basic load testing framework
- [x] Test fixture management
- [x] Database pool optimization (64 connections)
- [x] Foreign key constraint cleanup ordering
- [x] ULID UUID casting for FK relationships
- [ ] Advanced chaos engineering
- [ ] Distributed test coordination
- [ ] AI-driven test generation
- [ ] Real-time monitoring integration

*   **Relevant ADR:** (N/A directly, underpins CDD Guide and quality assurance)
*   **Original UG Context:** Section 29
*   **CDD Guide Reference:** Part III (Specialized Testing Strategies), Part IV (Development Ops)

This TIM details the infrastructure and tools for comprehensive testing of the Exocortex, covering event generation, load testing, chaos engineering, synthetic data, isolated environments, and tracing in tests.

## Recent Test Infrastructure Improvements (July 2025)

### Database Pool Optimization
- Optimized connection pool sizing from 16 to 64 connections
- Fixed resource contention issues in concurrent test execution
- Added comprehensive foreign key constraint handling in cleanup
- Improved test parallelism with 8 concurrent threads

### Foreign Key Constraint Handling
- Implemented proper cleanup order respecting FK dependencies
- Added ULID to UUID casting for foreign key relationships
- Fixed constraint violations in work_queue and related tables
- Comprehensive cleanup for all core tables in dependency order

### Test Logic Fixes
- Resolved timing-sensitive test failures
- Fixed impossible wait conditions in concurrent tests
- Added realistic delays for latency measurements
- Improved status-based verification over timing-based waits

## 1. Rationale Summary

A robust test framework is vital for ensuring reliability, performance, and correctness, especially when using Claude-Driven Development (CDD). It enables automated verification of features against specifications.

## 2. Event Generators and Load Testing [UG Sec 29.1, CR5]

### 2.1. Kafka for High-Throughput Event Generation (If Testing Kafka Ingest Path) [CR5]

*   **Mechanism:** Use Kafka producer tools (`kafkacat`) or client libraries to generate high volume of synthetic events if Exocortex ever uses a Kafka ingestion pipeline (not current MVP).
*   **Target [CR5]:** 100,000+ events/sec to stress test.

### 2.2. Custom Synthetic Event Generators (PostgreSQL Direct Ingest) [`openai_sinex_6.md` Sec 11, SA4]

*   **Mechanism:** Rust/Python scripts/apps generate realistic synthetic `raw.events` payloads (using Faker, see Sec 4 below) and insert directly into PostgreSQL `raw.events` table in configurable batches/rates.
*   **Rust Example (Conceptual Core from UG Sec 29.1.2):**
    ```rust
    // use rand::{Rng, distributions::Alphanumeric, seq::SliceRandom};
    // use sqlx::PgPool;
    // use serde_json::json;
    // use ulid::Ulid; // From 'ulid' crate
    // use tokio::time::{sleep, Duration};
    // use chrono::Utc;

    // async fn generate_and_insert_event_batch(db_pool: &PgPool, batch_size: usize) -> Result<(), sqlx::Error> {
    //     let mut rng = rand::thread_rng();
    //     let sources = ["ingestor_A", "ingestor_B", "synthetic_input"];
    //     let event_types = ["type_1", "type_2", "type_3", "type_4", "type_5"];
    //     let hosts = ["host_main", "host_laptop", "host_mobile"];

    //     let mut records_to_insert: Vec<(Ulid, String, String, chrono::DateTime<Utc>, String, serde_json::Value)> = Vec::with_capacity(batch_size);

    //     for _ in 0..batch_size {
    //         let event_id_ulid = Ulid::new();
    //         let source = sources.choose(&mut rng).unwrap().to_string();
    //         let event_type = event_types.choose(&mut rng).unwrap().to_string();
    //         let ts_orig_pg: chrono::DateTime<chrono::Utc> = Utc::now() - chrono::Duration::seconds(rng.gen_range(0..86400)); // Within last day
    //         let host = hosts.choose(&mut rng).unwrap().to_string();
    //         let payload_data = json!({
    //             "message": rng.sample_iter(&Alphanumeric).take(rng.gen_range(50..150)).map(char::from).collect::<String>(),
    //             "value": rng.gen_range(1..10000),
    //             "is_processed": rng.gen_bool(0.5),
    //             "nested_obj": {
    //                 "keyA": rng.gen_range(0.0..1.0),
    //                 "keyB": ["option1", "option2", "option3"].choose(&mut rng).unwrap().to_string()
    //             },
    //             // No direct correlation_id here as per recent discussions; agents derive correlations.
    //             "_provenance": { "generator_run_id": Ulid::new().to_string() }
    //         });
    //         records_to_insert.push((event_id_ulid, source, event_type, ts_orig_pg, host, payload_data));
    //     }
        
    //     // Batch insert using unnest for potentially better performance with sqlx
    //     let mut ulids: Vec<Ulid> = Vec::new();
    //     let mut sources_vec: Vec<String> = Vec::new();
    //     let mut event_types_vec: Vec<String> = Vec::new();
    //     let mut ts_origs_vec: Vec<chrono::DateTime<Utc>> = Vec::new();
    //     let mut hosts_vec: Vec<String> = Vec::new();
    //     let mut payloads_vec: Vec<serde_json::Value> = Vec::new();

    //     for r in records_to_insert {
    //         ulids.push(r.0);
    //         sources_vec.push(r.1);
    //         event_types_vec.push(r.2);
    //         ts_origs_vec.push(r.3);
    //         hosts_vec.push(r.4);
    //         payloads_vec.push(r.5);
    //     }

    //     sqlx::query!(
    //         "INSERT INTO raw.events (id, source, event_type, ts_orig, host, payload) \
    //          SELECT * FROM UNNEST($1::ulid[], $2::text[], $3::text[], $4::timestamptz[], $5::text[], $6::jsonb[])",
    //         &ulids, &sources_vec, &event_types_vec, &ts_origs_vec, &hosts_vec, &payloads_vec
    //     )
    //     .execute(db_pool)
    //     .await?;
        
    //     Ok(())
    // }
    ```

### 2.3. Load Testing Tools: k6, Gatling [UG Sec 29.1.3, CR5]

*   **k6:** JavaScript-based, good for API load testing (Exocortex HTTP/gRPC ingest/query endpoints). Thresholds for pass/fail (p95 latency, error rate).
*   **Gatling:** Scala-based, powerful for complex scenarios, various protocols.
*   **Target Performance Metrics [CR5]:** Define SLOs for event ingest ack latency, query latency, PKM save-to-queryable time under defined peak load (e.g., sub-200ms p95 for interactive ops).

## 3. Chaos Engineering [UG Sec 29.2, CR5, `openai_sinex_6.md` Sec 11]

Proactively inject failures to test resilience and recovery.

*   **LitmusChaos [CR5]:** For Kubernetes environments (future distributed Exocortex). Pre-defined experiments (pod delete, network latency/loss, disk fill, CPU/memory stress).
*   **Custom Chaos Scripts (Bash/Python/Rust):** For single-host NixOS.
    *   Service Disruption (systemd `stop`/`restart`/`kill -9` critical services like PostgreSQL, `sinex-promo-worker`).
        *   Example Bash loop from UG Sec 29.2.
    *   Network Issues: `tc` + `netem` to inject latency, packet loss on loopback (for PG) or primary interface.
    *   Disk Space Exhaustion: `fallocate`/`dd` to fill filesystems for PGDATA, git-annex.
    *   CPU/Memory Stress: `stress-ng`.
*   **Verification After Chaos:** Check for data loss, DLQ behavior, retries, system recovery, monitoring alerts.

## 4. Synthetic Data Generation with Faker [UG Sec 29.3, CR5]

Create realistic (but artificial) data for `raw.events` payloads, PKM notes, entity descriptions.
*   **Faker Library:** Python `Faker`, Rust `fake-rs` crate.
*   **Usage:** Integrate into synthetic event generators (Sec 2.2) to produce varied data for load testing, schema validation, PII detection tests, search relevance benchmarks.
    *   Example Python `Faker` for Hyprland payload from UG Sec 29.3.

## 5. Isolated Testing Environments with Testcontainers [UG Sec 29.4, CR5]

Programmatically manage ephemeral Docker containers for dependencies (PostgreSQL, Redis, Kafka) during integration tests.
*   **Benefits:** Isolation, reproducibility, no external deps, easy setup/teardown.
*   **Libraries:** `testcontainers-rs` (Rust), Python `testcontainers`, etc.
*   **Rust Example (Testcontainers-rs with `sqlx` for PostgreSQL - from UG Sec 29.4):**
    ```rust
    // #[cfg(test)]
    // mod integration_tests_with_testcontainers {
    //     use testcontainers_rs::{clients::Cli as DockerCli, images::postgres::Postgres as PostgresImage, Container};
    //     use sqlx::{PgPool, postgres::PgConnectOptions};
    //     use std::str::FromStr;

    //     // Define a struct to hold the container and pool for convenience
    //     struct TestDb<'a> {
    //         // Needs to hold onto the container instance to keep it alive
    //         _container_instance: Container<'a, PostgresImage>,
    //         pool: PgPool,
    //     }

    //     async fn setup_test_db(docker_cli: &DockerCli) -> Result<TestDb<'_>, anyhow::Error> {
    //         let image = PostgresImage::default().with_version(16).with_user("testuser")
    //             .with_password("testpass").with_db_name("testdb");
            
    //         // We need to leak the cli reference or use an Arc if TestDb is meant to be passed around more complexly.
    //         // For simple test scope, getting it on stack is fine.
    //         // If docker_cli is a static or shared resource, it's simpler.
    //         // For now, assume docker_cli outlives container_instance if TestDb is returned.
    //         // A common pattern is to pass a reference to the DockerCli into TestDb.
    //         let container_instance = docker_cli.run(image); // Starts the container

    //         let host_port = container_instance.get_host_port_ipv4(5432);
    //         let connection_string = format!(
    //             "postgres://testuser:testpass@localhost:{}/testdb",
    //             host_port
    //         );

    //         let mut connect_options = PgConnectOptions::from_str(&connection_string)?;
    //         // connect_options = connect_options.log_statements(log::LevelFilter::Trace);
    //         let pool = PgPool::connect_with(connect_options).await?;

    //         // Run migrations or DDL (ensure migrations path is correct relative to test execution)
    //         // This path needs to be accessible by the test runner.
    //         // sqlx::migrate!("./migrations_exocortex_core").run(&pool).await?; 
    //         // For simplicity if migrations are not set up for tests yet:
    //         sqlx::query("CREATE TABLE IF NOT EXISTS test_items (id SERIAL PRIMARY KEY, name TEXT);").execute(&pool).await?;


    //         Ok(TestDb { _container_instance: container_instance, pool })
    //     }


    //     #[tokio::test]
    //     async fn test_db_interaction_with_container() -> Result<(), anyhow::Error> {
    //         let docker_cli = DockerCli::default();
    //         let test_db = setup_test_db(&docker_cli).await?;
    //         let pool = &test_db.pool; // Borrow the pool

    //         let item_name = "Testcontainer Item";
    //         let rec = sqlx::query("INSERT INTO test_items (name) VALUES ($1) RETURNING id")
    //             .bind(item_name)
    //             .fetch_one(pool)
    //             .await?;
    //         let inserted_id: i32 = rec.try_get("id")?;

    //         let fetched_name: Option<String> = sqlx::query_scalar("SELECT name FROM test_items WHERE id = $1")
    //             .bind(inserted_id)
    //             .fetch_optional(pool)
    //             .await?;

    //         assert_eq!(fetched_name, Some(item_name.to_string()));
            
    //         // Container is automatically stopped/removed when `_container_instance` in TestDb is dropped.
    //         Ok(())
    //     }
    // }
    ```

## 6. Distributed Tracing in Test Scenarios [UG Sec 29.5, CR5]

Understand operation flow and bottlenecks in complex integration tests.
*   **Integration:**
    1.  Instrument test harness, mock components, and Exocortex components under test with OpenTelemetry (OTel) SDKs.
    2.  Run Jaeger All-In-One (can be a Testcontainer) as part of test setup.
    3.  Configure OTel SDKs to export traces to Jaeger (OTLP endpoint).
*   **Analysis:** After test run, inspect traces in Jaeger UI to debug, verify interactions, analyze latencies. Useful for `correlation_id` propagation testing (if specific workflows use it) or complex agent chains.

