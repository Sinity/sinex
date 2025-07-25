//! Performance benchmarks for sinex-test-utils
//!
//! Run with: cargo bench --bench test_utils_bench

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sinex_test_utils::prelude::*;
use tokio::runtime::Runtime;

fn bench_test_context_creation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("test_context_creation", |b| {
        b.iter(|| {
            rt.block_on(async {
                let ctx = TestContext::with_name("bench").await.unwrap();
                black_box(ctx);
            })
        })
    });
}

fn bench_event_creation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(TestContext::with_name("bench")).unwrap();
    
    let mut group = c.benchmark_group("event_creation");
    
    group.bench_function("simple_event", |b| {
        b.iter(|| {
            rt.block_on(async {
                let event = ctx.event()
                    .source("bench")
                    .type_("test")
                    .build()
                    .unwrap();
                black_box(event);
            })
        })
    });
    
    group.bench_function("filesystem_event", |b| {
        b.iter(|| {
            rt.block_on(async {
                let event = ctx.event()
                    .filesystem()
                    .path("/test/file.txt")
                    .size(1024)
                    .created()
                    .build()
                    .unwrap();
                black_box(event);
            })
        })
    });
    
    group.bench_function("complex_event", |b| {
        b.iter(|| {
            rt.block_on(async {
                let event = ctx.event()
                    .source("complex")
                    .type_("test")
                    .field("field1", "value1")
                    .field("field2", 42)
                    .field("field3", true)
                    .field("field4", serde_json::json!({"nested": "object"}))
                    .build()
                    .unwrap();
                black_box(event);
            })
        })
    });
    
    group.finish();
}

fn bench_event_insertion(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("event_insertion");
    group.sample_size(50); // Reduce sample size for database operations
    
    for size in [1, 10, 100].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let ctx = TestContext::with_name("bench_insert").await.unwrap();
                    
                    for i in 0..size {
                        ctx.event()
                            .source("bench")
                            .type_("test")
                            .field("index", i)
                            .insert()
                            .await
                            .unwrap();
                    }
                })
            })
        });
    }
    
    group.finish();
}

fn bench_event_querying(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    // Setup: Insert test data
    let ctx = rt.block_on(TestContext::with_name("bench_query")).unwrap();
    rt.block_on(async {
        for i in 0..100 {
            ctx.event()
                .source(if i % 2 == 0 { "even" } else { "odd" })
                .type_(format!("type_{}", i % 5))
                .field("index", i)
                .insert()
                .await
                .unwrap();
        }
    });
    
    let mut group = c.benchmark_group("event_querying");
    group.sample_size(100);
    
    group.bench_function("query_all", |b| {
        b.iter(|| {
            rt.block_on(async {
                let events = ctx.events().fetch().await.unwrap();
                black_box(events);
            })
        })
    });
    
    group.bench_function("query_by_source", |b| {
        b.iter(|| {
            rt.block_on(async {
                let events = ctx.events().by_source("even").fetch().await.unwrap();
                black_box(events);
            })
        })
    });
    
    group.bench_function("query_with_limit", |b| {
        b.iter(|| {
            rt.block_on(async {
                let events = ctx.events().limit(10).fetch().await.unwrap();
                black_box(events);
            })
        })
    });
    
    group.bench_function("count_query", |b| {
        b.iter(|| {
            rt.block_on(async {
                let count = ctx.events().count().await.unwrap();
                black_box(count);
            })
        })
    });
    
    group.finish();
}

fn bench_assertions(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(TestContext::with_name("bench_assert")).unwrap();
    
    let mut group = c.benchmark_group("assertions");
    
    group.bench_function("simple_assertion", |b| {
        b.iter(|| {
            let result = ctx.assert("test").eq(&5, &5);
            black_box(result);
        })
    });
    
    group.bench_function("chained_assertions", |b| {
        b.iter(|| {
            let vec = vec![1, 2, 3];
            let result = ctx.assert("test")
                .eq(&vec.len(), &3)
                .and_then(|a| a.not_empty(&vec))
                .and_then(|a| a.has_size(&vec, 3));
            black_box(result);
        })
    });
    
    group.finish();
}

fn bench_database_pool(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("database_pool");
    group.sample_size(50);
    
    group.bench_function("acquire_database", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = sinex_test_utils::database_pool::acquire_test_database()
                    .await
                    .unwrap();
                black_box(db);
                // Database automatically returned to pool on drop
            })
        })
    });
    
    group.bench_function("concurrent_acquisition", |b| {
        b.iter(|| {
            rt.block_on(async {
                let handles: Vec<_> = (0..4)
                    .map(|_| {
                        tokio::spawn(async {
                            sinex_test_utils::database_pool::acquire_test_database()
                                .await
                                .unwrap()
                        })
                    })
                    .collect();
                
                for handle in handles {
                    let db = handle.await.unwrap();
                    black_box(db);
                }
            })
        })
    });
    
    group.finish();
}

fn bench_mock_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(TestContext::with_name("bench_mock")).unwrap();
    
    let mut group = c.benchmark_group("mock_operations");
    
    group.bench_function("filesystem_mock", |b| {
        b.iter(|| {
            rt.block_on(async {
                let fs = ctx.mocks().filesystem();
                fs.create_file("/bench/test.txt", b"content").await.unwrap();
                let exists = fs.exists("/bench/test.txt").await;
                black_box(exists);
            })
        })
    });
    
    group.bench_function("database_mock", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = ctx.mocks().database();
                let conn = db.connection().await.unwrap();
                let result = conn.execute("INSERT INTO test VALUES (1)").await;
                black_box(result);
            })
        })
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_test_context_creation,
    bench_event_creation,
    bench_event_insertion,
    bench_event_querying,
    bench_assertions,
    bench_database_pool,
    bench_mock_operations
);
criterion_main!(benches);