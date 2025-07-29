//! Benchmarks for sinex-test-utils to ensure performance
//!
//! Run with: cargo bench --package sinex-test-utils

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use sinex_test_utils::prelude::*;
use std::time::Duration;

/// Benchmark event creation performance
fn bench_event_creation(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("event_creation");
    group.measurement_time(Duration::from_secs(10));

    // Benchmark single event creation
    group.bench_function("single_event", |b| {
        b.to_async(&runtime).iter(|| async {
            let ctx = TestContext::new().await.unwrap();
            black_box(
                ctx.event()
                    .source("bench")
                    .type_("test.event")
                    .field("index", 1)
                    .insert()
                    .await
                    .unwrap(),
            );
        });
    });

    // Benchmark batch event creation
    for size in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("batch_events", size), size, |b, &size| {
            b.to_async(&runtime).iter(|| async {
                let ctx = TestContext::new().await.unwrap();
                let batch = ctx.create_event_batch("bench", size);

                for builder in batch {
                    black_box(builder.insert().await.unwrap());
                }
            });
        });
    }

    group.finish();
}

/// Benchmark query performance
fn bench_queries(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    // Setup: create test data
    runtime.block_on(async {
        let ctx = TestContext::with_name("bench_setup").await.unwrap();

        // Create 1000 events for querying
        for i in 0..1000 {
            ctx.event()
                .source(if i % 2 == 0 { "source-a" } else { "source-b" })
                .type_(if i % 3 == 0 { "type-x" } else { "type-y" })
                .field("index", i)
                .insert()
                .await
                .unwrap();
        }
    });

    let mut group = c.benchmark_group("queries");

    // Benchmark different query patterns
    group.bench_function("count_all", |b| {
        b.to_async(&runtime).iter(|| async {
            let ctx = TestContext::with_name("bench_query").await.unwrap();
            black_box(ctx.events().count().await.unwrap());
        });
    });

    group.bench_function("fetch_limited", |b| {
        b.to_async(&runtime).iter(|| async {
            let ctx = TestContext::with_name("bench_query").await.unwrap();
            black_box(ctx.events().limit(10).fetch().await.unwrap());
        });
    });

    group.bench_function("filtered_fetch", |b| {
        b.to_async(&runtime).iter(|| async {
            let ctx = TestContext::with_name("bench_query").await.unwrap();
            black_box(
                ctx.events()
                    .by_source("source-a")
                    .by_type("type-x")
                    .fetch()
                    .await
                    .unwrap(),
            );
        });
    });

    group.finish();
}

/// Benchmark concurrent operations
fn bench_concurrent(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("concurrent");
    group.sample_size(10); // Reduce sample size for concurrent tests

    for workers in [2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::new("concurrent_tasks", workers),
            workers,
            |b, &workers| {
                b.to_async(&runtime).iter(|| async {
                    let ctx = TestContext::new().await.unwrap();
                    black_box(
                        ctx.run_concurrent(workers, |ctx, i| async move {
                            ctx.event()
                                .source("concurrent")
                                .type_("task")
                                .field("worker", i)
                                .insert()
                                .await
                        })
                        .await
                        .unwrap(),
                    );
                });
            },
        );
    }

    group.finish();
}

/// Benchmark TestContext creation and cleanup
fn bench_context_lifecycle(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("context_lifecycle");

    group.bench_function("context_creation", |b| {
        b.to_async(&runtime).iter(|| async {
            black_box(TestContext::new().await.unwrap());
        });
    });

    group.bench_function("context_with_name", |b| {
        b.to_async(&runtime).iter(|| async {
            black_box(TestContext::with_name("bench_test").await.unwrap());
        });
    });

    group.finish();
}

/// Benchmark assertion performance
fn bench_assertions(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("assertions");

    group.bench_function("simple_assertions", |b| {
        b.to_async(&runtime).iter(|| async {
            let ctx = TestContext::new().await.unwrap();

            // Multiple assertions
            ctx.assert("test1").eq(&5, &5).unwrap();
            ctx.assert("test2").that(true, "should be true").unwrap();
            ctx.assert("test3").not_empty(&vec![1, 2, 3]).unwrap();

            black_box(());
        });
    });

    group.bench_function("event_assertions", |b| {
        b.to_async(&runtime).iter(|| async {
            let ctx = TestContext::new().await.unwrap();

            let event = ctx.event().source("bench").type_("test").build().unwrap();

            ctx.assert("event check").event_eq(&event, &event).unwrap();

            black_box(());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_event_creation,
    bench_queries,
    bench_concurrent,
    bench_context_lifecycle,
    bench_assertions
);
criterion_main!(benches);
