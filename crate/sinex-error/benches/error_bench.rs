//! Benchmarks for sinex-error
//!
//! Run with: cargo bench --package sinex-error

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sinex_error::{ResultExt, SinexError};
use std::collections::HashMap;

fn bench_error_creation(c: &mut Criterion) {
    c.bench_function("create_simple_error", |b| {
        b.iter(|| {
            let err = SinexError::database(black_box("Connection failed"));
            black_box(err);
        });
    });

    c.bench_function("create_error_with_context", |b| {
        b.iter(|| {
            let err = SinexError::database(black_box("Query failed"))
                .with_context("table", black_box("users"))
                .with_context("query_time_ms", black_box(1500));
            black_box(err);
        });
    });

    c.bench_function("create_error_with_sources", |b| {
        b.iter(|| {
            let err = SinexError::service(black_box("Processing failed"))
                .with_source(black_box("Database unavailable"))
                .with_source(black_box("Connection timeout"));
            black_box(err);
        });
    });

    c.bench_function("create_complex_error", |b| {
        b.iter(|| {
            let err = SinexError::service(black_box("Request processing failed"))
                .with_context("request_id", black_box("abc-123"))
                .with_context("user_id", black_box("user-456"))
                .with_context("retry_count", black_box(3))
                .with_source(black_box("Database connection lost"))
                .with_source(black_box("Network timeout after 30s"));
            black_box(err);
        });
    });
}

fn bench_error_categorization(c: &mut Criterion) {
    let errors = vec![
        SinexError::timeout("Request timed out"),
        SinexError::validation("Invalid input"),
        SinexError::permission_denied("Access denied"),
        SinexError::database("Connection failed"),
    ];

    c.bench_function("is_retryable", |b| {
        b.iter(|| {
            for err in &errors {
                black_box(err.is_retryable());
            }
        });
    });

    c.bench_function("is_client_error", |b| {
        b.iter(|| {
            for err in &errors {
                black_box(err.is_client_error());
            }
        });
    });

    c.bench_function("is_permanent", |b| {
        b.iter(|| {
            for err in &errors {
                black_box(err.is_permanent());
            }
        });
    });

    c.bench_function("status_code", |b| {
        b.iter(|| {
            for err in &errors {
                black_box(err.status_code());
            }
        });
    });
}

fn bench_error_serialization(c: &mut Criterion) {
    let simple_error = SinexError::validation("Invalid input");
    let complex_error = SinexError::database("Query failed")
        .with_context("table", "users")
        .with_context("query", "SELECT * FROM users WHERE id = ?")
        .with_context("duration_ms", 1500)
        .with_source("Connection pool exhausted")
        .with_source("Too many connections");

    c.bench_function("serialize_simple_error", |b| {
        b.iter(|| {
            let json = serde_json::to_string(black_box(&simple_error)).unwrap();
            black_box(json);
        });
    });

    c.bench_function("serialize_complex_error", |b| {
        b.iter(|| {
            let json = serde_json::to_string(black_box(&complex_error)).unwrap();
            black_box(json);
        });
    });

    let simple_json = serde_json::to_string(&simple_error).unwrap();
    let complex_json = serde_json::to_string(&complex_error).unwrap();

    c.bench_function("deserialize_simple_error", |b| {
        b.iter(|| {
            let err: SinexError = serde_json::from_str(black_box(&simple_json)).unwrap();
            black_box(err);
        });
    });

    c.bench_function("deserialize_complex_error", |b| {
        b.iter(|| {
            let err: SinexError = serde_json::from_str(black_box(&complex_json)).unwrap();
            black_box(err);
        });
    });
}

fn bench_error_display(c: &mut Criterion) {
    let simple_error = SinexError::validation("Invalid input");
    let error_with_context = SinexError::database("Query failed")
        .with_context("table", "users")
        .with_context("rows", 1000);
    let error_with_sources = SinexError::service("Processing failed")
        .with_source("Database error")
        .with_source("Connection timeout")
        .with_source("Network unreachable");

    c.bench_function("display_simple_error", |b| {
        b.iter(|| {
            let s = format!("{}", black_box(&simple_error));
            black_box(s);
        });
    });

    c.bench_function("display_error_with_context", |b| {
        b.iter(|| {
            let s = format!("{}", black_box(&error_with_context));
            black_box(s);
        });
    });

    c.bench_function("display_error_with_sources", |b| {
        b.iter(|| {
            let s = format!("{}", black_box(&error_with_sources));
            black_box(s);
        });
    });
}

fn bench_error_conversions(c: &mut Criterion) {
    c.bench_function("io_error_conversion", |b| {
        b.iter(|| {
            let io_err =
                std::io::Error::new(std::io::ErrorKind::NotFound, black_box("File not found"));
            let sinex_err: SinexError = io_err.into();
            black_box(sinex_err);
        });
    });

    c.bench_function("json_error_conversion", |b| {
        let invalid_json = black_box("{ invalid json }");
        b.iter(|| {
            let json_err = serde_json::from_str::<serde_json::Value>(invalid_json).unwrap_err();
            let sinex_err: SinexError = json_err.into();
            black_box(sinex_err);
        });
    });
}

fn bench_result_ext(c: &mut Criterion) {
    fn failing_io_operation() -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Not found",
        ))
    }

    c.bench_function("result_context", |b| {
        b.iter(|| {
            let result = failing_io_operation().context(black_box("Operation failed"));
            black_box(result);
        });
    });

    c.bench_function("result_with_context", |b| {
        b.iter(|| {
            let result = failing_io_operation().with_context(|| {
                SinexError::service(black_box("Custom error"))
                    .with_context("component", black_box("test"))
            });
            black_box(result);
        });
    });
}

fn bench_context_operations(c: &mut Criterion) {
    c.bench_function("add_10_context_entries", |b| {
        b.iter(|| {
            let mut err = SinexError::database(black_box("Query failed"));
            for i in 0..10 {
                err = err.with_context(
                    black_box(format!("key_{}", i)),
                    black_box(format!("value_{}", i)),
                );
            }
            black_box(err);
        });
    });

    c.bench_function("add_100_context_entries", |b| {
        b.iter(|| {
            let mut err = SinexError::database(black_box("Query failed"));
            for i in 0..100 {
                err = err.with_context(
                    black_box(format!("key_{}", i)),
                    black_box(format!("value_{}", i)),
                );
            }
            black_box(err);
        });
    });

    let error_with_context = {
        let mut err = SinexError::database("Query failed");
        for i in 0..50 {
            err = err.with_context(format!("key_{}", i), format!("value_{}", i));
        }
        err
    };

    c.bench_function("access_context_map", |b| {
        b.iter(|| {
            let map = error_with_context.context_map();
            black_box(map.len());
        });
    });
}

criterion_group!(
    benches,
    bench_error_creation,
    bench_error_categorization,
    bench_error_serialization,
    bench_error_display,
    bench_error_conversions,
    bench_result_ext,
    bench_context_operations
);
criterion_main!(benches);
