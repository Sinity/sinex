# Benchmarking Infrastructure - Iai Extension

This document describes how to extend the benchmarking infrastructure with Iai for hardware counter analysis when needed.

## When to Add Iai

Consider adding Iai benchmarks when:
- You need precise instruction counts for optimization work
- Cache behavior analysis is critical (L1/L2 misses)
- You're optimizing tight loops or algorithmic complexity
- Timing variance is too high with Divan for reliable measurements
- You need deterministic, reproducible CPU metrics

## Inline Iai Benchmarks

While Iai requires separate binary targets, we can still keep benchmark logic inline:

```rust
// src/query_builder.rs - Benchmark logic stays inline
#[cfg(bench)]
pub mod iai_benches {
    use super::*;
    
    pub fn bench_query_builder_iai() {
        iai::black_box(QueryBuilder::new()
            .select("events")
            .columns(&["id", "source", "type"])
            .where_eq("source", "test")
            .limit(100)
            .build());
    }
    
    pub fn bench_query_optimization_iai() {
        let query = QueryBuilder::new()
            .select("events")
            .where_eq("source", "test")
            .where_gt("timestamp", 1234567890)
            .order_by("timestamp", Order::Desc)
            .limit(1000);
            
        iai::black_box(query.optimize().build());
    }
}
```

## Wrapper Organization Options

### Option 1: Individual Wrapper Files (Simple)

Create a thin wrapper for each module:

```rust
// benches/query_builder_iai.rs
use sinex_db::query_builder::iai_benches::*;

iai::main!(
    bench_query_builder_iai,
    bench_query_optimization_iai
);
```

### Option 2: Single Wrapper Per Crate (Better)

Re-export all iai benchmarks at crate level:

```rust
// sinex-db/src/lib.rs
#[cfg(bench)]
pub mod iai_benches {
    pub use crate::query_builder::iai_benches::*;
    pub use crate::events::iai_benches::*;
    pub use crate::distributed_locking::iai_benches::*;
}

// benches/sinex_db_iai.rs
use sinex_db::iai_benches::*;

iai::main!(
    bench_query_builder_iai,
    bench_query_optimization_iai,
    bench_event_parsing_iai,
    bench_advisory_lock_iai,
    // ... all benchmarks from sinex-db
);
```

### Option 3: Dedicated Benchmark Crate (Scalable)

Create a separate crate for all Iai wrappers:

```
workspace/
├── crate/
│   └── sinex-db/src/          # Contains pub mod iai_benches
└── sinex-benchmarks/          # Dedicated benchmark crate
    ├── Cargo.toml
    ├── benches/
    │   └── all_iai.rs         # Single file with all benchmarks
    └── build.rs               # Optional: auto-generate wrapper
```

```rust
// sinex-benchmarks/benches/all_iai.rs
use sinex_db::iai_benches as db;
use sinex_core::iai_benches as core;
use sinex_gateway::iai_benches as gateway;

iai::main!(
    // Database benchmarks
    db::bench_query_builder_iai,
    db::bench_event_parsing_iai,
    
    // Core benchmarks
    core::bench_ulid_generation_iai,
    core::bench_serialization_iai,
    
    // Gateway benchmarks
    gateway::bench_rpc_parsing_iai,
    gateway::bench_request_routing_iai,
);
```

## Auto-Generation Approach

For large projects, auto-generate the Iai wrapper:

```rust
// sinex-benchmarks/build.rs
use std::fs;
use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=../");
    
    let benchmarks = scan_workspace_for_iai_benchmarks();
    generate_iai_wrapper(&benchmarks);
}

fn scan_workspace_for_iai_benchmarks() -> Vec<(String, Vec<String>)> {
    let mut results = vec![];
    
    // Scan each crate in workspace
    for crate_name in ["sinex-db", "sinex-core", "sinex-gateway"] {
        let crate_path = format!("../crate/{}/src", crate_name);
        let functions = find_iai_functions(&crate_path);
        
        if !functions.is_empty() {
            results.push((crate_name.to_string(), functions));
        }
    }
    
    results
}

fn generate_iai_wrapper(benchmarks: &[(String, Vec<String>)]) {
    let mut content = String::new();
    
    // Generate imports
    for (crate_name, _) in benchmarks {
        let alias = crate_name.replace('-', "_");
        writeln!(content, "use {}::iai_benches as {};", crate_name, alias);
    }
    
    // Generate iai::main! call
    content.push_str("\niai::main!(\n");
    for (crate_name, functions) in benchmarks {
        let alias = crate_name.replace('-', "_");
        for func in functions {
            writeln!(content, "    {}::{},", alias, func);
        }
    }
    content.push_str(");\n");
    
    fs::write("benches/all_iai.rs", content).unwrap();
}
```

## Cargo Configuration

### For Individual Crates

```toml
# sinex-db/Cargo.toml
[features]
bench = ["dep:divan", "sinex-test-utils/bench"]
bench-iai = ["dep:iai", "bench"]

[dependencies]
divan = { version = "0.1", optional = true }
iai = { version = "0.1", optional = true }

[[bench]]
name = "sinex_db_iai"
harness = false
required-features = ["bench-iai"]
```

### For Dedicated Benchmark Crate

```toml
# sinex-benchmarks/Cargo.toml
[package]
name = "sinex-benchmarks"
version = "0.1.0"
edition = "2021"

[dependencies]
sinex-db = { path = "../crate/sinex-db", features = ["bench"] }
sinex-core = { path = "../crate/sinex-core", features = ["bench"] }
sinex-gateway = { path = "../crate/sinex-gateway", features = ["bench"] }
iai = "0.1"

[build-dependencies]
syn = "2.0"
quote = "1.0"
walkdir = "2.0"

[[bench]]
name = "all_iai"
harness = false
```

## Running Iai Benchmarks

```bash
# Run Iai benchmarks for specific crate
cargo bench --features bench-iai -p sinex-db

# Run all Iai benchmarks (with dedicated crate)
cd sinex-benchmarks
cargo bench

# Run specific Iai benchmark
cargo bench --bench sinex_db_iai

# Compare with baseline
cargo bench > baseline.txt
# ... make changes ...
cargo bench > current.txt
diff baseline.txt current.txt
```

## Result Collection

Extend the result collector to parse Iai output:

```rust
fn parse_iai_results(output: &str) -> Result<HashMap<String, IaiMetrics>> {
    let mut results = HashMap::new();
    let mut current_bench = None;
    let mut current_metrics = IaiMetrics::default();
    
    for line in output.lines() {
        if !line.starts_with(' ') && line.contains("_iai") {
            // Save previous benchmark
            if let Some(name) = current_bench {
                results.insert(name.to_string(), current_metrics);
            }
            
            // Start new benchmark
            current_bench = Some(line.trim());
            current_metrics = IaiMetrics::default();
        } else if current_bench.is_some() {
            // Parse metrics
            if let Some(caps) = INSTRUCTION_RE.captures(line) {
                current_metrics.instructions = caps[1].parse().ok();
            } else if let Some(caps) = L1_RE.captures(line) {
                current_metrics.l1_accesses = caps[1].parse().ok();
            } else if let Some(caps) = L2_RE.captures(line) {
                current_metrics.l2_accesses = caps[1].parse().ok();
            } else if let Some(caps) = RAM_RE.captures(line) {
                current_metrics.ram_accesses = caps[1].parse().ok();
            } else if let Some(caps) = CYCLES_RE.captures(line) {
                current_metrics.estimated_cycles = caps[1].parse().ok();
            }
        }
    }
    
    // Save last benchmark
    if let Some(name) = current_bench {
        results.insert(name.to_string(), current_metrics);
    }
    
    Ok(results)
}
```

## Just Commands

Add these to your `justfile`:

```makefile
# Run Iai benchmarks
bench-iai:
    cargo bench --features bench-iai > target/iai-results.txt
    just bench-parse-iai

# Run Iai for specific crate
bench-iai-crate crate:
    cargo bench --features bench-iai -p {{crate}} > target/iai-{{crate}}.txt

# Parse Iai results
bench-parse-iai:
    cargo run --bin iai-parser < target/iai-results.txt

# Compare Iai results
bench-iai-compare:
    cp target/iai-results.txt target/iai-baseline.txt
    @echo "Make your changes, then run: just bench-iai-diff"

bench-iai-diff:
    cargo bench --features bench-iai > target/iai-current.txt
    diff -u target/iai-baseline.txt target/iai-current.txt || true
```

## Integration with Main Infrastructure

The Iai results can be merged with Divan results:

```rust
// In BenchmarkResult struct
pub struct BenchmarkResult {
    // ... existing fields ...
    
    // Optional hardware counters from Iai
    pub instructions: Option<u64>,
    pub l1_accesses: Option<u64>,
    pub l2_accesses: Option<u64>,
    pub ram_accesses: Option<u64>,
    pub estimated_cycles: Option<u64>,
}

// When collecting results
fn collect_comprehensive_results() -> Result<BenchmarkRun> {
    let mut run = collect_divan_results()?;
    
    if let Ok(iai_metrics) = collect_iai_results() {
        // Merge Iai hardware counters into matching benchmarks
        for result in &mut run.benchmarks {
            if let Some(iai) = iai_metrics.get(&result.name) {
                result.instructions = iai.instructions;
                result.l1_accesses = iai.l1_accesses;
                result.l2_accesses = iai.l2_accesses;
                result.ram_accesses = iai.ram_accesses;
                result.estimated_cycles = iai.estimated_cycles;
            }
        }
    }
    
    Ok(run)
}
```

## Best Practices

1. **Start with Divan** - Only add Iai for specific hot paths
2. **Keep logic inline** - Iai wrappers should be thin
3. **Consistent naming** - Use `_iai` suffix for Iai benchmark functions
4. **Group by concern** - One Iai wrapper per module or crate
5. **Document why** - Explain why hardware counters are needed for specific benchmarks
6. **Version datasets** - Iai needs the same static fixtures as Divan

## Summary

This extension allows you to:
- Keep benchmark logic inline even with Iai
- Add hardware counter analysis when needed
- Scale from single benchmarks to whole-workspace coverage
- Maintain unified result collection
- Preserve the simplicity of the main Divan-based approach

Use Iai sparingly - only where hardware metrics provide clear value over timing-based measurements.