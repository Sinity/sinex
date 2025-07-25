# Advanced Rustdoc Techniques for Sinex

This document explores advanced rustdoc features and techniques that could provide exceptional value for documenting a complex system like Sinex.

## 1. Feature-Gated Documentation

Use Cargo features to provide different documentation views:

```rust
/// Core event type for the Sinex system.
/// 
/// # Basic Usage
/// 
/// ```rust
/// let event = Event::new("source", "type", payload);
/// ```
/// 
#[cfg_attr(feature = "doc-internal", doc = r#"
/// # Internal Architecture
/// 
/// This section is only visible when building with `--features doc-internal`.
/// 
/// ## Memory Layout
/// 
/// The Event struct is optimized for cache efficiency:
/// - ULID (16 bytes) aligned for SIMD operations
/// - String fields use small-string optimization
/// - Payload stored inline for small objects (<= 256 bytes)
/// 
/// ## Performance Considerations
/// 
/// - Cloning is expensive due to payload
/// - Use Arc<Event> for shared ownership
/// - Batch operations for better throughput
"#)]
pub struct Event {
    // ...
}
```

Enable different documentation levels:
```bash
# Public API documentation
cargo doc

# Internal documentation for developers
cargo doc --features doc-internal

# Full documentation including private items
cargo doc --document-private-items --features doc-internal,doc-experimental
```

## 2. Compile-Time Documentation Generation

Use build scripts to generate documentation from external sources:

```rust
// build.rs
use std::env;
use std::fs;
use std::path::Path;

fn main() {
    // Generate rustdoc from SQL schema
    let schema_sql = fs::read_to_string("migrations/schema.sql").unwrap();
    let schema_docs = generate_schema_docs(&schema_sql);
    
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("schema_docs.rs");
    fs::write(&dest_path, schema_docs).unwrap();
    
    println!("cargo:rerun-if-changed=migrations/schema.sql");
}

fn generate_schema_docs(sql: &str) -> String {
    // Parse SQL and generate Rust documentation
    format!(r#"
/// # Database Schema
/// 
/// Auto-generated from migrations/schema.sql
/// 
/// ## Tables
/// 
{}
"#, parse_tables(sql))
}
```

Then include in your code:
```rust
// Include generated documentation
include!(concat!(env!("OUT_DIR"), "/schema_docs.rs"));
```

## 3. Interactive Documentation with Playground Links

Create runnable examples that open in the Rust Playground:

```rust
/// Process events using the StatefulStreamProcessor pattern.
/// 
/// # Interactive Example
/// 
/// ```rust
/// # // This example can be run in the Rust Playground
/// # // Click "Run" to see it in action
/// use std::time::Duration;
/// 
/// #[derive(Debug)]
/// struct Event {
///     id: u64,
///     data: String,
/// }
/// 
/// fn process_events(events: Vec<Event>) {
///     for event in events {
///         println!("Processing: {:?}", event);
///         std::thread::sleep(Duration::from_millis(100));
///     }
/// }
/// 
/// fn main() {
///     let events = vec![
///         Event { id: 1, data: "First".into() },
///         Event { id: 2, data: "Second".into() },
///     ];
///     
///     process_events(events);
/// }
/// ```
/// 
/// [Open in Playground](https://play.rust-lang.org/?version=stable&mode=debug&edition=2021&code=use%20std%3A%3Atime%3A%3ADuration%3B%0A%0A%23%5Bderive(Debug)%5D%0Astruct%20Event%20%7B%0A%20%20%20%20id%3A%20u64%2C%0A%20%20%20%20data%3A%20String%2C%0A%7D%0A%0Afn%20process_events(events%3A%20Vec%3CEvent%3E)%20%7B%0A%20%20%20%20for%20event%20in%20events%20%7B%0A%20%20%20%20%20%20%20%20println!(%22Processing%3A%20%7B%3A%3F%7D%22%2C%20event)%3B%0A%20%20%20%20%20%20%20%20std%3A%3Athread%3A%3Asleep(Duration%3A%3Afrom_millis(100))%3B%0A%20%20%20%20%7D%0A%7D%0A%0Afn%20main()%20%7B%0A%20%20%20%20let%20events%20%3D%20vec!%5B%0A%20%20%20%20%20%20%20%20Event%20%7B%20id%3A%201%2C%20data%3A%20%22First%22.into()%20%7D%2C%0A%20%20%20%20%20%20%20%20Event%20%7B%20id%3A%202%2C%20data%3A%20%22Second%22.into()%20%7D%2C%0A%20%20%20%20%5D%3B%0A%20%20%20%20%0A%20%20%20%20process_events(events)%3B%0A%7D)
pub trait StatefulStreamProcessor {
    // ...
}
```

## 4. Documentation-Driven Testing

Use rustdoc examples as integration tests:

```rust
/// Complex event processing pipeline.
/// 
/// # Doctest as Integration Test
/// 
/// ```rust
/// # use sinex_test_utils::{TestContext, TestDatabase};
/// # tokio_test::block_on(async {
/// // This example serves as both documentation and integration test
/// let ctx = TestContext::new().await;
/// 
/// // Create test events
/// let events = vec![
///     ctx.create_event("fs", "created", json!({"path": "/test.txt"})),
///     ctx.create_event("fs", "modified", json!({"path": "/test.txt"})),
///     ctx.create_event("fs", "deleted", json!({"path": "/test.txt"})),
/// ];
/// 
/// // Insert into database
/// for event in &events {
///     ctx.insert_event(event).await.unwrap();
/// }
/// 
/// // Verify processing
/// let processed = ctx.query_events()
///     .source("fs")
///     .path("/test.txt")
///     .execute()
///     .await
///     .unwrap();
/// 
/// assert_eq!(processed.len(), 3);
/// assert_eq!(processed[0].event_type, "created");
/// assert_eq!(processed[1].event_type, "modified");
/// assert_eq!(processed[2].event_type, "deleted");
/// # });
/// ```
pub struct EventPipeline {
    // ...
}
```

Configure doctest features in Cargo.toml:
```toml
[dev-dependencies]
sinex-test-utils = { path = "../sinex-test-utils" }
tokio-test = "0.4"

[[test]]
name = "doctests"
path = "tests/doctests.rs"
doctest = true
```

## 5. Conditional Compilation for Documentation

Show platform-specific or feature-specific information:

```rust
/// System monitoring satellite.
/// 
/// Captures system-level events from various sources.
/// 
#[cfg_attr(target_os = "linux", doc = r#"
/// # Linux-Specific Features
/// 
/// On Linux, additional event sources are available:
/// - systemd journal monitoring
/// - D-Bus message capture  
/// - udev device events
/// - eBPF-based syscall tracing
/// 
/// ## Example
/// 
/// ```rust,no_run
/// use sinex_system_satellite::LinuxSystemMonitor;
/// 
/// let monitor = LinuxSystemMonitor::new()
///     .with_journal_events()
///     .with_dbus_monitoring()
///     .build()?;
/// ```
"#)]
#[cfg_attr(target_os = "macos", doc = r#"
/// # macOS-Specific Features
/// 
/// On macOS, system events are captured via:
/// - FSEvents for file system monitoring
/// - Distributed notifications
/// - Launch Services events
/// 
/// ## Example
/// 
/// ```rust,no_run
/// use sinex_system_satellite::MacSystemMonitor;
/// 
/// let monitor = MacSystemMonitor::new()
///     .with_fsevents()
///     .with_notifications()
///     .build()?;
/// ```
"#)]
pub struct SystemSatellite {
    // ...
}
```

## 6. Documentation Macros for Consistency

Create macros to ensure consistent documentation patterns:

```rust
/// Macro for documenting satellites with consistent format
macro_rules! document_satellite {
    (
        $name:ident,
        $description:expr,
        $sources:expr,
        $example:expr
    ) => {
        #[doc = concat!(
            "# ", stringify!($name), " Satellite\n\n",
            $description, "\n\n",
            "## Event Sources\n\n",
            $sources, "\n\n",
            "## Architecture\n\n",
            "This satellite implements the [`StatefulStreamProcessor`] trait and runs as a\n",
            "systemd service managed by NixOS. It communicates with sinex-ingestd via gRPC\n",
            "and publishes to Redis Streams for real-time processing.\n\n",
            "## Configuration\n\n",
            "```toml\n",
            "[satellite]\n",
            "name = \"", stringify!($name), "\"\n",
            "checkpoint_interval = \"30s\"\n",
            "batch_size = 1000\n",
            "```\n\n",
            "## Example\n\n",
            "```rust,no_run\n",
            $example,
            "\n```\n\n",
            "## Monitoring\n\n",
            "- Metrics: `sinex_", stringify!($name), "_*`\n",
            "- Logs: `journalctl -u sinex-", stringify!($name), "`\n",
            "- Status: `systemctl status sinex-", stringify!($name), "`\n"
        )]
        pub struct $name {
            // ...
        }
    };
}

// Usage:
document_satellite!(
    FilesystemSatellite,
    "Monitors filesystem changes using inotify (Linux) or FSEvents (macOS).",
    "- File creation, modification, deletion\n- Directory changes\n- Permission modifications\n- Metadata updates",
    r#"let satellite = FilesystemSatellite::new()
    .watch_path("/home/user")
    .with_recursive(true)
    .build()?;"#
);
```

## 7. Performance Documentation

Include performance characteristics in documentation:

```rust
/// High-performance event buffer for batch processing.
/// 
/// # Performance Characteristics
/// 
/// ## Benchmarks
/// 
/// | Operation | Throughput | Latency (p99) | Memory |
/// |-----------|------------|---------------|---------|
/// | Insert | 2M ops/sec | 0.5µs | O(1) |
/// | Batch read | 10M events/sec | 10µs | O(n) |
/// | Clear | O(1) | 1ns | - |
/// 
/// ## Optimization Notes
/// 
/// - Uses ring buffer with power-of-2 sizing
/// - Lock-free for single producer/consumer
/// - Memory-mapped for zero-copy operations
/// - CPU cache-aligned data structures
/// 
/// ## Example: Achieving Maximum Throughput
/// 
/// ```rust
/// use sinex_core::EventBuffer;
/// 
/// // Optimal configuration for throughput
/// let buffer = EventBuffer::builder()
///     .capacity(1 << 20)  // 1M events
///     .memory_mapped(true)
///     .prefetch_distance(64)
///     .build()?;
/// 
/// // Batch operations for best performance
/// let events: Vec<Event> = generate_events();
/// buffer.insert_batch(&events)?;
/// ```
/// 
/// ## Memory Usage
/// 
/// ```text
/// Base overhead: 64 bytes
/// Per event: 8 bytes (pointer) + event size
/// 
/// Example for 1M events averaging 200 bytes:
/// Total: 64B + 1M * (8B + 200B) ≈ 208MB
/// ```
pub struct EventBuffer {
    // ...
}
```

## 8. Versioning and Migration Documentation

Document API evolution and migration paths:

```rust
/// Event processing trait.
/// 
/// # Version History
/// 
/// ## v2.0 (Current)
/// 
/// ```rust
/// pub trait EventProcessor {
///     async fn process(&mut self, event: &Event) -> Result<ProcessResult>;
/// }
/// ```
/// 
/// ## v1.0 (Deprecated)
/// 
/// ```rust,ignore
/// pub trait EventProcessor {
///     fn process(&mut self, event: &Event) -> ProcessResult;
/// }
/// ```
/// 
/// # Migration Guide
/// 
/// ## From v1.0 to v2.0
/// 
/// The main change is the addition of async support:
/// 
/// ```rust
/// // Old (v1.0)
/// impl EventProcessor for MyProcessor {
///     fn process(&mut self, event: &Event) -> ProcessResult {
///         // Synchronous processing
///     }
/// }
/// 
/// // New (v2.0)  
/// impl EventProcessor for MyProcessor {
///     async fn process(&mut self, event: &Event) -> Result<ProcessResult> {
///         // Async processing with error handling
///     }
/// }
/// ```
/// 
/// ## Compatibility Layer
/// 
/// For gradual migration, use the compatibility adapter:
/// 
/// ```rust
/// use sinex_compat::v1_to_v2_adapter;
/// 
/// let old_processor = MyV1Processor::new();
/// let new_processor = v1_to_v2_adapter(old_processor);
/// ```
#[deprecated(since = "2.0.0", note = "Use the async version instead")]
pub trait EventProcessorV1 {
    // ...
}
```

## 9. Documentation Lints and Validation

Create custom lints for documentation quality:

```rust
// In lib.rs
#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(rustdoc::missing_doc_code_examples)]
#![warn(rustdoc::broken_intra_doc_links)]
#![deny(rustdoc::invalid_code_block_attributes)]

// Custom lint implementation
#[cfg(feature = "doc-lints")]
mod doc_lints {
    use syn::{visit::Visit, ItemFn};
    
    struct DocLinter;
    
    impl Visit<'_> for DocLinter {
        fn visit_item_fn(&mut self, func: &ItemFn) {
            // Check for required documentation sections
            let docs = extract_docs(func);
            
            if !docs.contains("# Examples") && func.vis.is_public() {
                panic!("Public function {} missing example section", func.sig.ident);
            }
            
            if !docs.contains("# Errors") && returns_result(func) {
                panic!("Function {} returns Result but missing Errors section", func.sig.ident);
            }
        }
    }
}
```

## 10. Rich Media Integration

Embed rich media in documentation:

```rust
/// Visual system architecture overview.
/// 
/// # Architecture Diagram
/// 
/// <div class="rustdoc-mermaid">
/// ```mermaid
/// graph TB
///     subgraph "Event Sources"
///         FS[Filesystem]
///         TERM[Terminal]
///         CLIP[Clipboard]
///         SYS[System]
///     end
///     
///     subgraph "Core"
///         ING[Ingestd]
///         DB[(PostgreSQL)]
///         REDIS[[Redis Streams]]
///     end
///     
///     subgraph "Processors"
///         HEALTH[Health]
///         CANON[Canonicalizer]
///         INDEX[Indexer]
///     end
///     
///     FS --> ING
///     TERM --> ING
///     CLIP --> ING
///     SYS --> ING
///     
///     ING --> DB
///     ING --> REDIS
///     
///     REDIS --> HEALTH
///     REDIS --> CANON
///     REDIS --> INDEX
///     
///     style DB fill:#f9f,stroke:#333,stroke-width:4px
///     style REDIS fill:#ff9,stroke:#333,stroke-width:2px
/// ```
/// </div>
/// 
/// # Video Walkthrough
/// 
/// <video controls width="100%">
///   <source src="https://sinex.dev/media/architecture-overview.mp4" type="video/mp4">
///   <a href="https://sinex.dev/media/architecture-overview.mp4">Download video</a>
/// </video>
/// 
/// # Interactive Demo
/// 
/// <iframe 
///     src="https://sinex.dev/demo/event-flow" 
///     width="100%" 
///     height="600px"
///     frameborder="0">
/// </iframe>
pub mod architecture {}
```

Add custom CSS for rich media:
```css
/* doc-header.html */
<style>
.rustdoc-mermaid {
    background: #f5f5f5;
    padding: 1em;
    border-radius: 4px;
}

video {
    max-width: 100%;
    height: auto;
    margin: 1em 0;
}

iframe {
    border: 1px solid #ddd;
    border-radius: 4px;
    margin: 1em 0;
}
</style>

<script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
<script>
document.addEventListener('DOMContentLoaded', function() {
    mermaid.initialize({ startOnLoad: true });
});
</script>
```

## Summary

These advanced techniques enable:

1. **Multi-audience documentation** via feature gates
2. **Auto-generated content** from external sources
3. **Interactive examples** with playground integration
4. **Documentation as tests** for correctness
5. **Platform-specific guides** via conditional compilation
6. **Consistent formatting** through macros
7. **Performance transparency** with benchmarks
8. **Clear migration paths** for API evolution
9. **Quality enforcement** via custom lints
10. **Rich media** for complex concepts

By leveraging these advanced rustdoc features, Sinex can create documentation that is not just comprehensive but also interactive, verifiable, and tailored to different audiences while maintaining a single source of truth.