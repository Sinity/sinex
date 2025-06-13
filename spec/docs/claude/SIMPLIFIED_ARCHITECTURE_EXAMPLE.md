# Simplified Architecture Example

## Before: Complex Trait-Based Architecture

```rust
// Complex trait hierarchy
trait EventType {
    type Payload: Serialize + JsonSchema;
    type SourceImpl: EventSourceProvider;
    const EVENT_NAME: &'static str;
}

trait EventSource {
    type Config;
    async fn initialize(config: Self::Config) -> Result<Self>;
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()>;
}

// Overlapping frameworks
struct IngestorApp<I: Ingestor> { /* ... */ }
struct IngestorRuntime<I: SimpleIngestor> { /* ... */ }

// Manual registry
fn create_registry() -> EventRegistry {
    // Manually populated despite "compile-time discovery" goal
}
```

## After: Direct, Simple Architecture

```rust
// Simple collector that knows what it needs to do
pub struct UnifiedCollector {
    filesystem_config: Option<FilesystemConfig>,
    terminal_config: Option<TerminalConfig>, 
    window_manager_config: Option<WindowManagerConfig>,
    enabled_events: HashSet<String>,
}

impl UnifiedCollector {
    pub async fn run(&mut self, runtime: &CollectorRuntime) -> Result<()> {
        let mut tasks = vec![];
        
        // Start only needed sources based on enabled events
        if self.needs_filesystem_events() {
            let config = self.filesystem_config.clone().unwrap_or_default();
            let tx = runtime.event_channel();
            tasks.push(tokio::spawn(async move {
                FilesystemWatcher::new(config).watch_events(tx).await
            }));
        }
        
        if self.needs_terminal_events() {
            let config = self.terminal_config.clone().unwrap_or_default();
            let tx = runtime.event_channel();
            tasks.push(tokio::spawn(async move {
                TerminalMonitor::new(config).monitor_events(tx).await
            }));
        }
        
        // Wait for tasks
        futures::future::join_all(tasks).await;
        Ok(())
    }
    
    fn needs_filesystem_events(&self) -> bool {
        self.enabled_events.iter().any(|e| e.starts_with("file."))
    }
    
    fn needs_terminal_events(&self) -> bool {
        self.enabled_events.iter().any(|e| e.starts_with("command."))
    }
}

// Single runtime that handles everything
pub struct CollectorRuntime {
    event_sink: Arc<dyn EventSink>,
    event_rx: mpsc::Receiver<RawEvent>,
    event_tx: mpsc::Sender<RawEvent>,
    metrics: Arc<Mutex<Metrics>>,
    config: RuntimeConfig,
}

impl CollectorRuntime {
    pub async fn run(config: Config, args: Args) -> Result<()> {
        // Initialize based on mode
        let event_sink = Self::create_event_sink(&config, &args).await?;
        let runtime = Self::new(event_sink, config.runtime)?;
        
        // Create and run collector
        let mut collector = UnifiedCollector::from_config(config)?;
        
        // Spawn background tasks
        let heartbeat_task = runtime.spawn_heartbeat();
        let processor_task = runtime.spawn_event_processor();
        
        // Run the collector
        let result = collector.run(&runtime).await;
        
        // Cleanup
        heartbeat_task.abort();
        processor_task.abort();
        
        result
    }
    
    fn event_channel(&self) -> mpsc::Sender<RawEvent> {
        self.event_tx.clone()
    }
}

// Direct event source implementations (no traits needed)
pub struct FilesystemWatcher {
    config: FilesystemConfig,
    watcher: notify::RecommendedWatcher,
}

impl FilesystemWatcher {
    pub async fn watch_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()> {
        // Direct implementation
    }
}
```

## Configuration remains simple

```toml
# unified-collector.toml
[enabled_events]
# Just list what you want
events = [
    "file.created",
    "file.modified", 
    "command.executed",
    "window.focused"
]

[sources.filesystem]
watch_paths = ["~/Documents", "~/Projects"]
ignore_patterns = ["*.tmp", "*.log"]

[sources.terminal]
socket_path = "/tmp/kitty"

[runtime]
heartbeat_interval_secs = 60
batch_size = 100
```

## Benefits Demonstrated

1. **Clear Flow**: Config → Runtime → Collector → Event Sources
2. **No Magic**: Everything is explicit and debuggable
3. **Testable**: Each component can be tested independently
4. **Extensible**: Add new sources by creating a struct with a `watch_events` method
5. **Type Safe**: Still get Rust's type safety without complex traits

## Migration Path

1. Keep EventSink abstraction (it's good)
2. Remove EventType/EventSource traits
3. Merge IngestorApp into CollectorRuntime
4. Simplify UnifiedCollector to direct implementation
5. Update event sources to be simple structs with async methods