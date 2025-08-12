# Event<T> Use Cases: Where It Could Shine

You're right - just because the current code immediately converts Event<T> to RawEvent doesn't mean that's the only way to use it. Let's explore where Event<T> could provide real value if used throughout its natural lifetime.

## Potential Use Cases for Event<T>

### 1. Type-Safe Event Pipelines Within a Service

```rust
// A service that processes only FileSystem events
struct FileSystemMonitor {
    pending: Vec<Event<FileCreatedPayload>>,
    processed: Vec<Event<FileCreatedPayload>>,
}

impl FileSystemMonitor {
    async fn scan_directory(&mut self, path: &Path) -> Result<()> {
        for entry in fs::read_dir(path)? {
            let metadata = entry.metadata()?;
            
            // Create typed event
            let event = Event::from_material(
                FileCreatedPayload {
                    path: entry.path(),
                    size: metadata.len(),
                    mode: metadata.permissions().mode(),
                    created: metadata.created()?,
                },
                material_id,
                anchor_byte,
            );
            
            // Work with it as Event<T>
            self.validate_event(&event)?;
            self.enrich_event(&mut event).await?;
            self.pending.push(event);
        }
        
        self.process_pending_batch().await
    }
    
    fn validate_event(&self, event: &Event<FileCreatedPayload>) -> Result<()> {
        // Direct typed access - no extraction needed
        if event.payload.size > MAX_FILE_SIZE {
            return Err(Error::FileTooLarge);
        }
        if event.payload.path.starts_with("/tmp") {
            return Err(Error::IgnoredPath);
        }
        Ok(())
    }
    
    async fn enrich_event(&self, event: &mut Event<FileCreatedPayload>) -> Result<()> {
        // Can mutate the typed payload directly
        event.payload.mime_type = detect_mime_type(&event.payload.path)?;
        event.payload.hash = calculate_hash(&event.payload.path).await?;
        Ok(())
    }
    
    async fn process_pending_batch(&mut self) -> Result<()> {
        // Can work with collections of typed events
        let batch: Vec<Event<FileCreatedPayload>> = self.pending.drain(..).collect();
        
        // Group by directory
        let by_dir: HashMap<PathBuf, Vec<Event<FileCreatedPayload>>> = 
            batch.into_iter()
                .group_by(|e| e.payload.path.parent().unwrap().to_owned())
                .collect();
        
        // Process each directory's events
        for (dir, events) in by_dir {
            self.process_directory_events(dir, events).await?;
        }
        
        Ok(())
    }
}
```

### 2. Compile-Time Event Routing

```rust
// Different handlers for different event types
trait TypedEventHandler<T: EventPayload> {
    async fn handle(&mut self, event: Event<T>) -> Result<()>;
}

struct FileHandler;
impl TypedEventHandler<FileCreatedPayload> for FileHandler {
    async fn handle(&mut self, event: Event<FileCreatedPayload>) -> Result<()> {
        println!("File created: {}", event.payload.path.display());
        Ok(())
    }
}

struct CommandHandler;
impl TypedEventHandler<CommandExecutedPayload> for CommandHandler {
    async fn handle(&mut self, event: Event<CommandExecutedPayload>) -> Result<()> {
        println!("Command executed: {}", event.payload.command);
        Ok(())
    }
}

// Router that preserves types
struct TypedRouter {
    file_handler: FileHandler,
    command_handler: CommandHandler,
}

impl TypedRouter {
    async fn route_file_event(&mut self, event: Event<FileCreatedPayload>) -> Result<()> {
        self.file_handler.handle(event).await
    }
    
    async fn route_command_event(&mut self, event: Event<CommandExecutedPayload>) -> Result<()> {
        self.command_handler.handle(event).await
    }
}
```

### 3. Type-Safe Event Transformations

```rust
// Transform one typed event into another
impl Event<FileCreatedPayload> {
    fn to_artifact_event(self) -> Event<ArtifactDiscoveredPayload> {
        Event::from_synthesis(
            ArtifactDiscoveredPayload {
                source_path: self.payload.path.clone(),
                artifact_type: detect_artifact_type(&self.payload),
                size_bytes: self.payload.size,
                discovered_at: self.ts_orig.unwrap_or_else(Utc::now),
            },
            vec![self.id.unwrap()],  // Parent is the file event
        )
    }
}

// Chain of typed transformations
let file_event = Event::<FileCreatedPayload>::from_material(payload, material_id, anchor);
let artifact_event = file_event.to_artifact_event();
let knowledge_event = artifact_event.to_knowledge_event();
```

### 4. Testing with Type Safety

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_file_processing() {
        // Create typed test events
        let event = Event::from_material(
            FileCreatedPayload {
                path: PathBuf::from("/test/file.txt"),
                size: 100,
                mode: 0o644,
            },
            Id::new(),
            0,
        );
        
        // Test with compile-time type checking
        assert_eq!(event.payload.size, 100);
        assert!(event.payload.path.is_absolute());
        
        // Can't accidentally access wrong fields
        // assert_eq!(event.payload.command, "test");  // Won't compile!
    }
    
    fn create_test_batch() -> Vec<Event<FileCreatedPayload>> {
        (0..10).map(|i| {
            Event::from_material(
                FileCreatedPayload {
                    path: PathBuf::from(format!("/test/file{}.txt", i)),
                    size: i * 100,
                    mode: 0o644,
                },
                Id::new(),
                i as i64,
            )
        }).collect()
    }
}
```

### 5. Domain-Specific Event Collections

```rust
// A service that deals only with terminal events
struct TerminalProcessor {
    commands: Vec<Event<CommandExecutedPayload>>,
    outputs: Vec<Event<TerminalOutputPayload>>,
    sessions: Vec<Event<SessionStartedPayload>>,
}

impl TerminalProcessor {
    // Can have methods that work with specific typed events
    fn get_failed_commands(&self) -> Vec<&Event<CommandExecutedPayload>> {
        self.commands
            .iter()
            .filter(|e| e.payload.exit_code != 0)
            .collect()
    }
    
    fn get_long_running_commands(&self) -> Vec<&Event<CommandExecutedPayload>> {
        self.commands
            .iter()
            .filter(|e| e.payload.duration_ms > 5000)
            .collect()
    }
}
```

## When Event<T> Makes Sense

Event<T> provides value when:

1. **Single event type processing** - A component only deals with one type of event
2. **Internal pipelines** - Events stay within a service boundary
3. **Compile-time routing** - Different code paths for different event types
4. **Testing** - Type safety in test fixtures and assertions
5. **Domain modeling** - Events are first-class domain objects, not just data

## When Event<T> Doesn't Help

Event<T> doesn't help when:

1. **Persistence** - Must serialize to JSON for database/network anyway
2. **Heterogeneous processing** - Mixing different event types (most processors)
3. **Dynamic dispatch** - Event type determined at runtime
4. **Cross-service boundaries** - Must serialize for RPC/messaging
5. **Query results** - Database returns RawEvent, not Event<T>

## The Reality Check

Looking at the actual Sinex architecture:

- **Satellites** create various event types (heterogeneous)
- **Ingestd** receives all event types (must use RawEvent)
- **Database** stores as JSONB (type information lost)
- **NATS** publishes as JSON (type information lost)
- **Automata** consume mixed event types (heterogeneous)

The architecture is fundamentally based on heterogeneous event streams, which is why Event<T> is immediately converted to RawEvent in practice.

## Conclusion

You're absolutely right that Event<T> COULD be used throughout its lifetime, not just immediately converted. There ARE valid use cases where keeping the typed wrapper provides value.

However, the Sinex architecture is fundamentally heterogeneous - events of all types flow through the same pipelines, get stored in the same table, and are processed by automata that handle multiple event types. This is why the current code immediately converts to RawEvent.

If we had domain-specific services that only dealt with one event type, Event<T> would be more valuable. But given the actual architecture, the typed payload helpers might be sufficient without the full Event<T> wrapper.