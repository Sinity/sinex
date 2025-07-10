use crate::{EventSender, Result};
use async_trait::async_trait;
use schemars::schema::RootSchema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

// ===== Event output configuration (from event_output.rs) =====

/// Simple event output configuration
#[derive(Debug, Clone, Default)]
pub struct EventOutput {
    pub write_to_db: bool,
    pub log_events: bool,
    pub debug_file: Option<PathBuf>,
}

impl EventOutput {
    pub fn database() -> Self {
        Self {
            write_to_db: true,
            log_events: false,
            debug_file: None,
        }
    }

    pub fn dry_run() -> Self {
        Self {
            write_to_db: false,
            log_events: true,
            debug_file: None,
        }
    }

    pub fn with_debug_file(mut self, path: PathBuf) -> Self {
        self.debug_file = Some(path);
        self
    }
}

// ===== Traits (from traits.rs) =====

/// Core trait for event types - events are primary entities
pub trait EventType: Send + Sync + 'static {
    /// The payload type for this event
    type Payload: Serialize + for<'de> Deserialize<'de> + JsonSchema + Send + Sync + 'static;

    /// The source implementation(s) that produce this event
    /// Can be a single source or tuple of sources
    type SourceImpl: EventSourceProvider;

    /// Canonical event name (e.g., "file.created")
    const EVENT_NAME: &'static str;

    /// Source name - derived from SourceImpl
    const SOURCE_NAME: &'static str = <Self::SourceImpl as EventSourceProvider>::SOURCE_NAME;
}

/// Trait for event sources - implementation details that produce events (subsumes EventFactory)
#[async_trait]
pub trait EventSource: Send + Sync + 'static {
    /// Configuration type for this source
    type Config: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;

    /// Canonical source name
    const SOURCE_NAME: &'static str;

    /// Initialize the source with context containing config and shared resources
    async fn initialize(ctx: crate::EventSourceContext) -> Result<Self>
    where
        Self: Sized;

    /// Stream ALL events this source can detect
    /// The registry will filter based on enabled events
    async fn stream_events(&mut self, tx: EventSender) -> Result<()>;

    /// Graceful shutdown
    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }

    // ===== Event Creation Methods (subsumes EventFactory) =====

    /// Create a generic event with manual payload
    fn create_event(&self, event_type: &str, payload: crate::JsonValue) -> crate::RawEvent {
        crate::RawEventBuilder::new(Self::SOURCE_NAME, event_type, payload)
            .with_host(self.source_host())
            .with_ingestor_version(self.source_version())
            .build()
    }

    /// Create a filesystem event builder
    fn filesystem(&self) -> sinex_events::FilesystemEventBuilder {
        sinex_events::FilesystemEventBuilder::new(Self::SOURCE_NAME, &self.source_host(), &self.source_version())
    }

    /// Create a terminal event builder
    fn terminal(&self) -> sinex_events::TerminalEventBuilder {
        sinex_events::TerminalEventBuilder::new(Self::SOURCE_NAME, &self.source_host(), &self.source_version())
    }

    /// Create a clipboard event builder
    fn clipboard(&self) -> sinex_events::ClipboardEventBuilder {
        sinex_events::ClipboardEventBuilder::new(Self::SOURCE_NAME, &self.source_host(), &self.source_version())
    }

    /// Create a window manager event builder
    fn window_manager(&self) -> sinex_events::WindowManagerEventBuilder {
        sinex_events::WindowManagerEventBuilder::new(Self::SOURCE_NAME, &self.source_host(), &self.source_version())
    }

    /// Create a system event builder
    fn system(&self) -> sinex_events::SystemEventBuilder {
        sinex_events::SystemEventBuilder::new(Self::SOURCE_NAME, &self.source_host(), &self.source_version())
    }

    // ===== Metadata Helper Methods =====

    /// Get host identifier (sources can override for caching)
    fn source_host(&self) -> String {
        gethostname::gethostname().to_string_lossy().to_string()
    }

    /// Get ingestor version (sources can override for caching)
    fn source_version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }
}

/// Trait for strongly-typed event sources (new architecture)
#[async_trait]
pub trait TypedEventSource: Send + Sync + 'static {
    /// Configuration type for this source
    type Config: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;

    /// Canonical source name
    const SOURCE_NAME: &'static str;

    /// Initialize the source with context containing config and shared resources
    async fn initialize(ctx: crate::EventSourceContext) -> Result<Self>
    where
        Self: Sized;

    /// Stream strongly-typed events
    async fn stream_events(&mut self, tx: crate::TypedEventSender) -> Result<()>;

    /// Graceful shutdown
    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Trait to support both single sources and tuples of sources
pub trait EventSourceProvider: Send + Sync + 'static {
    const SOURCE_NAME: &'static str;
}

// Single source implementation
impl<T: EventSource> EventSourceProvider for T {
    const SOURCE_NAME: &'static str = T::SOURCE_NAME;
}

// Tuple implementations for multiple sources
impl<A: EventSource, B: EventSource> EventSourceProvider for (A, B) {
    const SOURCE_NAME: &'static str = "multiple"; // Or could concatenate
}

impl<A: EventSource, B: EventSource, C: EventSource> EventSourceProvider for (A, B, C) {
    const SOURCE_NAME: &'static str = "multiple";
}

// ===== Registry (from registry.rs) =====

/// Event registry - compile-time discovered events
pub struct EventRegistry {
    /// All event types in the system
    pub event_types: &'static [&'static str],

    /// Mapping from event type to source name
    pub event_to_source: &'static [(&'static str, &'static str)],

    /// Schema generators for each event type
    pub schema_generators: HashMap<&'static str, fn() -> RootSchema>,
}

impl EventRegistry {
    /// Get source name for an event type
    pub fn source_for_event(&self, event_type: &str) -> Option<&'static str> {
        self.event_to_source
            .iter()
            .find(|(e, _)| *e == event_type)
            .map(|(_, s)| *s)
    }

    /// Get all events for a source
    pub fn events_for_source(&self, source: &str) -> Vec<&'static str> {
        self.event_to_source
            .iter()
            .filter(|(_, s)| *s == source)
            .map(|(e, _)| *e)
            .collect()
    }

    /// Get schema for an event type
    pub fn schema_for_event(&self, event_type: &str) -> Option<RootSchema> {
        self.schema_generators.get(event_type).map(|f| f())
    }

    /// Check if an event type exists
    pub fn has_event(&self, event_type: &str) -> bool {
        self.event_types.contains(&event_type)
    }

    /// Get all unique source names
    pub fn all_sources(&self) -> Vec<&'static str> {
        let mut sources: Vec<_> = self.event_to_source.iter().map(|(_, s)| *s).collect();
        sources.sort();
        sources.dedup();
        sources
    }

    /// Alias for has_event for compatibility
    pub fn is_valid_event_type(&self, event_type: &str) -> bool {
        self.has_event(event_type)
    }

    /// Get all event types
    pub fn all_event_types(&self) -> &[&'static str] {
        self.event_types
    }

    /// Alias for all_sources for compatibility
    pub fn get_all_sources(&self) -> Vec<&'static str> {
        self.all_sources()
    }

    /// Alias for all_event_types for compatibility
    pub fn get_all_event_types(&self) -> &[&'static str] {
        self.all_event_types()
    }
}

/// Trait for event crates to register their event types
pub trait EventRegistryProvider {
    fn register_events(registry: &mut EventRegistryBuilder);
}

/// Builder for constructing EventRegistry at runtime
pub struct EventRegistryBuilder {
    event_types: Vec<&'static str>,
    event_to_source: Vec<(&'static str, &'static str)>,
    schema_generators: HashMap<&'static str, fn() -> RootSchema>,
}

impl EventRegistryBuilder {
    pub fn new() -> Self {
        Self {
            event_types: Vec::new(),
            event_to_source: Vec::new(),
            schema_generators: HashMap::new(),
        }
    }
    
    pub fn add_event_type(
        &mut self, 
        event_name: &'static str,
        source_name: &'static str,
        schema_generator: fn() -> RootSchema
    ) {
        if !self.event_types.contains(&event_name) {
            self.event_types.push(event_name);
            self.schema_generators.insert(event_name, schema_generator);
        }
        self.event_to_source.push((event_name, source_name));
    }

    /// Auto-registration method for compatibility
    pub fn with_auto_registration<F>(mut self, register_fn: F) -> Self 
    where
        F: FnOnce(&mut Self),
    {
        register_fn(&mut self);
        self
    }
    
    pub fn build(mut self) -> EventRegistry {
        self.event_types.sort();
        EventRegistry {
            event_types: Box::leak(self.event_types.into_boxed_slice()),
            event_to_source: Box::leak(self.event_to_source.into_boxed_slice()),
            schema_generators: self.schema_generators,
        }
    }
}

impl Default for EventRegistryBuilder {
    fn default() -> Self {
        Self::new()
    }
}




#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_registry_builder() {
        let mut builder = EventRegistryBuilder::new();
        
        builder.add_event_type("test.event", "test.source", || {
            use schemars::schema::*;
            RootSchema {
                meta_schema: None,
                schema: SchemaObject {
                    instance_type: Some(InstanceType::Object.into()),
                    ..Default::default()
                },
                definitions: Default::default(),
            }
        });
        
        let registry = builder.build();
        
        assert_eq!(registry.event_types, &["test.event"]);
        assert_eq!(registry.event_to_source, &[("test.event", "test.source")]);
        assert!(registry.schema_generators.contains_key("test.event"));
        assert!(registry.has_event("test.event"));
        assert_eq!(registry.source_for_event("test.event"), Some("test.source"));
    }


    #[test]
    fn test_event_registry_deduplication() {
        let mut builder = EventRegistryBuilder::new();
        
        // Add the same event type multiple times
        builder.add_event_type("duplicate.event", "source1", || {
            use schemars::schema::*;
            RootSchema::default()
        });
        
        builder.add_event_type("duplicate.event", "source2", || {
            use schemars::schema::*;
            RootSchema::default()
        });
        
        let registry = builder.build();
        
        // Should only appear once in event_types
        assert_eq!(registry.event_types, &["duplicate.event"]);
        
        // But should have multiple source mappings
        assert_eq!(registry.event_to_source.len(), 2);
        assert!(registry.event_to_source.contains(&("duplicate.event", "source1")));
        assert!(registry.event_to_source.contains(&("duplicate.event", "source2")));
        
        // Should have only one schema generator
        assert_eq!(registry.schema_generators.len(), 1);
    }

    #[test]
    fn test_auto_registration_concept() {
        // This test shows how event crates can auto-register themselves
        let mut builder = EventRegistryBuilder::new();
        
        // Simulate what sinex-events-fs would do
        builder.add_event_type("file.created", "fs", || {
            use schemars::schema::*;
            RootSchema {
                meta_schema: None,
                schema: SchemaObject {
                    instance_type: Some(InstanceType::Object.into()),
                    object: Some(Box::new(ObjectValidation {
                        properties: {
                            let mut props = std::collections::BTreeMap::new();
                            props.insert(
                                "path".to_string(),
                                Schema::Object(SchemaObject {
                                    instance_type: Some(InstanceType::String.into()),
                                    ..Default::default()
                                }),
                            );
                            props.insert(
                                "size".to_string(),
                                Schema::Object(SchemaObject {
                                    instance_type: Some(InstanceType::Integer.into()),
                                    ..Default::default()
                                }),
                            );
                            props
                        },
                        required: vec!["path".to_string(), "size".to_string()]
                            .into_iter()
                            .collect(),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
                definitions: Default::default(),
            }
        });
        
        // Simulate what sinex-events-desktop would do
        builder.add_event_type("copied", "clipboard", || {
            use schemars::schema::*;
            RootSchema {
                meta_schema: None,
                schema: SchemaObject {
                    instance_type: Some(InstanceType::Object.into()),
                    object: Some(Box::new(ObjectValidation {
                        properties: {
                            let mut props = std::collections::BTreeMap::new();
                            props.insert(
                                "content_type".to_string(),
                                Schema::Object(SchemaObject {
                                    instance_type: Some(InstanceType::String.into()),
                                    ..Default::default()
                                }),
                            );
                            props
                        },
                        required: vec!["content_type".to_string()]
                            .into_iter()
                            .collect(),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
                definitions: Default::default(),
            }
        });
        
        let registry = builder.build();
        
        // Verify the auto-registered events work correctly
        assert!(registry.has_event("file.created"));
        assert!(registry.has_event("copied"));
        assert_eq!(registry.source_for_event("file.created"), Some("fs"));
        assert_eq!(registry.source_for_event("copied"), Some("clipboard"));
        
        // Verify schemas work
        let file_schema = registry.schema_for_event("file.created").unwrap();
        assert!(file_schema.schema.object.is_some());
        
        let clipboard_schema = registry.schema_for_event("copied").unwrap();
        assert!(clipboard_schema.schema.object.is_some());
    }
}
