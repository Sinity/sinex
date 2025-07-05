pub mod filesystem;

// Re-export filesystem event types
pub use filesystem::{
    DirCreated, DirCreatedPayload, DirDeleted, DirDeletedPayload, FileCreated, FileCreatedPayload,
    FileDeleted, FileDeletedPayload, FileModified, FileModifiedPayload, FileMoved, FileMovedPayload,
    FilesystemMonitor, FilesystemWatcher, FilesystemConfig,
};

/// Register all filesystem event types with the EventRegistry
/// This is an example of how event crates can auto-register their types
pub fn register_events(builder: &mut sinex_core::unified_collector::EventRegistryBuilder) {
    use sinex_core::EventType;
    
    // File events
    builder.add_event_type(
        FileCreated::EVENT_NAME,
        FileCreated::SOURCE_NAME,
        || {
            let gen = schemars::gen::SchemaGenerator::default();
            gen.into_root_schema_for::<FileCreatedPayload>()
        }
    );
    
    builder.add_event_type(
        FileModified::EVENT_NAME,
        FileModified::SOURCE_NAME,
        || {
            let gen = schemars::gen::SchemaGenerator::default();
            gen.into_root_schema_for::<FileModifiedPayload>()
        }
    );
    
    builder.add_event_type(
        FileDeleted::EVENT_NAME,
        FileDeleted::SOURCE_NAME,
        || {
            let gen = schemars::gen::SchemaGenerator::default();
            gen.into_root_schema_for::<FileDeletedPayload>()
        }
    );
    
    builder.add_event_type(
        FileMoved::EVENT_NAME,
        FileMoved::SOURCE_NAME,
        || {
            let gen = schemars::gen::SchemaGenerator::default();
            gen.into_root_schema_for::<FileMovedPayload>()
        }
    );
    
    // Directory events
    builder.add_event_type(
        DirCreated::EVENT_NAME,
        DirCreated::SOURCE_NAME,
        || {
            let gen = schemars::gen::SchemaGenerator::default();
            gen.into_root_schema_for::<DirCreatedPayload>()
        }
    );
    
    builder.add_event_type(
        DirDeleted::EVENT_NAME,
        DirDeleted::SOURCE_NAME,
        || {
            let gen = schemars::gen::SchemaGenerator::default();
            gen.into_root_schema_for::<DirDeletedPayload>()
        }
    );
}
