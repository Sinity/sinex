/// Macro-based event registry for simplified event type registration
/// 
/// This replaces the verbose auto-registration pattern with a single macro
/// that generates the same functionality with much less boilerplate.
///
/// Usage:
/// ```rust
/// register_events! {
///     "file.created" => (fs, FileCreatedPayload),
///     "copied" => (clipboard, ClipboardChangedPayload),
///     "command.executed" => (terminal.kitty, KittyCommandExecutedPayload),
/// }
/// ```

#[macro_export]
macro_rules! register_events {
    ($($event_name:literal => ($source_name:ident $(. $source_segment:ident)*, $payload_type:ty)),* $(,)?) => {
        /// Register all events with the EventRegistry builder
        pub fn register_events(builder: &mut $crate::unified_collector::EventRegistryBuilder) {
            $(
                builder.add_event_type(
                    $event_name,
                    register_events!(@source_name $source_name $(. $source_segment)*),
                    || {
                        let gen = schemars::gen::SchemaGenerator::default();
                        gen.into_root_schema_for::<$payload_type>()
                    }
                );
            )*
        }
    };
    
    // Helper macro to convert dotted source names to strings
    (@source_name $first:ident $(. $rest:ident)*) => {
        concat!(stringify!($first) $(, ".", stringify!($rest))*)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Serialize, Deserialize};
    use schemars::JsonSchema;
    
    #[derive(Serialize, Deserialize, JsonSchema)]
    struct TestPayload {
        message: String,
    }
    
    #[test]
    fn test_macro_basic() {
        register_events! {
            "test.event" => (test_source, TestPayload),
        }
        
        let mut builder = sinex_core::unified_collector::EventRegistryBuilder::new();
        register_events(&mut builder);
        let registry = builder.build();
        // Registry should contain our test event
        // This is a basic compilation test
    }
    
    #[test] 
    fn test_macro_dotted_source() {
        register_events! {
            "test.event" => (terminal.kitty, TestPayload),
        }
        
        let mut builder = sinex_core::unified_collector::EventRegistryBuilder::new();
        register_events(&mut builder);
        let registry = builder.build();
        // Should handle dotted source names correctly
    }
}