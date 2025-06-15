use sinex_core::EventSourceContext;
use serde_json::{json, Value};
use std::collections::HashMap;

#[test]
fn test_event_source_context_creation() {
    let config = json!({
        "enabled": true,
        "watch_paths": ["/home/user", "/etc"],
        "poll_interval": 1000
    });
    
    let context = EventSourceContext::new(config.clone());
    assert_eq!(context.config, config);
}

#[test]
fn test_event_source_context_empty_config() {
    let config = json!({});
    let context = EventSourceContext::new(config.clone());
    assert_eq!(context.config, config);
}

#[test]
fn test_event_source_context_complex_config() {
    let config = json!({
        "filesystem": {
            "watch_paths": ["/home", "/var/log"],
            "recursive": true,
            "ignore_patterns": ["*.tmp", "*.log"],
            "debounce_ms": 500
        },
        "terminal": {
            "capture_commands": true,
            "capture_output": false,
            "history_limit": 1000
        },
        "window_manager": {
            "track_focus": true,
            "track_workspace": true,
            "poll_interval": 100
        }
    });
    
    let context = EventSourceContext::new(config.clone());
    assert_eq!(context.config, config);
    
    // Verify nested access works
    assert_eq!(context.config["filesystem"]["recursive"], true);
    assert_eq!(context.config["terminal"]["history_limit"], 1000);
    assert_eq!(context.config["window_manager"]["poll_interval"], 100);
}

#[test]
fn test_event_source_context_config_access() {
    let config = json!({
        "enabled": true,
        "paths": ["/test1", "/test2"],
        "settings": {
            "recursive": false,
            "timeout": 5000
        }
    });
    
    let context = EventSourceContext::new(config);
    
    // Test direct field access
    assert_eq!(context.config["enabled"], true);
    assert_eq!(context.config["paths"][0], "/test1");
    assert_eq!(context.config["paths"][1], "/test2");
    assert_eq!(context.config["settings"]["recursive"], false);
    assert_eq!(context.config["settings"]["timeout"], 5000);
}

#[test]
fn test_event_source_context_with_null_values() {
    let config = json!({
        "enabled": true,
        "optional_field": null,
        "paths": ["/test", null, "/test2"],
        "settings": {
            "value": null
        }
    });
    
    let context = EventSourceContext::new(config.clone());
    assert_eq!(context.config, config);
    assert!(context.config["optional_field"].is_null());
    assert!(context.config["paths"][1].is_null());
    assert!(context.config["settings"]["value"].is_null());
}

#[test] 
fn test_event_source_context_config_types() {
    let config = json!({
        "boolean_val": true,
        "number_val": 42,
        "float_val": 3.14,
        "string_val": "test string",
        "array_val": [1, 2, 3],
        "object_val": {
            "nested": "value"
        }
    });
    
    let context = EventSourceContext::new(config);
    
    assert!(context.config["boolean_val"].is_boolean());
    assert!(context.config["number_val"].is_number());
    assert!(context.config["float_val"].is_number());
    assert!(context.config["string_val"].is_string());
    assert!(context.config["array_val"].is_array());
    assert!(context.config["object_val"].is_object());
    
    assert_eq!(context.config["boolean_val"], true);
    assert_eq!(context.config["number_val"], 42);
    assert_eq!(context.config["float_val"], 3.14);
    assert_eq!(context.config["string_val"], "test string");
    assert_eq!(context.config["array_val"].as_array().unwrap().len(), 3);
    assert_eq!(context.config["object_val"]["nested"], "value");
}

#[test]
fn test_event_source_context_clone() {
    let config = json!({
        "test": "value",
        "nested": {
            "array": [1, 2, 3]
        }
    });
    
    let context1 = EventSourceContext::new(config.clone());
    let context2 = context1.clone();
    
    assert_eq!(context1.config, context2.config);
    assert_eq!(context1.config["test"], context2.config["test"]);
    assert_eq!(context1.config["nested"]["array"], context2.config["nested"]["array"]);
}

#[test]
fn test_event_source_context_serialization() {
    let config = json!({
        "filesystem": {
            "paths": ["/home", "/etc"],
            "recursive": true
        },
        "enabled": true
    });
    
    let context = EventSourceContext::new(config.clone());
    
    // Test that we can serialize and deserialize through JSON
    let serialized = serde_json::to_string(&context.config).unwrap();
    let deserialized: Value = serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized, config);
    assert_eq!(deserialized["filesystem"]["paths"][0], "/home");
    assert_eq!(deserialized["enabled"], true);
}

#[test]
fn test_event_source_context_config_merging() {
    // Test scenario where context might be used to merge configs
    let base_config = json!({
        "enabled": true,
        "paths": ["/base"],
        "settings": {
            "timeout": 1000,
            "retry": 3
        }
    });
    
    let override_config = json!({
        "paths": ["/override"],
        "settings": {
            "timeout": 2000
        }
    });
    
    let context1 = EventSourceContext::new(base_config);
    let context2 = EventSourceContext::new(override_config);
    
    // Verify contexts maintain their separate configs
    assert_eq!(context1.config["paths"][0], "/base");
    assert_eq!(context2.config["paths"][0], "/override");
    assert_eq!(context1.config["settings"]["timeout"], 1000);
    assert_eq!(context2.config["settings"]["timeout"], 2000);
}

#[test]
fn test_event_source_context_large_config() {
    // Test with a large, realistic configuration
    let mut large_paths = Vec::new();
    for i in 0..100 {
        large_paths.push(format!("/path/to/directory/{}", i));
    }
    
    let config = json!({
        "filesystem": {
            "watch_paths": large_paths,
            "recursive": true,
            "ignore_patterns": [
                "*.tmp", "*.log", "*.cache", "*.lock",
                "node_modules/*", ".git/*", "target/*"
            ],
            "debounce_ms": 250,
            "buffer_size": 10000
        },
        "performance": {
            "max_events_per_second": 1000,
            "batch_size": 100,
            "flush_interval": 5000
        }
    });
    
    let context = EventSourceContext::new(config.clone());
    assert_eq!(context.config, config);
    
    // Verify large arrays are handled correctly
    assert_eq!(context.config["filesystem"]["watch_paths"].as_array().unwrap().len(), 100);
    assert_eq!(context.config["filesystem"]["watch_paths"][50], "/path/to/directory/50");
    assert_eq!(context.config["filesystem"]["ignore_patterns"].as_array().unwrap().len(), 7);
}

#[test]
fn test_event_source_context_unicode_config() {
    let config = json!({
        "paths": [
            "/home/用户/文档",
            "/домашний/документы", 
            "/माध्यम/फ़ाइलें",
            "/ホーム/ドキュメント"
        ],
        "message": "Testing unicode: 🚀 🎉 ✨",
        "emoji_config": {
            "success": "✅",
            "warning": "⚠️", 
            "error": "❌"
        }
    });
    
    let context = EventSourceContext::new(config.clone());
    assert_eq!(context.config, config);
    
    // Verify unicode paths are preserved
    assert_eq!(context.config["paths"][0], "/home/用户/文档");
    assert_eq!(context.config["paths"][1], "/домашний/документы");
    assert_eq!(context.config["message"], "Testing unicode: 🚀 🎉 ✨");
    assert_eq!(context.config["emoji_config"]["success"], "✅");
}