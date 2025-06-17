use sinex_core::EventSourceContext;
use serde_json::json;

// Removed many mock-in/assert-mock-out tests that just verified config assignment works

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