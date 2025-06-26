use crate::common::prelude::*;

// Removed many mock-in/assert-mock-out tests that just verified config assignment works

#[sinex_test]
async fn test_event_source_context_config_merging(_ctx: TestContext) -> TestResult {
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
    pretty_assertions::assert_eq!(context1.config["paths"][0], "/base");
    pretty_assertions::assert_eq!(context2.config["paths"][0], "/override");
    pretty_assertions::assert_eq!(context1.config["settings"]["timeout"], 1000);
    pretty_assertions::assert_eq!(context2.config["settings"]["timeout"], 2000);
    Ok(())
}

#[sinex_test]
async fn test_event_source_context_large_config(_ctx: TestContext) -> TestResult {
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
    pretty_assertions::assert_eq!(context.config, config);
    
    // Verify large arrays are handled correctly
    pretty_assertions::assert_eq!(context.config["filesystem"]["watch_paths"].as_array().unwrap().len(), 100);
    pretty_assertions::assert_eq!(context.config["filesystem"]["watch_paths"][50], "/path/to/directory/50");
    pretty_assertions::assert_eq!(context.config["filesystem"]["ignore_patterns"].as_array().unwrap().len(), 7);
    Ok(())
}

#[sinex_test]
async fn test_event_source_context_unicode_config(_ctx: TestContext) -> TestResult {
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
    pretty_assertions::assert_eq!(context.config, config);
    
    // Verify unicode paths are preserved
    pretty_assertions::assert_eq!(context.config["paths"][0], "/home/用户/文档");
    pretty_assertions::assert_eq!(context.config["paths"][1], "/домашний/документы");
    pretty_assertions::assert_eq!(context.config["message"], "Testing unicode: 🚀 🎉 ✨");
    pretty_assertions::assert_eq!(context.config["emoji_config"]["success"], "✅");
    Ok(())
}