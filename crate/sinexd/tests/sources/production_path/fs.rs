//! Wave B production-path obligation tests for the `fs` source.
//!
//! `fs` runs through the SDK's content-materializing file-drop adapter for
//! continuous capture. These tests pin the production registration surface so
//! the source cannot silently drift back to a raw source factory or lose its
//! replay/parser bridge.

use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_fs_descriptor_registered() -> TestResult<()> {
    use sinex_primitives::parser::SourceId;
    use sinexd::sources::registry::SourceContractRegistry;

    let registry = SourceContractRegistry::from_inventory();
    let id = SourceId::new("fs").unwrap();
    let descriptor = registry.find(&id);

    assert!(
        descriptor.is_some(),
        "fs descriptor must be registered in inventory"
    );

    let d = descriptor.unwrap();
    assert_eq!(d.id, "fs");
    assert_eq!(d.namespace, "filesystem");

    let event_types: Vec<&str> = d.event_types.iter().map(|(_, t)| *t).collect();
    for et in &[
        "file.created",
        "file.modified",
        "file.deleted",
        "file.moved",
    ] {
        assert!(
            event_types.contains(et),
            "fs must declare {et} in event_types; got {event_types:?}"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_fs_adapter_factory_and_parser_registered() -> TestResult<()> {
    use sinex_primitives::parser::SourceId;
    use sinexd::sources::dispatch::find_parser_factory;
    use sinexd::sources::source_factory::find_source_factory;

    let id = SourceId::new("fs").unwrap();
    let factory = find_source_factory(&id);
    let parser = find_parser_factory(&id);

    assert!(
        factory.is_some(),
        "fs must have an adapter-backed source factory registered"
    );
    assert!(
        parser.is_some(),
        "fs must have a parser factory registered for replay dispatch"
    );

    Ok(())
}

#[sinex_test]
async fn test_fs_binding_uses_content_drop_adapter() -> TestResult<()> {
    let binding = sinex_primitives::proof::source_runtime_bindings()
        .find(|binding| binding.source_id == "fs")
        .expect("fs source binding must be registered");

    assert_eq!(binding.adapter, "FileContentDropAdapter");
    assert_eq!(binding.material_policy, "inotify_anchor");
    assert_eq!(binding.checkpoint_policy, "append_stream");
    assert_eq!(
        binding.runtime_shape,
        sinex_primitives::proof::RuntimeShape::Continuous
    );
    assert_eq!(
        binding.checkpoint_family,
        sinex_primitives::proof::CheckpointFamily::AppendStream
    );

    Ok(())
}

#[sinex_test]
async fn test_fs_source_config_deserializes_as_file_content_drop() -> TestResult<()> {
    use camino::Utf8PathBuf;
    use sinexd::runtime::parser::{AdapterSourceConfig, FileContentDropConfig};

    let runtime_config: AdapterSourceConfig = serde_json::from_value(serde_json::json!({
        "watch_paths": ["/realm/project/sinex", "/realm/data/captures"],
        "max_depth": 10,
        "follow_symlinks": false,
        "max_capture_bytes": 10485760,
        "max_watches": 8192,
        "ignored_directory_names": [".git", ".direnv", "target"],
    }))?;

    let adapter_config: FileContentDropConfig = serde_json::from_value(runtime_config.adapter)?;

    assert_eq!(
        adapter_config.file_drop.watch_paths,
        vec![
            Utf8PathBuf::from("/realm/project/sinex"),
            Utf8PathBuf::from("/realm/data/captures"),
        ]
    );
    assert_eq!(adapter_config.file_drop.max_depth, Some(10));
    assert_eq!(adapter_config.file_drop.max_watches.get(), 8192);
    assert_eq!(
        adapter_config.file_drop.ignored_directory_names,
        vec![
            ".git".to_string(),
            ".direnv".to_string(),
            "target".to_string()
        ]
    );
    assert_eq!(adapter_config.max_capture_bytes, 10_485_760);

    Ok(())
}
