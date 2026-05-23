//! Wave B production-path obligation tests for the `fs` source unit.
//!
//! `fs` still runs through a raw `IngestorNode` factory for continuous capture:
//! the imperative watcher owns inotify directly, plus watch-budget planning,
//! dual-shape content/observation materialization, and capture concurrency.
//! The parser-dispatch bridge is now registered separately, so production-path
//! obligations can pin both the raw runtime and the parser bridge while the
//! remaining adapter-backed runtime swap is proven.

use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_fs_descriptor_registered(_ctx: TestContext) -> TestResult<()> {
    use sinex_primitives::parser::SourceUnitId;
    use sinex_source_worker::registry::SourceUnitRegistry;

    let registry = SourceUnitRegistry::from_inventory();
    let id = SourceUnitId::new("fs").unwrap();
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
async fn test_fs_factory_registered(_ctx: TestContext) -> TestResult<()> {
    use sinex_primitives::parser::SourceUnitId;
    use sinex_source_worker::node_factory::find_node_factory;

    let id = SourceUnitId::new("fs").unwrap();
    let factory = find_node_factory(&id);

    assert!(
        factory.is_some(),
        "fs must have a node factory registered (raw IngestorNode path)"
    );

    Ok(())
}
