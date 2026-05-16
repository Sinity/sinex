//! Wave B production-path obligation tests for the `fs` source unit.
//!
//! `fs` is registered as a raw `IngestorNode` (no `InputShapeAdapter`) — see
//! `crate::sources::fs` and the orchestrator's option (c) decision recorded in
//! its `mod.rs` docstring. The legacy `sinex-fs-ingestor` watcher owns inotify
//! directly, plus a watch-budget planner, dual-shape content/observation
//! materialization, and an `AcquisitionManager` + `FS_MAX_CONCURRENT_CAPTURES`
//! semaphore around content staging. None of that slots into the harness's
//! parser-dispatch obligations, which assume an adapter-backed
//! `MaterialParser` reachable through dispatch.
//!
//! Until the follow-up "Extend FileDropAdapter / introduce FsWatcherAdapter"
//! issue lands, the only obligations exercisable here are the structural ones
//! that match `system.monitor`'s situation: descriptor registration and node-
//! factory registration. The behavior obligations (initial_ingestion, replay,
//! drain, isolation, privacy) require the adapter-backed flow.

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
