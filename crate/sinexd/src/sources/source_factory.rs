//! Registry-driven source factory for source dispatch.
//!
//! Replaces the `match source_name` arm in `main.rs` with a compile-time
//! registry. Each source contributes a [`SourceFactoryEntry`] via
//! [`register_source_driver!`] at link time — no match arms.
//!
//! # How to add a new source
//!
//! 1. Implement `SourceDriver` for your source.
//! 2. Call `register_source_driver!("your.unit.id", YourSourceDriver)` in the
//!    source's module.
//!
//! The binary automatically discovers and dispatches to your factory.

use futures::future::BoxFuture;
use sinex_primitives::parser::SourceId;
use std::collections::HashMap;
use std::sync::LazyLock;

/// Type-erased factory function for running a source ingestor.
///
/// Takes the filtered argv and returns a boxed future that drives the ingestor
/// to completion. Using a `fn` pointer (not a closure) allows use inside
/// `inventory::submit!` which requires const-constructible items.
pub type SourceFactoryFn =
    fn(Vec<std::ffi::OsString>) -> BoxFuture<'static, Result<(), Box<dyn std::error::Error>>>;

/// A single entry in the compile-time source factory inventory.
pub struct SourceFactoryEntry {
    pub source_id: &'static str,
    pub factory_fn: SourceFactoryFn,
}

inventory::collect!(SourceFactoryEntry);

/// Global registry of source factories keyed by source id.
///
/// Populated at startup from the `inventory`-collected [`SourceFactoryEntry`]
/// items. First registration wins (consistent with link order).
static SOURCE_FACTORY_REGISTRY: LazyLock<HashMap<&'static str, SourceFactoryFn>> =
    LazyLock::new(|| {
        let mut map: HashMap<&'static str, SourceFactoryFn> = HashMap::new();
        for entry in inventory::iter::<SourceFactoryEntry>() {
            map.entry(entry.source_id).or_insert(entry.factory_fn);
        }
        map
    });

/// Look up a source factory function by source id.
#[must_use]
pub fn find_source_factory(source_id: &SourceId) -> Option<SourceFactoryFn> {
    SOURCE_FACTORY_REGISTRY.get(source_id.as_str()).copied()
}

/// List all registered source ids that have source factories.
#[must_use]
pub fn registered_source_factory_ids() -> Vec<SourceId> {
    let mut ids: Vec<SourceId> = SOURCE_FACTORY_REGISTRY
        .keys()
        .copied()
        .map(SourceId::from_static)
        .collect();
    ids.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));
    ids
}

/// Register a source's [`SourceDriver`] with the source factory registry.
///
/// # Example
///
/// ```rust,ignore
/// register_source_driver!("noop", NoopSourceDriver);
/// ```
///
/// The macro creates a `SourceFactoryEntry` with a `fn` pointer wrapper around
/// `run_ingestor::<I>(args)` and submits it to `inventory`.
#[macro_export]
macro_rules! register_source_driver {
    ($id:expr, $node_type:ty) => {
        $crate::__submit_registry_entry!(
            $crate::sources::source_factory::SourceFactoryEntry,
            $id,
            |args| {
                Box::pin($crate::sources::source_factory::run_ingestor::<$node_type>(
                    args,
                ))
            },
        );
    };
}

/// Shared `inventory::submit!` epilogue for `(source_id, factory_fn)`-shaped
/// registry entries. Used by `register_source_driver!`, `register_parser!`,
/// `register_adapter_ingestor!`, and `register_monitor_unit!` — all four submit
/// the same field shape to inventory, differing only in entry type and closure body.
///
/// Direct use is discouraged; reach for the named `register_*` macros instead.
#[doc(hidden)]
#[macro_export]
macro_rules! __submit_registry_entry {
    ($entry_path:path, $id:expr, $factory_fn:expr $(,)?) => {
        ::inventory::submit! {
            $entry_path {
                source_id: $id,
                factory_fn: $factory_fn,
            }
        }
    };
}

// `register_monitor_unit!` is defined in monitor_node.rs and exported here
// for documentation grouping. The macro itself lives in crate::sources::monitor_node
// because it needs pub access to that module's types.
// Re-export is not possible for macros with #[macro_export] — they live at
// the crate root automatically. Users call `crate::register_monitor_unit!`.

/// Register an adapter-backed ingestor in one shot.
///
/// This macro is the primary Wave-B authoring surface. It combines
/// `register_parser!` and `register_source_driver!` into a single call:
///
/// ```rust,ignore
/// register_adapter_ingestor!(
///     source_id: "terminal.atuin-history",
///     adapter:        SqliteRowAdapter,
///     parser:         AtuinHistoryRecord,
/// );
/// ```
///
/// The macro:
/// 1. Registers `parser` in the `ParserRegistryEntry` inventory under
///    `source_id` so the replay dispatch can reach it.
/// 2. Registers an `AdapterBackedIngestor<adapter, parser>` in the
///    `SourceFactoryEntry` inventory so `sinexd scan-source --source
///    <source_id>` can start it.
///
/// Both `adapter` and `parser` must implement `Default`.
/// Parser baseline config is supplied via `MaterialParser::baseline_adapter_config()`.
///
/// # Config shape
///
/// `AdapterBackedIngestor` deserializes the node JSON config into
/// `adapter::Config`. Place all adapter-specific fields (e.g. `path`,
/// `query`, `table`) at the top level of the source's config JSON.
/// The adapter type's `Config` must implement `serde::Deserialize` and
/// `Default`.
#[macro_export]
macro_rules! register_adapter_ingestor {
    (
        source_id: $id:expr,
        adapter: $adapter:ty,
        parser: $parser:ty $(,)?
    ) => {
        // 1. Register the parser in the dispatch registry (replay path).
        $crate::register_parser!($id, $parser);

        // 2. Register the source factory (continuous ingestion path).
        $crate::__submit_registry_entry!(
            $crate::sources::source_factory::SourceFactoryEntry,
            $id,
            |args| {
                Box::pin($crate::sources::source_factory::run_adapter_ingestor::<
                    $adapter,
                    $parser,
                >($id, args))
            },
        );
    };
}

/// Run an adapter-backed ingestor through the standard SDK lifecycle.
///
/// Parallel to `run_ingestor` but constructs `AdapterBackedIngestor<A, P>`
/// with the source id baked in. Called by `register_adapter_ingestor!`
/// generated factories.
pub async fn run_adapter_ingestor<A, P>(
    source_id: &'static str,
    args: Vec<std::ffi::OsString>,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: crate::node_sdk::parser::InputShapeAdapter
        + Default
        + Send
        + Sync
        + 'static
        + crate::node_sdk::parser::InputShapeAdapterExt,
    P: sinex_primitives::parser::MaterialParser + Default + Send + Sync + 'static,
    A::Config: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    A::Cursor: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    use crate::node_sdk::SourceDriverRuntime;
    use crate::node_sdk::node_cli::{NodeCli, NodeCliRunner};
    use crate::node_sdk::parser::AdapterBackedIngestor;
    use clap::Parser;

    let parsed = NodeCli::parse_from(args);
    let node = AdapterBackedIngestor::<A, P>::new(source_id);
    let adapter = SourceDriverRuntime::new(node);
    let mut runner = NodeCliRunner::new(adapter);
    runner.run(parsed).await.map_err(std::convert::Into::into)
}

/// Run a source ingestor through the standard SDK lifecycle.
///
/// Shared implementation used by all `register_source_driver!`-produced
/// factories. Handles CLI parsing, SDK wiring, and shutdown.
///
/// This function is `pub` so the macro can name it; callers should use the
/// macro rather than this function directly.
pub async fn run_ingestor<I>(
    args: Vec<std::ffi::OsString>,
) -> Result<(), Box<dyn std::error::Error>>
where
    I: crate::node_sdk::SourceDriver + Default + 'static,
{
    use crate::node_sdk::SourceDriverRuntime;
    use crate::node_sdk::node_cli::{NodeCli, NodeCliRunner};
    use clap::Parser;

    let parsed = NodeCli::parse_from(args);
    let node = SourceDriverRuntime::new(I::default());
    let mut runner = NodeCliRunner::new(node);
    runner.run(parsed).await.map_err(std::convert::Into::into)
}
