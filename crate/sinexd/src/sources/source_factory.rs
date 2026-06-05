//! Registry-driven source factory for source dispatch.
//!
//! Replaces the `match source_name` arm in `main.rs` with a compile-time
//! registry. Each source contributes a [`SourceFactoryEntry`] via
//! [`register_source!`] at link time — no match arms.
//!
//! # How to add a new source
//!
//! 1. Implement `SourceDriver` for your source.
//! 2. Call `register_source!(source_id: "your.unit.id", driver: YourSourceDriver)`
//!    in the source's module.
//!
//! The binary automatically discovers and dispatches to your factory.

use futures::future::BoxFuture;
use sinex_primitives::parser::SourceId;
use std::collections::HashMap;
use std::sync::LazyLock;

/// Type-erased factory function for running a source driver.
///
/// Takes the filtered argv and returns a boxed future that drives the source
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

/// Register a source with the parser/factory registries.
///
/// # Examples
///
/// ```rust,ignore
/// register_source!(source_id: "noop", driver: NoopSourceDriver);
/// register_source!(source_id: "weechat.message", parser: WeeChatMessageRecord);
/// register_source!(
///     source_id: "terminal.atuin-history",
///     adapter: SqliteRowAdapter,
///     parser: AtuinHistoryRecord,
/// );
/// register_source!(
///     source_id: "terminal.monitor",
///     emit_at: MonitorPhase::ServiceStart,
///     emit: emit_terminal_monitor,
/// );
/// ```
#[macro_export]
macro_rules! register_source {
    (source_id: $id:expr, driver: $driver:ty $(,)?) => {
        $crate::__submit_registry_entry!(
            $crate::sources::source_factory::SourceFactoryEntry,
            $id,
            |args| {
                Box::pin($crate::sources::source_factory::run_source_driver::<$driver>(args))
            },
        );
    };

    (source_id: $id:expr, parser: $parser:ty $(,)?) => {
        $crate::__submit_registry_entry!(
            $crate::sources::dispatch::ParserRegistryEntry,
            $id,
            || Box::new(<$parser>::default()) as Box<dyn $crate::sources::dispatch::ErasedParser>,
        );
    };

    (
        source_id: $id:expr,
        adapter: $adapter:ty,
        parser: $parser:ty $(,)?
    ) => {
        $crate::register_source!(source_id: $id, parser: $parser);
        $crate::__submit_registry_entry!(
            $crate::sources::source_factory::SourceFactoryEntry,
            $id,
            |args| {
                Box::pin($crate::sources::source_factory::run_adapter_source::<
                    $adapter,
                    $parser,
                >($id, args))
            },
        );
    };

    (
        source_id: $id:expr,
        emit_at: $phase:expr,
        emit: $emit_fn:expr $(,)?
    ) => {
        $crate::__submit_registry_entry!(
            $crate::sources::source_factory::SourceFactoryEntry,
            $id,
            |args| {
                Box::pin($crate::sources::monitor_driver::run_monitor_unit_delegated(
                    $id, $phase, $emit_fn, args,
                ))
            },
        );
    };
}

/// Shared `inventory::submit!` epilogue for `(source_id, factory_fn)`-shaped
/// registry entries.
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

/// Run an adapter-backed source through the standard runtime lifecycle.
///
/// Parallel to `run_source_driver` but constructs `AdapterBackedSource<A, P>`
/// with the source id baked in. Called by `register_source!`
/// generated factories.
pub async fn run_adapter_source<A, P>(
    source_id: &'static str,
    args: Vec<std::ffi::OsString>,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: crate::runtime::parser::InputShapeAdapter
        + Default
        + Send
        + Sync
        + 'static
        + crate::runtime::parser::InputShapeAdapterExt,
    P: sinex_primitives::parser::MaterialParser + Default + Send + Sync + 'static,
    A::Config: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    A::Cursor: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    use crate::runtime::SourceDriverRuntime;
    use crate::runtime::parser::AdapterBackedSource;
    use crate::runtime::runtime_cli::{RuntimeCli, RuntimeCliRunner};
    use clap::Parser;

    let parsed = RuntimeCli::parse_from(args);
    let node = AdapterBackedSource::<A, P>::new(source_id);
    let adapter = SourceDriverRuntime::new(node);
    let mut runner = RuntimeCliRunner::new(adapter);
    runner.run(parsed).await.map_err(std::convert::Into::into)
}

/// Run a source driver through the standard runtime lifecycle.
///
/// Shared implementation used by all `register_source!`-produced
/// factories. Handles CLI parsing, runtime wiring, and shutdown.
///
/// This function is `pub` so the macro can name it; callers should use the
/// macro rather than this function directly.
pub async fn run_source_driver<I>(
    args: Vec<std::ffi::OsString>,
) -> Result<(), Box<dyn std::error::Error>>
where
    I: crate::runtime::SourceDriver + Default + 'static,
{
    use crate::runtime::SourceDriverRuntime;
    use crate::runtime::runtime_cli::{RuntimeCli, RuntimeCliRunner};
    use clap::Parser;

    let parsed = RuntimeCli::parse_from(args);
    let node = SourceDriverRuntime::new(I::default());
    let mut runner = RuntimeCliRunner::new(node);
    runner.run(parsed).await.map_err(std::convert::Into::into)
}
