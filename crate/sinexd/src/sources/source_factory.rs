//! Registry-driven source factory for source dispatch.
//!
//! Replaces the `match source_name` arm in `main.rs` with a compile-time
//! registry. Each source contributes a [`SourceFactoryEntry`] via
//! [`register_source!`] at link time. Sources with multiple runtimes select
//! their factory by runtime-binding subject.
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
use std::sync::Arc;
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
    pub mode_subjects: &'static [&'static str],
    pub default: bool,
    pub factory_fn: SourceFactoryFn,
}

inventory::collect!(SourceFactoryEntry);

struct SourceFactoryRegistry {
    defaults: HashMap<&'static str, SourceFactoryFn>,
    modes: HashMap<&'static str, HashMap<&'static str, SourceFactoryFn>>,
}

impl SourceFactoryRegistry {
    fn from_entries<'a>(entries: impl IntoIterator<Item = &'a SourceFactoryEntry>) -> Self {
        let mut registry = Self {
            defaults: HashMap::new(),
            modes: HashMap::new(),
        };
        for entry in entries {
            if entry.default {
                assert!(
                    registry
                        .defaults
                        .insert(entry.source_id, entry.factory_fn)
                        .is_none(),
                    "duplicate default source factory for {}",
                    entry.source_id
                );
            }
            for &mode in entry.mode_subjects {
                assert!(
                    registry
                        .modes
                        .entry(entry.source_id)
                        .or_default()
                        .insert(mode, entry.factory_fn)
                        .is_none(),
                    "duplicate source factory for {} mode {}",
                    entry.source_id,
                    mode
                );
            }
        }
        registry
    }

    fn find(&self, source_id: &str, mode: Option<&str>) -> Option<SourceFactoryFn> {
        match mode {
            Some(mode) => self.modes.get(source_id).map_or_else(
                || self.defaults.get(source_id).copied(),
                |modes| modes.get(mode).copied(),
            ),
            None => self.defaults.get(source_id).copied(),
        }
    }
}

static SOURCE_FACTORY_REGISTRY: LazyLock<SourceFactoryRegistry> =
    LazyLock::new(|| SourceFactoryRegistry::from_entries(inventory::iter::<SourceFactoryEntry>()));

/// Look up a source factory function by source id.
#[must_use]
pub fn find_source_factory(source_id: &SourceId) -> Option<SourceFactoryFn> {
    find_source_factory_for_mode(source_id, None)
}

/// Select the runtime for a source and optional runtime-binding subject.
/// A source with explicit mode registrations never falls back to its default
/// for an unregistered mode.
#[must_use]
pub fn find_source_factory_for_mode(
    source_id: &SourceId,
    mode_subject: Option<&str>,
) -> Option<SourceFactoryFn> {
    SOURCE_FACTORY_REGISTRY.find(source_id.as_str(), mode_subject)
}

/// List all registered source ids that have source factories.
#[must_use]
pub fn registered_source_factory_ids() -> Vec<SourceId> {
    let mut ids: Vec<SourceId> = SOURCE_FACTORY_REGISTRY
        .defaults
        .keys()
        .chain(SOURCE_FACTORY_REGISTRY.modes.keys())
        .copied()
        .map(SourceId::from_static)
        .collect();
    ids.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));
    ids.dedup();
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
    (source_id: $id:expr, modes: [$($mode:expr),+ $(,)?], default: $default:expr, driver: $driver:ty $(,)?) => {
        $crate::__submit_mode_source_factory!(
            $id, &[$($mode),+], $default,
            |args| Box::pin($crate::sources::source_factory::run_source_driver::<$driver>(args)),
        );
    };

    (source_id: $id:expr, modes: [$($mode:expr),+ $(,)?], default: $default:expr, adapter: $adapter:ty, parser: $parser:ty $(,)?) => {
        $crate::register_source!(source_id: $id, parser: $parser);
        $crate::__submit_mode_source_factory!(
            $id, &[$($mode),+], $default,
            |args| Box::pin($crate::sources::source_factory::run_adapter_source::<$adapter, $parser>($id, args)),
        );
    };

    (source_id: $id:expr, driver: $driver:ty $(,)?) => {
        $crate::__submit_mode_source_factory!(
            $id,
            &[],
            true,
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
        $crate::__submit_mode_source_factory!(
            $id,
            &[],
            true,
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
        $crate::__submit_mode_source_factory!(
            $id,
            &[],
            true,
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

#[doc(hidden)]
#[macro_export]
macro_rules! __submit_mode_source_factory {
    ($id:expr, $modes:expr, $default:expr, $factory_fn:expr $(,)?) => {
        ::inventory::submit! {
            $crate::sources::source_factory::SourceFactoryEntry {
                source_id: $id,
                mode_subjects: $modes,
                default: $default,
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
    use crate::runtime::runtime_cli::{RuntimeCli, RuntimeCliRunner};
    use clap::Parser;

    let parsed = RuntimeCli::parse_from(args);
    let adapter = adapter_source_runtime::<A, P>(source_id);
    let replay_source_id = source_id;
    let mut runner = RuntimeCliRunner::new_with_factory(
        adapter,
        Arc::new(move || adapter_source_runtime::<A, P>(replay_source_id)),
    );
    runner.run(parsed).await.map_err(std::convert::Into::into)
}

fn adapter_source_runtime<A, P>(
    source_id: &'static str,
) -> crate::runtime::SourceDriverRuntime<crate::runtime::parser::AdapterBackedSource<A, P>>
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
    crate::runtime::SourceDriverRuntime::new(
        crate::runtime::parser::AdapterBackedSource::<A, P>::new(source_id),
    )
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
    let source_runtime = SourceDriverRuntime::new(I::default());
    let mut runner = RuntimeCliRunner::new(source_runtime);
    runner.run(parsed).await.map_err(std::convert::Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::RuntimeModule;
    use crate::runtime::parser::SqliteRowAdapter;
    use crate::sources::source_contracts::desktop::activitywatch::ActivityWatchParser;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn adapter_source_runtime_preserves_registered_source_id() -> TestResult<()> {
        let runtime = adapter_source_runtime::<SqliteRowAdapter, ActivityWatchParser>(
            "desktop.activitywatch",
        );

        assert_eq!(runtime.module_name(), "desktop.activitywatch");
        Ok(())
    }

    #[test]
    fn media_factories_follow_mode_subject_independent_of_link_order() {
        let entries: Vec<_> = inventory::iter::<SourceFactoryEntry>()
            .filter(|entry| {
                entry.source_id == "media.audio-transcript" || entry.source_id == "media.screen-ocr"
            })
            .collect();
        let forward = SourceFactoryRegistry::from_entries(entries.iter().copied());
        let reverse = SourceFactoryRegistry::from_entries(entries.iter().rev().copied());

        for (source_id, staged_modes, live_modes) in [
            (
                "media.audio-transcript",
                &[
                    "source:media.audio-transcript",
                    "source:media.audio-transcript.audio-bundle-staged",
                ][..],
                &[
                    "source:media.audio-transcript.on-demand-session",
                    "source:media.audio-transcript.live-session",
                ][..],
            ),
            (
                "media.screen-ocr",
                &[
                    "source:media.screen-ocr",
                    "source:media.screen-ocr.screenshot-ocr-staged",
                    "source:media.screen-ocr.video-staged",
                ][..],
                &[
                    "source:media.screen-ocr.on-demand-region",
                    "source:media.screen-ocr.live-session",
                ][..],
            ),
        ] {
            let registrations: Vec<_> = entries
                .iter()
                .filter(|entry| entry.source_id == source_id)
                .collect();
            assert_eq!(
                registrations.len(),
                2,
                "{source_id} needs staged and live factories"
            );
            let staged = registrations
                .iter()
                .find(|entry| entry.default)
                .expect("staged default factory");
            let live = registrations
                .iter()
                .find(|entry| !entry.default)
                .expect("live factory");
            assert_eq!(staged.mode_subjects, staged_modes);
            assert_eq!(live.mode_subjects, live_modes);
            assert!(!std::ptr::fn_addr_eq(staged.factory_fn, live.factory_fn));

            for registry in [&forward, &reverse] {
                assert!(std::ptr::fn_addr_eq(
                    registry.find(source_id, None).expect("staged default"),
                    staged.factory_fn
                ));
                for &mode in staged_modes {
                    assert!(std::ptr::fn_addr_eq(
                        registry.find(source_id, Some(mode)).expect("staged mode"),
                        staged.factory_fn
                    ));
                }
                for &mode in live_modes {
                    assert!(std::ptr::fn_addr_eq(
                        registry.find(source_id, Some(mode)).expect("live mode"),
                        live.factory_fn
                    ));
                }
                assert!(registry.find(source_id, Some("source:unknown")).is_none());
            }
            let id = SourceId::from_static(source_id);
            for &mode in live_modes {
                assert!(std::ptr::fn_addr_eq(
                    find_source_factory_for_mode(&id, Some(mode)).expect("linked live mode"),
                    live.factory_fn
                ));
            }
        }
    }

    #[test]
    #[should_panic(expected = "duplicate default source factory")]
    fn duplicate_default_factory_is_rejected() {
        let entry = inventory::iter::<SourceFactoryEntry>()
            .find(|entry| entry.source_id == "noop")
            .expect("noop factory registered");
        SourceFactoryRegistry::from_entries([entry, entry]);
    }
}
