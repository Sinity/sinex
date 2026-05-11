//! Registry-driven node factory for source-unit dispatch.
//!
//! Replaces the `match source_unit_name` arm in `main.rs` with a compile-time
//! registry. Each source unit contributes a [`NodeFactoryEntry`] via
//! [`register_node_factory!`] at link time — no match arms.
//!
//! # How to add a new source unit
//!
//! 1. Implement `IngestorNode` for your source unit.
//! 2. Call `register_node_factory!("your.unit.id", YourSourceUnit)` in the
//!    source unit's module.
//!
//! The binary automatically discovers and dispatches to your factory.

use futures::future::BoxFuture;
use std::sync::LazyLock;
use std::collections::HashMap;

/// Type-erased factory function for running a source-unit ingestor.
///
/// Takes the filtered argv and returns a boxed future that drives the ingestor
/// to completion. Using a `fn` pointer (not a closure) allows use inside
/// `inventory::submit!` which requires const-constructible items.
pub type NodeFactoryFn =
    fn(Vec<std::ffi::OsString>) -> BoxFuture<'static, Result<(), Box<dyn std::error::Error>>>;

/// A single entry in the compile-time node factory inventory.
pub struct NodeFactoryEntry {
    pub source_unit_id: &'static str,
    pub factory_fn: NodeFactoryFn,
}

inventory::collect!(NodeFactoryEntry);

/// Global registry of node factories keyed by source-unit id.
///
/// Populated at startup from the `inventory`-collected [`NodeFactoryEntry`]
/// items. First registration wins (consistent with link order).
static NODE_FACTORY_REGISTRY: LazyLock<HashMap<&'static str, NodeFactoryFn>> =
    LazyLock::new(|| {
        let mut map: HashMap<&'static str, NodeFactoryFn> = HashMap::new();
        for entry in inventory::iter::<NodeFactoryEntry>() {
            map.entry(entry.source_unit_id).or_insert(entry.factory_fn);
        }
        map
    });

/// Look up a node factory function by source-unit id.
#[must_use]
pub fn find_node_factory(source_unit_id: &str) -> Option<NodeFactoryFn> {
    NODE_FACTORY_REGISTRY.get(source_unit_id).copied()
}

/// List all registered source-unit ids that have node factories.
#[must_use]
pub fn registered_node_factory_ids() -> Vec<&'static str> {
    let mut ids: Vec<&'static str> = NODE_FACTORY_REGISTRY.keys().copied().collect();
    ids.sort_unstable();
    ids
}

/// Register a source unit's [`IngestorNode`] with the node factory registry.
///
/// # Example
///
/// ```rust,ignore
/// register_node_factory!("noop", NoopSourceUnit);
/// ```
///
/// The macro creates a `NodeFactoryEntry` with a `fn` pointer wrapper around
/// `run_ingestor::<I>(args)` and submits it to `inventory`.
#[macro_export]
macro_rules! register_node_factory {
    ($id:expr, $node_type:ty) => {
        ::inventory::submit! {
            $crate::node_factory::NodeFactoryEntry {
                source_unit_id: $id,
                factory_fn: |args| Box::pin($crate::node_factory::run_ingestor::<$node_type>(args)),
            }
        }
    };
}

/// Run a source-unit ingestor through the standard SDK lifecycle.
///
/// Shared implementation used by all `register_node_factory!`-produced
/// factories. Handles CLI parsing, SDK wiring, and shutdown.
///
/// This function is `pub` so the macro can name it; callers should use the
/// macro rather than this function directly.
pub async fn run_ingestor<I>(
    args: Vec<std::ffi::OsString>,
) -> Result<(), Box<dyn std::error::Error>>
where
    I: sinex_node_sdk::IngestorNode + Default + 'static,
{
    use clap::Parser;
    use sinex_node_sdk::IngestorNodeAdapter;
    use sinex_node_sdk::node_cli::{NodeCli, NodeCliRunner};

    let parsed = NodeCli::parse_from(args);
    let node = IngestorNodeAdapter::new(I::default());
    let mut runner = NodeCliRunner::new(node);
    runner.run(parsed).await.map_err(std::convert::Into::into)
}
