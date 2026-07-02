//! Parser dispatch: maps `source_id` to a `MaterialParser` and invokes it
//! against staged source material.
//!
//! The dispatch is fully registry-driven — no match arms. Source contracts register
//! their parsers at link time via [`register_source!`]; the dispatcher looks
//! them up by source id at call time.

use futures::future::BoxFuture;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use sinex_primitives::Uuid;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{
    MaterialAnchor, ParsedEventIntent, ParserContext, ParserFieldPrivacyMetadata, ParserManifest,
    SourceId, SourceRecord,
};
use sinex_primitives::temporal::Timestamp;

/// Outcome of a parser dispatch: the parsed event intents ready for admission.
#[derive(Debug)]
pub struct ParseOutcome {
    pub events: Vec<ParsedEventIntent>,
    pub parser_id: String,
    pub parser_version: String,
}

/// Function signature for parser dispatch: takes `source_id` + material bytes,
/// returns parsed event intents or an error.
///
/// This is the sync wrapper type used by the NATS parse listener. The actual
/// parsing delegates through the async `ErasedParser` registry via
/// `tokio::task::block_in_place`.
pub type ParserDispatchFn =
    Arc<dyn Fn(&str, &[u8], Option<Uuid>) -> Result<ParseOutcome, String> + Send + Sync>;

// =============================================================================
// Type-erased parser
// =============================================================================

/// Object-safe erased view of a `MaterialParser`.
///
/// `MaterialParser` is not object-safe (associated type + async fn). This trait
/// erases both by boxing the future. The registry instantiates parsers via
/// function pointers, then calls through this trait.
pub trait ErasedParser: Send + Sync {
    /// Return the parser's manifest.
    fn manifest(&self) -> ParserManifest;

    /// Return parser-declared field-level privacy metadata.
    fn field_privacy_metadata(&self) -> Vec<ParserFieldPrivacyMetadata>;

    /// Parse a single source record, returning a boxed future.
    fn parse_record_erased<'a>(
        &'a mut self,
        record: SourceRecord,
        ctx: &'a ParserContext,
    ) -> BoxFuture<'a, Result<Vec<ParsedEventIntent>, String>>;
}

impl<P> ErasedParser for P
where
    P: sinex_primitives::parser::MaterialParser + Send + Sync,
{
    fn manifest(&self) -> ParserManifest {
        sinex_primitives::parser::MaterialParser::manifest(self)
    }

    fn field_privacy_metadata(&self) -> Vec<ParserFieldPrivacyMetadata> {
        sinex_primitives::parser::MaterialParser::field_privacy_metadata(self)
    }

    fn parse_record_erased<'a>(
        &'a mut self,
        record: SourceRecord,
        ctx: &'a ParserContext,
    ) -> BoxFuture<'a, Result<Vec<ParsedEventIntent>, String>> {
        let fut = sinex_primitives::parser::MaterialParser::parse_record(self, record, ctx);
        Box::pin(async move { fut.await.map_err(|e| e.to_string()) })
    }
}

// =============================================================================
// Parser registry
// =============================================================================

/// Factory function that creates a fresh `Box<dyn ErasedParser>`.
///
/// Using a `fn` pointer (not a closure) allows use inside
/// `inventory::submit!` which requires const-constructible items.
pub type ParserFactoryFn = fn() -> Box<dyn ErasedParser>;

/// Entry in the compile-time parser inventory.
pub struct ParserRegistryEntry {
    pub source_id: &'static str,
    pub factory_fn: ParserFactoryFn,
}

inventory::collect!(ParserRegistryEntry);

/// Global registry of parser factories keyed by source id.
static PARSER_REGISTRY: LazyLock<HashMap<&'static str, ParserFactoryFn>> = LazyLock::new(|| {
    let mut map: HashMap<&'static str, ParserFactoryFn> = HashMap::new();
    for entry in inventory::iter::<ParserRegistryEntry>() {
        map.entry(entry.source_id).or_insert(entry.factory_fn);
    }
    map
});

/// Look up a parser factory function by source id.
#[must_use]
pub fn find_parser_factory(source_id: &SourceId) -> Option<ParserFactoryFn> {
    PARSER_REGISTRY.get(source_id.as_str()).copied()
}

/// Read-only parser inventory entry for audit/reporting surfaces.
#[derive(Debug, Clone)]
pub struct ParserInventoryRecord {
    pub source_id: String,
    pub manifest: ParserManifest,
    pub field_privacy_metadata: Vec<ParserFieldPrivacyMetadata>,
}

/// Enumerate parser factories, manifests, and field metadata from the compiled
/// source inventory.
#[must_use]
pub fn parser_inventory_records() -> Vec<ParserInventoryRecord> {
    let mut records = PARSER_REGISTRY
        .iter()
        .map(|(source_id, factory_fn)| {
            let parser = factory_fn();
            ParserInventoryRecord {
                source_id: (*source_id).to_string(),
                manifest: parser.manifest(),
                field_privacy_metadata: parser.field_privacy_metadata(),
            }
        })
        .collect::<Vec<_>>();
    records.sort_by(|a, b| a.source_id.cmp(&b.source_id));
    records
}

// =============================================================================
// Macro for registration
// =============================================================================

// =============================================================================
// Dispatch function — registry-driven, no match arms
// =============================================================================

/// Create a registry-driven parser dispatch function.
///
/// Looks up the parser for `source_id` in the compile-time registry. Returns
/// an error for unregistered source contracts. No match arms — registration via
/// [`register_source!`](crate::register_source) is the only path.
#[must_use]
pub fn default_parser_dispatch() -> ParserDispatchFn {
    Arc::new(
        move |source_id: &str, material_bytes: &[u8], material_id: Option<Uuid>| {
            // Validate the untrusted NATS-supplied source_id at the boundary.
            let source_id = SourceId::new(source_id)
                .map_err(|e| format!("invalid source_id '{source_id}': {e}"))?;

            let Some(factory_fn) = find_parser_factory(&source_id) else {
                let mut ids: Vec<&str> = PARSER_REGISTRY.keys().copied().collect();
                ids.sort_unstable();
                return Err(if ids.is_empty() {
                    format!("unknown source_id '{source_id}': no parsers registered in this binary")
                } else {
                    format!(
                        "unknown source_id '{source_id}': registered parsers are [{}]",
                        ids.join(", ")
                    )
                });
            };

            let mut parser = factory_fn();

            // Build a minimal SourceRecord from the raw bytes so the async
            // MaterialParser can consume it. The listener path doesn't have a
            // full material context, so we use a zero anchor.
            let mat_id = material_id
                .map(Id::<SourceMaterial>::from_uuid)
                .unwrap_or_default();

            let record = SourceRecord {
                material_id: mat_id,
                anchor: MaterialAnchor::ByteRange {
                    start: 0,
                    len: material_bytes.len() as u64,
                },
                bytes: material_bytes.to_vec(),
                logical_path: None,
                source_ts_hint: None,
                metadata: serde_json::Value::Null,
            };

            let ctx = ParserContext {
                source_id,
                source_material_id: mat_id,
                record_anchor: MaterialAnchor::ByteRange {
                    start: 0,
                    len: material_bytes.len() as u64,
                },
                operation_id: Uuid::now_v7(),
                job_id: Uuid::now_v7(),
                host: hostname(),
                acquisition_time: Timestamp::now(),
            };

            let manifest = parser.manifest();

            // Drive the async parse_record_erased synchronously via block_in_place.
            // The NATS listener calls this from within a Tokio runtime on a spawned
            // task, so block_in_place is safe here.
            let intents = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(parser.parse_record_erased(record, &ctx))
            })
            .map_err(|e| format!("parse error: {e}"))?;

            Ok(ParseOutcome {
                events: intents,
                parser_id: manifest.parser_id.to_string(),
                parser_version: manifest.parser_version,
            })
        },
    )
}

/// Get the local hostname for parser context.
fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "unknown-host".to_string())
}

// =============================================================================
// Test-only dispatch (unchanged interface)
// =============================================================================

/// Shared log of test-dispatch invocations: `(source_id, bytes, material_id)`.
type TestDispatchCallLog = Arc<Mutex<Vec<(String, Vec<u8>, Option<Uuid>)>>>;

/// A test-only parser dispatch that records invocations and returns no events.
#[must_use]
pub fn test_parser_dispatch() -> (ParserDispatchFn, TestDispatchCallLog) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_clone = calls.clone();
    let dispatch: ParserDispatchFn = Arc::new(move |source_id, bytes, material_id| {
        let mut calls = calls_clone
            .lock()
            .map_err(|_| "test parser dispatch call log lock poisoned".to_string())?;
        calls.push((source_id.to_string(), bytes.to_vec(), material_id));
        Ok(ParseOutcome {
            events: vec![],
            parser_id: source_id.to_string(),
            parser_version: "1.0.0".to_string(),
        })
    });
    (dispatch, calls)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
#[path = "dispatch_test.rs"]
mod tests;
