//! Parser dispatch: maps source_id to a `MaterialParser` and invokes it
//! against staged source material.
//!
//! The dispatch is fully registry-driven — no match arms. Source units register
//! their parsers at link time via [`register_parser!`]; the dispatcher looks
//! them up by source-unit id at call time.

use std::sync::{Arc, LazyLock, Mutex};
use std::collections::HashMap;
use futures::future::BoxFuture;

use sinex_primitives::parser::{
    ParsedEventIntent, ParserContext, ParserManifest, SourceUnitId, MaterialAnchor, SourceRecord,
};
use sinex_primitives::ids::Id;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::Uuid;
use sinex_primitives::temporal::Timestamp;

/// Outcome of a parser dispatch: the parsed event intents ready for admission.
#[derive(Debug)]
pub struct ParseOutcome {
    pub events: Vec<ParsedEventIntent>,
    pub parser_id: String,
    pub parser_version: String,
}

/// Function signature for parser dispatch: takes source_id + material bytes,
/// returns parsed event intents or an error.
///
/// This is the sync wrapper type used by the NATS parse listener. The actual
/// parsing delegates through the async `ErasedParser` registry via
/// `tokio::task::block_in_place`.
pub type ParserDispatchFn = Arc<
    dyn Fn(&str, &[u8], Option<Uuid>) -> Result<ParseOutcome, String> + Send + Sync
>;

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

    /// Parse a single source record, returning a boxed future.
    fn parse_record_erased<'a>(
        &'a mut self,
        record: SourceRecord,
        ctx: &'a ParserContext,
    ) -> BoxFuture<'a, Result<Vec<ParsedEventIntent>, String>>;
}

impl<P> ErasedParser for P
where
    P: sinex_node_sdk::parser::MaterialParser + Send + Sync,
{
    fn manifest(&self) -> ParserManifest {
        sinex_node_sdk::parser::MaterialParser::manifest(self)
    }

    fn parse_record_erased<'a>(
        &'a mut self,
        record: SourceRecord,
        ctx: &'a ParserContext,
    ) -> BoxFuture<'a, Result<Vec<ParsedEventIntent>, String>> {
        let fut = sinex_node_sdk::parser::MaterialParser::parse_record(self, record, ctx);
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
    pub source_unit_id: &'static str,
    pub factory_fn: ParserFactoryFn,
}

inventory::collect!(ParserRegistryEntry);

/// Global registry of parser factories keyed by source-unit id.
static PARSER_REGISTRY: LazyLock<HashMap<&'static str, ParserFactoryFn>> =
    LazyLock::new(|| {
        let mut map: HashMap<&'static str, ParserFactoryFn> = HashMap::new();
        for entry in inventory::iter::<ParserRegistryEntry>() {
            map.entry(entry.source_unit_id).or_insert(entry.factory_fn);
        }
        map
    });

/// Look up a parser factory function by source-unit id.
#[must_use]
pub fn find_parser_factory(source_unit_id: &SourceUnitId) -> Option<ParserFactoryFn> {
    PARSER_REGISTRY.get(source_unit_id.as_str()).copied()
}

// =============================================================================
// Macro for registration
// =============================================================================

/// Register a `MaterialParser` implementation with the parser registry.
///
/// The parser type must implement `Default` and `MaterialParser`.
///
/// # Example
///
/// ```rust,ignore
/// register_parser!("weechat", WeeChatLogParser);
/// ```
#[macro_export]
macro_rules! register_parser {
    ($source_unit_id:expr, $parser_type:ty) => {
        ::inventory::submit! {
            $crate::dispatch::ParserRegistryEntry {
                source_unit_id: $source_unit_id,
                factory_fn: || Box::new(<$parser_type>::default()) as Box<dyn $crate::dispatch::ErasedParser>,
            }
        }
    };
}

// =============================================================================
// Dispatch function — registry-driven, no match arms
// =============================================================================

/// Create a registry-driven parser dispatch function.
///
/// Looks up the parser for `source_id` in the compile-time registry. Returns
/// an error for unregistered source units. No match arms — registration via
/// [`register_parser!`] is the only path.
pub fn default_parser_dispatch() -> ParserDispatchFn {
    Arc::new(move |source_id: &str, material_bytes: &[u8], material_id: Option<Uuid>| {
        // Validate the untrusted NATS-supplied source_id at the boundary.
        let source_unit_id = SourceUnitId::new(source_id)
            .map_err(|e| format!("invalid source_id '{source_id}': {e}"))?;

        let factory_fn = match find_parser_factory(&source_unit_id) {
            Some(f) => f,
            None => {
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
            }
        };

        let mut parser = factory_fn();

        // Build a minimal SourceRecord from the raw bytes so the async
        // MaterialParser can consume it. The listener path doesn't have a
        // full material context, so we use a zero anchor.
        let mat_id = material_id
            .map(Id::<SourceMaterial>::from_uuid)
            .unwrap_or_else(Id::new);

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
            source_unit_id,
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
            tokio::runtime::Handle::current()
                .block_on(parser.parse_record_erased(record, &ctx))
        })
        .map_err(|e| format!("parse error: {e}"))?;

        Ok(ParseOutcome {
            events: intents,
            parser_id: manifest.parser_id.to_string(),
            parser_version: manifest.parser_version,
        })
    })
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

/// A test-only parser dispatch that records invocations and returns no events.
pub fn test_parser_dispatch() -> (
    ParserDispatchFn,
    Arc<Mutex<Vec<(String, Vec<u8>, Option<Uuid>)>>>,
) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_clone = calls.clone();
    let dispatch: ParserDispatchFn = Arc::new(move |source_id, bytes, material_id| {
        calls_clone.lock().unwrap().push((
            source_id.to_string(),
            bytes.to_vec(),
            material_id,
        ));
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
mod tests {
    use super::*;
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn test_dispatch_returns_error_for_unknown_source() -> xtask::sandbox::TestResult<()> {
        let dispatch = default_parser_dispatch();
        let result = dispatch("completely-unknown-source-xyz", b"data", None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("unknown source_id 'completely-unknown-source-xyz'"),
            "got: {err}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_parser_dispatch_records_calls() -> xtask::sandbox::TestResult<()> {
        let (dispatch, calls) = test_parser_dispatch();
        let result = dispatch("any-source", b"data", None);
        assert!(result.is_ok());
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "any-source");
        assert_eq!(calls[0].1, b"data");
        assert_eq!(calls[0].2, None);
        Ok(())
    }

    #[sinex_test]
    async fn test_parser_dispatch_with_material_id() -> xtask::sandbox::TestResult<()> {
        let (dispatch, calls) = test_parser_dispatch();
        let material_id = Uuid::now_v7();
        let result = dispatch("weechat", b"some bytes", Some(material_id));
        assert!(result.is_ok());
        let calls = calls.lock().unwrap();
        assert_eq!(calls[0].2, Some(material_id));
        Ok(())
    }
}
