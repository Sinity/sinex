//! Parser dispatch: maps source_id to a parser capability and invokes it
//! against staged source material.
//!
//! For now this is a manual match; once #1100 lands, it becomes a registry of
//! `#[derive(SourceRecord)]` parsers with automatic dispatch.

use std::sync::Arc;
use sinex_primitives::parser::ParsedEventIntent;
use sinex_primitives::Uuid;

/// Outcome of a parser dispatch: the parsed event intents ready for admission.
#[derive(Debug)]
pub struct ParseOutcome {
    pub events: Vec<ParsedEventIntent>,
    pub parser_id: String,
    pub parser_version: String,
}

/// Function signature for parser dispatch: takes source_id + material bytes,
/// returns parsed event intents or an error.
pub type ParserDispatchFn = Arc<
    dyn Fn(&str, &[u8], Option<Uuid>) -> Result<ParseOutcome, String> + Send + Sync
>;

/// Create a parser dispatch that handles known source_ids.
///
/// Currently dispatches WeeChat; other sources are stubbed. Once #1100
/// (`#[derive(SourceRecord)]`) lands, this becomes a registry lookup.
pub fn default_parser_dispatch() -> ParserDispatchFn {
    Arc::new(move |source_id: &str, _material_bytes: &[u8], _material_id: Option<Uuid>| {
        match source_id {
            "weechat" => {
                // WeeChat log parsing via SDK's WeeChatLogParser.
                // For now this is stubbed — the parser needs a SourceRecord
                // from the input-shape adapter. The adapter produces records
                // from material bytes; wiring the adapter → parser chain is
                // the next slice after the parse listener is proven.
                Err("weechat parser dispatch: material bytes → SourceRecord → parser not yet wired".to_string())
            }
            other => Err(format!("unknown source_id: {other}")),
        }
    })
}

/// A test-only parser dispatch that records invocation and returns no events.
#[cfg(test)]
pub fn test_parser_dispatch() -> (
    ParserDispatchFn,
    Arc<std::sync::Mutex<Vec<(String, Vec<u8>, Option<Uuid>)>>>,
) {
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
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
