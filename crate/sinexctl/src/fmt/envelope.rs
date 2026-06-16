//! ViewEnvelope format rendering — separates `json` (finite document) from `ndjson` (line-oriented).
//!
//! # Format semantics
//!
//! - `json`   → one pretty-printed JSON document (the whole `ViewEnvelope`).
//! - `ndjson` → one compact JSON object per line for each `item` in `items`.
//!              Envelope-level metadata (caveats, freshness, `query_echo`) is intentionally
//!              omitted from each line; use `json` when you need the full envelope context.
//! - `yaml`   → one YAML document (the whole `ViewEnvelope`).
//! - `dot`    → always returns a typed error; a `ViewEnvelope` is not a graph.
//! - `table`  → returns `None`; callers own table rendering.

use color_eyre::eyre::eyre;
use serde::Serialize;
use sinex_primitives::views::ViewEnvelope;

use crate::Result;
use crate::fmt::format_yaml;
use crate::model::OutputFormat;

/// Render a [`ViewEnvelope`] in the requested machine-readable output format.
///
/// - `json`  → `Ok(Some(_))` with the whole envelope as one pretty-printed JSON document.
/// - `ndjson`→ `Ok(Some(_))` with one compact JSON line per element of `items`; envelope
///             metadata (caveats, freshness, etc.) is omitted — use `json` for that context.
/// - `yaml`  → `Ok(Some(_))` with the whole envelope as a YAML document.
/// - `dot`   → `Err(…)` — non-graph view; the caller should propagate with `?`.
/// - `table` → `Ok(None)` — caller is responsible for table rendering.
pub fn render_envelope<T: Serialize, I: Serialize>(
    envelope: &ViewEnvelope<T>,
    items: &[I],
    format: OutputFormat,
) -> Result<Option<String>> {
    match format {
        OutputFormat::Json => Ok(Some(serde_json::to_string_pretty(envelope)?)),
        OutputFormat::Ndjson => {
            let mut out = String::new();
            for item in items {
                out.push_str(&serde_json::to_string(item)?);
                out.push('\n');
            }
            Ok(Some(out))
        }
        OutputFormat::Yaml => Ok(Some(format_yaml(envelope)?)),
        OutputFormat::Dot => Err(eyre!(
            "format `dot` requires a graph view; this view is not a graph \
             — use json, ndjson, yaml, or table"
        )),
        OutputFormat::Table => Ok(None),
    }
}

/// Render a finite [`ViewEnvelope`] in formats that preserve the whole document.
///
/// Unlike [`render_envelope`], this rejects `ndjson`: finite read surfaces have
/// envelope-level metadata that would be lost in line-oriented output, and
/// NDJSON is reserved for true streaming surfaces.
pub fn render_finite_envelope<T: Serialize>(
    envelope: &ViewEnvelope<T>,
    format: OutputFormat,
) -> Result<Option<String>> {
    match format {
        OutputFormat::Json => Ok(Some(serde_json::to_string_pretty(envelope)?)),
        OutputFormat::Yaml => Ok(Some(format_yaml(envelope)?)),
        OutputFormat::Ndjson => Err(eyre!(
            "format `ndjson` requires a streaming view; this finite view renders as json or yaml"
        )),
        OutputFormat::Dot => Err(eyre!(
            "format `dot` requires a graph view; this view is not a graph \
             - use json, yaml, or table"
        )),
        OutputFormat::Table => Ok(None),
    }
}

/// Print a finite [`ViewEnvelope`] and report whether machine rendering handled it.
///
/// Returns `Ok(false)` for `table`, allowing callers to fall through to their
/// existing human renderer unchanged.
pub fn print_finite_envelope<T: Serialize>(
    envelope: &ViewEnvelope<T>,
    format: OutputFormat,
) -> Result<bool> {
    let Some(output) = render_finite_envelope(envelope, format)? else {
        return Ok(false);
    };

    print!("{output}");
    if !output.is_empty() && !output.ends_with('\n') {
        println!();
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use serde_json::json;
    use sinex_primitives::views::{
        EVENT_CARD_LIST_SCHEMA_VERSION, EventCardListView, VIEW_ENVELOPE_SCHEMA_VERSION,
        ViewEnvelope,
    };
    use xtask::sandbox::sinex_test;

    fn fixture_envelope(count: usize) -> ViewEnvelope<EventCardListView> {
        ViewEnvelope::new(
            "sinexctl.events.recent",
            EventCardListView {
                schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
                count,
                cards: vec![],
            },
        )
    }

    fn fixture_items(count: usize) -> Vec<serde_json::Value> {
        (0..count)
            .map(|i| json!({"index": i, "summary": format!("item {i}")}))
            .collect()
    }

    /// `json` format: output parses as a single JSON value equal to the envelope.
    /// Invariant holds across a range of item counts (parametric).
    #[sinex_test]
    async fn json_renders_one_finite_document_across_counts() -> xtask::TestResult<()> {
        for count in [0_usize, 1, 5, 100] {
            let envelope = fixture_envelope(count);
            let items = fixture_items(count);

            let output = render_envelope(&envelope, &items, OutputFormat::Json)?
                .expect("json must return Some");

            let parsed: serde_json::Value = serde_json::from_str(&output).map_err(|e| {
                color_eyre::eyre::eyre!("json output did not parse (count={count}): {e}")
            })?;

            assert_eq!(
                parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION,
                "json must include envelope schema_version (count={count})"
            );
            assert_eq!(
                parsed["source_surface"], "sinexctl.events.recent",
                "json must include source_surface (count={count})"
            );
            assert_eq!(
                parsed["payload"]["count"], count,
                "json must embed payload count (count={count})"
            );
            // Sanity: only one top-level JSON value (parser would error if there were two)
        }
        Ok(())
    }

    /// `ndjson` format: exactly N lines for N items, each line independently parseable.
    #[sinex_test]
    async fn ndjson_line_count_equals_items_len() -> xtask::TestResult<()> {
        for count in [0_usize, 1, 3, 10] {
            let envelope = fixture_envelope(count);
            let items = fixture_items(count);

            let output = render_envelope(&envelope, &items, OutputFormat::Ndjson)?
                .expect("ndjson must return Some");

            if count == 0 {
                assert!(
                    output.is_empty(),
                    "ndjson with 0 items must produce empty output"
                );
                continue;
            }

            assert!(
                output.ends_with('\n'),
                "ndjson output must end with a newline"
            );

            // Strip the trailing newline before splitting so we don't get a spurious empty line
            let lines: Vec<&str> = output.trim_end_matches('\n').split('\n').collect();
            assert_eq!(
                lines.len(),
                count,
                "ndjson line count must equal items.len() (count={count})"
            );

            for (i, line) in lines.iter().enumerate() {
                let parsed: serde_json::Value = serde_json::from_str(line).map_err(|e| {
                    color_eyre::eyre::eyre!("ndjson line {i} did not parse (count={count}): {e}")
                })?;
                assert_eq!(
                    parsed["index"], i,
                    "each ndjson line must independently parse as its own item (line={i}, count={count})"
                );
            }
        }
        Ok(())
    }

    /// `dot` format: returns a typed error for non-graph views.
    #[sinex_test]
    async fn dot_returns_error_for_non_graph_view() -> xtask::TestResult<()> {
        let envelope = fixture_envelope(0);
        let items: Vec<serde_json::Value> = vec![];

        let result = render_envelope(&envelope, &items, OutputFormat::Dot);
        assert!(result.is_err(), "dot must return Err for a non-graph view");

        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("dot"),
            "error message must name the rejected format: {msg}"
        );
        assert!(
            msg.contains("graph"),
            "error message must explain why dot is rejected: {msg}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn finite_envelope_rejects_ndjson() -> xtask::TestResult<()> {
        let envelope = fixture_envelope(1);

        let result = render_finite_envelope(&envelope, OutputFormat::Ndjson);

        assert!(result.is_err(), "finite views must not render as ndjson");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("ndjson"),
            "error must name rejected format: {msg}"
        );
        assert!(
            msg.contains("streaming"),
            "error must explain ndjson is stream-only: {msg}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn finite_envelope_json_preserves_whole_document() -> xtask::TestResult<()> {
        let envelope = fixture_envelope(2);

        let output = render_finite_envelope(&envelope, OutputFormat::Json)?
            .expect("json must render finite envelope");
        let parsed: serde_json::Value = serde_json::from_str(&output)?;

        assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(parsed["source_surface"], "sinexctl.events.recent");
        assert_eq!(parsed["payload"]["count"], 2);
        Ok(())
    }

    /// `table` format: returns `None` so the caller can own table rendering.
    #[sinex_test]
    async fn table_returns_none() -> xtask::TestResult<()> {
        let envelope = fixture_envelope(2);
        let items = fixture_items(2);

        let result = render_envelope(&envelope, &items, OutputFormat::Table)?;
        assert!(result.is_none(), "table must return None");
        Ok(())
    }

    /// All machine formats must not contain ANSI escape sequences.
    #[sinex_test]
    async fn machine_formats_contain_no_ansi_sequences() -> xtask::TestResult<()> {
        let envelope = fixture_envelope(2);
        let items = fixture_items(2);

        for format in [OutputFormat::Json, OutputFormat::Ndjson, OutputFormat::Yaml] {
            let output = render_envelope(&envelope, &items, format)?
                .expect("machine format must return Some");
            assert!(
                !output.contains("\x1b["),
                "format {format:?} must not contain ANSI escape sequences"
            );
        }
        Ok(())
    }
}
