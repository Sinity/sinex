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
#[path = "envelope_test.rs"]
mod tests;
