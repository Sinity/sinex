//! Import roots are walked once per format; each matching file has its own position.
use sinex_source_core::{Binding, InputFormat, Settings};
use sinex_source_core::discovery::{DiscoveryContext, DiscoveredInput, DiscoverySpec, Location, Owner};
use sinex_source_core::registry::SourceSpec;
use std::path::{Path, PathBuf};

pub const fn spec(root: &'static str) -> DiscoverySpec {
    DiscoverySpec { owner: Owner::System, locations: if root.as_bytes()[0] == b't' { &[Location { root: "input", path: "transcripts" }] } else { &[Location { root: "input", path: "ocr" }] }, capabilities: &[], credentials: &[], request_only: false, requests: &[], streams: &[] }
}

pub fn discover(format: &InputFormat, context: &DiscoveryContext) -> Vec<DiscoveredInput> {
    crate::discovery::discover_format(format, context)
}

pub fn plan(spec: &SourceSpec, input: &Path, settings: &Settings) -> Result<Vec<Binding>, String> {
    if input.as_os_str().is_empty() { return Err("import needs a path".into()); }
    let path = std::path::absolute(input).map_err(|e| e.to_string())?;
    let mut files = Vec::new();
    if path.is_dir() { walk(&path, &mut files)?; } else { files.push(path); }
    files.sort();
    let mut bindings = Vec::new();
    for path in files {
        if input.is_dir() && !matches(spec.parser, &path)? { continue; }
        bindings.push(Binding::of(spec, &path, settings.clone()).named(spec.parser));
    }
    Ok(bindings)
}

fn walk(root: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in std::fs::read_dir(root).map_err(|e| format!("{}: {e}", root.display()))? {
        let entry = entry.map_err(|e| e.to_string())?;
        let kind = entry.file_type().map_err(|e| e.to_string())?;
        if kind.is_dir() { walk(&entry.path(), files)?; }
        else if kind.is_file() { files.push(entry.path()); }
    }
    Ok(())
}

fn matches(parser: &str, path: &Path) -> Result<bool, String> {
    let extension = path.extension().and_then(|s| s.to_str()).unwrap_or_default();
    Ok(match (parser, extension) {
        ("webvtt", "vtt") | ("srt", "srt") | ("ocr-tsv", "tsv") |
        ("transcript-text" | "ocr-text", "txt") => true,
        ("transcript-json" | "whisper-json" | "ocr-json", "json") => {
            // JSON syntax determines the format, not a required filename convention.
            let file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
            let shape: Shape = match serde_json::from_reader(std::io::BufReader::new(file)) { Ok(shape) => shape, Err(_) => return Ok(matches!(parser, "transcript-json" | "ocr-json")) };
            parser == match shape { Shape::Array(rows) => { let _ = rows; "transcript-json" }, Shape::Object(object) if object.contains_key("engine") || object.contains_key("coordinate_units") => "ocr-json", Shape::Object(_) => "whisper-json" }
        },
        _ => false,
    })
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum Shape {
    Array(Vec<serde::de::IgnoredAny>),
    Object(std::collections::BTreeMap<String, serde::de::IgnoredAny>),
}
