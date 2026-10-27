//! Import roots are walked once per format; each matching file has its own position.
use sinex_source_core::discovery::{
    DiscoveredInput, DiscoveryContext, DiscoverySpec, Location, Owner,
};
use sinex_source_core::registry::SourceSpec;
use sinex_source_core::{Binding, InputFormat, Settings};
use std::path::{Path, PathBuf};

pub const fn spec(transcripts: bool) -> DiscoverySpec {
    DiscoverySpec {
        owner: Owner::System,
        locations: if transcripts {
            &[Location {
                root: "input",
                path: "transcripts",
            }]
        } else {
            &[Location {
                root: "input",
                path: "ocr",
            }]
        },
        capabilities: &[],
        credentials: &[],
        request_only: false,
        requests: &[],
        streams: &[],
    }
}

pub fn discover(format: &InputFormat, context: &DiscoveryContext) -> Vec<DiscoveredInput> {
    let mut found = Vec::new();
    for mut input in crate::discovery::discover_format(format, context) {
        let selected = input.input.as_ref().is_some_and(|path| {
            context
                .inputs
                .get(format.name)
                .is_some_and(|paths| paths.contains(path))
                || context
                    .sources
                    .get(&input.source)
                    .is_some_and(|configuration| {
                        configuration.format.as_deref() == Some(format.name)
                            && configuration
                                .paths
                                .iter()
                                .any(|configured| Path::new(configured) == path.as_path())
                    })
        });
        let mut bindings = Vec::new();
        let mut failures = Vec::new();
        for binding in std::mem::take(&mut input.bindings) {
            if !selected {
                match matches(format.name, &binding.input) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(reason) => {
                        let mut failed = input.clone();
                        failed.input = Some(binding.input.clone());
                        failed.settings = binding.settings.clone();
                        failed.availability =
                            sinex_source_core::discovery::Availability::Unavailable;
                        failed.reason = Some(reason);
                        failures.push(failed);
                        continue;
                    }
                }
            }
            bindings.push(binding);
        }
        input.bindings = bindings;
        found.push(input);
        found.extend(failures);
    }
    found
}

pub fn plan(spec: &SourceSpec, input: &Path, settings: &Settings) -> Result<Vec<Binding>, String> {
    if input.as_os_str().is_empty() {
        return Err("import needs a path".into());
    }
    let path = std::path::absolute(input).map_err(|e| e.to_string())?;
    let mut files = Vec::new();
    if path.is_dir() {
        walk(&path, &mut files)?;
    } else {
        files.push(path);
    }
    files.sort();
    let mut bindings = Vec::new();
    for path in files {
        bindings.push(Binding::of(spec, &path, settings.clone()).named(spec.parser));
    }
    Ok(bindings)
}

fn walk(root: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in std::fs::read_dir(root).map_err(|e| format!("{}: {e}", root.display()))? {
        let entry = entry.map_err(|e| e.to_string())?;
        let kind = entry.file_type().map_err(|e| e.to_string())?;
        if kind.is_dir() {
            walk(&entry.path(), files)?;
        } else if kind.is_file() {
            files.push(entry.path());
        }
    }
    Ok(())
}

fn matches(parser: &str, path: &Path) -> Result<bool, String> {
    let extension = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    Ok(match (parser, extension.as_str()) {
        ("webvtt", "vtt") | ("srt", "srt") | ("ocr-tsv", "tsv") => true,
        ("transcript-json" | "whisper-json" | "ocr-json", "json") => {
            use std::io::Read;
            let file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
            let mut first = None;
            for byte in std::io::BufReader::new(file).bytes() {
                let byte = byte.map_err(|e| format!("{}: {e}", path.display()))?;
                if !byte.is_ascii_whitespace() {
                    first = Some(byte);
                    break;
                }
            }
            // The declared root identifies OCR; transcript JSON distinguishes its array and object forms.
            // The parser reports malformed JSON after its bytes have been captured.
            match parser {
                "ocr-json" => true,
                "whisper-json" => first == Some(b'{'),
                _ => first != Some(b'{'),
            }
        }
        ("transcript-text", extension) => !matches!(extension, "json" | "vtt" | "srt"),
        ("ocr-text", extension) => !matches!(extension, "json" | "tsv"),
        _ => false,
    })
}
