//! The media library's two sources.
//!
//! **Scan** (`library.scan`): a walk of a library root. By default changed files
//! are hashed, sniffed, streamed into the content store unless the root is
//! indexed by reference, and described by one JSON line (`library.copy`).
//! Explicitly selected Ollama blobs instead retain file facts and locations
//! without reading contents (`document.captured`). The material is those lines. A completion line closes
//! each changed scan once, without another copy event. A file that left its
//! path and whose bytes turned up at a new path is a move (`moved_from`);
//! matching device and inode are considered first. An edited file names the
//! bytes it held (`previous_content`). A file gone from a completed walk, and
//! not moved, gets a vanished line.
//!
//! **Requests** (`library.request`): what the operator or an agent writes to
//! the library (previews, curation records, views), stored as the request's
//! own JSON lines and read back by the request parser. The parser takes the
//! author, channel and time from the material's manifest, which the daemon
//! fills from the authenticated caller, never from the lines.
use serde_json::json;
use sinex_source_core::discovery::{DiscoverySpec, Location};
use sinex_source_core::drivers::export_archive::{Framing, frame_lines};
use sinex_source_core::registry::{Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{Framer, binding};

mod files;
mod requests;
mod scan;

pub use files::{Digests, digest, dimensions, media_type_for_path};
pub use requests::{
    RequestParser, RequestPayload, RequestWriter, request_material, request_source,
};
pub use scan::{Known, LibraryReader, LibraryScan, ScanParser, ScanState, scan_source, skipped};

pub const SCAN: SourceSpec = SourceSpec {
    name: "library.scan",
    origin: Origin::Input(Shape::DirSnapshot),
    expect: Expectation::cadence_days(1),
    purpose: "the media library",
    parser: "library-scan",
    parser_version: 3,
    event_types: &["library.copy", "document.captured"],
    framer: Framer::export(frame_lines, Framing::Lines),
    ..SourceSpec::DEFAULT
};

pub const REQUEST: SourceSpec = SourceSpec {
    name: "library.request",
    origin: Origin::Request,
    expect: Expectation::OnDemand,
    purpose: "the media library",
    parser: "library-request",
    parser_version: 2,
    event_types: sinex_model::event_types::library::REQUEST_TYPES,
    framer: Framer::export(frame_lines, Framing::Lines),
    ..SourceSpec::DEFAULT
};

super::input_format! {
    /// Library roots are bindings like any other; library requests are supplied by the API.
    pub FORMAT = "library" {
    specs: [SCAN, REQUEST],
    source: |b| {
        if b.source == SCAN.name {
            Ok(Box::new(scan_source()))
        } else {
            Ok(Box::new(request_source()))
        }
    },
    discovery: DiscoverySpec::system(&[Location::new("declared", "library")]),
    plan: |path, settings| {
        let path = std::fs::canonicalize(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let mut settings = settings.clone();
        let root = if let Some(root) = settings.option("root") {
            std::fs::canonicalize(root).map_err(|error| format!("{root}: {error}"))?
        } else if path.is_dir() {
            path.clone()
        } else {
            path.parent()
                .ok_or("library input has no parent")?
                .to_path_buf()
        };
        let selection = path
            .strip_prefix(&root)
            .map_err(|_| "library selection is outside its root")?;
        if !selection.as_os_str().is_empty() {
            if !settings.options.is_object() {
                settings.options = json!({});
            }
            settings.options["only"] = json!(selection.to_string_lossy());
        }
        Ok(binding::single(&SCAN, &root, &settings))
    },
    reader: |b| {
        if b.source != SCAN.name {
            return Err("library requests are supplied by the API".into());
        }
        let metadata: Vec<std::path::PathBuf> = b.settings.options.get("ollama_blobs_metadata")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose().map_err(|error| format!("ollama_blobs_metadata: {error}"))?
            .unwrap_or_default();
        if metadata.iter().any(|path| path.as_os_str().is_empty()
            || path.components().any(|part| !matches!(part, std::path::Component::Normal(_)))) {
            return Err("ollama_blobs_metadata must contain nonempty root-relative paths".into());
        }
        Ok(binding::polled(LibraryReader::new(LibraryScan {
            root: b.input.clone(),
            host: b.settings.host.clone(),
            keep_bytes: !b.settings.boolean("reference")?.unwrap_or(false),
            probe: true,
            bytes_per_poll: 1 << 30,
            only: b.settings.option("only").map(str::to_owned),
            ollama_blobs_metadata: metadata,
        })))
    },
} }

#[cfg(test)]
mod tests;
