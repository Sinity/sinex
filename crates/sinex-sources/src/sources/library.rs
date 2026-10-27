//! The media library's two sources.
//!
//! **Scan** (`library.scan`): a walk of a library root. Each new or changed
//! file is hashed once (BLAKE3, SHA-256, MD5), its type sniffed from its
//! bytes, its bytes streamed into the content store unless the root is
//! indexed by reference only, and described by one JSON line. The material is
//! those lines; one line is one `library.copy`. A file that left its
//! path but whose device and inode turned up at a new path with the same bytes
//! is a rename (`moved_from`); a file whose bytes changed in place names the
//! bytes it held (`previous_content`). A file gone from a completed walk, and
//! not moved, gets a vanished line.
//!
//! **Requests** (`library.request`): what the operator or an agent writes to
//! the library (items, assignments, collections, ratings, views), stored as
//! the request's own JSON lines and read back by the request parser. The
//! parser takes the author and time from the material's manifest, which the
//! daemon fills from the authenticated caller, never from the lines.
use serde_json::{Value, json};
use sinex_model::Class;
use sinex_source_core::registry::{Driver, SourceSpec};

mod files;
mod requests;
mod scan;

pub use files::{Digests, digest, dimensions};
pub use requests::{RequestParser, RequestPayload, request_material, request_source};
pub use scan::{Known, LibraryReader, LibraryScan, ScanParser, ScanState, scan_source, skipped};

pub const SCAN: SourceSpec = SourceSpec {
    name: "library.scan",
    driver: Driver::DirSnapshot,
    excisable: false,
    purpose: "the media library",
    class: Class::Open,
    parser: "library-scan",
    parser_version: 1,
    event_types: &["library.copy"],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_lines,
        sinex_source_core::drivers::export_archive::Framing::Lines,
    ),
};

pub const REQUEST: SourceSpec = SourceSpec {
    name: "library.request",
    driver: Driver::ApiRequest,
    excisable: false,
    purpose: "the media library",
    class: Class::Open,
    parser: "library-request",
    parser_version: 1,
    event_types: sinex_model::event_types::library::REQUEST_TYPES,
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_lines,
        sinex_source_core::drivers::export_archive::Framing::Lines,
    ),
};

/// Library roots are ordinary bindings; library requests are supplied by the API.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "library",
        specs: || vec![&SCAN, &REQUEST],
        plan: Some(|path, settings| {
            let path =
                std::fs::canonicalize(path).map_err(|e| format!("{}: {e}", path.display()))?;
            let mut settings = settings.clone();
            let root = if path.is_dir() {
                path
            } else {
                let name = path
                    .file_name()
                    .ok_or("library input has no file name")?
                    .to_string_lossy()
                    .into_owned();
                if !settings.options.is_object() {
                    settings.options = json!({});
                }
                settings.options["only"] = json!(name);
                path.parent()
                    .ok_or("library input has no parent")?
                    .to_path_buf()
            };
            Ok(sinex_source_core::binding::single(&SCAN, &root, &settings))
        }),
        source: |b| {
            if b.source == SCAN.name {
                Ok(Box::new(scan_source()))
            } else {
                Ok(Box::new(request_source()))
            }
        },
        reader: Some(|b| {
            if b.source != SCAN.name {
                return Err("library requests are supplied by the API".into());
            }
            Ok(sinex_source_core::binding::polled(LibraryReader::new(
                LibraryScan {
                    root: b.input.clone(),
                    host: b.settings.host.clone(),
                    keep_bytes: !b
                        .settings
                        .options
                        .get("reference")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    probe: true,
                    bytes_per_poll: 1 << 30,
                    only: b.settings.option("only").map(str::to_owned),
                },
            )))
        }),
        capture: None,
    };

#[cfg(test)]
mod tests;
