//! The Wayland clipboard and primary selection. `wl-paste --watch <command>`
//! runs `<command>` with the new content on its standard input each time the
//! selection changes; the command (`sinex-session clipboard-frame`) prints one
//! JSON line describing it, and capture spools those lines on `LiveStream`.
//!
//! An owner can mark content secret with the `x-kde-passwordManagerHint` type
//! (password managers do); its content is then not read, only that a secret
//! was copied.
use crate::drivers::live_stream::{self, LiveStream, Rotation, SpoolWriter};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use base64::Engine;
use serde_json::{Map, Value, json};
use sinex_model::event_types::session::ClipboardCopied;
use sinex_model::{Class, InputRef};
use std::ffi::OsString;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

pub const SPEC: SourceSpec = SourceSpec {
    name: "desktop.clipboard",
    driver: Driver::LiveStream,
    excisable: true,
    purpose: "recall: what was copied where",
    // Anything the operator copies, passwords included unless marked secret.
    class: Class::Guarded,
    parser: "wl-clipboard",
    parser_version: 2,
    event_types: &["clipboard.copied"],
    framer: crate::Framer::incremental(crate::drivers::append_file::lines),
};

const SECRET_HINT: &str = "x-kde-passwordManagerHint";

/// The type `wl-paste` reads when none is asked for: text when offered.
fn chosen(types: &[String]) -> Option<String> {
    types
        .iter()
        .find(|t| t.starts_with("text/plain"))
        .or_else(|| types.iter().find(|t| t.starts_with("text/")))
        .or_else(|| types.first())
        .cloned()
}

/// Types offered for `selection` now.
pub fn types(selection: &str) -> Vec<String> {
    let mut command = Command::new("wl-paste");
    if selection == "primary" {
        command.arg("--primary");
    }
    command
        .arg("--list-types")
        .stderr(Stdio::null())
        .output()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// The line `clipboard-frame` prints for `content`.
pub fn frame(selection: &str, types: &[String], content: &[u8]) -> Vec<u8> {
    let secret = types.iter().any(|t| t == SECRET_HINT);
    let mut v = json!({"selection": selection, "types": types, "secret": secret});
    if !secret {
        v["mime"] = json!(chosen(types));
        match std::str::from_utf8(content) {
            Ok(text) => v["text"] = json!(text),
            Err(_) => {
                v["content_b64"] = json!(base64::engine::general_purpose::STANDARD.encode(content))
            }
        }
    }
    serde_json::to_vec(&v).expect("JSON serializes")
}

/// `sinex-session clipboard-frame`: read the content `wl-paste --watch` gives
/// on standard input and print its frame.
pub fn print_frame(selection: &str) -> std::io::Result<()> {
    let mut content = Vec::new();
    std::io::stdin().read_to_end(&mut content)?;
    let types = types(selection);
    let mut line = frame(selection, &types, &content);
    line.push(b'\n');
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    out.write_all(&line)?;
    out.flush()
}

/// Spool changes of `selection` until stopped (or `wl-paste` ends).
/// `helper` is the command that prints a frame (`sinex-session
/// clipboard-frame --selection <selection>`).
pub fn capture(
    selection: &str,
    helper: &[OsString],
    spool: &Path,
    stop: &crate::capture::CaptureStop,
) -> std::io::Result<u64> {
    let mut writer = SpoolWriter::open(spool, Rotation::default())?;
    let mut command = Command::new("wl-paste");
    if selection == "primary" {
        command.arg("--primary");
    }
    command
        .arg("--watch")
        .args(helper)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let Some(mut child) = stop.spawn(&mut command)? else {
        return Ok(0);
    };
    let stdout = child.stdout().expect("piped stdout");
    let n =
        live_stream::capture_lines(BufReader::new(stdout), &mut writer, Duration::from_secs(1))?;
    let _ = child.wait();
    Ok(n)
}

pub struct ClipboardParser;

impl Parser for ClipboardParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = ClipboardCopied;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<ClipboardCopied>, ()), RecordError> {
        let (received_at, bytes) = live_stream::received(record).map_err(RecordError::new)?;
        let f: Map<String, Value> = serde_json::from_slice(&bytes)
            .map_err(|e| RecordError::new(format!("not a clipboard frame: {e}")))?;
        let secret = f.get("secret").and_then(Value::as_bool).unwrap_or(false);
        let content: Option<Vec<u8>> = match (f.get("text"), f.get("content_b64")) {
            (Some(Value::String(t)), _) => Some(t.as_bytes().to_vec()),
            (_, Some(Value::String(b))) => Some(
                base64::engine::general_purpose::STANDARD
                    .decode(b)
                    .map_err(|e| RecordError::new(format!("content is not base64: {e}")))?,
            ),
            _ => None,
        };
        let mime_type = f.get("mime").and_then(Value::as_str).map(str::to_owned);
        let text = f
            .get("text")
            .and_then(Value::as_str)
            .filter(|_| mime_type.as_deref().is_none_or(|m| m.starts_with("text/")))
            .map(str::to_owned);
        Ok((
            vec![ClipboardCopied {
                captured_by: sinex_model::event_types::session::CaptureProgram::Sinex,
                received_at,
                selection: f
                    .get("selection")
                    .and_then(Value::as_str)
                    .unwrap_or("clipboard")
                    .to_owned(),
                mime_types: f.get("types").and_then(Value::as_array).map(|xs| {
                    xs.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                }),
                mime_type,
                text,
                bytes: content.as_ref().map(|c| c.len() as u64),
                content_hash: content
                    .as_ref()
                    .map(|c| blake3::hash(c).to_hex().to_string()),
                sha256: None,
                window: None,
                imported_from: None,
                secret,
                extra: Map::new(),
            }],
            (),
        ))
    }
}

pub fn source() -> DeclaredSource<ClipboardParser> {
    DeclaredSource {
        spec: SPEC,
        parser: ClipboardParser,
    }
}

pub fn driver(spool: PathBuf, host: String, selection: &str) -> LiveStream {
    LiveStream {
        spool,
        input: InputRef {
            kind: "wayland".into(),
            locator: format!("wayland:{selection}"),
            host,
        },
    }
}

/// The selection a binding captures: the `selection` option, `clipboard` by
/// default.
fn selection(b: &crate::binding::Binding) -> &'static str {
    match b.settings.option("selection") {
        Some("primary") => "primary",
        _ => "clipboard",
    }
}

/// `--read clipboard=SPOOL`: the clipboard (or, with the option
/// `selection: primary`, the primary selection), captured into SPOOL. The
/// frame printer is this program's own `clipboard-frame` command.
pub const FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "clipboard",
    specs: || vec![&SPEC],
    plan: Some(|spool, settings| {
        Ok(crate::binding::single(&SPEC, spool, settings)
            .into_iter()
            .map(|b| {
                let s = selection(&b);
                b.named(s)
            })
            .collect())
    }),
    source: |_| Ok(Box::new(source())),
    reader: Some(|b| {
        Ok(crate::binding::stateless(driver(
            b.input.clone(),
            b.settings.host.clone(),
            selection(b),
        )))
    }),
    capture: Some(|b, stop| {
        let s = selection(b);
        let exe = std::env::current_exe().map_err(|e| e.to_string())?;
        let helper: Vec<OsString> = vec![
            exe.into(),
            "clipboard-frame".into(),
            "--selection".into(),
            s.into(),
        ];
        capture(s, &helper, &b.input, stop).map_err(|e| e.to_string())
    }),
};

#[cfg(test)]
mod tests;
