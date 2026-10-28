//! Outlook mail exports: `.pst` stores and the CSV exports Outlook writes in
//! the system's ANSI code page (Windows-1250 on a Polish system).
//!
//! No maintained Rust reader understands PST, so a store is staged first:
//! `readpst` (libpst) converts each folder to an mbox file, and those files are
//! read like any mbox export, as the source `communication.outlook`. The
//! converted mbox is what Sinex stores and replays; its material path names the
//! PST and the folder it came from. The PST stays where the operator keeps it.
//!
//! The CSV export carries no message ID and no date, only the people, subject,
//! body and a few Outlook fields. Each row is identified by its record
//! location; the same message in the PST is a separate record,
//! combined by fusion if at all.
use crate::sources::{fields, mail};
use sinex_model::Class;
use sinex_model::event_types::communication::MailMessage;
use sinex_source_core::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::path::{Path, PathBuf};
use std::process::Command;

pub const PST: SourceSpec = SourceSpec {
    name: "communication.outlook",
    driver: Driver::ExportArchive,
    excisable: false,
    purpose: "mail recall and correspondence history",
    class: Class::Open,
    parser: "rfc5322-mail",
    parser_version: 3,
    event_types: &["mail.message"],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_mbox,
        sinex_source_core::drivers::export_archive::Framing::Mbox,
    ),
};

pub const CSV: SourceSpec = SourceSpec {
    name: "communication.outlook-csv",
    driver: Driver::ExportArchive,
    excisable: false,
    purpose: "mail recall and correspondence history",
    class: Class::Open,
    parser: "outlook-csv",
    parser_version: 3,
    event_types: &["mail.message"],
    framer: sinex_source_core::Framer::export(
        frame_csv,
        sinex_source_core::drivers::export_archive::Framing::Csv,
    ),
};

pub fn pst_source() -> DeclaredSource<mail::MailParser> {
    DeclaredSource {
        spec: PST,
        parser: mail::MailParser,
    }
}

/// Convert `pst` into one mbox file per folder under `out`, returning them in
/// a stable order. The files are named `<pst stem>/<folder path>.mbox`.
fn stage_pst(pst: &Path, out: &Path) -> Result<Vec<PathBuf>, String> {
    let stem = pst
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| format!("{}: no file name", pst.display()))?;
    let dir = out.join(stem);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    // -r: one directory per folder; -8: UTF-8 output; -q: quiet; -b: no
    // RTF bodies (the plain and HTML bodies are kept).
    let status = Command::new("readpst")
        .args(["-r", "-8", "-q", "-b", "-o"])
        .arg(&dir)
        .arg(pst)
        .status()
        .map_err(|e| format!("readpst (libpst) is needed to read {}: {e}", pst.display()))?;
    if !status.success() {
        return Err(format!("readpst failed on {}: {status}", pst.display()));
    }
    let mut found = Vec::new();
    collect_mbox(&dir, &dir, &mut found)?;
    found.sort();
    Ok(found)
}

/// readpst writes each folder's messages to a file named `mbox`; rename it to
/// `<folder>.mbox` so the material path names the folder.
fn collect_mbox(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = std::fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    for entry in entries {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.is_dir() {
            collect_mbox(root, &path, out)?;
        } else if path.file_name().is_some_and(|n| n == "mbox") {
            let folder = path.parent().unwrap_or(root);
            let named = folder.with_extension("mbox");
            let named = if folder == root {
                root.join("root.mbox")
            } else {
                named
            };
            std::fs::rename(&path, &named).map_err(|e| format!("{}: {e}", named.display()))?;
            out.push(named);
        }
    }
    Ok(())
}

pub fn pst_driver(mbox: PathBuf, host: String) -> ExportArchive {
    mail::mbox_driver(mbox, host)
}

pub struct CsvParser;

/// The header row's cells, decoded.
type Header = Vec<String>;

impl Parser for CsvParser {
    type Record<'a> = &'a [u8];
    type Carry = Header;
    type Payload = MailMessage;

    fn parse(
        &self,
        record: &&[u8],
        header: &Header,
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<MailMessage>, Header), RecordError> {
        let (decoded, _, _) = encoding_rs::WINDOWS_1250.decode(record);
        let cells = fields::csv_cells(decoded.as_bytes())?;
        if cells.first().map(String::as_str) == Some("Temat") {
            return Ok((vec![], cells));
        }
        if header.is_empty() {
            return Err(RecordError::new("a message row before the header row"));
        }
        let column = |name: &str| {
            header
                .iter()
                .position(|h| h == name)
                .and_then(|i| cells.get(i))
                .map(|c| c.trim())
                .filter(|c| !c.is_empty())
        };
        let people = |kind: &str| -> Vec<String> {
            let names = column(&format!("{kind}: (imię/nazwisko)"));
            let addresses = column(&format!("{kind}: (adres)"));
            let names: Vec<&str> = names.map(|n| n.split(';').collect()).unwrap_or_default();
            let addresses: Vec<&str> = addresses
                .map(|a| a.split(';').collect())
                .unwrap_or_default();
            (0..names.len().max(addresses.len()))
                .map(|i| {
                    let n = names.get(i).map(|s| s.trim()).unwrap_or("");
                    let a = addresses.get(i).map(|s| s.trim()).unwrap_or("");
                    match (n.is_empty(), a.is_empty()) {
                        (false, false) if n != a => format!("{n} <{a}>"),
                        (_, false) => a.to_owned(),
                        _ => n.to_owned(),
                    }
                })
                .filter(|p| !p.is_empty())
                .collect()
        };
        let cc = people("DW");
        let bcc = people("UDW");
        let labels = column("Kategorie")
            .map(|c| c.split(';').map(|l| l.trim().to_owned()).collect())
            .unwrap_or_default();
        Ok((
            vec![MailMessage {
                account: None,
                message_id: None,
                native_id: None,
                date: None,
                from: people("Od"),
                to: people("Do"),
                cc,
                bcc,
                subject: column("Temat").map(str::to_owned),
                in_reply_to: None,
                references: vec![],
                labels,
                thread_id: None,
                text: column("Treść").map(str::to_owned),
                html_only: false,
                attachments: vec![],
                size: record.len() as u64,
            }],
            header.clone(),
        ))
    }
}

pub fn csv_source() -> DeclaredSource<CsvParser> {
    DeclaredSource {
        spec: CSV,
        parser: CsvParser,
    }
}

pub fn frame_csv(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Csv.frame(bytes, true)
}

pub fn csv_driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: |m| m.to_ascii_lowercase().ends_with(".csv"),
        framing: Framing::Csv,
        part_bytes: PART_BYTES,
    }
}

/// Where PST stores are converted to mbox files before they are read: under
/// `TMPDIR`, which the NixOS module points at the daemon's state directory,
/// since a PST can outgrow a tmpfs. The mbox bytes go to the content store,
/// so this is only a cache.
fn staging() -> PathBuf {
    std::env::temp_dir().join("sinex-outlook")
}

/// `--read outlook=PATH`: each `.pst` under PATH, converted with `readpst`
/// to one mbox per folder, each folder its own binding.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "outlook",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[sinex_source_core::discovery::Location {
                root: "input",
                path: "outlook",
            }],
            capabilities: &[],
            credentials: &[],
            streams: &[],
            requests: &[],
            request_only: false,
        },
        discover: crate::discovery::discover_format,
        specs: || vec![&PST],
        plan: Some(|input, settings| {
            let stage = staging();
            let mut out = Vec::new();
            for pst in sinex_source_core::binding::files(input, true, |p| {
                p.extension().is_some_and(|e| e.eq_ignore_ascii_case("pst"))
            }) {
                for mbox in stage_pst(&pst, &stage)? {
                    out.push(sinex_source_core::binding::Binding::of(
                        &PST,
                        &mbox,
                        settings.clone(),
                    ));
                }
            }
            Ok(out)
        }),
        source: |_| Ok(Box::new(pst_source())),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::shared(pst_driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: None,
    };

/// `--read outlook-csv=PATH`: each Outlook CSV export under PATH.
pub const CSV_FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "outlook-csv",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[sinex_source_core::discovery::Location {
                root: "input",
                path: "outlook-csv",
            }],
            capabilities: &[],
            credentials: &[],
            streams: &[],
            requests: &[],
            request_only: false,
        },
        discover: crate::discovery::discover_format,
        specs: || vec![&CSV],
        plan: Some(|input, settings| {
            Ok(sinex_source_core::binding::per_file(
                &CSV,
                input,
                settings,
                true,
                |p| p.extension().is_some_and(|e| e.eq_ignore_ascii_case("csv")),
            ))
        }),
        source: |_| Ok(Box::new(csv_source())),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::shared(csv_driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: None,
    };

#[cfg(test)]
mod tests;
