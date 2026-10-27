//! Document text (`document.text`): a per-event derivation over library copies
//! that reads each stored document's bytes and writes its text, so documents
//! are found by the same search as everything else. A PDF is read with the
//! local PDF reader (`pdftotext`, from Poppler) and gives one event per page;
//! text, Markdown, HTML, Word (`.docx`) and OpenDocument
//! (`.odt`) files are read here, whole. The format comes from the bytes and,
//! for text, the file's extension; a copy that is not a document, or whose
//! bytes were not stored, gives nothing.
//!
//! It lives here rather than in `sinex-derivations` because it runs a local
//! tool. Without the PDF reader, PDFs are counted as failed inputs and read
//! again when a version with the reader runs.
use sinex_derivations::{Env, Input, PerEvent, Reads};
use sinex_model::event_types::library::{DocumentText as DocumentTextPayload, FileCopy};
use sinex_model::{Event, EventPayload, Origin, ReadingOrder};
use std::process::Command;

mod extract;

/// The PDF reader's version line, or why there is none.
pub(crate) fn pdf_reader() -> Result<String, String> {
    let out = Command::new("pdftotext")
        .arg("-v")
        .output()
        .map_err(|e| format!("pdftotext is not available: {e}"))?;
    if !out.status.success() {
        return Err(format!("pdftotext -v exited with {}", out.status));
    }
    let text =
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
    Ok(text.lines().next().unwrap_or("pdftotext").trim().to_owned())
}

/// The text of each page of a PDF.
fn pdf_pages(bytes: &[u8]) -> Result<Vec<String>, String> {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let file = dir.path().join("document.pdf");
    std::fs::write(&file, bytes).map_err(|e| e.to_string())?;
    // At idle priority, like every reader of stored bytes.
    let out = Command::new("nice")
        .args(["-n", "19", "pdftotext", "-enc", "UTF-8"])
        .arg(&file)
        .arg("-")
        .output()
        .map_err(|e| format!("pdftotext: {e}"))?;
    if !out.status.success() {
        return Err(format!("pdftotext exited with {}", out.status));
    }
    let text = String::from_utf8(out.stdout)
        .map_err(|_| "pdftotext returned text that is not UTF-8".to_owned())?;
    // Pages end with a form feed, the last one too.
    let mut pages: Vec<String> = text.split('\u{c}').map(|p| p.trim().to_owned()).collect();
    if pages.last().is_some_and(String::is_empty) {
        pages.pop();
    }
    Ok(pages)
}

pub(crate) struct DocumentText {
    /// The PDF reader's version line, when one is installed.
    pub pdf_reader: Option<String>,
}

impl PerEvent for DocumentText {
    const NAME: &'static str = "document.text";
    /// Bump when the reading of a format changes.
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &[FileCopy::EVENT_TYPE];
    const OUTPUTS: &'static [&'static str] = &[DocumentTextPayload::EVENT_TYPE];
    const READS: Reads = Reads::Blob("content");

    fn version(&self) -> u32 {
        let definition = format!(
            "{}:{}:{}",
            Self::NAME,
            Self::VERSION,
            self.pdf_reader.as_deref().unwrap_or("unavailable")
        );
        let hash = blake3::hash(definition.as_bytes());
        (u32::from_le_bytes(hash.as_bytes()[..4].try_into().expect("four bytes")) >> 2) + 1
    }

    fn reads_for(&self, event: &Event) -> Reads {
        let Some(copy) = event.payload_as::<FileCopy>() else {
            return Reads::Nothing;
        };
        if copy.stored
            && copy.vanished_at.is_none()
            && matches!(
                copy.media_type.as_str(),
                "application/pdf"
                    | "application/zip"
                    | "text/plain"
                    | "text/html"
                    | "application/xhtml+xml"
                    | "text/markdown"
                    | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    | "application/vnd.oasis.opendocument.text"
            )
        {
            Self::READS
        } else {
            Reads::Nothing
        }
    }

    fn derive(&self, input: Input<'_>, _: &Env) -> Result<Vec<Event>, String> {
        let copy_event = input.event;
        let Some(copy) = copy_event.payload_as::<FileCopy>() else {
            return Err("not a library copy".into());
        };
        let Some(bytes) = input.bytes else {
            return Ok(Vec::new());
        };
        let Some(format) = extract::format(&copy.media_type, &copy.path, bytes) else {
            return Ok(Vec::new());
        };
        let (reader, parts): (String, Vec<(u64, Option<String>, String)>) = match format {
            extract::Format::Pdf => {
                let reader = self
                    .pdf_reader
                    .clone()
                    .ok_or("a PDF needs pdftotext, which is not installed")?;
                let pages = pdf_pages(bytes)?;
                (
                    reader,
                    pages
                        .into_iter()
                        .enumerate()
                        .map(|(i, text)| {
                            (
                                u64::try_from(i + 1).expect("page count fits u64"),
                                None,
                                text,
                            )
                        })
                        .collect(),
                )
            }
            other => {
                let (title, text) = extract::text(other, bytes)?;
                ("sinexd".to_owned(), vec![(0, title, text)])
            }
        };
        parts
            .into_iter()
            .filter(|(_, _, text)| !text.is_empty())
            .map(|(part, title, text)| {
                let payload = DocumentTextPayload {
                    content: copy.content.clone(),
                    path: copy.path.clone(),
                    format: format.name().to_owned(),
                    part,
                    title,
                    text,
                    reader: reader.clone(),
                    modified_at: copy.modified_at,
                };
                let mut hasher = blake3::Hasher::new();
                hasher.update(&serde_json::to_vec(&payload).expect("payloads serialize"));
                hasher.update(copy_event.id.0.as_bytes());
                Event::build(
                    &payload,
                    Self::NAME,
                    Origin::Derived {
                        parents: vec![copy_event.id],
                    },
                    // The runner sets the reading order from the version and
                    // the input's commit position.
                    ReadingOrder(0),
                    *hasher.finalize().as_bytes(),
                    // A document's text is as private as the copy.
                    copy_event.class,
                )
                .map_err(|e| e.to_string())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests;
