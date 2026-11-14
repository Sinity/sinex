//! Document text (`document.text`): a per-event derivation over library copies
//! that reads each stored document's bytes and writes its text, so documents
//! are found by the same search as everything else. A PDF is read with the
//! local PDF reader (`pdftotext`, from Poppler) and gives one event per page;
//! text, Markdown, HTML, Word (`.docx`) and OpenDocument (`.odt`) files are
//! read here, a Word or OpenDocument file in pieces rather than decompressed
//! whole into memory. The format comes from the bytes and, for text, the
//! file's extension; a copy that is not a document, or whose
//! bytes were not stored, gives nothing.
//!
//! It lives here rather than in `sinex-derivations` because it runs a local
//! tool. Without the PDF reader, PDFs are counted as failed inputs and read
//! again when a version with the reader runs.
use crate::{Error, Result};
use sinex_derivations::{Env, Input, PerEvent, Reads};
use sinex_model::event_types::library::{DocumentText as DocumentTextPayload, FileCopy};
use sinex_model::{Event, EventPayload, Origin, ReadingOrder};
use std::process::Command;

mod extract;

/// The PDF reader's version line, or why there is none.
pub(crate) fn pdf_reader() -> Result<String> {
    let out = Command::new("pdftotext")
        .arg("-v")
        .output()
        .map_err(|e| Error::from(e).within("pdftotext is not available"))?;
    if !out.status.success() {
        return Err(Error::internal(format!(
            "pdftotext -v exited with {}",
            out.status
        )));
    }
    let text =
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
    Ok(text.lines().next().unwrap_or("pdftotext").trim().to_owned())
}

/// How long the PDF reader may take on a document of `len` bytes: a minute,
/// plus ten seconds per MiB. A reader still running after that is stuck on
/// this document, and the document counts as a failed input.
pub(crate) fn pdf_time(len: usize) -> std::time::Duration {
    std::time::Duration::from_secs(60 + 10 * (len as u64).div_ceil(1024 * 1024))
}

/// The text of each page of a PDF.
fn pdf_pages(bytes: &[u8]) -> Result<Vec<String>> {
    pdf_pages_within(bytes, pdf_time(bytes.len()), "pdftotext")
}

fn pdf_pages_within(bytes: &[u8], limit: std::time::Duration, reader: &str) -> Result<Vec<String>> {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("document.pdf");
    let text_file = dir.path().join("document.txt");
    std::fs::write(&file, bytes)?;
    // At idle priority, like every reader of stored bytes. The text goes to
    // a file, so waiting never depends on draining a pipe.
    let mut child = Command::new("nice")
        .args(["-n", "19", reader, "-enc", "UTF-8"])
        .arg(&file)
        .arg(&text_file)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|e| Error::from(e).within("pdftotext"))?;
    let deadline = std::time::Instant::now() + limit;
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|e| Error::from(e).within("pdftotext"))?
        {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(Error::internal(format!(
                "pdftotext did not finish a {} byte PDF within {} s",
                bytes.len(),
                limit.as_secs()
            )));
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    };
    if !status.success() {
        return Err(Error::internal(format!("pdftotext exited with {status}")));
    }
    let text = String::from_utf8(std::fs::read(&text_file)?)
        .map_err(|_| Error::internal("pdftotext returned text that is not UTF-8"))?;
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
    const VERSION: u32 = 2;
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

    fn derive(&self, input: Input<'_>, _: &Env) -> std::result::Result<Vec<Event>, String> {
        self.read_events(input).map_err(|error| error.to_string())
    }
}

impl DocumentText {
    fn read_events(&self, input: Input<'_>) -> Result<Vec<Event>> {
        let copy_event = input.event;
        let Some(copy) = copy_event.payload_as::<FileCopy>() else {
            return Err(Error::internal("not a library copy"));
        };
        let Some(bytes) = input.bytes else {
            return if self.reads_for(copy_event) == Reads::Nothing {
                Ok(Vec::new())
            } else {
                Err(Error::internal("the stored document's bytes were not read"))
            };
        };
        let Some(format) = extract::format(&copy.media_type, &copy.path, bytes) else {
            return Ok(Vec::new());
        };
        let (reader, parts): (String, Vec<(u64, Option<String>, String)>) = match format {
            extract::Format::Pdf => {
                let reader = self.pdf_reader.clone().ok_or_else(|| {
                    Error::not_configured("a PDF needs pdftotext, which is not installed")
                })?;
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
                    record: serde_json::to_string(&(
                        &copy_event.source,
                        &copy_event.event_type,
                        sinex_model::record::identity(copy_event),
                    ))
                    .expect("record identity serializes"),
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
                .map_err(Error::from)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests;
