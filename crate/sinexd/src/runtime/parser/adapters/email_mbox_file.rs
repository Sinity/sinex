//! Streaming adapter for staged email MBOX files and Takeout archives.
//!
//! General file-content drops intentionally cap materialized payload size.
//! Takeout and local MBOX exports can be gigabytes, so email needs an adapter
//! that walks containers and yields one RFC822 message record at a time.

use async_trait::async_trait;
use camino::Utf8PathBuf;
use futures::StreamExt;
use futures::stream::BoxStream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

use crate::runtime::parser::{InputShapeAdapter, ParserError, ParserResult};

const META_MAILBOX_FORMAT: &str = "mailbox_format";
const META_MBOX_MESSAGE_INDEX: &str = "mbox_message_index";
const META_MBOX_FILE: &str = "mbox_file";
const META_MBOX_BYTE_START: &str = "mbox_byte_start";
const META_MBOX_BYTE_END: &str = "mbox_byte_end";
const META_MBOX_NEXT_BYTE_OFFSET: &str = "mbox_next_byte_offset";
const META_FOLDER: &str = "folder";
const META_ARCHIVE_FILE: &str = "archive_file";
const DEFAULT_MAX_MESSAGE_BYTES: u64 = 64 * 1024 * 1024;
const RECORD_CHANNEL_CAPACITY: usize = 16;

/// Adapter for staged MBOX/MBOXRD files and MBOX entries inside Takeout archives.
#[derive(Debug, Clone, Default)]
pub struct EmailMboxFileAdapter;

/// Configuration for [`EmailMboxFileAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EmailMboxFileConfig {
    /// MBOX files to scan.
    #[serde(default)]
    #[schemars(with = "Vec<String>")]
    pub paths: Vec<Utf8PathBuf>,
    /// ZIP or tar.gz/tgz archives containing MBOX entries, such as Google Takeout.
    #[serde(default)]
    #[schemars(with = "Vec<String>")]
    pub archive_paths: Vec<Utf8PathBuf>,
    /// Optional folder/mailbox label to stamp on every emitted message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder: Option<String>,
    /// Maximum RFC822 message payload accepted from the MBOX container.
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: u64,
}

/// Cursor after the last emitted MBOX message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmailMboxFileCursor {
    pub path: String,
    pub next_byte_offset: u64,
}

fn default_max_message_bytes() -> u64 {
    DEFAULT_MAX_MESSAGE_BYTES
}

#[async_trait]
impl InputShapeAdapter for EmailMboxFileAdapter {
    type Config = EmailMboxFileConfig;
    type Cursor = EmailMboxFileCursor;
    const KIND: InputShapeKind = InputShapeKind::Archive;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let paths = config.paths.clone();
        let archive_paths = config.archive_paths.clone();
        let folder = config.folder.clone();
        let max_message_bytes = config.max_message_bytes;
        let cursor = cursor.clone();
        let (tx, rx) = mpsc::channel(RECORD_CHANNEL_CAPACITY);

        tokio::task::spawn_blocking(move || {
            let mut emit = |record: ParserResult<SourceRecord>| tx.blocking_send(record).is_ok();
            for path in paths {
                if !stream_mbox_path(
                    material_id,
                    &path,
                    None,
                    folder.as_deref(),
                    max_message_bytes,
                    cursor.as_ref(),
                    &mut emit,
                ) {
                    return;
                }
            }
            for archive_path in archive_paths {
                let keep_going = match archive_kind(&archive_path) {
                    Some(EmailArchiveKind::Zip) => stream_zip_archive(
                        material_id,
                        &archive_path,
                        folder.as_deref(),
                        max_message_bytes,
                        cursor.as_ref(),
                        &mut emit,
                    ),
                    Some(EmailArchiveKind::TarGz) => stream_tar_gz_archive(
                        material_id,
                        &archive_path,
                        folder.as_deref(),
                        max_message_bytes,
                        cursor.as_ref(),
                        &mut emit,
                    ),
                    None => emit(Err(ParserError::Adapter(format!(
                        "unsupported email archive format: {archive_path}"
                    )))),
                };
                if !keep_going {
                    return;
                }
            }
        });

        Ok(ReceiverStream::new(rx).boxed())
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        let path = record
            .metadata
            .get(META_MBOX_FILE)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ParserError::Cursor("MBOX record missing mbox_file metadata".into()))?
            .to_string();
        let next_byte_offset = record
            .metadata
            .get(META_MBOX_NEXT_BYTE_OFFSET)
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                ParserError::Cursor("MBOX record missing mbox_next_byte_offset metadata".into())
            })?;
        Ok(EmailMboxFileCursor {
            path,
            next_byte_offset,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmailArchiveKind {
    Zip,
    TarGz,
}

fn archive_kind(path: &Utf8PathBuf) -> Option<EmailArchiveKind> {
    let lower = path.as_str().to_ascii_lowercase();
    if lower.ends_with(".zip") {
        return Some(EmailArchiveKind::Zip);
    }
    if lower.ends_with(".tar.gz") || lower.ends_with(".tgz") {
        return Some(EmailArchiveKind::TarGz);
    }
    None
}

fn stream_zip_archive(
    material_id: Id<SourceMaterial>,
    archive_path: &Utf8PathBuf,
    folder: Option<&str>,
    max_message_bytes: u64,
    cursor: Option<&EmailMboxFileCursor>,
    emit: &mut impl FnMut(ParserResult<SourceRecord>) -> bool,
) -> bool {
    let file = match File::open(archive_path.as_std_path()) {
        Ok(file) => file,
        Err(error) => return emit(Err(ParserError::Io(error))),
    };
    let mut archive = match zip::ZipArchive::new(file) {
        Ok(archive) => archive,
        Err(error) => {
            return emit(Err(ParserError::Adapter(format!(
                "failed to read ZIP archive {archive_path}: {error}"
            ))));
        }
    };

    for index in 0..archive.len() {
        let mut entry = match archive.by_index(index) {
            Ok(entry) => entry,
            Err(error) => {
                return emit(Err(ParserError::Adapter(format!(
                    "failed to read ZIP entry {index} in {archive_path}: {error}"
                ))));
            }
        };
        if !entry.is_file() || !entry.name().ends_with(".mbox") {
            continue;
        }
        let entry_path = Utf8PathBuf::from(entry.name());
        let path_key = archive_entry_key(archive_path, &entry_path);
        if path_before_cursor(&path_key, cursor) {
            continue;
        }
        let skip_until = cursor
            .filter(|cursor| cursor.path == path_key)
            .map_or(0, |cursor| cursor.next_byte_offset);
        let mut reader = BufReader::new(&mut entry);
        if !stream_mbox_reader(
            material_id,
            &entry_path,
            Some(archive_path),
            folder,
            max_message_bytes,
            skip_until,
            &path_key,
            &mut reader,
            emit,
        ) {
            return false;
        }
    }
    true
}

fn stream_tar_gz_archive(
    material_id: Id<SourceMaterial>,
    archive_path: &Utf8PathBuf,
    folder: Option<&str>,
    max_message_bytes: u64,
    cursor: Option<&EmailMboxFileCursor>,
    emit: &mut impl FnMut(ParserResult<SourceRecord>) -> bool,
) -> bool {
    let file = match File::open(archive_path.as_std_path()) {
        Ok(file) => file,
        Err(error) => return emit(Err(ParserError::Io(error))),
    };
    let decoder = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);
    let entries = match archive.entries() {
        Ok(entries) => entries,
        Err(error) => {
            return emit(Err(ParserError::Adapter(format!(
                "failed to read tar.gz archive {archive_path}: {error}"
            ))));
        }
    };
    for entry in entries {
        let mut entry = match entry {
            Ok(entry) => entry,
            Err(error) => return emit(Err(ParserError::Io(error))),
        };
        let path = match entry.path() {
            Ok(path) => path.into_owned(),
            Err(error) => return emit(Err(ParserError::Io(error))),
        };
        let Ok(path) = Utf8PathBuf::from_path_buf(path) else {
            continue;
        };
        if !path.as_str().ends_with(".mbox") {
            continue;
        }
        let path_key = archive_entry_key(archive_path, &path);
        if path_before_cursor(&path_key, cursor) {
            continue;
        }
        let skip_until = cursor
            .filter(|cursor| cursor.path == path_key)
            .map_or(0, |cursor| cursor.next_byte_offset);
        let mut reader = BufReader::new(&mut entry);
        if !stream_mbox_reader(
            material_id,
            &path,
            Some(archive_path),
            folder,
            max_message_bytes,
            skip_until,
            &path_key,
            &mut reader,
            emit,
        ) {
            return false;
        }
    }
    true
}

fn stream_mbox_path(
    material_id: Id<SourceMaterial>,
    path: &Utf8PathBuf,
    archive_path: Option<&Utf8PathBuf>,
    folder: Option<&str>,
    max_message_bytes: u64,
    cursor: Option<&EmailMboxFileCursor>,
    emit: &mut impl FnMut(ParserResult<SourceRecord>) -> bool,
) -> bool {
    let path_key = path.to_string();
    if path_before_cursor(&path_key, cursor) {
        return true;
    }
    let skip_until = cursor
        .filter(|cursor| cursor.path == path_key)
        .map_or(0, |cursor| cursor.next_byte_offset);
    let file = match File::open(path.as_std_path()) {
        Ok(file) => file,
        Err(error) => return emit(Err(ParserError::Io(error))),
    };
    let mut reader = BufReader::new(file);
    stream_mbox_reader(
        material_id,
        path,
        archive_path,
        folder,
        max_message_bytes,
        skip_until,
        &path_key,
        &mut reader,
        emit,
    )
}

fn stream_mbox_reader<R: BufRead>(
    material_id: Id<SourceMaterial>,
    path: &Utf8PathBuf,
    archive_path: Option<&Utf8PathBuf>,
    folder: Option<&str>,
    max_message_bytes: u64,
    skip_until: u64,
    path_key: &str,
    reader: &mut R,
    emit: &mut impl FnMut(ParserResult<SourceRecord>) -> bool,
) -> bool {
    let mut byte_offset = 0_u64;
    let mut message_start: Option<u64> = None;
    let mut message_index = 0_u64;
    let mut message_bytes = Vec::new();
    let mut line = Vec::new();

    loop {
        line.clear();
        let read = match reader.read_until(b'\n', &mut line) {
            Ok(read) => read,
            Err(error) => return emit(Err(ParserError::Io(error))),
        };
        if read == 0 {
            if let Some(start) = message_start {
                return emit_mbox_record(
                    material_id,
                    path,
                    archive_path,
                    folder,
                    message_index,
                    start,
                    byte_offset,
                    byte_offset,
                    &message_bytes,
                    skip_until,
                    path_key,
                    emit,
                );
            }
            return true;
        }

        let line_start = byte_offset;
        byte_offset += read as u64;

        if line.starts_with(b"From ") {
            if let Some(start) = message_start {
                if !emit_mbox_record(
                    material_id,
                    path,
                    archive_path,
                    folder,
                    message_index,
                    start,
                    line_start,
                    line_start,
                    &message_bytes,
                    skip_until,
                    path_key,
                    emit,
                ) {
                    return false;
                }
                message_index += 1;
            }
            message_start = Some(byte_offset);
            message_bytes.clear();
            continue;
        }

        if message_start.is_some() {
            let next_len = message_bytes.len() as u64 + read as u64;
            if next_len > max_message_bytes {
                return emit(Err(ParserError::Adapter(format!(
                    "MBOX message in {path_key} exceeded max_message_bytes={max_message_bytes}"
                ))));
            }
            message_bytes.extend_from_slice(&line);
        }
    }
}

fn emit_mbox_record(
    material_id: Id<SourceMaterial>,
    path: &Utf8PathBuf,
    archive_path: Option<&Utf8PathBuf>,
    folder: Option<&str>,
    message_index: u64,
    start: u64,
    end: u64,
    next_byte_offset: u64,
    message_bytes: &[u8],
    skip_until: u64,
    path_key: &str,
    emit: &mut impl FnMut(ParserResult<SourceRecord>) -> bool,
) -> bool {
    match build_mbox_record(
        material_id,
        path,
        archive_path,
        folder,
        message_index,
        start,
        end,
        next_byte_offset,
        message_bytes,
        skip_until,
        path_key,
    ) {
        Ok(Some(record)) => emit(Ok(record)),
        Ok(None) => true,
        Err(error) => emit(Err(error)),
    }
}

fn archive_entry_key(archive_path: &Utf8PathBuf, entry_path: &Utf8PathBuf) -> String {
    format!("{archive_path}::{entry_path}")
}

fn path_before_cursor(path: &str, cursor: Option<&EmailMboxFileCursor>) -> bool {
    let Some(cursor) = cursor else {
        return false;
    };
    path < cursor.path.as_str()
}

fn build_mbox_record(
    material_id: Id<SourceMaterial>,
    path: &Utf8PathBuf,
    archive_path: Option<&Utf8PathBuf>,
    folder: Option<&str>,
    message_index: u64,
    start: u64,
    end: u64,
    next_byte_offset: u64,
    message_bytes: &[u8],
    skip_until: u64,
    path_key: &str,
) -> ParserResult<Option<SourceRecord>> {
    if end <= skip_until {
        return Ok(None);
    }

    let trimmed = trim_trailing_newlines(message_bytes);
    if trimmed.is_empty() {
        return Ok(None);
    }
    let len = trimmed.len() as u64;
    let byte_end = start + len;
    let folder = folder
        .map(str::to_string)
        .or_else(|| mbox_folder_from_path(path));

    let mut metadata = serde_json::Map::new();
    metadata.insert(META_MAILBOX_FORMAT.into(), serde_json::json!("mbox-staged"));
    metadata.insert(
        META_MBOX_MESSAGE_INDEX.into(),
        serde_json::json!(message_index),
    );
    metadata.insert(META_MBOX_FILE.into(), serde_json::json!(path_key));
    metadata.insert(META_MBOX_BYTE_START.into(), serde_json::json!(start));
    metadata.insert(META_MBOX_BYTE_END.into(), serde_json::json!(byte_end));
    metadata.insert(
        META_MBOX_NEXT_BYTE_OFFSET.into(),
        serde_json::json!(next_byte_offset),
    );
    if let Some(folder) = folder {
        metadata.insert(META_FOLDER.into(), serde_json::json!(folder));
    }
    if let Some(archive_path) = archive_path {
        metadata.insert(
            META_ARCHIVE_FILE.into(),
            serde_json::json!(archive_path.to_string()),
        );
    }

    Ok(Some(SourceRecord {
        material_id,
        anchor: MaterialAnchor::ByteRange { start, len },
        bytes: trimmed.to_vec(),
        logical_path: Some(path.clone()),
        source_ts_hint: None,
        metadata: serde_json::Value::Object(metadata),
    }))
}

fn trim_trailing_newlines(bytes: &[u8]) -> &[u8] {
    let mut end = bytes.len();
    while end > 0 && matches!(bytes[end - 1], b'\n' | b'\r') {
        end -= 1;
    }
    &bytes[..end]
}

fn mbox_folder_from_path(path: &Utf8PathBuf) -> Option<String> {
    path.file_stem()
        .or_else(|| path.file_name())
        .filter(|name| !name.is_empty())
        .map(str::to_string)
}
