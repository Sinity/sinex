//! Parser for filesystem [`FileDropAdapter`](crate::runtime::parser::FileDropAdapter) records.

use std::os::unix::fs::PermissionsExt;

use crate::runtime::parser::{
    FileDropEventKind, FileDropMoveRole, FileDropRecordMetadata, MaterialParser, ParserError,
};
use async_trait::async_trait;
use sinex_macros::SourceMeta;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};
use sinex_primitives::{
    domain::{EventSource, EventType, RecordedPath},
    events::{
        EventPayload,
        enums::FileModificationType,
        payloads::filesystem::{
            FileCreatedPayload, FileDeletedPayload, FileModifiedPayload, FileMovedPayload,
        },
    },
    parser::{
        InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceId,
        SourceRecord, TimingEvidence,
    },
    privacy::ProcessingContext,
    temporal::Timestamp,
};

const PARSER_ID: ParserId = ParserId::from_static("fs");
const PARSER_VERSION: &str = "1.0.0";

#[derive(Default, SourceMeta)]
#[source_meta(
    id = "fs",
    namespace = "filesystem",
    event_source = "fs-watcher",
    event_type = "file.created",
    event_types = "file.modified, file.deleted, file.moved",
    adapter = "FileContentDropAdapter",
    privacy_tier = PrivacyTier::Secret,
    horizons(Horizon::Continuous, Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Anchor,
    access_scope = AccessScope::ConfiguredRoots,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Metadata,
    resource_profile = ResourceProfile::LiveWatcher,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::Continuous
)]
pub struct FilesystemParser;

#[async_trait]
impl MaterialParser for FilesystemParser {
    type Config = serde_json::Value;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: PARSER_ID,
            parser_version: PARSER_VERSION.into(),
            accepted_input_shapes: vec![InputShapeKind::FileDrop],
            source_id: SourceId::from_static("fs"),
            declared_event_types: vec![
                (FileCreatedPayload::SOURCE, FileCreatedPayload::EVENT_TYPE),
                (FileModifiedPayload::SOURCE, FileModifiedPayload::EVENT_TYPE),
                (FileDeletedPayload::SOURCE, FileDeletedPayload::EVENT_TYPE),
                (FileMovedPayload::SOURCE, FileMovedPayload::EVENT_TYPE),
            ],
            privacy_contexts: vec![ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
            description: "Maps FileDropAdapter records to filesystem event payloads.".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> Result<Vec<ParsedEventIntent>, ParserError> {
        let metadata = FileDropRecordMetadata::from_value(&record.metadata)?;
        let Some(kind) = metadata.event_kind() else {
            return Ok(Vec::new());
        };
        if metadata.content_skipped_reason.as_deref() == Some("oversized") {
            return Ok(Vec::new());
        }

        let timing = record
            .source_ts_hint
            .clone()
            .unwrap_or(TimingEvidence::StagedAtFallback);
        let Some(intent) =
            intent_for_file_drop_record(record, metadata, kind, ctx, ctx.acquisition_time, timing)
                .await?
        else {
            return Ok(Vec::new());
        };
        Ok(vec![intent])
    }
}

async fn intent_for_file_drop_record(
    record: SourceRecord,
    metadata: FileDropRecordMetadata,
    kind: FileDropEventKind,
    ctx: &ParserContext,
    ts: Timestamp,
    timing: TimingEvidence,
) -> Result<Option<ParsedEventIntent>, ParserError> {
    match kind {
        FileDropEventKind::Created => {
            let payload = FileCreatedPayload {
                path: recorded_path(&metadata.path)?,
                size: file_size_hint(&record, &metadata).await,
                created_at: ts,
                permissions: file_permissions_hint(&record).await,
            };
            intent(
                ctx,
                &record,
                FileCreatedPayload::EVENT_TYPE,
                payload,
                ts,
                timing,
            )
            .map(Some)
        }
        FileDropEventKind::Modified => {
            let payload = FileModifiedPayload {
                path: recorded_path(&metadata.path)?,
                size: file_size_hint(&record, &metadata).await,
                modified_at: ts,
                modification_type: FileModificationType::Content,
            };
            intent(
                ctx,
                &record,
                FileModifiedPayload::EVENT_TYPE,
                payload,
                ts,
                timing,
            )
            .map(Some)
        }
        FileDropEventKind::Deleted => {
            let payload = FileDeletedPayload {
                path: recorded_path(&metadata.path)?,
                deleted_at: ts,
            };
            intent(
                ctx,
                &record,
                FileDeletedPayload::EVENT_TYPE,
                payload,
                ts,
                timing,
            )
            .map(Some)
        }
        FileDropEventKind::Moved => moved_intent(ctx, &record, &metadata, ts, timing),
    }
}

fn moved_intent(
    ctx: &ParserContext,
    record: &SourceRecord,
    metadata: &FileDropRecordMetadata,
    ts: Timestamp,
    timing: TimingEvidence,
) -> Result<Option<ParsedEventIntent>, ParserError> {
    if metadata.move_role() == Some(FileDropMoveRole::From) {
        return Ok(None);
    }
    let old_path = metadata.move_from_path.as_deref().unwrap_or(&metadata.path);
    let new_path = metadata.move_to_path.as_deref().unwrap_or(&metadata.path);
    let payload = FileMovedPayload {
        old_path: recorded_path(old_path)?,
        new_path: recorded_path(new_path)?,
        moved_at: ts,
    };
    intent(
        ctx,
        record,
        FileMovedPayload::EVENT_TYPE,
        payload,
        ts,
        timing,
    )
    .map(Some)
}

fn intent<T: serde::Serialize>(
    ctx: &ParserContext,
    record: &SourceRecord,
    event_type: EventType,
    payload: T,
    ts: Timestamp,
    timing: TimingEvidence,
) -> Result<ParsedEventIntent, ParserError> {
    Ok(ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(PARSER_ID)
        .parser_version(PARSER_VERSION)
        .event_type(event_type)
        .event_source(EventSource::from_static("fs-watcher"))
        .payload(serde_json::to_value(payload).map_err(|error| {
            ParserError::Parse(format!("filesystem payload serialization failed: {error}"))
        })?)
        .ts_orig(ts)
        .timing(timing)
        .anchor(record.anchor.clone())
        .privacy_context(ProcessingContext::Metadata)
        .build())
}

fn recorded_path(path: &str) -> Result<RecordedPath, ParserError> {
    RecordedPath::from_observed(path).map_err(ParserError::Field)
}

async fn file_size_hint(record: &SourceRecord, metadata: &FileDropRecordMetadata) -> u64 {
    if let Some(size) = metadata.content_size_bytes {
        return size;
    }
    if let Some(path) = record.logical_path.as_ref()
        && let Ok(metadata) = tokio::fs::metadata(path.as_std_path()).await
        && metadata.is_file()
    {
        return metadata.len();
    }
    std::fs::metadata(&metadata.path)
        .ok()
        .filter(std::fs::Metadata::is_file)
        .map_or(0, |metadata| metadata.len())
}

async fn file_permissions_hint(record: &SourceRecord) -> Option<u32> {
    let path = record.logical_path.as_ref()?;
    let metadata = tokio::fs::metadata(path.as_std_path()).await.ok()?;
    Some(metadata.permissions().mode())
}

#[cfg(test)]
#[path = "parser_test.rs"]
mod tests;
