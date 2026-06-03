//! Parser for filesystem [`FileDropAdapter`](crate::node_sdk::parser::FileDropAdapter) records.

use std::os::unix::fs::PermissionsExt;

use crate::node_sdk::parser::{
    FileDropEventKind, FileDropMoveRole, FileDropRecordMetadata, MaterialParser, ParserError,
};
use async_trait::async_trait;
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
        InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceRecord,
        SourceUnitId, TimingEvidence,
    },
    privacy::ProcessingContext,
    temporal::Timestamp,
};

const PARSER_ID: ParserId = ParserId::from_static("fs");
const PARSER_VERSION: &str = "1.0.0";

#[derive(Default)]
pub struct FilesystemParser;

#[async_trait]
impl MaterialParser for FilesystemParser {
    type Config = serde_json::Value;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: PARSER_ID,
            parser_version: PARSER_VERSION.into(),
            accepted_input_shapes: vec![InputShapeKind::FileDrop],
            source_unit_id: SourceUnitId::from_static("fs"),
            declared_event_types: vec![
                (FileCreatedPayload::SOURCE, FileCreatedPayload::EVENT_TYPE),
                (FileModifiedPayload::SOURCE, FileModifiedPayload::EVENT_TYPE),
                (FileDeletedPayload::SOURCE, FileDeletedPayload::EVENT_TYPE),
                (FileMovedPayload::SOURCE, FileMovedPayload::EVENT_TYPE),
            ],
            privacy_contexts: vec![ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
            proof_obligations: vec![
                "filesystem_event_kind_dispatch".into(),
                "privacy_context_declared".into(),
            ],
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
        .source_unit_id(ctx.source_unit_id.clone())
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
mod tests {
    use camino::Utf8PathBuf;
    use sinex_primitives::{
        Id, Uuid,
        events::SourceMaterial,
        parser::{MaterialAnchor, ParserContext},
    };
    use xtask::sandbox::prelude::sinex_test;

    use super::*;

    fn parser_context() -> ParserContext {
        let material_id = Id::from_uuid(Uuid::now_v7());
        ParserContext {
            source_unit_id: SourceUnitId::from_static("fs"),
            source_material_id: material_id,
            record_anchor: MaterialAnchor::DirectoryEntry {
                path: Utf8PathBuf::from("/tmp/sinex-test"),
                content_hash: None,
            },
            operation_id: Uuid::now_v7(),
            job_id: Uuid::now_v7(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn record(metadata: serde_json::Value, path: &str) -> SourceRecord {
        let path = Utf8PathBuf::from(path);
        SourceRecord {
            material_id: Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
            anchor: MaterialAnchor::DirectoryEntry {
                path: path.clone(),
                content_hash: None,
            },
            bytes: path.as_str().as_bytes().to_vec(),
            logical_path: Some(path),
            source_ts_hint: None,
            metadata,
        }
    }

    #[sinex_test]
    async fn filesystem_parser_maps_created_content_record() -> xtask::sandbox::TestResult<()> {
        let metadata = serde_json::json!({
            "event_kind": "Created",
            "path": "/tmp/sinex-created.txt",
            "content_materialized": true,
            "content_size_bytes": 13
        });
        let mut parser = FilesystemParser;
        let intents = parser
            .parse_record(
                record(metadata, "/tmp/sinex-created.txt"),
                &parser_context(),
            )
            .await?;

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].event_type, FileCreatedPayload::EVENT_TYPE);
        assert_eq!(intents[0].event_source, FileCreatedPayload::SOURCE);
        assert_eq!(intents[0].payload["path"], "/tmp/sinex-created.txt");
        assert_eq!(intents[0].payload["size"], 13);
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_parser_maps_modified_content_record() -> xtask::sandbox::TestResult<()> {
        let metadata = serde_json::json!({
            "event_kind": "Modified",
            "path": "/tmp/sinex-modified.txt",
            "content_materialized": true,
            "content_size_bytes": 21
        });
        let mut parser = FilesystemParser;
        let intents = parser
            .parse_record(
                record(metadata, "/tmp/sinex-modified.txt"),
                &parser_context(),
            )
            .await?;

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].event_type, FileModifiedPayload::EVENT_TYPE);
        assert_eq!(intents[0].event_source, FileModifiedPayload::SOURCE);
        assert_eq!(intents[0].payload["path"], "/tmp/sinex-modified.txt");
        assert_eq!(intents[0].payload["size"], 21);
        assert_eq!(intents[0].payload["modification_type"], "content");
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_parser_maps_deleted_observation_record() -> xtask::sandbox::TestResult<()> {
        let metadata = serde_json::json!({
            "event_kind": "Deleted",
            "path": "/tmp/sinex-deleted.txt"
        });
        let mut parser = FilesystemParser;
        let intents = parser
            .parse_record(
                record(metadata, "/tmp/sinex-deleted.txt"),
                &parser_context(),
            )
            .await?;

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].event_type, FileDeletedPayload::EVENT_TYPE);
        assert_eq!(intents[0].event_source, FileDeletedPayload::SOURCE);
        assert_eq!(intents[0].payload["path"], "/tmp/sinex-deleted.txt");
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_parser_skips_oversized_content_records() -> xtask::sandbox::TestResult<()> {
        let metadata = serde_json::json!({
            "event_kind": "Created",
            "path": "/tmp/sinex-oversized.txt",
            "content_materialized": false,
            "content_size_bytes": 10485761_u64,
            "content_skipped_reason": "oversized"
        });
        let mut parser = FilesystemParser;
        let intents = parser
            .parse_record(
                record(metadata, "/tmp/sinex-oversized.txt"),
                &parser_context(),
            )
            .await?;

        assert!(intents.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_parser_emits_one_event_for_paired_move_to_record()
    -> xtask::sandbox::TestResult<()> {
        let metadata = serde_json::json!({
            "event_kind": "Moved",
            "path": "/tmp/new.txt",
            "move_from_path": "/tmp/old.txt",
            "move_to_path": "/tmp/new.txt",
            "move_role": "to"
        });
        let mut parser = FilesystemParser;
        let intents = parser
            .parse_record(record(metadata, "/tmp/new.txt"), &parser_context())
            .await?;

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].event_type, FileMovedPayload::EVENT_TYPE);
        assert_eq!(intents[0].payload["old_path"], "/tmp/old.txt");
        assert_eq!(intents[0].payload["new_path"], "/tmp/new.txt");
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_parser_skips_paired_move_from_record() -> xtask::sandbox::TestResult<()> {
        let metadata = serde_json::json!({
            "event_kind": "Moved",
            "path": "/tmp/old.txt",
            "move_from_path": "/tmp/old.txt",
            "move_to_path": "/tmp/new.txt",
            "move_role": "from"
        });
        let mut parser = FilesystemParser;
        let intents = parser
            .parse_record(record(metadata, "/tmp/old.txt"), &parser_context())
            .await?;

        assert!(intents.is_empty());
        Ok(())
    }
}
