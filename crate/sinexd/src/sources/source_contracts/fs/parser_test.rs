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
        source_id: SourceId::from_static("fs"),
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
