use super::*;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{MaterialAnchor, SourceRecord};
use sinex_primitives::primitives::Uuid;
use xtask::sandbox::prelude::*;

fn parser_context() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("terminal.bash-history"),
        source_material_id: Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
        record_anchor: MaterialAnchor::Line {
            line: 1,
            byte_start: 0,
        },
        operation_id: Uuid::now_v7(),
        job_id: Uuid::now_v7(),
        host: "test-host".into(),
        acquisition_time: sinex_primitives::temporal::Timestamp::now(),
    }
}

fn line_record(line: u64, text: &str) -> SourceRecord {
    SourceRecord {
        material_id: Id::<SourceMaterial>::from_uuid(Uuid::nil()),
        anchor: MaterialAnchor::Line {
            line,
            byte_start: line.saturating_sub(1) * 10,
        },
        bytes: text.as_bytes().to_vec(),
        logical_path: Some("/home/sinity/.bash_history".into()),
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    }
}

#[sinex_test]
async fn checkpointed_dedup_window_suppresses_rotation_overlap_after_restart() -> TestResult<()> {
    let ctx = parser_context();
    let mut parser = BashHistoryParser::default();

    let first = parser.parse_record(line_record(1, "echo one"), &ctx).await?;
    assert_eq!(first.len(), 1);
    let checkpoint = parser
        .checkpoint_state()?
        .ok_or_else(|| SinexError::validation("bash parser should checkpoint dedup state"))?;

    let mut restarted = BashHistoryParser::default();
    restarted.restore_checkpoint_state(Some(&checkpoint))?;

    let duplicate = restarted
        .parse_record(line_record(1, "echo one"), &ctx)
        .await?;
    assert!(
        duplicate.is_empty(),
        "restart after rotation must not re-emit a command retained in the dedup checkpoint"
    );

    let next = restarted.parse_record(line_record(2, "echo two"), &ctx).await?;
    assert_eq!(next.len(), 1);
    Ok(())
}
