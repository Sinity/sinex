use super::{DlqMessagePeek, DlqPeekResponse};
use xtask::sandbox::prelude::{TestResult, sinex_test};

#[sinex_test]
async fn dlq_groups_use_error_code_from_truncated_structured_preview() -> TestResult<()> {
    let response = DlqPeekResponse::from_messages(vec![
        DlqMessagePeek {
            subject: "dev.events.dlq.event_engine".to_string(),
            sequence: 2928,
            retry_count: 0,
            original_subject: None,
            payload_preview:
                "{\"error\":\"buffered_slice_limit_exceeded\",\"material_id\":\"019f22f2\",..."
                    .to_string(),
            payload_redacted: false,
            privacy_caveats: Vec::new(),
        },
        DlqMessagePeek {
            subject: "dev.events.dlq.event_engine".to_string(),
            sequence: 2929,
            retry_count: 0,
            original_subject: None,
            payload_preview:
                "{\"error\": \"orphaned_sensing_material\", \"material_id\": \"019f22d3\",..."
                    .to_string(),
            payload_redacted: false,
            privacy_caveats: Vec::new(),
        },
    ]);

    assert_eq!(
        response.groups[0].reason_bucket,
        "error_payload.buffered_slice_limit_exceeded"
    );
    assert_eq!(
        response.groups[1].reason_bucket,
        "error_payload.orphaned_sensing_material"
    );
    Ok(())
}
