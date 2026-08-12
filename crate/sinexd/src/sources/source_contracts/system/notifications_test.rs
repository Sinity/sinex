use super::*;

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::MaterialAnchor;
use sinex_primitives::primitives::Uuid;
use xtask::sandbox::prelude::*;

fn make_ctx(mid: Id<SourceMaterial>) -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("desktop.notification"),
        source_material_id: mid,
        record_anchor: MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index: 0,
        },
        operation_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn make_notification_record(mid: Id<SourceMaterial>, payload: serde_json::Value) -> SourceRecord {
    SourceRecord {
        material_id: mid,
        anchor: MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index: 0,
        },
        bytes: serde_json::to_vec(&payload).unwrap(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::json!({}),
    }
}

/// sinex-audit-notification-dedupkey: the `#[source_meta]` contract declares
/// `occurrence_identity = Uuid5From("(app_name, summary, body, ts)")`, but
/// the actual `OccurrenceKey` built in `parse_record` only carries
/// `["app", "summary"]` -- both the wrong field names (`app` vs `app_name`)
/// and missing `body`/`ts` entirely. Two distinct notifications sharing an
/// app+summary (e.g. repeated "Battery low" alerts, or any templated
/// notification with a fixed title) collide as the same occurrence even
/// though their body/timestamp differ.
#[sinex_test]
#[ignore = "sinex-audit-notification-dedupkey open: notification OccurrenceKey drops body/ts from its own declared identity, colliding on app+summary alone"]
async fn distinct_notifications_with_same_app_and_summary_do_not_collide() -> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let mut parser = NotificationParser;
    let ctx = make_ctx(mid);

    let first = parser
        .parse_record(
            make_notification_record(
                mid,
                serde_json::json!({
                    "app_name": "battery-monitor",
                    "summary": "Battery low",
                    "body": "12% remaining",
                }),
            ),
            &ctx,
        )
        .await
        .unwrap();
    let second = parser
        .parse_record(
            make_notification_record(
                mid,
                serde_json::json!({
                    "app_name": "battery-monitor",
                    "summary": "Battery low",
                    "body": "3% remaining -- plug in now",
                }),
            ),
            &ctx,
        )
        .await
        .unwrap();

    let key_first = first[0].occurrence_key.as_ref().unwrap();
    let key_second = second[0].occurrence_key.as_ref().unwrap();

    assert_ne!(
        key_first, key_second,
        "sinex-audit-notification-dedupkey: two notifications with the same app+summary but \
         different body text must not collide on occurrence identity -- both currently produce \
         {key_first:?}"
    );
    Ok(())
}
