use super::*;

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::MaterialAnchor;
use sinex_primitives::primitives::Uuid;
use xtask::sandbox::prelude::*;

fn make_ctx(mid: Id<SourceMaterial>) -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("desktop.notification.action"),
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

fn make_record(mid: Id<SourceMaterial>, body: serde_json::Value) -> SourceRecord {
    SourceRecord {
        material_id: mid,
        anchor: MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index: 0,
        },
        bytes: serde_json::to_vec(&body).unwrap(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::json!({}),
    }
}

/// sinex-xfz3: same fabricated-Intrinsic-timestamp bug as notifications.rs,
/// same directory, same sibling-parser asymmetry with udev.rs/dbus.rs.
#[sinex_test]
#[ignore = "sinex-xfz3 open: NotificationActionParser fabricates ts_orig via \
            Timestamp::now() and falsely tags it Intrinsic instead of Atemporal"]
async fn notification_action_ts_orig_is_atemporal_not_fabricated_intrinsic() -> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let record = make_record(
        mid,
        serde_json::json!({
            "notification_id": 7,
            "action_key": "default",
        }),
    );

    let mut parser = NotificationActionParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await.unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(
        intents[0].timing,
        TimingEvidence::Atemporal,
        "no genuine intrinsic timestamp exists in the D-Bus signal payload -- must be \
         Atemporal, not Intrinsic"
    );
    Ok(())
}
