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

/// sinex-xfz3: `ts_orig` is `Timestamp::now()` called by the PARSER itself --
/// there is no genuine intrinsic timestamp field anywhere in the D-Bus
/// `notification.sent` signal payload. Sibling parsers facing the identical
/// no-real-intrinsic-time situation in the same directory (udev.rs, dbus.rs)
/// correctly tag this `Atemporal`. Tagging it `Intrinsic` actively blocks
/// admission's `raw.temporal_ledger` resolution from ever correcting it, so
/// on replay the persisted `ts_orig` silently becomes "when replay ran."
#[sinex_test]
#[ignore = "sinex-xfz3 open: NotificationParser fabricates ts_orig via \
            Timestamp::now() and falsely tags it Intrinsic instead of \
            Atemporal, unlike sibling udev.rs/dbus.rs parsers"]
async fn notification_sent_ts_orig_is_atemporal_not_fabricated_intrinsic() -> TestResult<()> {
    let mid = Id::<SourceMaterial>::new();
    let record = make_record(
        mid,
        serde_json::json!({
            "app_name": "test-app",
            "summary": "hello",
            "body": "world",
            "urgency": 1,
            "timeout": -1,
            "actions": [],
            "hints": {},
        }),
    );

    let mut parser = NotificationParser;
    let ctx = make_ctx(mid);
    let intents = parser.parse_record(record, &ctx).await.unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(
        intents[0].timing,
        TimingEvidence::Atemporal,
        "no genuine intrinsic timestamp exists in the D-Bus signal payload -- the \
         parser injects Timestamp::now() itself, so this must be Atemporal (matching \
         udev.rs/dbus.rs for the same situation), not Intrinsic"
    );
    Ok(())
}
