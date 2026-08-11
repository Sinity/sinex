use super::*;
use crate::events::builder::{OffsetKind, Provenance};
use crate::ids::Id;
use serde_json::json;

/// sinex-k5im: `DynamicPayload::into_event` must defer to temporal-ledger
/// resolution for Material-provenance events (leave `ts_orig = None`), the
/// same contract `EventPayload::into_event` upholds via the builder. It
/// currently bypasses the builder entirely via `Event::new_json`, which
/// unconditionally stamps `ts_orig = Some(Timestamp::now())` regardless of
/// provenance kind.
#[test]
#[ignore = "sinex-k5im open: DynamicPayload::into_event stamps wall-clock ts_orig even for Material provenance, violating the #1570 Prong B deferred-resolution contract"]
fn dynamic_payload_into_event_defers_ts_orig_for_material_provenance() {
    let payload = DynamicPayload::new("test.source", "test.type", json!({"k": "v"}));
    let provenance = Provenance::Material {
        id: Id::from(crate::primitives::Uuid::now_v7()),
        anchor_byte: 0,
        offset_start: None,
        offset_end: None,
        offset_kind: OffsetKind::Byte,
    };

    let event = payload.into_event(provenance);

    assert!(
        event.ts_orig.is_none(),
        "Material-provenance events must leave ts_orig unset so admission can resolve it \
         from the source-material temporal ledger; got {:?}",
        event.ts_orig
    );
}
