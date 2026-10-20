use super::*;
use crate::{EventPayload, KeyKind, ReplaceRule};

#[test]
fn a_unit_is_keyed_by_archive_and_position_and_never_replaced() {
    assert_eq!(PolylogueRows::EVENT_TYPE, "polylogue.rows");
    assert!(matches!(
        PolylogueRows::REPLACE,
        ReplaceRule::SuppressRepeat
    ));
    assert!(matches!(PolylogueRows::KEY, KeyKind::Fields(_)));
    let schema = PolylogueRows::payload_schema();
    assert!(schema.to_string().contains("position"));
}
