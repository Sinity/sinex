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

fn manifest_for(kind: &str) -> serde_json::Value {
    let mut value = serde_json::json!({"version":1,"replacements":[{
        "unit":{"commit_seq":1,"kind":kind,"key":"note1"},
        "batch":null,"bytes":0,"rows":0,"blobs":[]
    }],"retired_batches":[]});
    value.sort_all_objects();
    let hash = blake3::hash(&serde_json::to_vec(&value).unwrap());
    value["digest"] = format!("blake3:{hash}").into();
    value
}

#[test]
fn excision_metadata_checks_the_exact_mapping_before_admission() {
    let value = manifest_for("note");
    let decoded: ExcisionPublication = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(decoded.replacements[0].unit.key, "note1");
    for field in ["key", "kind", "commit_seq"] {
        let mut changed = value.clone();
        changed["replacements"][0]["unit"][field] = serde_json::json!("changed");
        assert!(serde_json::from_value::<ExcisionPublication>(changed).is_err());
    }
    let mut body = value;
    body["replacements"][0]["batch"] = "blake3:bad".into();
    assert!(serde_json::from_value::<ExcisionPublication>(body).is_err());
}

#[test]
fn original_rows_without_excision_metadata_remain_readable() {
    let value = serde_json::json!({"archive":"archive1","position":1,"local_seq":1,
        "unit":"note","key":"note1","batch":format!("blake3:{}","0".repeat(64)),
        "rows":1,"blobs":[]});
    let decoded: PolylogueRows = serde_json::from_value(value).unwrap();
    assert!(decoded.excision.is_none());
}

#[test]
fn independently_appended_facts_can_be_replaced_or_excised() {
    for kind in ["observation", "delivery_event"] {
        let value = manifest_for(kind);
        let decoded: ExcisionPublication = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.replacements[0].unit.kind, kind);
    }
    assert!(serde_json::from_value::<ExcisionPublication>(manifest_for("unknown")).is_err());
}
