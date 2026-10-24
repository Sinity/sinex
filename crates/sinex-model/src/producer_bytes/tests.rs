use super::*;

#[test]
fn typed_descriptor_commitment_sorts_nested_objects_without_changing_values() {
    let mut frame = MaterialFrame {
        id: MaterialId::new(),
        input: InputRef {
            kind: "file".into(),
            locator: "fixture".into(),
            host: "h".into(),
        },
        blob: format!("blake3:{}", blake3::hash(b"body").to_hex()),
        length: 4,
        parser: "fixture".into(),
        parser_version: 1,
        part_index: 0,
        previous_part: None,
        manifest: serde_json::from_str(r#"{"z":0,"a":{"y":2,"b":1},"list":[{"z":3,"a":4},null]}"#)
            .unwrap(),
    };
    let before = Material::from_frame(&frame);
    frame.manifest =
        serde_json::from_str(r#"{"list":[{"a":4,"z":3},null],"a":{"b":1,"y":2},"z":0}"#).unwrap();
    assert_eq!(before, Material::from_frame(&frame));
    frame.manifest["a"]["b"] = serde_json::json!("1");
    assert_ne!(before, Material::from_frame(&frame));
    frame.manifest["a"]["b"] = serde_json::json!(1);
    frame.parser_version += 1;
    assert_ne!(before, Material::from_frame(&frame));
}

#[test]
fn compact_declaration_does_not_contain_manifest_or_impose_a_new_frame_name_limit() {
    let frame = MaterialFrame {
        id: MaterialId::new(),
        input: InputRef {
            kind: "file".into(),
            locator: "fixture".into(),
            host: "h".into(),
        },
        blob: format!("blake3:{}", blake3::hash(b"body").to_hex()),
        length: 4,
        parser: "fixture".into(),
        parser_version: 1,
        part_index: 0,
        previous_part: None,
        manifest: serde_json::json!({"large":"x".repeat(2_000_000)}),
    };
    let mut p = Publication {
        version: 1,
        frame: "final".into(),
        class: Class::Guarded,
        material: Some(Material::from_frame(&frame)),
        envelope: None,
    };
    assert!(serde_json::to_vec(&p).unwrap().len() < 512);
    p.frame = "f".repeat(5000);
    assert!(p.validate().is_ok());
    p.version = 2;
    assert!(p.validate().is_err());
}
