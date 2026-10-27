use super::*;
use sinex_model::{EventPayload, KeyKind, Manifest};
fn read(format: Format, text: &[u8], carry: &Carry) -> Result<(Vec<OcrText>, Carry), RecordError> {
    let manifest = Manifest::from_json(
        serde_json::json!({"path":"example.tsv","file_metadata":{"engine":"example-engine","external_run":"example-run"}}),
    );
    let text = if matches!(format, Format::Json) {
        format!(
            "{{\"segments\":[{}],\"engine\":\"example-engine\",\"external_run\":\"example-run\"}}",
            std::str::from_utf8(text).unwrap_or("invalid")
        )
        .into_bytes()
    } else {
        text.to_vec()
    };
    OcrParser {
        input: "example".into(),
        format,
    }
    .parse(
        &text.as_slice(),
        carry,
        &RecordContext {
            manifest: &manifest,
            member: None,
        },
    )
}
#[devkit::test]
fn ocr_boxes_keep_units_and_confidence_scale() {
    let (rows,_) = read(Format::Json,br#"{"id":"word-a","text":"word","bbox":[1,2,3,4],"coordinate_units":"pixels","confidence":87,"confidence_scale":"0-100","page":1,"custom":true}"#,&Carry::default()).unwrap();
    let p = &rows[0];
    assert_eq!(p.bounding_box.as_ref().unwrap().width, 3.0);
    assert_eq!(p.confidence, Some(87.0));
    assert_eq!(p.confidence_scale.as_deref(), Some("0-100"));
    assert_eq!(p.external_run.as_deref(), Some("example-run"));
    assert_eq!(p.extra["custom"], true);
    assert!(matches!(p.key_kind(), KeyKind::Fields(_)));
    assert_eq!(p.ts_orig().at, None);
}
#[devkit::test]
fn tesseract_header_survives_restart_and_hierarchy_is_not_a_word() {
    let header = b"level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n";
    let (_, carry) = read(Format::Tsv, header, &Carry::default()).unwrap();
    let carry: Carry = serde_json::from_slice(&serde_json::to_vec(&carry).unwrap()).unwrap();
    let (rows, carry) = read(
        Format::Tsv,
        b"1\t1\t0\t0\t0\t0\t0\t0\t100\t100\t-1\t\n",
        &carry,
    )
    .unwrap();
    assert!(rows.is_empty());
    let (rows, _) = read(
        Format::Tsv,
        b"5\t1\t1\t1\t1\t1\t10\t20\t30\t40\t0\tword\n",
        &carry,
    )
    .unwrap();
    assert_eq!(rows[0].text, "word");
    assert_eq!(rows[0].confidence, Some(0.0));
    assert_eq!(rows[0].confidence_scale.as_deref(), Some("tesseract-0-100"));
    assert_eq!(rows[0].bounding_box.as_ref().unwrap().left, 10.0);
    assert!(matches!(rows[0].key_kind(), KeyKind::Location));
}
#[devkit::test]
fn invalid_ocr_is_diagnosed_and_missing_confidence_stays_unknown() {
    for bytes in [
        br#"{"text":"x","bbox":[1,2,3,4]}"#.as_slice(),
        br#"{"text":"x","left":1}"#,
        br#"{"text":"x","bbox":[1,2,-3,4],"coordinate_units":"pixels"}"#,
        br#"{"text":1}"#,
        b"[]",
        &[0xff],
    ] {
        assert!(read(Format::Json, bytes, &Carry::default()).is_err());
    }
    let (rows, _) = read(Format::Json, br#"{"text":"x"}"#, &Carry::default()).unwrap();
    assert_eq!(rows[0].confidence, None);
    assert_eq!(rows[0].bounding_box, None);
    let (rows, _) = read(Format::Text, "Żółw\r\n".as_bytes(), &Carry::default()).unwrap();
    assert_eq!(rows[0].text, "Żółw");
}
