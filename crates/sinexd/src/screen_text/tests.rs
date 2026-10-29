use super::*;
use sinex_model::event_types::session::MediaSegment;
fn segment(text: Option<&str>) -> MediaSegment {
    MediaSegment {
        kind: "screen".into(),
        started_at: sinex_model::Timestamp::from_unix_secs(0).unwrap(),
        ended_at: sinex_model::Timestamp::from_unix_secs(0).unwrap(),
        mime: "image/png".into(),
        bytes: 7,
        target: None,
        width: None,
        height: None,
        accessibility_text: text.map(str::to_owned),
    }
}
#[devkit::test]
fn accessibility_text_wins_without_reading_bytes_or_starting_ocr() {
    for text in ["Recorded widget text", ""] {
        let (method, actual) =
            text_for(&segment(Some(text)), None, || panic!("OCR must not run")).unwrap();
        assert_eq!(method, "accessibility");
        assert_eq!(actual, text);
    }
}
#[devkit::test]
fn absent_accessibility_uses_local_ocr_and_preserves_an_unavailable_engine() {
    assert_eq!(
        text_for(&segment(None), Some("local/v1"), || Ok("OCR words".into())).unwrap(),
        ("local/v1".into(), "OCR words".into())
    );
    assert!(
        text_for(&segment(None), None, || panic!("no engine"))
            .unwrap_err()
            .contains("no local OCR")
    );
}
#[devkit::test]
fn accessible_text_is_derived_from_the_exact_guarded_frame_without_material_read() {
    let frame = Event::build(
        &segment(Some("widget text")),
        "synthetic.screen",
        Origin::Record {
            location: sinex_model::RecordLocation {
                material: sinex_model::MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Guarded,
    )
    .unwrap();
    let producer = ScreenText { engine: None };
    assert_eq!(producer.reads_for(&frame), Reads::Nothing);
    let output = producer
        .derive(
            Input {
                event: &frame,
                bytes: None,
            },
            &Env::at(frame.ts_orig.at.unwrap(), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting")),
        )
        .unwrap();
    assert_eq!(
        output[0].origin,
        Origin::Derived {
            parents: vec![frame.id]
        }
    );
    assert_eq!(output[0].class, Class::Guarded);
    assert_eq!(output[0].payload["text"], "widget text");
    assert_eq!(output[0].payload["engine"], "accessibility");
}
