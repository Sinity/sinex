use super::*;
use sinex_model::event_types::library::FileCopy;
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation, Timestamp};
fn copy(path: &str) -> Event {
    let payload: FileCopy = serde_json::from_value(serde_json::json!({"scan":"example-scan","part_index":0,"item_key":"example-key","item":"example-item","scanned_at":"1960-01-01T00:00:00Z","host":"example-host","root":"/example","path":path,"content":"blake3:example","size":1,"sha256":"example","md5":"example","media_type":"application/pdf","width":null,"height":null,"duration_ms":null,"stored":false,"device":null,"inode":null,"modified_at":"1960-01-01T00:00:00Z","vanished_at":null,"moved_from":null,"previous_content":null,"move_candidates":[]})).unwrap();
    Event::build(
        &payload,
        "library.scan",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Guarded,
    )
    .unwrap()
}
#[devkit::test]
fn hints_keep_content_and_parent_without_touching_authored_fields() {
    let event = copy("/example/Ada Example - Some Book - Inner Title (2042).pdf");
    let before = event.payload.clone();
    let out = Hints
        .derive(
            Input {
                event: &event,
                bytes: None,
            },
            &Env::at(Timestamp::now(), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting")),
        )
        .unwrap();
    assert_eq!(event.payload, before);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].payload["author_hint"], "Ada Example");
    assert_eq!(out[0].payload["title_hint"], "Some Book - Inner Title");
    assert_eq!(out[0].payload["year_hint"], 2042);
    assert_eq!(out[0].payload["content"], "blake3:example");
    assert_eq!(out[0].class, Class::Guarded);
    assert_eq!(
        out[0].origin,
        Origin::Derived {
            parents: vec![event.id]
        }
    );
    assert!(out[0].ts_orig.at.is_none());
}
#[devkit::test]
fn isolated_years_and_hex_hints_have_no_date_cutoff() {
    for (text, expected) in [
        ("Book (1800)", Some("1800")),
        ("Book (2099)", Some("2099")),
        ("Book 12345", None),
    ] {
        assert_eq!(isolated(text, 4, |c| c.is_ascii_digit()), expected);
    }
    assert_eq!(
        isolated(
            "book -- 0123456789abcdef0123456789abcdef -- example",
            32,
            |c| c.is_ascii_hexdigit()
        ),
        Some("0123456789abcdef0123456789abcdef")
    );
    assert_eq!(
        isolated("00123456789abcdef0123456789abcdef00", 32, |c| c
            .is_ascii_hexdigit()),
        None
    );
    let event = copy("/example/965.jpg");
    let out = Hints
        .derive(
            Input {
                event: &event,
                bytes: None,
            },
            &Env::at(Timestamp::now(), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting")),
        )
        .unwrap();
    for field in ["author_hint", "title_hint", "year_hint", "external_id_hint"] {
        assert!(out[0].payload[field].is_null());
    }
}
#[devkit::test]
fn removed_copy_does_not_supply_current_hints() {
    let mut event = copy("/example/Book.pdf");
    event.record_state = RecordState::Removed;
    assert!(
        Hints
            .derive(
                Input {
                    event: &event,
                    bytes: None
                },
                &Env::at(Timestamp::now(), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"))
            )
            .unwrap()
            .is_empty()
    );
}
