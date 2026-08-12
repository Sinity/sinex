use super::*;
use std::str::FromStr;

/// sinex-ss40: `EventName::from_str` splits on the FIRST `.` (`split_once('.')`),
/// but `EventSource` explicitly allows dots (e.g. "wm.hyprland" is a real,
/// widely-used source — see attention_test.rs/daily_test.rs). For a dotted
/// source, `Display` produces "wm.hyprland.window.focused" and `FromStr`
/// mis-splits it into source="wm", event_type="hyprland.window.focused" —
/// not a true inverse of Display.
#[test]
fn event_name_from_str_is_not_a_true_inverse_of_display_for_dotted_sources() {
    let original = EventName::new(
        EventSource::new("wm.hyprland").unwrap(),
        EventType::new("window.focused").unwrap(),
    );

    let wire = original.to_string();
    assert_eq!(wire, "wm.hyprland.window.focused");

    let roundtripped =
        EventName::from_str(&wire).expect("a Display'd EventName must always re-parse via FromStr");

    // This is the actual bug: round-tripping silently produces a DIFFERENT
    // EventName instead of failing or matching the original.
    assert_eq!(
        roundtripped, original,
        "EventName::from_str(name.to_string()) must reproduce the original EventName; \
         got source={:?} event_type={:?} instead of source={:?} event_type={:?} \
         (split_once('.') took the FIRST dot, not the source/event_type boundary)",
        roundtripped.source, roundtripped.event_type, original.source, original.event_type,
    );
}
