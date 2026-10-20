use super::*;

#[test]
fn day_settings_parse_and_label() {
    let s = Settings::new("Europe/Warsaw", "04:00").unwrap();
    assert_eq!(s.label, "04:00 Europe/Warsaw");
    assert!(Settings::new("Europe/Warsaw", "4am").is_err());
    assert!(Settings::new("Nowhere/Else", "04:00").is_err());
    let d = parse_day("2026-10-18").unwrap();
    let span = s.day_span(d, d).unwrap();
    assert_eq!(span.len(), MICROS_PER_DAY);
}

mod database;
