use super::*;

#[devkit::test]
fn the_day_setting_is_the_one_figures_are_derived_under() {
    let s = Settings::operator_default();
    assert_eq!(s.label, "04:00 Europe/Warsaw");
    let d = parse_day("2026-10-18").unwrap();
    let span = s.day_span(d, d).unwrap();
    assert_eq!(span.len(), 86_400_000_000);
    assert!(parse_day("18.10.2026").is_err());
}

mod database;
