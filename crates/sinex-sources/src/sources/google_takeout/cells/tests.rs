use super::*;

const SEARCH: &str = r#"<div class="outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp"><div class="mdl-grid"><div class="header-cell mdl-cell mdl-cell--12-col"><p class="mdl-typography--title">Search<br></p></div><div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">Searched for <a href="https://www.google.com/search?q=a+%26+b">a &amp; b</a><br>Aug 26, 2026, 9:34:44 PM CEST<br></div><div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1 mdl-typography--text-right"></div><div class="content-cell mdl-cell mdl-cell--12-col mdl-typography--caption"><b>Products:</b><br>&emsp;Search<br><b>Locations:</b><br>&emsp;At <a href="https://maps.example/x">this general area</a> - From your device<br></div></div></div>"#;

#[devkit::test]
fn reads_a_search_cell() {
    let c = parse(SEARCH).unwrap();
    assert_eq!(c.service, "Search");
    assert_eq!(c.lines.len(), 1);
    assert_eq!(c.lines[0].text, "Searched for a & b");
    assert_eq!(
        c.lines[0].links,
        vec![(
            "https://www.google.com/search?q=a+%26+b".to_owned(),
            "a & b".to_owned()
        )]
    );
    assert_eq!(
        c.at,
        crate::sources::fields::rfc3339("2026-08-26T19:34:44Z").unwrap()
    );
    assert_eq!(c.zone_label, "CEST");
    assert_eq!(c.section("Products"), vec!["Search"]);
    assert_eq!(c.section("Locations").len(), 1);
}

#[devkit::test]
fn a_bare_ampersand_before_multibyte_text() {
    assert_eq!(decode("Kraków & Łódź żółć"), "Kraków & Łódź żółć");
    assert_eq!(decode("a &amp; ż&#x17c;"), "a & żż");
}

#[devkit::test]
fn one_moment_in_two_zones_hashes_the_same() {
    let utc = SEARCH.replace(
        "Aug 26, 2026, 9:34:44 PM CEST",
        "Aug 26, 2026, 7:34:44 PM UTC",
    );
    assert_eq!(
        <Cell as RecordShape>::hash(SEARCH.as_bytes(), &[]),
        <Cell as RecordShape>::hash(utc.as_bytes(), &[])
    );
    let other = SEARCH.replace("9:34:44", "9:34:45");
    assert_ne!(
        <Cell as RecordShape>::hash(SEARCH.as_bytes(), &[]),
        <Cell as RecordShape>::hash(other.as_bytes(), &[])
    );
}

#[devkit::test]
fn times_with_narrow_spaces_noon_and_midnight() {
    let (t, z) = time("Dec 15, 2025, 12:05:21\u{202f}AM CET")
        .unwrap()
        .unwrap();
    assert_eq!(z, "CET");
    assert_eq!(
        t,
        crate::sources::fields::rfc3339("2025-12-14T23:05:21Z").unwrap()
    );
    let (t, _) = time("Aug 7, 2019, 12:34:48 PM UTC").unwrap().unwrap();
    assert_eq!(
        t,
        crate::sources::fields::rfc3339("2019-08-07T12:34:48Z").unwrap()
    );
    assert!(time("Searched for Aug 7, 2019").is_none());
    assert!(time("Aug 7, 2019, 4:34:48 PM XYZT").unwrap().is_err());
}

#[devkit::test]
fn the_time_line_is_found_by_pattern_in_multiline_text() {
    let html = SEARCH.replace(
        "<br>Aug 26, 2026",
        "<br>first answer line<br>second line<br>Aug 26, 2026",
    );
    let c = parse(&html).unwrap();
    assert_eq!(c.lines.len(), 3);
    assert_eq!(c.lines[2].text, "second line");
}
