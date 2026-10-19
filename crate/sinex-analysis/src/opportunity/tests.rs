use super::*;

#[test]
fn a_polled_source_is_observed_until_it_was_last_read() {
    let e = Evidence {
        records: Some(Span::new(100, 500)),
        last_read: Some(900),
        runs: vec![],
    };
    let o = opportunity("atuin", Basis::Polled, Completeness::Complete, &e);
    assert_eq!(o[0].span, Span::new(100, 900));
    let ex = opportunity("spotify", Basis::Export, Completeness::Complete, &e);
    assert_eq!(ex[0].span, Span::new(100, 501));
    let live = Evidence {
        runs: vec![Span::new(10, 20), Span::new(30, 40)],
        ..Default::default()
    };
    assert_eq!(
        opportunity("journald", Basis::Live, Completeness::Complete, &live).len(),
        2
    );
    assert!(
        opportunity(
            "x",
            Basis::Polled,
            Completeness::Sample,
            &Evidence::default()
        )
        .is_empty()
    );
}
