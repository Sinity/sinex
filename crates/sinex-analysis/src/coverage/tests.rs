use super::*;

#[devkit::test]
fn a_polled_source_is_covered_until_it_was_last_read() {
    let e = CoverageEvidence {
        records: Some(Span::new(100, 500)),
        last_read: Some(900),
        runs: vec![],
    };
    let o = coverage("atuin", CaptureMode::Polled, Completeness::Complete, &e);
    assert_eq!(o[0].span, Span::new(100, 900));
    let ex = coverage("spotify", CaptureMode::Export, Completeness::Complete, &e);
    assert_eq!(ex[0].span, Span::new(100, 501));
    let live = CoverageEvidence {
        runs: vec![Span::new(10, 20), Span::new(30, 40)],
        ..Default::default()
    };
    assert_eq!(
        coverage("journald", CaptureMode::Live, Completeness::Complete, &live).len(),
        2
    );
    assert!(
        coverage(
            "x",
            CaptureMode::Polled,
            Completeness::Sample,
            &CoverageEvidence::default()
        )
        .is_empty()
    );
}

#[devkit::test]
fn every_source_a_metric_names_declares_what_it_sees() {
    for m in sinex_derivations::figures::METRICS {
        for s in m.sources {
            assert_ne!(
                completeness(s),
                Completeness::Unknown,
                "{s} has no declared completeness"
            );
        }
    }
}
