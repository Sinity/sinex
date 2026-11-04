use super::*;
#[devkit::test]
fn signal_frame_replay_preserves_whole_original_json_and_host_line_anchors() {
    let native=b"  {\n \"kind\": \"signals\",\n \"frame\": \"synthetic\",\n \"reports\": [],\n \"class\": \"open\"\n}\n  ";
    assert_eq!(frame(native, true), (vec![(0, native.len())], native.len()));
    for split in 0..native.len() {
        assert_eq!(
            frame(&native[..split], false),
            (vec![], 0),
            "transport frame prefix {split} is not a different source record"
        );
    }
    let one = b"{\"record\":\"synthetic\",\"value\":{\"text\":\"line one\"}}\n";
    let two = b"{\"record\":\"synthetic\",\"value\":{\"text\":\"line two\"}}\n";
    let mut host = one.to_vec();
    host.extend_from_slice(two);
    let expected = vec![(0, one.len()), (one.len(), host.len())];
    assert_eq!(frame(&host, true), (expected.clone(), host.len()));
    for split in 0..host.len() {
        let (rows, end) = frame(&host[..split], false);
        assert_eq!(
            rows,
            expected
                .iter()
                .copied()
                .filter(|(_, end)| *end <= split)
                .collect::<Vec<_>>()
        );
        assert_eq!(end, rows.last().map_or(0, |(_, end)| *end));
    }
}

fn frame_with(report: signals::Report) -> Value {
    serde_json::to_value(sinex_protocol::Ingress::Signals {
        frame: "synthetic".into(),
        reports: vec![report],
        class: Class::Open,
    })
    .unwrap()
}

/// A producer forwards only its own family's evaluator reports, so it can
/// neither report Sinex's rules nor another product's.
#[devkit::test]
fn forwarded_reports_carry_their_producer_and_refuse_another_product() {
    let at = Timestamp::from_unix_secs(1_800_000_000).unwrap();
    let own = signals::Report::Progress {
        product: "polylogue".into(),
        at: 1,
        incomplete: Default::default(),
    };
    let records = forwarded_reports(&frame_with(own.clone()), "primary", "polylogue", at).unwrap();
    assert_eq!(
        records,
        vec![SignalRecord::Report(Box::new(ReportReceived {
            producer: "primary".into(),
            family: "polylogue".into(),
            report: own,
            received_at: at,
        }))]
    );
    for foreign in [
        signals::Report::Progress {
            product: "sinex".into(),
            at: 1,
            incomplete: Default::default(),
        },
        signals::Report::Withdrawn {
            product: "other".into(),
            id: "rule".into(),
        },
    ] {
        assert!(forwarded_reports(&frame_with(foreign), "primary", "polylogue", at).is_err());
    }
}
