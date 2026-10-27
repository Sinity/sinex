use super::*;
use crate::sources::fields;
use serde_json::json;
use sinex_model::{EventPayload, KeyKind};

fn parser(lane: &'static str) -> RouterParser {
    RouterParser {
        lane,
        zone: jiff::tz::TimeZone::get("Europe/Warsaw").unwrap(),
    }
}

fn record(lane: &str, ts: f64, payload: Value) -> Vec<u8> {
    let mut line = json!({"host": "host-a", "lane": lane, "payload": payload, "raw_ref": null,
        "schema": "sinnix-capture-v1", "schema_version": 1, "seq": 3230, "ts": ts})
    .to_string()
    .into_bytes();
    line.push(b'\n');
    line
}

fn parse(lane: &'static str, ts: f64, payload: Value) -> Result<Vec<Router>, RecordError> {
    parser(lane)
        .parse(
            &record(lane, ts, payload).as_slice(),
            &(),
            &crate::RecordContext::none(),
        )
        .map(|(out, ())| out)
}

fn at(rfc3339: &str) -> Timestamp {
    fields::rfc3339(rfc3339).unwrap()
}

fn log_lines(ts: f64, lines: &[&str]) -> Vec<RouterLogLine> {
    parse("syslog", ts, json!({ "lines": lines }))
        .unwrap()
        .into_iter()
        .map(|e| match e {
            Router::Line(line) => line,
            other => panic!("{other:?}"),
        })
        .collect()
}

#[devkit::test]
fn each_log_line_is_an_event_in_the_routers_local_time() {
    // 2026-10-20 01:55 in Warsaw, summer time.
    let lines = log_lines(
        1_792_454_100.5,
        &[
            "Tue Oct 20 01:52:08 2026 authpriv.info dropbear[23661]: Exit (root) from <192.0.2.10:48722>: Disconnect received",
            "Tue Oct 20 01:52:14 2026 user.notice service-a [27332]: Stopping service-a ✓",
            "Tue Oct 20 01:53:01 2026 kern.info kernel: [ 12.345] eth0: link up",
            "Tue Oct  6 01:53:02 2026 daemon.err odd tag: text",
            "Tue Oct 20 01:53:03 2026 daemon.notice",
            "a line the router wrote without a header",
        ],
    );
    assert_eq!(lines.len(), 6);
    let first = &lines[0];
    assert_eq!(first.logged_at, Some(at("2026-10-19T23:52:08Z")));
    assert_eq!(
        (first.facility.as_deref(), first.severity.as_deref()),
        (Some("authpriv"), Some("info"))
    );
    assert_eq!(
        (first.program.as_deref(), first.pid),
        (Some("dropbear"), Some(23661))
    );
    assert_eq!(
        first.message,
        "Exit (root) from <192.0.2.10:48722>: Disconnect received"
    );
    assert_eq!(first.sampled_at.unix_ns(), 1_792_454_100_500_000_000);
    assert_eq!((first.host.as_str(), first.seq), ("host-a", 3230));
    assert_eq!(
        (lines[1].program.as_deref(), lines[1].pid),
        (Some("service-a"), Some(27332))
    );
    assert_eq!(
        (
            lines[2].program.as_deref(),
            lines[2].pid,
            lines[2].message.as_str()
        ),
        (Some("kernel"), None, "[ 12.345] eth0: link up")
    );
    assert_eq!(lines[3].logged_at, Some(at("2026-10-05T23:53:02Z")));
    assert_eq!(lines[3].program, None);
    assert_eq!(lines[3].message, "odd tag: text");
    assert_eq!(lines[4].facility, None);
    assert_eq!(lines[4].message, "daemon.notice");
    assert_eq!(lines[5].logged_at, None, "no header, no time");
    assert_eq!(lines[5].message, "a line the router wrote without a header");
    assert_eq!(lines[5].ts_orig().at, None);
}

#[devkit::test]
fn the_repeated_hour_is_placed_by_the_sample() {
    let line = "Sun Oct 25 02:30:00 2026 daemon.info a: b";
    // Sampled at 01:10 UTC: the second 02:30 (01:30 UTC) had not happened yet.
    let early = log_lines(at("2026-10-25T01:10:00Z").unix_ns() as f64 / 1e9, &[line]);
    assert_eq!(early[0].logged_at, Some(at("2026-10-25T00:30:00Z")));
    let late = log_lines(at("2026-10-25T02:00:00Z").unix_ns() as f64 / 1e9, &[line]);
    assert_eq!(late[0].logged_at, Some(at("2026-10-25T01:30:00Z")));
    // Winter time after the change.
    let winter = log_lines(
        at("2026-10-26T12:00:00Z").unix_ns() as f64 / 1e9,
        &["Mon Oct 26 12:30:00 2026 daemon.info a: b"],
    );
    assert_eq!(winter[0].logged_at, Some(at("2026-10-26T11:30:00Z")));
}

#[devkit::test]
fn each_lease_is_an_event() {
    let out = parse(
        "leases",
        1_792_455_727.9,
        json!([
            {"client_id": "01:02:00:00:00:00:01", "expires": 1_792_491_375, "hostname": "phone-a",
                "ip": "192.0.2.95", "mac": "02:00:00:00:00:01"},
            {"client_id": "*", "expires": 0, "hostname": "*", "ip": "192.0.2.52",
                "mac": "02:00:00:00:00:02", "tag": "x"}
        ]),
    )
    .unwrap();
    let [Router::Lease(a), Router::Lease(b)] = out.as_slice() else {
        panic!("{out:?}");
    };
    assert_eq!(a.hostname.as_deref(), Some("phone-a"));
    assert_eq!(a.expires_at, Timestamp::from_unix_secs(1_792_491_375).ok());
    assert_eq!(
        (b.hostname.as_deref(), b.client_id.as_deref()),
        (None, None)
    );
    assert_eq!(b.expires_at, None, "0 is a lease that does not expire");
    assert_eq!(b.extra["tag"], "x");
    assert_eq!(a.ts_orig().at, Some(a.sampled_at));
    assert!(parse("leases", 1.0, json!({"not": "a list"})).is_err());
}

#[devkit::test]
fn traffic_rows_of_closed_periods_are_keyed() {
    let row = json!({"conns": 471_184, "family": 4, "ip": "192.0.2.10", "layer7": "HTTPS",
        "mac": "02:00:00:00:00:03", "port": "443", "proto": "TCP", "rx_bytes": 341_172_234_743_u64,
        "rx_pkts": 258_599_013, "tx_bytes": 28_431_522_587_u64, "tx_pkts": 143_022_533});
    let current = parse(
        "nlbw",
        1.0,
        json!({"period": "current", "rows": [row.clone(), {"conns": 1, "family": 4, "ip": "192.0.2.11",
            "layer7": "", "mac": "02:00:00:00:00:04", "port": 0, "proto": "IP"}]}),
    )
    .unwrap();
    let closed = parse("nlbw", 2.0, json!({"period": "2026-09-30", "rows": [row]})).unwrap();
    let ([Router::Traffic(sample), Router::Traffic(other)], [Router::Traffic(month)]) =
        (current.as_slice(), closed.as_slice())
    else {
        panic!("{current:?} {closed:?}");
    };
    assert_eq!(sample.port, Some(443), "a number written as text");
    assert_eq!(sample.rx_bytes, Some(341_172_234_743));
    assert_eq!(other.layer7, None);
    assert_eq!(other.rx_bytes, None);
    assert_eq!(sample.key_kind(), KeyKind::Location);
    assert_eq!(
        month.key_kind(),
        KeyKind::Fields(&["period", "mac", "ip", "family", "proto", "port", "layer7"])
    );
    assert_eq!(
        DeviceTrafficSampled::REPLACE,
        sinex_model::ReplaceRule::SuppressRepeat
    );
}

#[devkit::test]
fn each_wifi_client_is_an_event() {
    let out = parse(
        "associations",
        1.0,
        json!({"aps": [
            {"ap": "phy0-ap0", "data": {"clients": {}, "freq": 2452}},
            {"ap": "phy0-ap1", "data": {}},
            {"ap": "phy1-ap1", "data": {"freq": 5500, "clients": {
                "02:00:00:00:00:05": {"aid": 1, "airtime": {"rx": 6, "tx": 7}, "assoc": true,
                    "auth": true, "authorized": true, "bytes": {"rx": 19, "tx": 11},
                    "capabilities": {}, "he": false, "ht": true,
                    "packets": {"rx": 1, "tx": 2, "retries": 3}, "rate": {"rx": 7_220_000, "tx": 7_220_000},
                    "signal": -54, "wmm": true}}}}
        ]}),
    )
    .unwrap();
    let [Router::Client(client)] = out.as_slice() else {
        panic!("{out:?}");
    };
    assert_eq!((client.ap.as_str(), client.freq), ("phy1-ap1", Some(5500)));
    assert_eq!(client.mac, "02:00:00:00:00:05");
    assert_eq!(client.signal, Some(-54));
    assert_eq!(client.bytes.as_ref().unwrap().rx, Some(19));
    assert_eq!(client.packets.as_ref().unwrap().tx, Some(2));
    assert_eq!(client.extra["packets"], json!({"retries": 3}));
    assert_eq!(client.extra["ht"], true);
    assert!(!client.extra.contains_key("signal"));
}

#[devkit::test]
fn records_of_another_lane_or_schema_fail() {
    let leases = record("leases", 1.0, json!([]));
    assert!(
        parser("syslog")
            .parse(&leases.as_slice(), &(), &crate::RecordContext::none())
            .is_err()
    );
    let mut other: Value =
        serde_json::from_slice(&record("syslog", 1.0, json!({"lines": []}))).unwrap();
    other["schema"] = json!("something-else");
    let bytes = other.to_string().into_bytes();
    assert!(
        parser("syslog")
            .parse(&bytes.as_slice(), &(), &crate::RecordContext::none())
            .is_err()
    );
    assert!(parse("syslog", 1.0, json!({"lines": "x"})).is_err());
}

#[devkit::test]
fn day_files_are_planned_per_lane() {
    let owned = tempfile::tempdir().unwrap();
    let root = owned.path();
    for (lane, files) in [
        (
            "syslog",
            &[
                "syslog-20261020.jsonl",
                "syslog-20261020.jsonl.hw",
                "syslog-index.jsonl",
                "syslog.seq",
            ][..],
        ),
        ("nlbw", &["nlbw-20261020.jsonl", "nlbw-20261021.jsonl"][..]),
        (".state", &["syslog-watermark"][..]),
    ] {
        std::fs::create_dir_all(root.join(lane)).unwrap();
        for f in files {
            std::fs::write(root.join(lane).join(f), "").unwrap();
        }
    }
    let settings = crate::Settings {
        zone: Some("Europe/Warsaw".into()),
        ..crate::Settings::new("host-a")
    };
    let plan = FORMAT.plan.unwrap();
    let planned = plan(root, &settings).unwrap();
    let parsers: Vec<&str> = planned.iter().map(|b| b.parser.as_str()).collect();
    assert_eq!(parsers, ["router-nlbw", "router-nlbw", "router-syslog"]);
    assert!(planned.iter().all(|b| b.source == "network.router"));
    assert_eq!(planned[0].settings.zone.as_deref(), Some("Europe/Warsaw"));
    assert_eq!(plan(&root.join("syslog"), &settings).unwrap().len(), 1);
    for b in &planned {
        assert_eq!((FORMAT.source)(b).unwrap().spec().parser, b.parser);
    }
}
