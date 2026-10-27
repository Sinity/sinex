use super::*;
use sinex_model::EventPayload;

fn settings() -> Settings {
    Settings {
        host: "example-host".into(),
        zone: Some("Europe/Warsaw".into()),
        options: serde_json::json!({"read_sleep_csv":true,"dataset":"example-sleep"}),
    }
}

const HEADER: &[u8] = b"sh_datauuid,start_local,end_local,sh_duration_minutes,sa_vs_sh_duration_minutes,trimmed_event_count,hr_avg,hr_min,hr_max,events_hr,events_light,events_deep,events_rem,sa_comment,unknown\n";

fn parse(row: &[u8]) -> Result<SleepSessionImported, RecordError> {
    let parser = validate(&settings()).unwrap();
    let (_, carry) = parser.parse(&HEADER, &Carry::default(), &RecordContext::none())?;
    Ok(parser
        .parse(&row, &carry, &RecordContext::none())?
        .0
        .remove(0))
}

#[devkit::test]
fn optional_measures_comments_and_unknown_columns_survive() {
    let a = parse(b"example-1,2030-01-01T01:00:00+01:00,2030-01-01T03:00:00+01:00,120,-2,6,,,,0,1,0,0,,kept\n").unwrap();
    assert_eq!(a.dataset, "example-sleep");
    assert_eq!(a.provider_session_id, "example-1");
    assert_eq!(a.sh_duration_minutes, Some(120.0));
    assert_eq!(a.sa_vs_sh_duration_minutes, Some(-2.0));
    assert_eq!(a.hr_avg, None);
    assert_eq!(a.events_hr, Some(0));
    assert_eq!(a.sa_comment, None);
    assert_eq!(a.extra["unknown"], "kept");
    assert_eq!(
        serde_json::to_value(a.started_at).unwrap(),
        "2030-01-01T00:00:00Z"
    );
    let b = parse(b"example-2,2030-01-02T01:00:00,2030-01-02T02:00:00,,,,70.02,57,87,29,2,2,2,\"two\nlines\",\n").unwrap();
    assert_eq!(b.hr_avg, Some(70.02));
    assert_eq!(b.hr_min, Some(57.0));
    assert_eq!(b.hr_max, Some(87.0));
    assert_eq!(b.trimmed_event_count, None);
    assert_eq!(b.sa_comment.as_deref(), Some("two\nlines"));
    assert_eq!(
        serde_json::to_value(b.started_at).unwrap(),
        "2030-01-02T00:00:00Z"
    );
}

#[devkit::test]
fn dst_fold_and_gap_remain_unresolved_but_offsets_resolve() {
    for (start, end, reason) in [
        (
            "2030-10-27T02:10:00",
            "2030-10-27T02:40:00",
            "ambiguous local time",
        ),
        (
            "2030-03-31T02:10:00",
            "2030-03-31T02:40:00",
            "nonexistent local time",
        ),
    ] {
        let row = format!("example-dst,{start},{end},,,,,,,,,,,,\n");
        let payload = parse(row.as_bytes()).unwrap();
        assert_eq!(payload.started_at, None);
        assert_eq!(payload.ended_at, None);
        assert_eq!(payload.start_unresolved.as_deref(), Some(reason));
        assert_eq!(payload.end_unresolved.as_deref(), Some(reason));
        assert_eq!(payload.ts_orig().at, None);
    }
    let payload =
        parse(b"example-offset,2030-10-27T02:10:00+02:00,2030-10-27T02:40:00+01:00,,,,,,,,,,,,\n")
            .unwrap();
    assert!(payload.started_at.is_some());
    assert_eq!(payload.start_unresolved, None);
}

#[devkit::test]
fn iso_offsets_without_seconds_are_resolved_from_the_supplied_offset() {
    let payload =
        parse(b"example-offset,2030-10-27T02:10+02:00,2030-10-27T02:40+01:00,,,,,,,,,,,,\n")
            .unwrap();
    assert_eq!(
        serde_json::to_value(payload.started_at).unwrap(),
        "2030-10-27T00:10:00Z"
    );
    assert_eq!(
        serde_json::to_value(payload.ended_at).unwrap(),
        "2030-10-27T01:40:00Z"
    );
}

#[devkit::test]
fn invalid_dates_numbers_counts_and_headers_fail() {
    for row in [
        "example,bad,2030-01-01T03:00:00,,,,,,,,,,,,\n",
        "example,2030-02-30T01:00:00,2030-03-01T01:00:00,,,,,,,,,,,,\n",
        "example,2030-01-01T03:00:00,2030-01-01T01:00:00,,,,,,,,,,,,\n",
        "example,2030-01-01T01:00:00,2030-01-01T02:00:00,NaN,,,,,,,,,,,\n",
        "example,2030-01-01T01:00:00,2030-01-01T02:00:00,,,1.5,,,,,,,,,\n",
    ] {
        assert!(parse(row.as_bytes()).is_err(), "{row}");
    }
    let parser = validate(&settings()).unwrap();
    for raw in [
        b"sh_datauuid,start_local\n".as_slice(),
        b"sh_datauuid,start_local,end_local,end_local\n",
        b"\xff\n",
    ] {
        assert!(
            parser
                .parse(&raw, &Carry::default(), &RecordContext::none())
                .is_err()
        );
    }
}

#[devkit::test]
fn disabled_and_incomplete_settings_fail_before_reading() {
    let input = std::path::Path::new("example.csv");
    assert!((FORMAT.plan.unwrap())(input, &Settings::new("example-host")).is_err());
    for options in [
        serde_json::json!({"read_sleep_csv":true}),
        serde_json::json!({"read_sleep_csv":false,"dataset":"example"}),
    ] {
        let mut settings = settings();
        settings.options = options;
        assert!((FORMAT.plan.unwrap())(input, &settings).is_err());
    }
    let mut no_zone = settings();
    no_zone.zone = None;
    assert!((FORMAT.plan.unwrap())(input, &no_zone).is_err());
    assert_eq!((FORMAT.plan.unwrap())(input, &settings()).unwrap().len(), 1);
}

#[devkit::test]
fn multiline_ranges_and_header_carry_survive_restart() {
    let row = b"example-2,2030-01-02T01:00:00,2030-01-02T02:00:00,,,,70.02,57,87,29,2,2,2,\"Caf\xc3\xa9\nsecond line\",\n";
    let input = [HEADER, row].concat();
    let ranges = SPEC
        .framer
        .ranges(&mut std::io::Cursor::new(&input))
        .unwrap();
    assert_eq!(ranges, [(0, HEADER.len()), (HEADER.len(), input.len())]);
    let parser = validate(&settings()).unwrap();
    let (_, carry) = parser
        .parse(&HEADER, &Carry::default(), &RecordContext::none())
        .unwrap();
    let carry: Carry = serde_json::from_slice(&serde_json::to_vec(&carry).unwrap()).unwrap();
    let payload = parser
        .parse(&row.as_slice(), &carry, &RecordContext::none())
        .unwrap()
        .0
        .remove(0);
    assert_eq!(payload.sa_comment.as_deref(), Some("Café\nsecond line"));
    assert_eq!(
        serde_json::to_value(&payload).unwrap(),
        serde_json::to_value(parse(row).unwrap()).unwrap()
    );
}

#[path = "../hledger/intake_tests.rs"]
mod intake;

#[devkit::test]
fn shared_intake_restart_and_replay_keep_header_and_corrected_session_keys() {
    let first = b"example-1,2030-01-01T01:00:00,2030-01-01T03:00:00,,,,70.0,,,,,,,,\n";
    let corrected =
        b"example-1,2030-01-01T01:00:00,2030-01-01T03:00:00,,,,71.0,,,,,,,\"two\nlines\",\n";
    let input = [HEADER, first, corrected].concat();
    let events = intake::export_restart(
        &input,
        driver("example.csv".into(), "example-host".into()),
        &source(&settings()).unwrap(),
        2,
        0,
    );
    assert_eq!(events[0].equivalence_key, events[1].equivalence_key);
    assert_eq!(events[0].payload["hr_avg"], 70.0);
    assert_eq!(events[1].payload["hr_avg"], 71.0);
    assert_eq!(events[1].payload["sa_comment"], "two\nlines");
}

#[devkit::test]
fn repeated_archive_member_names_start_with_their_own_headers() {
    let mut builder = tar::Builder::new(Vec::new());
    for text in [
        "sh_datauuid,start_local,end_local,hr_avg\nexample-1,2030-01-01T01:00:00,2030-01-01T02:00:00,70\n",
        "hr_avg,end_local,start_local,sh_datauuid\n71,2030-01-02T02:00:00,2030-01-02T01:00:00,example-2\n",
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_size(text.len() as u64);
        header.set_mode(0o600);
        header.set_cksum();
        builder
            .append_data(&mut header, "same.csv", text.as_bytes())
            .unwrap();
    }
    let input = builder.into_inner().unwrap();
    let mut driver = driver("example.tar".into(), "example-host".into());
    driver.part_bytes = 8;
    let events = intake::export_restart(&input, driver, &source(&settings()).unwrap(), 2, 0);
    assert_eq!(events[0].payload["provider_session_id"], "example-1");
    assert_eq!(events[1].payload["provider_session_id"], "example-2");
    assert_eq!(events[1].payload["hr_avg"], 71.0);
}
