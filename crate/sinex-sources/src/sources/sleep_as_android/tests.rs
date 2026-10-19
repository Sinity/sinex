use super::*;

const HEADER: &[u8] = b"Id,Tz,From,To,Sched,Hours,Rating,Comment,Framerate,Snore,Noise,Cycles,DeepSleep,LenAdjust,Geo,\"Event\",\"Event\"\n";

#[test]
fn derives_the_offset_from_from_and_id_and_drops_sentinels() {
    let (_, header) = SleepParser.parse(&HEADER, &Carry::default()).unwrap();
    // 2025-10-05 02:49 UTC, written as 04:49 local: +2 h, whatever Tz says.
    let row = b"\"1759632540000\",\"GMT-02:00\",\"05. 10. 2025 4:49\",\"05. 10. 2025 6:53\",\"05. 10. 2025 6:53\",\"2.070\",\"0.0\",\" #cloud\",\"10008\",\"-1\",\"-1.0\",\"3\",\"0.48\",\"-40\",\"\",\"LIGHT_START-1759632540000\",\"HR-1759632900000-88.0\"\n";
    let (out, _) = SleepParser.parse(&&row[..], &header).unwrap();
    let r = &out[0];
    assert_eq!(r.offset_minutes, Some(120));
    assert_eq!(r.start, fields::from_unix_ms(1_759_632_540_000).ok());
    assert_eq!(
        r.end.unwrap().unix_ns() - r.start.unwrap().unix_ns(),
        (2 * 3600 + 4 * 60) as i128 * 1_000_000_000
    );
    assert_eq!(r.values["snore"], Value::Null, "-1 is absent");
    assert_eq!(r.values["cycles"], json!(3.0));
    assert_eq!(r.events.len(), 2);
    assert_eq!(r.events[1]["value"], json!(88.0));
}

#[test]
fn a_row_without_its_header_is_an_error() {
    assert!(
        SleepParser
            .parse(&&b"\"1\",\"x\"\n"[..], &Carry::default())
            .is_err()
    );
}

#[test]
fn a_noise_row_belongs_to_the_session_before_it() {
    let (_, header) = SleepParser.parse(&HEADER, &Carry::default()).unwrap();
    let row = b"\"1759632540000\",\"GMT-02:00\",\"05. 10. 2025 4:49\",\"05. 10. 2025 6:53\",\"05. 10. 2025 6:53\",\"2.070\",\"0.0\",\"\",\"10008\",\"-1\",\"-1.0\",\"3\",\"0.48\",\"-40\",\"\",\"\",\"\"\n";
    let (_, after) = SleepParser.parse(&&row[..], &header).unwrap();
    let noise = b",,,,,,,,,,,,,\"20643.4\",\"287.5\"\n";
    let (out, after) = SleepParser.parse(&&noise[..], &after).unwrap();
    assert_eq!(out[0].category, "sleep-noise");
    assert_eq!(out[0].native_id, "1759632540000:noise");
    assert_eq!(out[0].values["noise"], json!([20643.4, 287.5]));
    assert!(
        SleepParser.parse(&&noise[..], &after).is_err(),
        "only one per session"
    );
}
