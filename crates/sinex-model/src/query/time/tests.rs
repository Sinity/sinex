use super::*;

fn ctx() -> Context {
    Context::new(
        "2026-09-14T10:00:00Z".parse().unwrap(),
        TimeZone::get("Europe/Warsaw").unwrap(),
        Time::constant(4, 0, 0, 0),
    )
}

#[devkit::test]
fn logical_days_start_at_four() {
    let c = ctx();
    // 01:30 local on the 14th belongs to the 13th.
    let early = c.local("2026-09-14T01:30".parse().unwrap()).unwrap();
    assert_eq!(c.logical_date(early), "2026-09-13".parse::<Date>().unwrap());
    let (start, end) = c.day("2026-09-14".parse().unwrap()).unwrap();
    assert_eq!(start.to_string(), "2026-09-14T02:00:00Z");
    assert_eq!(end.to_string(), "2026-09-15T02:00:00Z");
}

#[devkit::test]
fn days_across_a_change_of_offset_have_their_real_length() {
    let c = ctx();
    // Europe/Warsaw leaves summer time at 03:00 on 2026-10-25, before the
    // 04:00 day start, so the long day is logical day 2026-10-24.
    let (start, end) = c.day("2026-10-24".parse().unwrap()).unwrap();
    assert_eq!(end.duration_since(start).as_secs(), 25 * 3600);
}

#[devkit::test]
fn parses_words() {
    assert_eq!(duration("5m"), Some(300.0));
    assert_eq!(duration("3d"), Some(259_200.0));
    assert_eq!(duration("5x"), None);
    assert_eq!(month("2026-08"), Some((2026, 8)));
    assert_eq!(year("2026"), Some(2026));
    assert_eq!(clock("14:05"), Some(Time::constant(14, 5, 0, 0)));
    let c = ctx();
    assert_eq!(
        instant(&c, "2026-03-02T14:05").unwrap().to_string(),
        "2026-03-02T13:05:00Z"
    );
    assert_eq!(
        instant(&c, "2026-03-02T14:05:00Z").unwrap().to_string(),
        "2026-03-02T14:05:00Z"
    );
}

#[devkit::test]
fn weeks_start_on_monday() {
    let c = ctx();
    let (start, _) = c.week_of("2026-09-17".parse().unwrap()).unwrap();
    assert_eq!(c.logical_date(start), "2026-09-14".parse::<Date>().unwrap());
}

#[devkit::test]
fn shared_day_start_in_a_dst_gap_resolves_to_the_transition() {
    let ctx = Context::new(
        "2026-03-29T12:00:00Z".parse().unwrap(),
        TimeZone::get("Europe/Warsaw").unwrap(),
        Time::constant(2, 30, 0, 0),
    );
    let (start, _) = ctx.day("2026-03-29".parse().unwrap()).unwrap();
    assert_eq!(start.to_string(), "2026-03-29T01:00:00Z");
}
