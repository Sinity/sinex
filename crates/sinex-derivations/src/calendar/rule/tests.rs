use super::*;
use jiff::civil::date;

fn expand(rule: &str, first: DateTime, last: DateTime) -> Vec<DateTime> {
    Rule::parse(rule).unwrap().starts(first, |at| at <= last)
}

fn days(starts: &[DateTime]) -> Vec<String> {
    starts.iter().map(|s| s.date().to_string()).collect()
}

#[devkit::test]
fn weekly_on_two_days_counts_the_first_start() {
    // 2011-01-28 is a Friday.
    let first = date(2011, 1, 28).at(15, 0, 0, 0);
    let starts = expand("FREQ=WEEKLY;COUNT=5;BYDAY=SU,FR", first, DateTime::MAX);
    assert_eq!(
        days(&starts),
        [
            "2011-01-28",
            "2011-01-30",
            "2011-02-04",
            "2011-02-06",
            "2011-02-11"
        ]
    );
    assert!(starts.iter().all(|s| s.time() == first.time()));
}

#[devkit::test]
fn the_first_start_counts_even_off_the_rule() {
    // A Wednesday start of a Monday rule: RFC 5545 counts it all the same.
    let starts = expand(
        "FREQ=WEEKLY;COUNT=3;BYDAY=MO",
        date(2024, 1, 3).at(9, 0, 0, 0),
        DateTime::MAX,
    );
    assert_eq!(days(&starts), ["2024-01-03", "2024-01-08", "2024-01-15"]);
}

#[devkit::test]
fn monthly_by_ordinal_weekday_until_a_utc_end() {
    let starts = expand(
        "FREQ=MONTHLY;UNTIL=20131203T180000Z;BYDAY=1TU",
        date(2013, 9, 3).at(18, 0, 0, 0),
        date(2013, 12, 3).at(18, 0, 0, 0),
    );
    assert_eq!(
        days(&starts),
        ["2013-09-03", "2013-10-01", "2013-11-05", "2013-12-03"]
    );
}

#[devkit::test]
fn last_weekday_and_negative_month_days() {
    let last_sunday = expand(
        "FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU",
        date(2020, 3, 29).at(2, 0, 0, 0),
        date(2023, 12, 31).at(0, 0, 0, 0),
    );
    assert_eq!(
        days(&last_sunday),
        ["2020-03-29", "2021-03-28", "2022-03-27", "2023-03-26"]
    );
    let month_end = expand(
        "FREQ=MONTHLY;COUNT=3;BYMONTHDAY=-1",
        date(2024, 1, 31).at(8, 0, 0, 0),
        DateTime::MAX,
    );
    assert_eq!(days(&month_end), ["2024-01-31", "2024-02-29", "2024-03-31"]);
}

#[devkit::test]
fn a_month_without_the_day_is_skipped() {
    let starts = expand(
        "FREQ=MONTHLY;COUNT=3",
        date(2024, 1, 31).at(8, 0, 0, 0),
        DateTime::MAX,
    );
    assert_eq!(days(&starts), ["2024-01-31", "2024-03-31", "2024-05-31"]);
}

#[devkit::test]
fn last_workday_of_the_month_by_set_position() {
    let starts = expand(
        "FREQ=MONTHLY;COUNT=3;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1",
        date(2024, 5, 31).at(17, 0, 0, 0),
        DateTime::MAX,
    );
    assert_eq!(days(&starts), ["2024-05-31", "2024-06-28", "2024-07-31"]);
}

#[devkit::test]
fn every_other_week_with_a_sunday_week_start() {
    let starts = expand(
        "FREQ=WEEKLY;INTERVAL=2;COUNT=4;BYDAY=TU,SU;WKST=SU",
        date(1997, 8, 5).at(9, 0, 0, 0),
        DateTime::MAX,
    );
    // RFC 5545's own example: WKST changes which days fall in one period.
    assert_eq!(
        days(&starts),
        ["1997-08-05", "1997-08-17", "1997-08-19", "1997-08-31"]
    );
}

#[devkit::test]
fn yearly_without_parts_skips_years_without_the_29th() {
    let starts = expand(
        "FREQ=YEARLY;COUNT=2",
        date(2024, 2, 29).at(0, 0, 0, 0),
        DateTime::MAX,
    );
    assert_eq!(days(&starts), ["2024-02-29", "2028-02-29"]);
}

#[devkit::test]
fn daily_with_hours_and_a_horizon() {
    let starts = expand(
        "FREQ=DAILY;BYHOUR=9,17",
        date(2024, 1, 1).at(9, 0, 0, 0),
        date(2024, 1, 2).at(23, 0, 0, 0),
    );
    assert_eq!(starts.len(), 4, "{starts:?}");
    assert_eq!(starts[1], date(2024, 1, 1).at(17, 0, 0, 0));
}

#[devkit::test]
fn a_rule_that_never_matches_ends_at_the_horizon() {
    let starts = expand(
        "FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=30",
        date(2024, 1, 1).at(0, 0, 0, 0),
        date(2030, 1, 1).at(0, 0, 0, 0),
    );
    assert_eq!(starts, [date(2024, 1, 1).at(0, 0, 0, 0)]);
}

#[devkit::test]
fn unread_rules_are_refused_by_name() {
    assert!(Rule::parse("FREQ=HOURLY").unwrap_err().contains("HOURLY"));
    assert!(
        Rule::parse("FREQ=YEARLY;BYWEEKNO=20")
            .unwrap_err()
            .contains("BYWEEKNO")
    );
    assert!(Rule::parse("BYDAY=MO").unwrap_err().contains("FREQ"));
    assert!(Rule::parse("FREQ=DAILY;COUNT=2;UNTIL=20240101").is_err());
    assert!(Rule::parse("FREQ=DAILY;X-NAME=1").is_ok());
}
