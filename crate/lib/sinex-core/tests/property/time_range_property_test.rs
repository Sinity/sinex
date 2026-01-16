use chrono::{DateTime, Duration, Utc};
use proptest::prelude::*;
use sinex_test_utils::sinex_prop;
use sinex_test_utils::TestResult;

/// Generate arbitrary time ranges within a useful bound.
fn arb_time_range() -> impl Strategy<Value = (DateTime<Utc>, DateTime<Utc>)> {
    let base_time = Utc::now();
    let year_ago = base_time - Duration::days(365);
    let year_future = base_time + Duration::days(365);

    (
        year_ago.timestamp()..=year_future.timestamp(),
        1i64..=Duration::days(30).num_seconds(),
    )
        .prop_map(move |(start_ts, duration_secs)| {
            let start = DateTime::from_timestamp(start_ts, 0).unwrap_or(base_time);
            let end = start + Duration::seconds(duration_secs);
            (start, end)
        })
}

fn ranges_overlap(
    (start1, end1): (DateTime<Utc>, DateTime<Utc>),
    (start2, end2): (DateTime<Utc>, DateTime<Utc>),
) -> bool {
    !(end1 < start2 || end2 < start1)
}

fn intersect_ranges(
    range1: (DateTime<Utc>, DateTime<Utc>),
    range2: (DateTime<Utc>, DateTime<Utc>),
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let start = range1.0.max(range2.0);
    let end = range1.1.min(range2.1);
    (start <= end).then_some((start, end))
}

fn partition_range(
    (start, end): (DateTime<Utc>, DateTime<Utc>),
    count: usize,
) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
    if count == 0 {
        return Vec::new();
    }

    let total = end - start;
    let step = total / (count as i32);
    let mut parts = Vec::with_capacity(count);
    let mut cursor = start;

    for i in 0..count {
        let next = if i == count - 1 { end } else { cursor + step };
        parts.push((cursor, next));
        cursor = next;
    }

    parts
}

#[sinex_prop]
fn time_range_ordering_invariant(
    #[strategy(arb_time_range())] range: (DateTime<Utc>, DateTime<Utc>),
) -> TestResult<()> {
    let (start, end) = range;
    prop_assert!(start <= end);
    prop_assert!((end - start) >= chrono::Duration::zero());
    Ok(())
}

#[sinex_prop]
fn time_range_overlap_symmetry(
    #[strategy(proptest::collection::vec(arb_time_range(), 2..=2))] ranges: Vec<(
        DateTime<Utc>,
        DateTime<Utc>,
    )>,
) -> TestResult<()> {
    let a = ranges[0];
    let b = ranges[1];
    prop_assert_eq!(ranges_overlap(a, b), ranges_overlap(b, a));
    if let Some((start, end)) = intersect_ranges(a, b) {
        prop_assert!(ranges_overlap(a, b));
        prop_assert!(start <= end);
    }
    Ok(())
}

#[sinex_prop]
fn time_range_partition_covers_interval(
    #[strategy(
        arb_time_range().prop_flat_map(|r| (Just(r), 1usize..=16))
    )]
    range_and_count: ((DateTime<Utc>, DateTime<Utc>), usize),
) -> TestResult<()> {
    let (range, count) = range_and_count;
    let parts = partition_range(range, count);
    prop_assert_eq!(parts.len(), count);
    if let Some(first) = parts.first() {
        prop_assert_eq!(first.0, range.0);
    }
    if let Some(last) = parts.last() {
        prop_assert_eq!(last.1, range.1);
    }

    for window in parts.windows(2) {
        let a = window[0];
        let b = window[1];
        prop_assert!(a.1 <= b.0);
    }
    Ok(())
}
