use super::joined;
use time::OffsetDateTime;

fn at(second: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000 + second).unwrap()
}

/// Spans that share an instant are asked as one; a gap keeps them apart,
/// since an output inside the gap covers part of neither. A span that ends
/// before it starts is asked as offered.
#[test]
fn spans_sharing_an_instant_are_asked_once() {
    let mut spans = vec![
        (at(40), at(50)),
        (at(0), at(10)),
        (at(10), at(20)),
        (at(5), at(8)),
        (at(0), at(10)),
        (at(60), at(30)),
        (at(21), at(22)),
    ];
    assert_eq!(
        joined(&mut spans),
        [
            (at(0), at(20)),
            (at(21), at(22)),
            (at(40), at(50)),
            (at(60), at(30)),
        ]
    );
}
