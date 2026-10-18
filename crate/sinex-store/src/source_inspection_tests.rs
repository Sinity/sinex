use super::statement_millis;
use std::time::{Duration, Instant};

#[test]
fn detail_statements_share_one_deadline_capped_at_100ms() {
    let start = Instant::now();
    let deadline = start + Duration::from_millis(200);
    assert_eq!(statement_millis(deadline, start), Some(100));
    assert_eq!(
        statement_millis(deadline, start + Duration::from_millis(175)),
        Some(25)
    );
    assert_eq!(
        statement_millis(deadline, deadline - Duration::from_micros(500)),
        None
    );
    assert_eq!(statement_millis(deadline, deadline), None);
    assert_eq!(
        statement_millis(deadline, deadline + Duration::from_millis(1)),
        None
    );
}
