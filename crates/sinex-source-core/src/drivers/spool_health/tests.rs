use super::*;
use serde_json::json;

#[devkit::test]
fn acknowledgments_clear_completed_segments_and_partial_bytes() {
    let mut backlog = Backlog::default();
    backlog.captured_line(0, 10);
    backlog.captured(3, 0);
    backlog.segments.get_mut(&0).unwrap().bytes += 3;
    backlog.captured_line(1, 20);
    backlog.captured_line(1, 30);
    let first = json!({"segment":0,"inner":{"offset":10}});
    backlog.prepare(&first, 1);
    backlog.recovered(&first);
    assert_eq!((backlog.counts.bytes, backlog.counts.records), (53, 2));
    let next = json!({"segment":1,"inner":{"offset":20}});
    backlog.prepare(&next, 1);
    // A failed publish leaves every byte counted while capture continues.
    backlog.captured_line(1, 40);
    assert_eq!((backlog.counts.bytes, backlog.counts.records), (93, 3));
    backlog.recovered(&next);
    assert_eq!((backlog.counts.bytes, backlog.counts.records), (70, 2));
    let end = json!({"segment":1,"inner":{"offset":90}});
    backlog.prepare(&end, 2);
    backlog.recovered(&end);
    assert_eq!((backlog.counts.bytes, backlog.counts.records), (0, 0));
}
