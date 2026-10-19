use super::*;

fn set(v: &[(i64, i64)]) -> SpanSet {
    SpanSet::from_spans(v.iter().map(|&(a, b)| Span::new(a, b)))
}

#[test]
fn overlapping_spans_union_instead_of_summing() {
    let s = set(&[(0, 10), (5, 15), (20, 30), (30, 31)]);
    assert_eq!(s.spans(), &[Span::new(0, 15), Span::new(20, 31)]);
    assert_eq!(s.len(), 26);
}

#[test]
fn intersect_subtract_and_clip_agree_on_lengths() {
    let a = set(&[(0, 10), (20, 40)]);
    let b = set(&[(5, 25), (35, 50)]);
    assert_eq!(a.intersect(&b).len(), 5 + 5 + 5);
    assert_eq!(a.subtract(&b).len(), 5 + 10);
    assert_eq!(a.intersect(&b).len() + a.subtract(&b).len(), a.len());
    assert_eq!(a.clip(Span::new(8, 22)).len(), 4);
    assert_eq!(a.union(&b).len(), 50);
}
