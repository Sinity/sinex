use super::*;

#[test]
fn matches_the_textbook_procedure() {
    let p = [0.01, 0.04, 0.03, 0.005];
    let q: Vec<f64> = bh_qvalues(&p.map(Some))
        .into_iter()
        .map(Option::unwrap)
        .collect();
    // Sorted p: .005 .01 .03 .04 → ×4/rank: .02 .02 .04 .04.
    for (got, want) in q.iter().zip([0.02, 0.04, 0.04, 0.02]) {
        assert!((got - want).abs() < 1e-12, "{q:?}");
    }
}

#[test]
fn non_estimable_cells_keep_the_family_size() {
    let q = bh_qvalues(&[Some(0.01), None]);
    assert!((q[0].unwrap() - 0.02).abs() < 1e-12);
    assert_eq!(q[1], None);
}
