//! Benjamini-Hochberg q-values across a declared family (`analysis.md` §6.3).
//! The family is fixed before results are seen, and cells that produced no
//! estimate stay in it as non-findings rather than shrinking it.

/// q-values for `p`, in the input order. `None` entries (cells that were not
/// estimable) count toward the family size and get no q.
pub fn bh_qvalues(p: &[Option<f64>]) -> Vec<Option<f64>> {
    let m = p.len() as f64;
    let mut idx: Vec<usize> = (0..p.len()).filter(|&i| p[i].is_some()).collect();
    idx.sort_by(|&a, &b| p[a].unwrap_or(1.0).total_cmp(&p[b].unwrap_or(1.0)));
    let mut q = vec![None; p.len()];
    let mut running = 1.0f64;
    for (rank, &i) in idx.iter().enumerate().rev() {
        let raw = p[i].unwrap_or(1.0) * m / (rank + 1) as f64;
        running = running.min(raw).min(1.0);
        q[i] = Some(running);
    }
    q
}

#[cfg(test)]
mod tests {
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
}
