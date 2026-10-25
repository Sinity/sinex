//! Benjamini-Hochberg q-values across a declared family. The family is fixed
//! before results are seen, and cells that produced no estimate stay in it as
//! non-findings rather than shrinking it.

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
mod tests;
