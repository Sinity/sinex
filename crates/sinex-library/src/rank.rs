//! Sorting by pairwise comparison, in the style of Gwern's Resorter: every judgement "A is better than B" is kept, and a
//! Bradley–Terry model is fitted to all of them. Each item gets a strength;
//! the order is by strength. A weak prior (one virtual win and one virtual
//! loss against an average item) keeps strengths finite for items that never
//! lost or never won.
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq)]
pub struct Ranked {
    pub item: String,
    /// Bradley–Terry strength, normalized so the geometric mean is 1.
    pub strength: f64,
    pub wins: u32,
    pub losses: u32,
}

/// Fit strengths to `(winner, loser)` judgements and return items strongest
/// first. Ties in strength order by item ID, so the order is repeatable.
pub fn bradley_terry(judgements: &[(String, String)]) -> Vec<Ranked> {
    let items: BTreeSet<&str> = judgements
        .iter()
        .flat_map(|(w, l)| [w.as_str(), l.as_str()])
        .collect();
    if items.is_empty() {
        return Vec::new();
    }
    let index: BTreeMap<&str, usize> = items.iter().enumerate().map(|(i, s)| (*s, i)).collect();
    let n = items.len();
    let mut wins = vec![0u32; n];
    let mut losses = vec![0u32; n];
    // Games between each pair, in either order.
    let mut games: BTreeMap<(usize, usize), f64> = BTreeMap::new();
    for (w, l) in judgements {
        let (wi, li) = (index[w.as_str()], index[l.as_str()]);
        if wi == li {
            continue;
        }
        wins[wi] += 1;
        losses[li] += 1;
        *games.entry((wi.min(li), wi.max(li))).or_default() += 1.0;
    }
    // The prior: each item played one virtual game won and one lost against
    // a pseudo-item of strength 1.
    let mut p = vec![1.0f64; n];
    for _ in 0..200 {
        let mut next = vec![0.0f64; n];
        let mut denominators = vec![0.0f64; n];
        for (&(i, j), &count) in &games {
            let d = count / (p[i] + p[j]);
            denominators[i] += d;
            denominators[j] += d;
        }
        let mut change = 0.0f64;
        for i in 0..n {
            let w = f64::from(wins[i]) + 1.0;
            let d = denominators[i] + 2.0 / (p[i] + 1.0);
            next[i] = w / d;
        }
        let log_mean = next.iter().map(|v| v.ln()).sum::<f64>() / n as f64;
        for (i, v) in next.iter_mut().enumerate() {
            *v /= log_mean.exp();
            change = change.max((*v - p[i]).abs());
        }
        p = next;
        if change < 1e-9 {
            break;
        }
    }
    let mut out: Vec<Ranked> = items
        .iter()
        .enumerate()
        .map(|(i, item)| Ranked {
            item: (*item).to_owned(),
            strength: p[i],
            wins: wins[i],
            losses: losses[i],
        })
        .collect();
    out.sort_by(|a, b| {
        b.strength
            .partial_cmp(&a.strength)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.item.cmp(&b.item))
    });
    out
}

/// The pair whose comparison would tell the most: the two adjacent items in
/// the current order with the fewest games between them, so the next
/// judgement goes where the order is least certain.
pub fn next_pair(ranked: &[Ranked], judgements: &[(String, String)]) -> Option<(String, String)> {
    let played = |a: &str, b: &str| {
        judgements
            .iter()
            .filter(|(w, l)| (w == a && l == b) || (w == b && l == a))
            .count()
    };
    ranked
        .windows(2)
        .min_by_key(|w| played(&w[0].item, &w[1].item))
        .map(|w| (w[0].item.clone(), w[1].item.clone()))
}

#[cfg(test)]
mod tests;
