//! Suggestions for a mistyped first word.
use super::*;

/// A first word within two typing mistakes of a table, alias or command.
pub(super) fn typo(word: &str) -> Option<&'static str> {
    if word.len() < 4 || RESERVED.contains(&word) || Table::Events.field(word).is_some() {
        return None;
    }
    table_like_words().find(|other| *other != word && distance(word, other) <= 2)
}

/// Levenshtein distance over bytes.
pub(super) fn distance(a: &str, b: &str) -> usize {
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    for (i, x) in a.bytes().enumerate() {
        let mut next = vec![i + 1];
        for (j, y) in b.bytes().enumerate() {
            next.push(
                (previous[j + 1] + 1)
                    .min(next[j] + 1)
                    .min(previous[j] + usize::from(x != y)),
            );
        }
        previous = next;
    }
    previous[b.len()]
}
