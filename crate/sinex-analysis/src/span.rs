//! Half-open time spans in microseconds since the Unix epoch, and unions of
//! them. Durations are always unions, never sums of overlapping windows
//! (`analysis.md` §4.2, law `UnionDuration`).
use serde::{Deserialize, Serialize};

pub const MICROS_PER_SECOND: i64 = 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Span {
    pub start: i64,
    pub end: i64,
}

impl Span {
    /// A span; `end` before `start` gives an empty span at `start`.
    pub fn new(start: i64, end: i64) -> Self {
        Self {
            start,
            end: end.max(start),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.end <= self.start
    }

    pub fn len(&self) -> i64 {
        self.end - self.start
    }

    pub fn seconds(&self) -> f64 {
        self.len() as f64 / MICROS_PER_SECOND as f64
    }

    pub fn contains(&self, t: i64) -> bool {
        self.start <= t && t < self.end
    }

    pub fn intersect(&self, other: &Span) -> Option<Span> {
        let s = Span::new(self.start.max(other.start), self.end.min(other.end));
        (!s.is_empty()).then_some(s)
    }
}

/// A set of disjoint, sorted spans.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanSet(Vec<Span>);

impl SpanSet {
    pub fn from_spans(spans: impl IntoIterator<Item = Span>) -> Self {
        let mut v: Vec<Span> = spans.into_iter().filter(|s| !s.is_empty()).collect();
        v.sort();
        let mut out: Vec<Span> = Vec::with_capacity(v.len());
        for s in v {
            match out.last_mut() {
                Some(last) if s.start <= last.end => last.end = last.end.max(s.end),
                _ => out.push(s),
            }
        }
        Self(out)
    }

    pub fn spans(&self) -> &[Span] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Total covered length in microseconds.
    pub fn len(&self) -> i64 {
        self.0.iter().map(Span::len).sum()
    }

    /// The part of this set inside `window`.
    pub fn clip(&self, window: Span) -> SpanSet {
        SpanSet(self.0.iter().filter_map(|s| s.intersect(&window)).collect())
    }

    pub fn union(&self, other: &SpanSet) -> SpanSet {
        SpanSet::from_spans(self.0.iter().chain(other.0.iter()).copied())
    }

    pub fn intersect(&self, other: &SpanSet) -> SpanSet {
        let (mut i, mut j) = (0, 0);
        let mut out = Vec::new();
        while i < self.0.len() && j < other.0.len() {
            if let Some(s) = self.0[i].intersect(&other.0[j]) {
                out.push(s);
            }
            if self.0[i].end < other.0[j].end {
                i += 1;
            } else {
                j += 1;
            }
        }
        SpanSet(out)
    }

    /// This set with `other` removed.
    pub fn subtract(&self, other: &SpanSet) -> SpanSet {
        let mut out = Vec::new();
        for s in &self.0 {
            let mut cur = s.start;
            for o in &other.0 {
                if o.end <= cur || o.start >= s.end {
                    continue;
                }
                if o.start > cur {
                    out.push(Span::new(cur, o.start));
                }
                cur = cur.max(o.end);
            }
            if cur < s.end {
                out.push(Span::new(cur, s.end));
            }
        }
        SpanSet(out)
    }
}

#[cfg(test)]
mod tests;
