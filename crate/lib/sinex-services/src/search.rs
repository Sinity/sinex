#![doc = include_str!("../docs/search.md")]

//! Search service orchestration.

use crate::error::ServiceResult;
use serde::{Deserialize, Serialize};
use sinex_db::{
    repositories::{DbPoolExt, EventSearchFilters},
    DbPool,
};
use sinex_primitives::{
    domain::HostName, EventSource, EventType, Pagination, TimeRange, Timestamp, Ulid,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    pub text: Option<String>,
    pub sources: Vec<EventSource>,
    pub event_types: Vec<EventType>,
    pub start_time: Option<Timestamp>,
    pub end_time: Option<Timestamp>,
    pub limit: i32,
    pub offset: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub event_id: Ulid,
    pub source: EventSource,
    pub event_type: EventType,
    pub host: HostName,
    pub timestamp: Timestamp,
    pub snippet: String,
    pub score: f64,
}

pub struct SearchService {
    pool: DbPool,
}

impl SearchService {
    #[must_use]
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Search events based on criteria using parameterized `SQLx` query building
    pub async fn search_events(&self, query: SearchQuery) -> ServiceResult<Vec<SearchResult>> {
        let prepared = PreparedSearch::new(query)?;
        let snippet_text = prepared.search_text.as_deref();

        let rows = self.pool.events().search(prepared.filters).await?;

        let results = rows
            .into_iter()
            .map(|row| {
                let snippet = row.snippet.map_or_else(
                    || Self::extract_snippet(&row.payload, snippet_text),
                    |value| value.replace("<b>", "").replace("</b>", ""),
                );
                let score = row.score.unwrap_or(1.0);
                SearchResult {
                    event_id: row.id,
                    source: row.source,
                    event_type: row.event_type,
                    host: row.host,
                    timestamp: row.ts_ingest,
                    snippet,
                    score,
                }
            })
            .collect();

        Ok(results)
    }

    /// Extract a text snippet from the payload with UTF-8 safe truncation
    fn extract_snippet(payload: &serde_json::Value, search_text: Option<&str>) -> String {
        // Compact serialization — prettier formatting adds noise to a one-line snippet.
        let payload_str = serde_json::to_string(payload).unwrap_or_default();

        if let Some(text) = search_text {
            let haystack = payload_str.to_lowercase();
            let needle = text.to_lowercase();
            if let Some(pos) = haystack.find(&needle) {
                // SAFETY: extract context from `haystack` (not `payload_str`).
                // `pos` is a valid byte boundary in `haystack` but may not be in
                // `payload_str` because `to_lowercase()` can change byte lengths
                // (e.g., İ U+0130 is 2 bytes but lowercases to i + combining mark,
                // 3 bytes). Working solely within `haystack` avoids this panic risk.
                return Self::safe_substring_with_context(&haystack, pos, needle.len(), 50);
            }
        }

        // Return first 150 chars if no search text or not found
        Self::safe_truncate(&payload_str, 150)
    }

    /// Safely truncate a string at a character boundary.
    ///
    /// Stops at `max_chars` characters rather than counting all N first (O(1) exit).
    fn safe_truncate(s: &str, max_chars: usize) -> String {
        match s.char_indices().nth(max_chars) {
            None => s.to_string(),
            Some((byte_pos, _)) => format!("{}...", &s[..byte_pos]),
        }
    }

    /// Extract a window of `context_chars` characters around a match in `s`.
    ///
    /// `match_pos` and `match_len` must be valid byte offsets/lengths within `s`.
    fn safe_substring_with_context(
        s: &str,
        match_pos: usize,
        match_len: usize,
        context_chars: usize,
    ) -> String {
        let chars: Vec<char> = s.chars().collect();
        let total_chars = chars.len();

        let char_pos = s[..match_pos].chars().count();
        let match_char_len = s[match_pos..match_pos + match_len].chars().count();

        let start = char_pos.saturating_sub(context_chars);
        let end = (char_pos + match_char_len + context_chars).min(total_chars);

        let substring: String = chars[start..end].iter().collect();
        format!("...{substring}...")
    }
}

#[derive(Debug)]
struct PreparedSearch {
    filters: EventSearchFilters,
    search_text: Option<String>,
}

impl PreparedSearch {
    fn new(query: SearchQuery) -> ServiceResult<Self> {
        let SearchQuery {
            text,
            sources,
            event_types,
            start_time,
            end_time,
            limit,
            offset,
        } = query;

        let pagination = Pagination::with_bounds(
            Some(i64::from(limit)),
            Some(i64::from(offset)),
            Pagination::DEFAULT_LIMIT,
            Pagination::MAX_LIMIT,
        );

        let time_range = match (start_time, end_time) {
            (None, None) => None,
            (start, end) => Some(TimeRange::new(start, end)?),
        };

        let search_text = text.and_then(|t| {
            let trimmed = t.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

        let filters = EventSearchFilters {
            sources,
            event_types,
            text_query: search_text.clone(),
            time_range,
            pagination,
            ..Default::default()
        };

        Ok(Self {
            filters,
            search_text,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use xtask::sandbox::sinex_test;

    #[sinex_test]
    fn prepared_search_clamps_pagination() -> TestResult<()> {
        let query = SearchQuery {
            text: None,
            sources: vec![],
            event_types: vec![],
            start_time: None,
            end_time: None,
            limit: 10_000,
            offset: -5,
        };

        let prepared = PreparedSearch::new(query).expect("preparation succeeds");
        assert_eq!(prepared.filters.pagination.limit(), Pagination::MAX_LIMIT);
        assert_eq!(prepared.filters.pagination.offset(), 0);

        let query = SearchQuery {
            text: None,
            sources: vec![],
            event_types: vec![],
            start_time: None,
            end_time: None,
            limit: 0,
            offset: 0,
        };
        let prepared = PreparedSearch::new(query).expect("preparation succeeds");
        assert_eq!(
            prepared.filters.pagination.limit(),
            Pagination::DEFAULT_LIMIT
        );
        assert_eq!(prepared.filters.pagination.offset(), 0);
        Ok(())
    }

    #[sinex_test]
    fn prepared_search_validates_time_range() -> TestResult<()> {
        let start = Timestamp::now();
        let end = start - time::Duration::hours(1);

        let query = SearchQuery {
            text: None,
            sources: vec![],
            event_types: vec![],
            start_time: Some(start),
            end_time: Some(end),
            limit: 10,
            offset: 0,
        };

        assert!(PreparedSearch::new(query).is_err());
        Ok(())
    }
}
