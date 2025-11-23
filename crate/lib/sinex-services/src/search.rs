#![doc = include_str!("../doc/search.md")]

//! Search service orchestration.

use crate::error::ServiceResult;
use serde::{Deserialize, Serialize};
use sinex_core::db::{
    repositories::{DbPoolExt, EventSearchFilters},
    DbPool,
};
use sinex_core::types::{
    domain::{EventSource, EventType},
    ulid::Ulid,
    Pagination, TimeRange,
};
use sqlx::types::chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    pub text: Option<String>,
    pub sources: Vec<String>,
    pub event_types: Vec<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub limit: i32,
    pub offset: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub event_id: Ulid,
    pub source: String,
    pub event_type: String,
    pub host: String,
    pub timestamp: DateTime<Utc>,
    pub snippet: String,
    pub score: f64,
}

pub struct SearchService {
    pool: DbPool,
}

impl SearchService {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Search events based on criteria using parameterized SQLx query building
    pub async fn search_events(&self, query: SearchQuery) -> ServiceResult<Vec<SearchResult>> {
        let prepared = PreparedSearch::new(query)?;
        let snippet_text = prepared.search_text.as_deref();

        let rows = self.pool.events().search(prepared.filters).await?;

        let results = rows
            .into_iter()
            .map(|row| SearchResult {
                event_id: row.id,
                source: row.source.into_string(),
                event_type: row.event_type.into_string(),
                host: row.host.into_string(),
                timestamp: row.ts_ingest,
                snippet: Self::extract_snippet(&row.payload, snippet_text),
                score: 1.0,
            })
            .collect();

        Ok(results)
    }

    /// Extract a text snippet from the payload with UTF-8 safe truncation
    fn extract_snippet(payload: &serde_json::Value, search_text: Option<&str>) -> String {
        let payload_str = serde_json::to_string_pretty(payload).unwrap_or_default();

        if let Some(text) = search_text {
            // Find the search text and return surrounding context
            let haystack = payload_str.to_ascii_lowercase();
            let needle = text.to_ascii_lowercase();
            if let Some(pos) = haystack.find(&needle) {
                return Self::safe_substring_with_context(&payload_str, pos, text.len(), 50);
            }
        }

        // Return first 150 chars if no search text or not found
        Self::safe_truncate(&payload_str, 150)
    }

    /// Safely truncate a string at UTF-8 character boundaries
    fn safe_truncate(s: &str, max_chars: usize) -> String {
        if s.chars().count() <= max_chars {
            s.to_string()
        } else {
            let truncated: String = s.chars().take(max_chars).collect();
            format!("{}...", truncated)
        }
    }

    /// Safely extract substring with context around a match position
    fn safe_substring_with_context(
        s: &str,
        match_pos: usize,
        match_len: usize,
        context_chars: usize,
    ) -> String {
        let chars: Vec<char> = s.chars().collect();
        let total_chars = chars.len();

        // Convert byte position to character position (approximately)
        let char_pos = s[..match_pos].chars().count();
        let match_char_len = s[match_pos..match_pos + match_len].chars().count();

        let start = char_pos.saturating_sub(context_chars);
        let end = (char_pos + match_char_len + context_chars).min(total_chars);

        let substring: String = chars[start..end].iter().collect();
        format!("...{}...", substring)
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
            Some(limit as i64),
            Some(offset as i64),
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

        let sources = sources.into_iter().map(EventSource::from).collect();
        let event_types = event_types.into_iter().map(EventType::from).collect();

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
    use sinex_test_utils::sinex_test;

    #[allow(dead_code)]
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
        let start = Utc::now();
        let end = start - chrono::Duration::hours(1);

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
