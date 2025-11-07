#![doc = include_str!("../doc/search.md")]

//! Search service orchestration.

use crate::error::ServiceResult;
use serde::{Deserialize, Serialize};
use sinex_core::db::DbPool;
use sinex_core::types::ulid::Ulid;
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

/// Database row for search query results
#[derive(Debug, sqlx::FromRow)]
struct SearchResultRow {
    event_id: Option<String>,
    source: String,
    event_type: String,
    host: String,
    ts_ingest: DateTime<Utc>,
    payload: serde_json::Value,
    score: f64,
}

pub struct SearchService {
    pool: DbPool,
}

const MAX_LIMIT: i32 = 1000;

impl SearchService {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Search events based on criteria using parameterized SQLx query building
    pub async fn search_events(&self, query: SearchQuery) -> ServiceResult<Vec<SearchResult>> {
        let query = normalize_query(query);

        // Base select. Score is a placeholder (1.0) to keep response shape stable.
        let mut sql = String::from(
            "SELECT id::text AS event_id, source, event_type, host, ts_ingest, payload, 1.0::float8 AS score \
             FROM core.events",
        );

        // Dynamic parameters (we'll append WHERE clauses and numbered placeholders below)

        // We’ll switch to a simpler approach: assemble dynamic SQL and keep an ordered list of bind values in an enum.

        #[derive(Debug)]
        enum Param {
            Text(String),
            Time(DateTime<Utc>),
            ILike(String),
            Limit(i64),
            Offset(i64),
        }

        let mut params: Vec<Param> = Vec::new();

        // Append filters
        let mut first_clause = true;
        let mut push_clause = |clause: &str| {
            if first_clause {
                sql.push_str(" WHERE ");
                first_clause = false;
            } else {
                sql.push_str(" AND ");
            }
            sql.push_str(clause);
        };

        if !query.sources.is_empty() {
            // e.g., source IN ($1,$2,...)
            let start = params.len();
            for s in &query.sources {
                params.push(Param::Text(s.clone()));
            }
            let count = params.len() - start;
            let placeholders: Vec<String> =
                (0..count).map(|i| format!("${}", start + i + 1)).collect();
            push_clause(&format!("source IN ({})", placeholders.join(",")));
        }

        if !query.event_types.is_empty() {
            let start = params.len();
            for t in &query.event_types {
                params.push(Param::Text(t.clone()));
            }
            let count = params.len() - start;
            let placeholders: Vec<String> =
                (0..count).map(|i| format!("${}", start + i + 1)).collect();
            push_clause(&format!("event_type IN ({})", placeholders.join(",")));
        }

        if let Some(start) = query.start_time {
            let idx = params.len() + 1;
            push_clause(&format!("ts_ingest >= ${}", idx));
            params.push(Param::Time(start));
        }
        if let Some(end) = query.end_time {
            let idx = params.len() + 1;
            push_clause(&format!("ts_ingest <= ${}", idx));
            params.push(Param::Time(end));
        }

        if let Some(text) = &query.text {
            let idx = params.len() + 1;
            push_clause(&format!("payload::text ILIKE ${}", idx));
            params.push(Param::ILike(format!("%{}%", text)));
        }

        // Order, limit, offset
        sql.push_str(" ORDER BY ts_ingest DESC");
        let limit_idx = params.len() + 1;
        sql.push_str(&format!(" LIMIT ${}", limit_idx));
        params.push(Param::Limit(query.limit as i64));
        let offset_idx = params.len() + 1;
        sql.push_str(&format!(" OFFSET ${}", offset_idx));
        params.push(Param::Offset(query.offset as i64));

        // Prepare query and bind in order
        let mut q = sqlx::query_as::<_, SearchResultRow>(&sql);
        for p in params {
            q = match p {
                Param::Text(s) => q.bind(s),
                Param::Time(ts) => q.bind(ts),
                Param::ILike(s) => q.bind(s),
                Param::Limit(v) => q.bind(v),
                Param::Offset(v) => q.bind(v),
            };
        }

        let rows = q.fetch_all(&self.pool).await?;

        let results = rows
            .into_iter()
            .filter_map(|row| {
                row.event_id
                    .and_then(|id| id.parse::<Ulid>().ok())
                    .map(|ulid| SearchResult {
                        event_id: ulid,
                        source: row.source,
                        event_type: row.event_type,
                        host: row.host,
                        timestamp: row.ts_ingest,
                        snippet: Self::extract_snippet(&row.payload, query.text.as_deref()),
                        score: row.score,
                    })
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

fn normalize_query(mut query: SearchQuery) -> SearchQuery {
    if query.limit <= 0 {
        query.limit = 50;
    }
    query.limit = query.limit.min(MAX_LIMIT);
    if query.offset < 0 {
        query.offset = 0;
    }
    query
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_query_enforces_bounds() {
        let query = SearchQuery {
            text: None,
            sources: vec![],
            event_types: vec![],
            start_time: None,
            end_time: None,
            limit: 10_000,
            offset: -5,
        };

        let normalized = normalize_query(query);
        assert_eq!(normalized.limit, MAX_LIMIT);
        assert_eq!(normalized.offset, 0);

        let query = SearchQuery {
            text: None,
            sources: vec![],
            event_types: vec![],
            start_time: None,
            end_time: None,
            limit: 0,
            offset: 0,
        };
        let normalized = normalize_query(query);
        assert_eq!(normalized.limit, 50);
    }
}
