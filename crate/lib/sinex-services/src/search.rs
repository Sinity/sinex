//! Search service for querying events and content

use crate::error::ServiceResult;
use sea_query::{Alias, Expr, PostgresQueryBuilder, Query};
use serde::{Deserialize, Serialize};
use sinex_core::db::{schema::Events, seaquery_helpers::SeaQueryUlidExt, DbPool};
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
    ts_ingest: DateTime<Utc>,
    payload: serde_json::Value,
    score: f64,
}

pub struct SearchService {
    pool: DbPool,
}

impl SearchService {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Search events based on criteria using SeaQuery for type safety
    pub async fn search_events(&self, query: SearchQuery) -> ServiceResult<Vec<SearchResult>> {
        // Build dynamic query using SeaQuery for type safety and SQL injection prevention
        let mut select_query = Query::select()
            .expr_as(
                Expr::col((
                    Alias::new("core"),
                    Alias::new(Events::Table),
                    Alias::new(Events::Id),
                ))
                .cast_as(Alias::new("text")),
                Alias::new("event_id"),
            )
            .column((
                Alias::new("core"),
                Alias::new(Events::Table),
                Alias::new(Events::Source),
            ))
            .column((
                Alias::new("core"),
                Alias::new(Events::Table),
                Alias::new(Events::EventType),
            ))
            .column((
                Alias::new("core"),
                Alias::new(Events::Table),
                Alias::new(Events::TsIngest),
            ))
            .column((
                Alias::new("core"),
                Alias::new(Events::Table),
                Alias::new(Events::Payload),
            ))
            .expr_as(Expr::val(1.0_f64), Alias::new("score"))
            .from(Events::table_iden())
            .to_owned();

        // Add source filter using proper parameterization
        if !query.sources.is_empty() {
            select_query.and_where(
                Expr::col((
                    Alias::new("core"),
                    Alias::new(Events::Table),
                    Alias::new(Events::Source),
                ))
                .is_in(query.sources.iter().cloned()),
            );
        }

        // Add event type filter using proper parameterization
        if !query.event_types.is_empty() {
            select_query.and_where(
                Expr::col((
                    Alias::new("core"),
                    Alias::new(Events::Table),
                    Alias::new(Events::EventType),
                ))
                .is_in(query.event_types.iter().cloned()),
            );
        }

        // Add time range filters with proper type handling
        if let Some(start) = query.start_time {
            select_query.and_where(
                Expr::col((
                    Alias::new("core"),
                    Alias::new(Events::Table),
                    Alias::new(Events::TsIngest),
                ))
                .gte(start),
            );
        }

        if let Some(end) = query.end_time {
            select_query.and_where(
                Expr::col((
                    Alias::new("core"),
                    Alias::new(Events::Table),
                    Alias::new(Events::TsIngest),
                ))
                .lte(end),
            );
        }

        // Add text search with proper parameterization (SeaQuery prevents SQL injection)
        if let Some(text) = &query.text {
            select_query.and_where(
                Expr::col((
                    Alias::new("core"),
                    Alias::new(Events::Table),
                    Alias::new(Events::Payload),
                ))
                .cast_as(Alias::new("text"))
                .ilike(Expr::val(format!("%{}%", text))),
            );
        }

        // Add ordering and limits
        select_query
            .order_by(
                (
                    Alias::new("core"),
                    Alias::new(Events::Table),
                    Alias::new(Events::TsIngest),
                ),
                sea_query::Order::Desc,
            )
            .limit(query.limit as u64)
            .offset(query.offset as u64);

        // Build the SQL query
        let (sql, _values) = select_query.build(PostgresQueryBuilder);

        // Execute the type-safe query using the dedicated struct
        let rows = sqlx::query_as::<_, SearchResultRow>(&sql)
            .fetch_all(&self.pool)
            .await?;

        let results = rows
            .into_iter()
            .filter_map(|row| {
                row.event_id
                    .and_then(|id| id.parse::<Ulid>().ok())
                    .map(|ulid| SearchResult {
                        event_id: ulid,
                        source: row.source,
                        event_type: row.event_type,
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
            if let Some(pos) = payload_str.to_lowercase().find(&text.to_lowercase()) {
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
