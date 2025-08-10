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
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::ID),
                ))
                .cast_as(Alias::new("text")),
                Alias::new("event_id"),
            )
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::SOURCE),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::EVENT_TYPE),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::TS_INGEST),
            ))
            .column((
                Alias::new(Events::SCHEMA),
                Alias::new(Events::TABLE),
                Alias::new(Events::PAYLOAD),
            ))
            .expr_as(Expr::val(1.0_f64), Alias::new("score"))
            .from((Alias::new(Events::SCHEMA), Alias::new(Events::TABLE)))
            .to_owned();

        // Add source filter using proper parameterization
        if !query.sources.is_empty() {
            select_query.and_where(
                Expr::col((
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::SOURCE),
                ))
                .is_in(query.sources.iter().cloned()),
            );
        }

        // Add event type filter using proper parameterization
        if !query.event_types.is_empty() {
            select_query.and_where(
                Expr::col((
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::EVENT_TYPE),
                ))
                .is_in(query.event_types.iter().cloned()),
            );
        }

        // Add time range filters with proper type handling
        if let Some(start) = query.start_time {
            select_query.and_where(
                Expr::col((
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::TS_INGEST),
                ))
                .gte(start),
            );
        }

        if let Some(end) = query.end_time {
            select_query.and_where(
                Expr::col((
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::TS_INGEST),
                ))
                .lte(end),
            );
        }

        // Add text search with proper parameterization (SeaQuery prevents SQL injection)
        if let Some(text) = &query.text {
            select_query.and_where(
                Expr::col((
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::PAYLOAD),
                ))
                .cast_as(Alias::new("text"))
                .ilike(format!("%{}%", text)),
            );
        }

        // Add ordering and limits
        select_query
            .order_by(
                (
                    Alias::new(Events::SCHEMA),
                    Alias::new(Events::TABLE),
                    Alias::new(Events::TS_INGEST),
                ),
                sea_query::Order::Desc,
            )
            .limit(query.limit as u64)
            .offset(query.offset as u64);

        // Build the SQL query
        let (sql, _values) = select_query.build(PostgresQueryBuilder);

        // Execute the type-safe query
        let rows = sqlx::query_as::<
            _,
            (
                Option<String>,
                String,
                String,
                DateTime<Utc>,
                serde_json::Value,
                f64,
            ),
        >(&sql)
        .fetch_all(&self.pool)
        .await?;

        let results = rows
            .into_iter()
            .filter_map(
                |(event_id, source, event_type, timestamp, payload, score)| {
                    event_id
                        .and_then(|id| id.parse::<Ulid>().ok())
                        .map(|ulid| SearchResult {
                            event_id: ulid,
                            source,
                            event_type,
                            timestamp,
                            snippet: Self::extract_snippet(&payload, query.text.as_deref()),
                            score,
                        })
                },
            )
            .collect();

        Ok(results)
    }

    /// Extract a text snippet from the payload
    fn extract_snippet(payload: &serde_json::Value, search_text: Option<&str>) -> String {
        let payload_str = serde_json::to_string_pretty(payload).unwrap_or_default();

        if let Some(text) = search_text {
            // Find the search text and return surrounding context
            if let Some(pos) = payload_str.to_lowercase().find(&text.to_lowercase()) {
                let start = pos.saturating_sub(50);
                let end = (pos + text.len() + 50).min(payload_str.len());
                return format!("...{}...", &payload_str[start..end]);
            }
        }

        // Return first 150 chars if no search text or not found
        if payload_str.len() > 150 {
            format!("{}...", &payload_str[..150])
        } else {
            payload_str
        }
    }
}
