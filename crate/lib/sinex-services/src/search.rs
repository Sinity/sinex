//! Search service for querying events and content

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

    /// Search events based on criteria
    pub async fn search_events(&self, query: SearchQuery) -> ServiceResult<Vec<SearchResult>> {
        let mut sql = String::from(
            r#"
            SELECT 
                id::text as event_id,
                source,
                event_type,
                ts_ingest,
                payload,
                1.0 as score
            FROM core.events
            WHERE 1=1
            "#,
        );

        let mut params: Vec<String> = Vec::new();
        let mut param_count = 0;

        // Add source filter
        if !query.sources.is_empty() {
            param_count += 1;
            sql.push_str(&format!(" AND source = ANY(${})", param_count));
            params.push(format!("{{{}}}", query.sources.join(",")));
        }

        // Add event type filter
        if !query.event_types.is_empty() {
            param_count += 1;
            sql.push_str(&format!(" AND event_type = ANY(${})", param_count));
            params.push(format!("{{{}}}", query.event_types.join(",")));
        }

        // Add time range filter
        if let Some(start) = query.start_time {
            param_count += 1;
            sql.push_str(&format!(" AND ts_ingest >= ${}", param_count));
            params.push(start.to_rfc3339());
        }

        if let Some(end) = query.end_time {
            param_count += 1;
            sql.push_str(&format!(" AND ts_ingest <= ${}", param_count));
            params.push(end.to_rfc3339());
        }

        // Add text search if provided
        if let Some(text) = &query.text {
            param_count += 1;
            sql.push_str(&format!(" AND payload::text ILIKE ${}", param_count));
            params.push(format!("%{}%", text));
        }

        // Add ordering and limits
        sql.push_str(" ORDER BY ts_ingest DESC");
        sql.push_str(&format!(" LIMIT {} OFFSET {}", query.limit, query.offset));

        // Execute the dynamic query
        // Note: This is a simplified version. In production, you'd use
        // a query builder or more sophisticated full-text search
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
