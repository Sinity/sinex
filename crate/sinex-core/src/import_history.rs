use crate::{DbPool, Result, CoreError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_ulid::Ulid;
use std::str::FromStr;

/// Import history record derived from scanner events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportRecord {
    pub scan_id: Ulid,
    pub source_name: String,
    pub scan_started_at: DateTime<Utc>,
    pub scan_completed_at: Option<DateTime<Utc>>,
    pub content_hash: Option<String>,
    pub blob_id: Option<Ulid>,
    pub events_generated: u64,
    pub time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    pub duration_ms: Option<u64>,
    pub was_duplicate: bool,
}

/// Sensor coverage period derived from sensor events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorCoverage {
    pub source_name: String,
    pub started_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub is_active: bool,
}

/// Import history query helper that uses events instead of dedicated tables
pub struct ImportHistoryQuerier {
    pool: DbPool,
}

impl ImportHistoryQuerier {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Query previous import history for a source using scanner events
    pub async fn query_previous_imports(&self, source_name: &str) -> Result<Vec<ImportRecord>> {
        let records = sqlx::query!(
            r#"
            SELECT 
                started.payload->>'scan_id' as scan_id,
                started.source as source_name,
                started.ts_ingest as scan_started_at,
                completed.ts_ingest as scan_completed_at,
                completed.payload->>'content_hash' as content_hash,
                (completed.payload->>'blob_id')::uuid as blob_id_uuid,
                (completed.payload->>'events_generated')::bigint as events_generated,
                (completed.payload->>'min_time')::timestamptz as min_time,
                (completed.payload->>'max_time')::timestamptz as max_time,
                (completed.payload->>'duration_ms')::bigint as duration_ms,
                (completed.payload->>'was_duplicate')::boolean as was_duplicate
            FROM raw.events started
            LEFT JOIN raw.events completed 
                ON completed.payload->>'scan_id' = started.payload->>'scan_id'
                AND completed.event_type = 'scan.completed'
                AND completed.source = started.source
            WHERE started.source = $1
                AND started.event_type = 'scan.started'
            ORDER BY started.ts_ingest DESC
            LIMIT 50
            "#,
            source_name
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::Database(format!("Failed to query import history: {}", e)))?;

        let mut imports = Vec::new();
        for row in records {
            let scan_id = row.scan_id.as_ref()
                .and_then(|s| Ulid::from_str(s).ok())
                .unwrap_or_else(Ulid::new);

            let time_range = match (row.min_time, row.max_time) {
                (Some(min), Some(max)) => Some((min, max)),
                _ => None,
            };

            let blob_id = row.blob_id_uuid
                .map(|uuid| Ulid::from_uuid(uuid));

            imports.push(ImportRecord {
                scan_id,
                source_name: row.source_name,
                scan_started_at: row.scan_started_at.unwrap_or_else(Utc::now),
                scan_completed_at: row.scan_completed_at,
                content_hash: row.content_hash,
                blob_id,
                events_generated: row.events_generated.unwrap_or(0) as u64,
                time_range,
                duration_ms: row.duration_ms.map(|d| d as u64),
                was_duplicate: row.was_duplicate.unwrap_or(false),
            });
        }

        Ok(imports)
    }

    /// Query sensor coverage periods for a source using sensor events
    pub async fn query_sensor_coverage(&self, source_name: &str) -> Result<Vec<SensorCoverage>> {
        let records = sqlx::query!(
            r#"
            SELECT 
                started.source as source_name,
                started.ts_ingest as started_at,
                stopped.ts_ingest as stopped_at
            FROM raw.events started
            LEFT JOIN raw.events stopped 
                ON stopped.source = started.source
                AND stopped.event_type = 'sensor.stopped'
                AND stopped.ts_ingest > started.ts_ingest
                AND NOT EXISTS (
                    -- No other start after this start but before this stop
                    SELECT 1 FROM raw.events other_start
                    WHERE other_start.source = started.source
                        AND other_start.event_type = 'sensor.started'
                        AND other_start.ts_ingest > started.ts_ingest
                        AND other_start.ts_ingest < COALESCE(stopped.ts_ingest, NOW())
                )
            WHERE started.source = $1
                AND started.event_type = 'sensor.started'
            ORDER BY started.ts_ingest DESC
            LIMIT 10
            "#,
            source_name
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::Database(format!("Failed to query sensor coverage: {}", e)))?;

        let mut coverage = Vec::new();
        for row in records {
            coverage.push(SensorCoverage {
                source_name: row.source_name,
                started_at: row.started_at.unwrap_or_else(Utc::now),
                stopped_at: row.stopped_at,
                is_active: row.stopped_at.is_none(),
            });
        }

        Ok(coverage)
    }

    /// Check if content with this hash was already imported
    pub async fn check_previous_import_by_hash(&self, content_hash: &str) -> Result<Option<ImportRecord>> {
        let record = sqlx::query!(
            r#"
            SELECT 
                started.payload->>'scan_id' as scan_id,
                started.source as source_name,
                started.ts_ingest as scan_started_at,
                completed.ts_ingest as scan_completed_at,
                completed.payload->>'content_hash' as content_hash,
                (completed.payload->>'blob_id')::uuid as blob_id_uuid,
                (completed.payload->>'events_generated')::bigint as events_generated,
                (completed.payload->>'duration_ms')::bigint as duration_ms,
                (completed.payload->>'was_duplicate')::boolean as was_duplicate
            FROM raw.events completed
            JOIN raw.events started 
                ON started.payload->>'scan_id' = completed.payload->>'scan_id'
                AND started.event_type = 'scan.started'
                AND started.source = completed.source
            WHERE completed.event_type = 'scan.completed'
                AND completed.payload->>'content_hash' = $1
            ORDER BY completed.ts_ingest DESC
            LIMIT 1
            "#,
            content_hash
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CoreError::Database(format!("Failed to check previous import: {}", e)))?;

        if let Some(row) = record {
            let scan_id = row.scan_id.as_ref()
                .and_then(|s| Ulid::from_str(s).ok())
                .unwrap_or_else(Ulid::new);

            let blob_id = row.blob_id_uuid
                .map(|uuid| Ulid::from_uuid(uuid));

            Ok(Some(ImportRecord {
                scan_id,
                source_name: row.source_name,
                scan_started_at: row.scan_started_at.unwrap_or_else(Utc::now),
                scan_completed_at: row.scan_completed_at,
                content_hash: Some(content_hash.to_string()),
                blob_id,
                events_generated: row.events_generated.unwrap_or(0) as u64,
                time_range: None, // Not queried in this function
                duration_ms: row.duration_ms.map(|d| d as u64),
                was_duplicate: row.was_duplicate.unwrap_or(false),
            }))
        } else {
            Ok(None)
        }
    }

    /// Display formatted import history for a source
    pub async fn display_import_history(&self, source_name: &str) -> Result<String> {
        let imports = self.query_previous_imports(source_name).await?;
        let sensor_periods = self.query_sensor_coverage(source_name).await?;

        let formatted_imports = format_imports(&imports);
        let formatted_sensors = format_sensor_periods(&sensor_periods);

        Ok(format!(
            "📁 Import History for {}\n\
             ━━━━━━━━━━━━━━━━━━━━━━━━\n\
             Previous imports:\n{}\n\
             Sensor coverage:\n{}",
            source_name, formatted_imports, formatted_sensors
        ))
    }

    /// Get import statistics for a source
    pub async fn get_import_stats(&self, source_name: &str) -> Result<ImportStats> {
        // Get basic scan count
        let total_scans = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM raw.events WHERE source = $1 AND event_type = 'scan.started'",
            source_name
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CoreError::Database(format!("Failed to get scan count: {}", e)))?
        .unwrap_or(0) as u64;

        // Get duplicate count
        let duplicate_scans = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM raw.events WHERE source = $1 AND event_type = 'scan.completed' AND payload->>'was_duplicate' = 'true'",
            source_name
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CoreError::Database(format!("Failed to get duplicate count: {}", e)))?
        .unwrap_or(0) as u64;

        // Get latest scan time
        let last_scan_completed = sqlx::query_scalar!(
            "SELECT MAX(ts_ingest) FROM raw.events WHERE source = $1 AND event_type = 'scan.completed'",
            source_name
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CoreError::Database(format!("Failed to get last scan time: {}", e)))?;

        Ok(ImportStats {
            total_scans,
            duplicate_scans,
            total_events_generated: 0, // Would need more complex query to calculate
            last_scan_completed,
            avg_duration_ms: None, // Would need more complex query to calculate
        })
    }
}

/// Import statistics for a source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportStats {
    pub total_scans: u64,
    pub duplicate_scans: u64,
    pub total_events_generated: u64,
    pub last_scan_completed: Option<DateTime<Utc>>,
    pub avg_duration_ms: Option<u64>,
}

/// Format import records for display
fn format_imports(imports: &[ImportRecord]) -> String {
    if imports.is_empty() {
        return "  No previous imports found".to_string();
    }

    let mut output = String::new();
    for (i, import) in imports.iter().enumerate() {
        let status = if import.was_duplicate {
            "🔄 Duplicate"
        } else if import.scan_completed_at.is_some() {
            "✅ Completed"
        } else {
            "⏳ In progress"
        };

        let duration = import.duration_ms
            .map(|ms| format!("{}ms", ms))
            .unwrap_or_else(|| "—".to_string());

        let events = if import.was_duplicate {
            "0 (duplicate)".to_string()
        } else {
            format!("{} events", import.events_generated)
        };

        output.push_str(&format!(
            "  {}. {} {} | {} | {}\n",
            i + 1,
            status,
            import.scan_started_at.format("%Y-%m-%d %H:%M:%S"),
            duration,
            events
        ));
    }
    output
}

/// Format sensor coverage periods for display
fn format_sensor_periods(periods: &[SensorCoverage]) -> String {
    if periods.is_empty() {
        return "  No sensor coverage found".to_string();
    }

    let mut output = String::new();
    for (i, period) in periods.iter().enumerate() {
        let status = if period.is_active {
            "🟢 Active"
        } else {
            "🔴 Stopped"
        };

        let duration = if let Some(stopped) = period.stopped_at {
            let dur = stopped.signed_duration_since(period.started_at);
            format!("({} hours)", dur.num_hours())
        } else {
            let dur = Utc::now().signed_duration_since(period.started_at);
            format!("({} hours, ongoing)", dur.num_hours())
        };

        output.push_str(&format!(
            "  {}. {} {} {} {}\n",
            i + 1,
            status,
            period.started_at.format("%Y-%m-%d %H:%M:%S"),
            period.stopped_at
                .map(|t| format!("to {}", t.format("%H:%M:%S")))
                .unwrap_or_else(|| "ongoing".to_string()),
            duration
        ));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_imports_empty() {
        let imports = vec![];
        let formatted = format_imports(&imports);
        assert_eq!(formatted, "  No previous imports found");
    }

    #[test]
    fn test_format_sensor_periods_empty() {
        let periods = vec![];
        let formatted = format_sensor_periods(&periods);
        assert_eq!(formatted, "  No sensor coverage found");
    }
}