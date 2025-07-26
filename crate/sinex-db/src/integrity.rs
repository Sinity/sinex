//! Comprehensive data integrity testing and validation utilities
//!
//! This module provides specialized validation for:
//! - Schema validation testing with malformed event detection
//! - ULID ordering verification and corruption detection  
//! - Checkpoint consistency checks across automatons
//! - Data corruption detection and recovery guidance

use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::Value;
use std::time::{Duration, Instant};
use tracing::{debug, info};

use crate::constants::tables;
use crate::queries::IntegrityQueries;
use crate::validation::{
    CheckpointInconsistency, DataIntegrityValidator, IntegrityCheckReport, IntegritySeverity,
    SchemaViolation, UlidOrderingViolation,
};
use crate::RawEvent;
use sinex_ulid::Ulid;

/// High-level integrity testing orchestrator
pub struct IntegrityTester<'a> {
    validator: DataIntegrityValidator<'a>,
}

/// Configuration for integrity testing runs
#[derive(Debug, Clone)]
pub struct IntegrityTestConfig {
    /// Maximum number of events to sample for testing
    pub max_events_to_check: u64,
    /// Time window to check for recent integrity issues
    pub check_window_hours: u32,
    /// Whether to include expensive deep validation checks
    pub include_deep_validation: bool,
    /// Whether to validate checkpoint consistency
    pub validate_checkpoints: bool,
    /// Whether to check ULID ordering properties
    pub validate_ulid_ordering: bool,
    /// Whether to perform schema validation testing
    pub validate_schemas: bool,
}

/// Results from running integrity tests with detailed diagnostics
#[derive(Debug, Clone)]
pub struct IntegrityTestResults {
    pub config: IntegrityTestConfig,
    pub check_report: IntegrityCheckReport,
    pub recommendations: Vec<IntegrityRecommendation>,
    pub test_metadata: IntegrityTestMetadata,
}

/// Recommendations for addressing integrity issues
#[derive(Debug, Clone)]
pub struct IntegrityRecommendation {
    pub priority: RecommendationPriority,
    pub category: RecommendationCategory,
    pub description: String,
    pub action_steps: Vec<String>,
    pub affected_components: Vec<String>,
}

/// Test execution metadata
#[derive(Debug, Clone)]
pub struct IntegrityTestMetadata {
    pub test_start_time: DateTime<Utc>,
    pub test_duration: Duration,
    pub events_sampled: u64,
    pub automatons_checked: u32,
    pub schemas_validated: u32,
    pub performance_metrics: PerformanceMetrics,
}

/// Performance metrics from integrity testing
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub schema_validation_rate_per_sec: f64,
    pub ulid_validation_rate_per_sec: f64,
    pub checkpoint_check_duration: Duration,
    pub database_query_count: u32,
    pub memory_usage_mb: u64,
}

#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, strum::Display)]
pub enum RecommendationPriority {
    Critical,
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RecommendationCategory {
    DataCorruption,
    PerformanceImpact,
    SecurityConcern,
    SystemStability,
    Maintenance,
}

impl Default for IntegrityTestConfig {
    fn default() -> Self {
        Self {
            max_events_to_check: 10_000,
            check_window_hours: 24,
            include_deep_validation: false,
            validate_checkpoints: true,
            validate_ulid_ordering: true,
            validate_schemas: true,
        }
    }
}

impl<'a> IntegrityTester<'a> {
    /// Create a new integrity tester
    pub async fn new(pool: &'a crate::DbPool) -> Result<Self> {
        let validator = DataIntegrityValidator::new(pool).await?;
        Ok(Self { validator })
    }

    /// Run comprehensive integrity tests with configuration
    pub async fn run_integrity_tests(
        &self,
        config: IntegrityTestConfig,
    ) -> Result<IntegrityTestResults> {
        let test_start_time = Utc::now();
        let start_instant = Instant::now();

        info!(
            "Starting comprehensive integrity tests with config: {:?}",
            config
        );

        // Run the core integrity validation
        let check_report = if config.include_deep_validation {
            self.run_deep_integrity_validation(&config).await?
        } else {
            self.validator.validate_integrity().await?
        };

        // Generate recommendations based on findings
        let recommendations = self.generate_recommendations(&check_report, &config);

        // Collect performance metrics
        let test_duration = start_instant.elapsed();
        let performance_metrics = self.calculate_performance_metrics(&check_report, test_duration);

        let test_metadata = IntegrityTestMetadata {
            test_start_time,
            test_duration,
            events_sampled: check_report.total_events_checked,
            automatons_checked: check_report.checkpoint_inconsistencies.len() as u32,
            schemas_validated: check_report.schema_violations.len() as u32,
            performance_metrics,
        };

        info!(
            "Integrity tests completed in {:?}. Severity: {:?}, Events checked: {}",
            test_duration, check_report.severity, check_report.total_events_checked
        );

        Ok(IntegrityTestResults {
            config,
            check_report,
            recommendations,
            test_metadata,
        })
    }

    /// Run deep validation with additional checks beyond standard validation
    async fn run_deep_integrity_validation(
        &self,
        config: &IntegrityTestConfig,
    ) -> Result<IntegrityCheckReport> {
        info!("Running deep integrity validation");

        // Start with standard validation
        let mut report = self.validator.validate_integrity().await?;

        // Add deep validation checks
        if config.validate_ulid_ordering {
            let ordering_violations = self.deep_ulid_ordering_validation(config).await?;
            report.ulid_ordering_violations.extend(ordering_violations);
        }

        if config.validate_schemas {
            let schema_violations = self.deep_schema_validation(config).await?;
            report.schema_violations.extend(schema_violations);
        }

        if config.validate_checkpoints {
            let checkpoint_issues = self.deep_checkpoint_validation(config).await?;
            report.checkpoint_inconsistencies.extend(checkpoint_issues);
        }

        // Recalculate severity with additional findings
        report.severity = self.recalculate_severity(&report);

        Ok(report)
    }

    /// Deep ULID ordering validation with historical analysis
    async fn deep_ulid_ordering_validation(
        &self,
        config: &IntegrityTestConfig,
    ) -> Result<Vec<UlidOrderingViolation>> {
        debug!("Performing deep ULID ordering validation");

        let mut violations = Vec::new();

        // Check for monotonicity violations in event batches
        let batch_violations = sqlx::query!(
            r#"
            WITH event_batches AS (
                SELECT 
                    event_id::uuid as event_id,
                    ts_orig,
                    source,
                    ROW_NUMBER() OVER (ORDER BY event_id) as row_num,
                    LAG(event_id::uuid) OVER (ORDER BY event_id) as prev_event_id,
                    LAG(ts_orig) OVER (ORDER BY event_id) as prev_ts_orig
                FROM core.events
                WHERE ts_ingest > NOW() - INTERVAL '1 day' * $1
                ORDER BY event_id
                LIMIT $2
            ),
            violations AS (
                SELECT event_id, ts_orig, prev_event_id, prev_ts_orig, source
                FROM event_batches
                WHERE prev_event_id IS NOT NULL
                  AND (
                    event_id::text < prev_event_id::text OR
                    ts_orig < prev_ts_orig - INTERVAL '1 minute'
                  )
            )
            SELECT event_id, ts_orig, prev_event_id, prev_ts_orig, source
            FROM violations
            "#,
            config.check_window_hours as i32,
            config.max_events_to_check as i64
        )
        .fetch_all(self.validator.pool())
        .await?;

        for violation in batch_violations {
            if let (Some(event_id), Some(prev_event_id), Some(ts_orig), Some(prev_ts_orig)) = (
                violation.event_id,
                violation.prev_event_id,
                violation.ts_orig,
                violation.prev_ts_orig,
            ) {
                violations.push(UlidOrderingViolation {
                    event_id_1: Ulid::from_uuid(prev_event_id),
                    event_id_2: Ulid::from_uuid(event_id),
                    timestamp_1: prev_ts_orig,
                    timestamp_2: ts_orig,
                    violation_type: crate::validation::OrderingViolationType::UlidRegression,
                    details: format!(
                        "Deep validation: ULID ordering violation in source {}",
                        violation.source
                    ),
                });
            }
        }

        Ok(violations)
    }

    /// Deep schema validation with pattern analysis
    async fn deep_schema_validation(
        &self,
        config: &IntegrityTestConfig,
    ) -> Result<Vec<SchemaViolation>> {
        debug!("Performing deep schema validation");

        let mut violations = Vec::new();

        // Check for events with suspicious payload patterns
        // This query is complex and specific, keeping it as raw SQL for now
        let suspicious_events = sqlx::query!(
            r#"
            SELECT 
                event_id::uuid as event_id,
                source,
                event_type,
                payload,
                jsonb_typeof(payload) as payload_type,
                pg_column_size(payload) as payload_size
            FROM core.events
            WHERE ts_ingest > NOW() - INTERVAL '1 day' * $1
              AND (
                jsonb_typeof(payload) != 'object' OR
                pg_column_size(payload) > 100000 OR
                payload ?| array['__proto__', 'constructor', 'prototype'] OR
                payload::text ~ '\\\\u0000|\\\\x00'
              )
            ORDER BY ts_ingest DESC
            LIMIT $2
            "#,
            config.check_window_hours as i32,
            config.max_events_to_check as i64
        )
        .fetch_all(self.validator.pool())
        .await?;

        for event in suspicious_events {
            let violation_type = if event.payload_type != Some("object".to_string()) {
                crate::validation::SchemaViolationType::MalformedPayload
            } else if event.payload_size.unwrap_or(0) > 100000 {
                crate::validation::SchemaViolationType::PayloadTooLarge
            } else {
                crate::validation::SchemaViolationType::InvalidCharacters
            };

            if let Some(event_id) = event.event_id {
                violations.push(SchemaViolation {
                    event_id: Ulid::from_uuid(event_id),
                    source: event.source,
                    event_type: event.event_type,
                    violation_type,
                    details: format!("Deep validation: Suspicious payload pattern detected"),
                    payload_sample: Some(event.payload),
                });
            }
        }

        Ok(violations)
    }

    /// Deep checkpoint validation with gap analysis
    async fn deep_checkpoint_validation(
        &self,
        config: &IntegrityTestConfig,
    ) -> Result<Vec<CheckpointInconsistency>> {
        debug!("Performing deep checkpoint validation");

        let mut inconsistencies = Vec::new();

        // Analyze checkpoint gaps and processing delays
        let checkpoint_gaps = sqlx::query!(
            r#"
            WITH checkpoint_analysis AS (
                SELECT 
                    ac.automaton_name,
                    ac.last_processed_id::uuid as last_processed_id,
                    ac.processed_count,
                    ac.last_activity,
                    COUNT(e.event_id) as events_after_checkpoint,
                    MIN(e.ts_ingest) as first_unprocessed_event_time,
                    MAX(e.ts_ingest) as last_unprocessed_event_time
                FROM core.automaton_checkpoints ac
                LEFT JOIN core.events e ON e.event_id > COALESCE(ac.last_processed_id, '00000000000000000000000000'::ulid)
                    AND e.ts_ingest > COALESCE(ac.last_activity, NOW() - INTERVAL '1 hour')
                GROUP BY ac.automaton_name, ac.last_processed_id, ac.processed_count, ac.last_activity
            )
            SELECT 
                automaton_name,
                last_processed_id,
                processed_count,
                last_activity,
                events_after_checkpoint,
                first_unprocessed_event_time,
                last_unprocessed_event_time
            FROM checkpoint_analysis
            WHERE events_after_checkpoint > 100
               OR last_activity < NOW() - INTERVAL '1 hour' * $1
            "#,
            config.check_window_hours as i32
        )
        .fetch_all(self.validator.pool())
        .await?;

        for gap in checkpoint_gaps {
            let events_count = gap.events_after_checkpoint.unwrap_or(0);
            let inconsistency_type = if events_count > 100 {
                crate::validation::CheckpointInconsistencyType::CheckpointBehindEvents
            } else {
                crate::validation::CheckpointInconsistencyType::StaleCheckpoint
            };

            inconsistencies.push(CheckpointInconsistency {
                automaton_name: gap.automaton_name,
                checkpoint_ulid: gap
                    .last_processed_id
                    .map(crate::query_helpers::uuid_to_ulid),
                last_processed_ulid: gap
                    .last_processed_id
                    .map(crate::query_helpers::uuid_to_ulid),
                inconsistency_type,
                details: format!(
                    "Deep validation: {} unprocessed events detected",
                    events_count
                ),
                events_potentially_missed: events_count as u64,
            });
        }

        Ok(inconsistencies)
    }

    /// Generate actionable recommendations based on integrity findings
    fn generate_recommendations(
        &self,
        report: &IntegrityCheckReport,
        config: &IntegrityTestConfig,
    ) -> Vec<IntegrityRecommendation> {
        let mut recommendations = Vec::new();

        // Critical data corruption recommendations
        if !report.data_corruption_indicators.is_empty() {
            recommendations.push(IntegrityRecommendation {
                priority: RecommendationPriority::Critical,
                category: RecommendationCategory::DataCorruption,
                description: "Data corruption detected in event storage".to_string(),
                action_steps: vec![
                    "Immediately backup current database state".to_string(),
                    "Investigate corruption root cause".to_string(),
                    "Run data recovery procedures".to_string(),
                    "Implement additional validation checks".to_string(),
                ],
                affected_components: vec![tables::EVENTS.to_string(), "data ingestion".to_string()],
            });
        }

        // ULID ordering violations
        if !report.ulid_ordering_violations.is_empty() {
            let priority = if report.ulid_ordering_violations.len() > 10 {
                RecommendationPriority::Critical
            } else {
                RecommendationPriority::High
            };

            recommendations.push(IntegrityRecommendation {
                priority,
                category: RecommendationCategory::SystemStability,
                description: format!(
                    "{} ULID ordering violations detected",
                    report.ulid_ordering_violations.len()
                ),
                action_steps: vec![
                    "Check system clock synchronization".to_string(),
                    "Review ULID generation logic".to_string(),
                    "Investigate concurrent insertion patterns".to_string(),
                    "Consider implementing stricter ordering constraints".to_string(),
                ],
                affected_components: vec![
                    "ULID generation".to_string(),
                    "event ordering".to_string(),
                ],
            });
        }

        // Checkpoint consistency issues
        if !report.checkpoint_inconsistencies.is_empty() {
            recommendations.push(IntegrityRecommendation {
                priority: RecommendationPriority::High,
                category: RecommendationCategory::SystemStability,
                description: format!(
                    "{} checkpoint inconsistencies found",
                    report.checkpoint_inconsistencies.len()
                ),
                action_steps: vec![
                    "Review automaton checkpoint update logic".to_string(),
                    "Check for automaton processing delays".to_string(),
                    "Verify checkpoint persistence mechanisms".to_string(),
                    "Consider implementing checkpoint verification".to_string(),
                ],
                affected_components: vec![
                    "automaton checkpoints".to_string(),
                    "event processing".to_string(),
                ],
            });
        }

        // Schema validation issues
        if !report.schema_violations.is_empty() {
            recommendations.push(IntegrityRecommendation {
                priority: RecommendationPriority::Medium,
                category: RecommendationCategory::DataCorruption,
                description: format!(
                    "{} schema validation violations found",
                    report.schema_violations.len()
                ),
                action_steps: vec![
                    "Review event schema definitions".to_string(),
                    "Strengthen input validation".to_string(),
                    "Update validation rules".to_string(),
                    "Implement schema migration procedures".to_string(),
                ],
                affected_components: vec![
                    "event schemas".to_string(),
                    "validation pipeline".to_string(),
                ],
            });
        }

        // Performance recommendations
        if config.include_deep_validation && report.total_events_checked > 50_000 {
            recommendations.push(IntegrityRecommendation {
                priority: RecommendationPriority::Medium,
                category: RecommendationCategory::PerformanceImpact,
                description: "Large dataset detected - consider optimization".to_string(),
                action_steps: vec![
                    "Implement data partitioning strategies".to_string(),
                    "Add database indexes for integrity queries".to_string(),
                    "Consider automated integrity checking".to_string(),
                    "Optimize query performance".to_string(),
                ],
                affected_components: vec![
                    "database performance".to_string(),
                    "integrity checks".to_string(),
                ],
            });
        }

        recommendations
    }

    /// Calculate performance metrics from test execution
    fn calculate_performance_metrics(
        &self,
        report: &IntegrityCheckReport,
        duration: Duration,
    ) -> PerformanceMetrics {
        let duration_secs = duration.as_secs_f64();

        PerformanceMetrics {
            schema_validation_rate_per_sec: report.schema_violations.len() as f64 / duration_secs,
            ulid_validation_rate_per_sec: report.ulid_ordering_violations.len() as f64
                / duration_secs,
            checkpoint_check_duration: duration / 4, // Estimate checkpoint portion
            database_query_count: 10,                // Estimate based on validation queries
            memory_usage_mb: std::process::id() as u64, // Placeholder - would need proper memory tracking
        }
    }

    /// Recalculate severity after deep validation
    fn recalculate_severity(&self, report: &IntegrityCheckReport) -> IntegritySeverity {
        let total_issues = report.schema_violations.len()
            + report.ulid_ordering_violations.len()
            + report.checkpoint_inconsistencies.len()
            + report.data_corruption_indicators.len();

        if total_issues == 0 {
            IntegritySeverity::Clean
        } else if total_issues > 100 || !report.data_corruption_indicators.is_empty() {
            IntegritySeverity::Critical
        } else if total_issues > 20 {
            IntegritySeverity::Warning
        } else {
            IntegritySeverity::Minor
        }
    }
}

/// Utility functions for malformed event detection
pub mod malformed_detection {
    use super::*;

    /// Check if an event payload is malformed
    pub fn is_malformed_payload(payload: &Value) -> bool {
        match payload {
            Value::Null => true,
            Value::Object(map) if map.is_empty() => true,
            Value::Object(map) => {
                // Check for suspicious patterns
                map.contains_key("__proto__")
                    || map.contains_key("constructor")
                    || payload.to_string().contains('\0')
            }
            _ => false,
        }
    }

    /// Detect potential schema violations in event structure
    pub fn detect_schema_anomalies(event: &RawEvent) -> Vec<String> {
        let mut anomalies = Vec::new();

        // Check for empty required fields
        if event.source.is_empty() {
            anomalies.push("Empty source field".to_string());
        }
        if event.event_type.is_empty() {
            anomalies.push("Empty event_type field".to_string());
        }

        // Check for suspicious field values
        if event.source.contains('\0') || event.event_type.contains('\0') {
            anomalies.push("Null bytes in string fields".to_string());
        }

        // Check payload size
        let payload_size = event.payload.to_string().len();
        if payload_size > 1_000_000 {
            // 1MB
            anomalies.push(format!("Oversized payload: {} bytes", payload_size));
        }

        // Check for malformed payload
        if is_malformed_payload(&event.payload) {
            anomalies.push("Malformed payload structure".to_string());
        }

        anomalies
    }

    /// Generate test events with known malformations for testing
    pub fn generate_malformed_test_events() -> Vec<RawEvent> {
        use crate::RawEvent;

        vec![
            // Event with null payload
            RawEvent {
                id: Ulid::new(),
                source: "test.malformed".to_string(),
                event_type: "null_payload".to_string(),
                ts_orig: Some(Utc::now()),
                ts_ingest: Utc::now(),
                host: "test-host".to_string(),
                payload: Value::Null,
                source_event_ids: None,
                anchor_byte: None,
                source_material_id: None,
                source_material_offset_start: None,
                source_material_offset_end: None,
                associated_blob_ids: Some(Vec::new()),
                ingestor_version: Some("test".to_string()),
                payload_schema_id: None,
            },
            // Event with empty source
            RawEvent {
                id: Ulid::new(),
                source: "".to_string(),
                event_type: "empty_source".to_string(),
                ts_orig: Some(Utc::now()),
                ts_ingest: Utc::now(),
                host: "test-host".to_string(),
                payload: serde_json::json!({"valid": "payload"}),
                source_event_ids: None,
                anchor_byte: None,
                source_material_id: None,
                source_material_offset_start: None,
                source_material_offset_end: None,
                associated_blob_ids: Some(Vec::new()),
                ingestor_version: Some("test".to_string()),
                payload_schema_id: None,
            },
            // Event with oversized payload
            RawEvent {
                id: Ulid::new(),
                source: "test.malformed".to_string(),
                event_type: "oversized_payload".to_string(),
                ts_orig: Some(Utc::now()),
                ts_ingest: Utc::now(),
                host: "test-host".to_string(),
                payload: serde_json::json!({"large_field": "x".repeat(1_000_001)}),
                source_event_ids: None,
                anchor_byte: None,
                source_material_id: None,
                source_material_offset_start: None,
                source_material_offset_end: None,
                associated_blob_ids: Some(Vec::new()),
                ingestor_version: Some("test".to_string()),
                payload_schema_id: None,
            },
        ]
    }
}

/// ULID ordering verification utilities
pub mod ulid_verification {
    use super::*;

    /// Verify ULID ordering properties in a sequence
    pub fn verify_ulid_sequence_ordering(ulids: &[Ulid]) -> Result<(), String> {
        for window in ulids.windows(2) {
            if window[0] >= window[1] {
                return Err(format!(
                    "ULID ordering violation: {} >= {}",
                    window[0], window[1]
                ));
            }
        }
        Ok(())
    }

    /// Check if ULIDs have reasonable timestamp progression
    pub fn verify_timestamp_progression(
        ulids: &[Ulid],
        max_regression_ms: i64,
    ) -> Result<(), String> {
        for window in ulids.windows(2) {
            let ts1 = window[0].timestamp().timestamp_millis();
            let ts2 = window[1].timestamp().timestamp_millis();

            if ts2 < ts1 - max_regression_ms {
                return Err(format!(
                    "Timestamp regression exceeds threshold: {} -> {} ({}ms regression)",
                    window[0],
                    window[1],
                    ts1 - ts2
                ));
            }
        }
        Ok(())
    }

    /// Generate test ULIDs with known ordering violations for testing
    pub fn generate_ordering_violation_test_ulids() -> Vec<(Ulid, String)> {
        let now = Utc::now();

        vec![
            // Future timestamp
            (
                Ulid::from_datetime(now + ChronoDuration::hours(2)),
                "Future timestamp".to_string(),
            ),
            // Past timestamp (before reasonable epoch)
            (
                Ulid::from_datetime(DateTime::from_timestamp(0, 0).unwrap()),
                "Ancient timestamp".to_string(),
            ),
            // Normal ULID for comparison
            (Ulid::new(), "Normal ULID".to_string()),
        ]
    }
}

/// Checkpoint consistency verification utilities
pub mod checkpoint_verification {
    use super::*;

    /// Verify checkpoint consistency for an automaton
    pub async fn verify_automaton_checkpoint_consistency(
        pool: &sqlx::PgPool,
        automaton_name: &str,
    ) -> Result<Vec<String>> {
        let mut issues = Vec::new();

        // Get checkpoint info
        #[derive(sqlx::FromRow)]
        struct CheckpointDetail {
            last_processed_id: Option<sqlx::types::Uuid>,
            #[allow(dead_code)]
            processed_count: Option<i64>,
            last_activity: Option<DateTime<Utc>>,
        }

        let checkpoint = IntegrityQueries::get_checkpoint(automaton_name.to_string())
            .fetch_optional::<CheckpointDetail>(pool)
            .await?;

        match checkpoint {
            Some(cp) => {
                // Check if checkpoint points to valid event
                if let Some(last_processed_uuid) = &cp.last_processed_id {
                    let event_exists = IntegrityQueries::event_exists(*last_processed_uuid)
                        .fetch_optional::<(i32,)>(pool)
                        .await?
                        .is_some();

                    if !event_exists {
                        issues.push(format!(
                            "Checkpoint references non-existent event: {}",
                            last_processed_uuid
                        ));
                    }
                }

                // Check for stale checkpoints
                if let Some(last_activity) = cp.last_activity {
                    let hours_since_update =
                        Utc::now().signed_duration_since(last_activity).num_hours();
                    if hours_since_update > 2 {
                        issues.push(format!(
                            "Checkpoint not updated for {} hours",
                            hours_since_update
                        ));
                    }
                }
            }
            None => {
                issues.push(format!(
                    "No checkpoint found for automaton: {}",
                    automaton_name
                ));
            }
        }

        Ok(issues)
    }

    /// Get all automatons that should have checkpoints
    pub async fn get_expected_automatons(pool: &sqlx::PgPool) -> Result<Vec<String>> {
        let automatons = IntegrityQueries::get_expected_automatons()
            .fetch_all::<(String,)>(pool)
            .await?
            .into_iter()
            .map(|(name,)| name)
            .collect::<Vec<String>>();

        Ok(automatons)
    }
}
