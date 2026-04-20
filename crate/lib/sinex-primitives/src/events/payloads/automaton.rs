//! Automaton event payloads
//!
//! Typed payloads for events emitted by automatons (health, search, analytics, content, pkm).

use crate::Timestamp;
use crate::activity::ActivitySourceKind;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sinex_macros::EventPayload;
use std::collections::{BTreeMap, HashMap};

// ============================================================================
// Health Automaton Payloads
// ============================================================================

/// Health status for a system component (health automaton)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub enum ComponentHealthStatus {
    Healthy,
    Warning,
    Critical,
    Unknown,
}

/// Component health report from health automaton
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "health-automaton",
    event_type = "health.component_report",
    version = "1.0.0"
)]
pub struct HealthComponentReportPayload {
    pub report_type: String, // "component_health"
    pub component_name: String,
    pub status: ComponentHealthStatus,
    pub last_seen: Timestamp,
    pub metrics: HashMap<String, f64>,
    pub recent_event_count: usize,
    pub minutes_since_last_update: i64,
    pub generated_at: Timestamp,
}

/// System-wide health status summary
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "health-automaton",
    event_type = "health.system_status",
    version = "1.0.0"
)]
pub struct HealthSystemStatusPayload {
    pub report_type: String, // "system_health"
    pub overall_status: ComponentHealthStatus,
    pub total_components: usize,
    pub healthy_components: usize,
    pub warning_components: usize,
    pub critical_components: usize,
    pub health_score: f64,
    pub component_summary: HashMap<String, ComponentHealthStatus>,
    pub generated_at: Timestamp,
}

/// Health alert for unhealthy components
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "health-automaton",
    event_type = "health.alert",
    version = "1.0.0"
)]
pub struct HealthAlertPayload {
    pub alert_type: String, // "health_alert"
    pub component_name: String,
    pub alert_level: String, // "critical", "warning", "unknown"
    pub last_seen: Timestamp,
    pub minutes_since_update: i64,
    pub current_status: ComponentHealthStatus,
    pub recent_metrics: HashMap<String, f64>,
    pub generated_at: Timestamp,
}

// ============================================================================
// Search Automaton Payloads
// ============================================================================

/// Search index built event
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "search-automaton",
    event_type = "search.index_built",
    version = "1.0.0"
)]
pub struct SearchIndexBuiltPayload {
    pub analysis_type: String, // "search.index"
    pub total_entries: usize,
    pub content_type_distribution: HashMap<String, usize>,
    pub avg_score_by_type: HashMap<String, f64>,
    pub top_entries: Vec<SearchIndexTopEntry>,
    pub index_size_limit: usize,
    pub indexing_window_hours: u64,
    pub generated_at: Timestamp,
}

/// Top search index entry summary
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SearchIndexTopEntry {
    pub title: String,
    pub event_type: String,
    pub search_score: f64,
    pub keywords: Vec<String>,
    pub timestamp: Timestamp,
}

/// Search analytics event
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "search-automaton",
    event_type = "search.analytics",
    version = "1.0.0"
)]
pub struct SearchAnalyticsPayload {
    pub analysis_type: String, // "search.analytics"
    pub top_content_types: Vec<ContentTypeCount>,
    pub index_size: usize,
    pub semantic_enabled: bool,
}

/// Content type count for search analytics
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ContentTypeCount {
    pub event_type: String,
    pub count: usize,
}

/// Search discoverability analysis
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "search-automaton",
    event_type = "search.discoverability",
    version = "1.0.0"
)]
pub struct SearchDiscoverabilityPayload {
    pub analysis_type: String, // "search.discoverability"
    pub issues: Vec<DiscoverabilityIssue>,
    pub recommendations: Vec<String>,
}

/// Discoverability issue for search
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DiscoverabilityIssue {
    pub event_type: String,
    pub message: String,
}

// ============================================================================
// Analytics Automaton Payloads
// ============================================================================

/// Frequency analysis of events
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "analytics-automaton",
    event_type = "analytics.frequency",
    version = "1.0.0"
)]
pub struct AnalyticsFrequencyPayload {
    pub analysis_type: String, // "frequency"
    pub events_per_minute: f64,
    pub top_event_types: Vec<(String, usize)>,
    pub top_sources: Vec<(String, usize)>,
    pub anomalies: Vec<FrequencyAnomaly>,
    pub window_seconds: u64,
}

/// Frequency anomaly detection
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FrequencyAnomaly {
    pub event_type: String,
    pub share: f64,
}

/// Pattern detection event
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "analytics-automaton",
    event_type = "analytics.pattern.detected",
    version = "1.0.0"
)]
pub struct AnalyticsPatternDetectedPayload {
    pub pattern_type: String, // "transition"
    pub from_event: String,
    pub to_event: String,
    pub occurrences: usize,
    pub avg_delta_ms: i64,
    pub last_seen: Option<Timestamp>,
}

/// Correlation analysis between event types
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "analytics-automaton",
    event_type = "analytics.correlation",
    version = "1.0.0"
)]
pub struct AnalyticsCorrelationPayload {
    pub analysis_type: String, // "correlation"
    pub pairs: Vec<CorrelationPair>,
    pub window_seconds: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActivityWindowCloseReason {
    Gap,
    MaxDuration,
    MaxEventCount,
}

/// Completed bounded activity window derived from trusted activity signals.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "derived.activity-window",
    event_type = "activity.window.summary",
    version = "1.0.0"
)]
pub struct ActivityWindowSummaryPayload {
    pub window_id: String,
    pub window_start: Timestamp,
    pub window_end: Timestamp,
    pub duration_secs: u64,
    pub event_count: u64,
    pub source_count: u64,
    pub sources: Vec<String>,
    pub activity_sources: Vec<ActivitySourceKind>,
    pub activity_source_counts: BTreeMap<ActivitySourceKind, u64>,
    pub primary_source: ActivitySourceKind,
    pub close_reason: ActivityWindowCloseReason,
}

/// Completed activity session derived from trusted activity signals.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "derived.session-detector",
    event_type = "activity.session.boundary",
    version = "1.0.0"
)]
pub struct ActivitySessionBoundaryPayload {
    pub session_id: String,
    pub start_time: Timestamp,
    pub end_time: Timestamp,
    pub duration_secs: u64,
    pub event_count: u64,
    pub window_count: u64,
    pub source_count: u64,
    pub sources: Vec<String>,
    pub activity_sources: Vec<ActivitySourceKind>,
    pub activity_source_counts: BTreeMap<ActivitySourceKind, u64>,
    pub primary_source: ActivitySourceKind,
}

/// Correlation pair
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CorrelationPair {
    pub event_a: String,
    pub event_b: String,
    pub occurrences: usize,
    pub avg_gap_ms: i64,
}

// ============================================================================
// Content Automaton Payloads
// ============================================================================

/// Text content analysis
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "content-automaton",
    event_type = "content.analyzed",
    version = "1.0.0"
)]
pub struct ContentAnalyzedPayload {
    pub analysis_type: String,           // "text_analysis"
    pub source_event_id: Option<String>, // Event ID serialized
    pub word_count: usize,
    pub character_count: usize,
    pub line_count: usize,
    pub detected_language: String,
    pub top_keywords: Vec<(String, usize)>,
    pub content_preview: String,
    pub generated_at: Timestamp,
}

/// Content classification
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "content-automaton",
    event_type = "content.classified",
    version = "1.0.0"
)]
pub struct ContentClassifiedPayload {
    pub analysis_type: String, // "content_classification"
    pub source_event_id: Option<String>,
    pub categories: Vec<String>,
    pub confidence: f64,
    pub content_length: usize,
    pub generated_at: Timestamp,
}

/// Content similarity detection
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "content-automaton",
    event_type = "content.similarity_detected",
    version = "1.0.0"
)]
pub struct ContentSimilarityDetectedPayload {
    pub analysis_type: String,   // "content_similarity"
    pub similarity_type: String, // "potential_duplicate"
    pub event_group_size: usize,
    pub content_fingerprint: String,
    pub similar_event_ids: Vec<String>,
    pub generated_at: Timestamp,
}

// ============================================================================
// PKM Automaton Payloads
// ============================================================================

/// Knowledge extraction insights
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "pkm-automaton",
    event_type = "pkm.knowledge_extraction",
    version = "1.0.0"
)]
pub struct PKMKnowledgeExtractionPayload {
    pub analysis_type: String, // "knowledge_extraction"
    pub total_knowledge_items: usize,
    pub type_distribution: HashMap<String, usize>,
    pub top_keywords: Vec<(String, usize)>,
    pub recent_items: Vec<JsonValue>,
    pub time_window_hours: u64,
    pub generated_at: Timestamp,
}

/// Learning session tracking
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "pkm-automaton",
    event_type = "pkm.learning_session",
    version = "1.0.0"
)]
pub struct PKMLearningSessionPayload {
    pub analysis_type: String, // "learning_session"
    pub start_time: Timestamp,
    pub end_time: Timestamp,
    pub duration_minutes: i64,
    pub activity_count: usize,
    pub intensity: f64, // activities per hour
    pub generated_at: Timestamp,
}

/// Knowledge graph relationships
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "pkm-automaton",
    event_type = "pkm.knowledge_graph",
    version = "1.0.0"
)]
pub struct PKMKnowledgeGraphPayload {
    pub analysis_type: String, // "knowledge_graph"
    pub total_nodes: usize,
    pub total_relationships: usize,
    pub relationships: Vec<JsonValue>,
    pub graph_density: f64,
    pub generated_at: Timestamp,
}

/// Workflow pattern detection
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "pkm-automaton",
    event_type = "pkm.workflow_pattern",
    version = "1.0.0"
)]
pub struct PKMWorkflowPatternPayload {
    pub analysis_type: String, // "workflow_pattern"
    pub pattern: String,
    pub frequency: usize,
    pub pattern_type: String, // "activity_sequence"
    pub generated_at: Timestamp,
}

// Test helpers for external tests
#[cfg(any(test, feature = "testing"))]
impl HealthComponentReportPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            report_type: "component_health".into(),
            component_name: "test-component".into(),
            status: ComponentHealthStatus::Healthy,
            last_seen: crate::temporal::now(),
            metrics: HashMap::new(),
            recent_event_count: 0,
            minutes_since_last_update: 0,
            generated_at: crate::temporal::now(),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl HealthSystemStatusPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            report_type: "system_health".into(),
            overall_status: ComponentHealthStatus::Healthy,
            total_components: 0,
            healthy_components: 0,
            warning_components: 0,
            critical_components: 0,
            health_score: 100.0,
            component_summary: HashMap::new(),
            generated_at: crate::temporal::now(),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl HealthAlertPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            alert_type: "health_alert".into(),
            component_name: "test-component".into(),
            alert_level: "warning".into(),
            last_seen: crate::temporal::now(),
            minutes_since_update: 0,
            current_status: ComponentHealthStatus::Warning,
            recent_metrics: HashMap::new(),
            generated_at: crate::temporal::now(),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl SearchIndexBuiltPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            analysis_type: "search.index".into(),
            total_entries: 0,
            content_type_distribution: HashMap::new(),
            avg_score_by_type: HashMap::new(),
            top_entries: vec![],
            index_size_limit: 1000,
            indexing_window_hours: 24,
            generated_at: crate::temporal::now(),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl SearchAnalyticsPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            analysis_type: "search.analytics".into(),
            top_content_types: vec![],
            index_size: 0,
            semantic_enabled: false,
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl SearchDiscoverabilityPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            analysis_type: "search.discoverability".into(),
            issues: vec![],
            recommendations: vec![],
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl AnalyticsFrequencyPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            analysis_type: "frequency".into(),
            events_per_minute: 0.0,
            top_event_types: vec![],
            top_sources: vec![],
            anomalies: vec![],
            window_seconds: 60,
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl AnalyticsPatternDetectedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            pattern_type: "transition".into(),
            from_event: "test.event1".into(),
            to_event: "test.event2".into(),
            occurrences: 0,
            avg_delta_ms: 0,
            last_seen: None,
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl AnalyticsCorrelationPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            analysis_type: "correlation".into(),
            pairs: vec![],
            window_seconds: 60,
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl ContentAnalyzedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            analysis_type: "text_analysis".into(),
            source_event_id: None,
            word_count: 0,
            character_count: 0,
            line_count: 0,
            detected_language: "en".into(),
            top_keywords: vec![],
            content_preview: String::new(),
            generated_at: crate::temporal::now(),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl ContentClassifiedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            analysis_type: "content_classification".into(),
            source_event_id: None,
            categories: vec![],
            confidence: 0.0,
            content_length: 0,
            generated_at: crate::temporal::now(),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl ContentSimilarityDetectedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            analysis_type: "content_similarity".into(),
            similarity_type: "potential_duplicate".into(),
            event_group_size: 0,
            content_fingerprint: String::new(),
            similar_event_ids: vec![],
            generated_at: crate::temporal::now(),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl PKMKnowledgeExtractionPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            analysis_type: "knowledge_extraction".into(),
            total_knowledge_items: 0,
            type_distribution: HashMap::new(),
            top_keywords: vec![],
            recent_items: vec![],
            time_window_hours: 24,
            generated_at: crate::temporal::now(),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl PKMLearningSessionPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            analysis_type: "learning_session".into(),
            start_time: crate::temporal::now(),
            end_time: crate::temporal::now(),
            duration_minutes: 0,
            activity_count: 0,
            intensity: 0.0,
            generated_at: crate::temporal::now(),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl PKMKnowledgeGraphPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            analysis_type: "knowledge_graph".into(),
            total_nodes: 0,
            total_relationships: 0,
            relationships: vec![],
            graph_density: 0.0,
            generated_at: crate::temporal::now(),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl PKMWorkflowPatternPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            analysis_type: "workflow_pattern".into(),
            pattern: "test_pattern".into(),
            frequency: 0,
            pattern_type: "activity_sequence".into(),
            generated_at: crate::temporal::now(),
        }
    }
}
