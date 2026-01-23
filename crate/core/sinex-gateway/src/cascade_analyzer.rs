#![doc = include_str!("../docs/cascade_analyzer.md")]

use color_eyre::eyre::{eyre, Result};
use serde::{Deserialize, Serialize};
use sinex_core::db::query_helpers::{db_error, UlidArrayExt};
use sinex_core::db::repositories::EventRepositoryTx;
use sinex_core::types::ulid::Ulid;
use sqlx::PgPool;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use tracing::{debug, info, warn};
use uuid::Uuid;

// Default cascade analyzer configuration values
const DEFAULT_CASCADE_BATCH_SIZE: usize = 1000;
const DEFAULT_CASCADE_MAX_DEPTH: usize = 100;
const DEFAULT_CASCADE_MEMORY_LIMIT: usize = 1024 * 1024 * 1024; // 1GB
const DEFAULT_CASCADE_TIMEOUT_SECS: u64 = 60;

/// Analysis of cascade effects for a replay operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CascadeAnalysis {
    /// Maximum cascade depth found
    pub max_depth: usize,
    /// Histogram of cascade depths (depth -> count)
    pub depth_histogram: HashMap<usize, usize>,
    /// Events that would violate integrity if archived
    pub integrity_violations: Vec<IntegrityViolation>,
    /// Total events affected
    pub total_affected: usize,
    /// Events with circular dependencies
    pub circular_dependencies: Vec<CircularDependency>,
    /// Memory estimate for full analysis (bytes)
    pub memory_estimate: usize,
}

/// Integrity violation detected during analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityViolation {
    /// Event that would be archived
    pub archived_event_id: Ulid,
    /// Live event that references it
    pub live_event_id: Ulid,
    /// Type of violation
    pub violation_type: ViolationType,
    /// Severity of the violation
    pub severity: Severity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ViolationType {
    /// Live event references archived event
    LiveToArchived,
    /// Material anchor would be orphaned
    OrphanedAnchor,
    /// Schema version mismatch
    SchemaMismatch,
    /// Temporal paradox (child before parent)
    TemporalParadox,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Severity {
    Critical, // Must be fixed
    Warning,  // Should be reviewed
    Info,     // Informational only
}

/// Circular dependency detected
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircularDependency {
    /// Events involved in the cycle
    pub cycle: Vec<Ulid>,
    /// Whether this is a strong cycle (all edges mandatory)
    pub is_strong: bool,
}

/// Configuration for cascade analysis
#[derive(Debug, Clone)]
pub struct CascadeAnalyzerConfig {
    /// Maximum batch size for processing events at each depth
    pub batch_size: usize,
    /// Maximum cascade depth to analyze
    pub max_depth: usize,
    /// Whether to include weak dependencies
    pub include_weak_dependencies: bool,
    /// Memory limit for analysis (bytes)
    pub memory_limit_bytes: Option<usize>,
    /// Timeout for analysis operations (prevents indefinite transaction hold)
    pub timeout: Duration,
}

impl Default for CascadeAnalyzerConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_CASCADE_BATCH_SIZE,
            max_depth: DEFAULT_CASCADE_MAX_DEPTH,
            include_weak_dependencies: false,
            memory_limit_bytes: Some(DEFAULT_CASCADE_MEMORY_LIMIT),
            timeout: Duration::from_secs(DEFAULT_CASCADE_TIMEOUT_SECS),
        }
    }
}

impl CascadeAnalyzerConfig {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        Self {
            batch_size: env_var_usize("SINEX_CASCADE_BATCH_SIZE", DEFAULT_CASCADE_BATCH_SIZE),
            max_depth: env_var_usize("SINEX_CASCADE_MAX_DEPTH", DEFAULT_CASCADE_MAX_DEPTH),
            include_weak_dependencies: env_var_bool("SINEX_CASCADE_INCLUDE_WEAK", false),
            memory_limit_bytes: Some(env_var_usize(
                "SINEX_CASCADE_MEMORY_LIMIT_BYTES",
                DEFAULT_CASCADE_MEMORY_LIMIT,
            )),
            timeout: Duration::from_secs(env_var_u64(
                "SINEX_CASCADE_TIMEOUT_SECS",
                DEFAULT_CASCADE_TIMEOUT_SECS,
            )),
        }
    }
}

fn env_var_usize(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_var_u64(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_var_bool(var: &str, default: bool) -> bool {
    std::env::var(var)
        .ok()
        .map(|s| matches!(s.to_lowercase().as_str(), "true" | "1" | "yes"))
        .unwrap_or(default)
}

/// Memory-efficient cascade analyzer using streaming algorithms
pub struct StreamingCascadeAnalyzer {
    pool: PgPool,
    config: CascadeAnalyzerConfig,
}

impl StreamingCascadeAnalyzer {
    fn quote_identifier(name: &str) -> String {
        let mut quoted = String::with_capacity(name.len() + 2);
        quoted.push('"');
        for ch in name.chars() {
            if ch == '"' {
                quoted.push('"');
            }
            quoted.push(ch);
        }
        quoted.push('"');
        quoted
    }

    /// Create new analyzer with default configuration
    pub fn new(pool: PgPool) -> Self {
        Self::with_config(pool, CascadeAnalyzerConfig::default())
    }

    /// Validate session ID to prevent SQL injection
    fn validate_session_id(session_id: &str) -> Result<()> {
        // Session ID should only contain alphanumeric characters and underscores
        // and be reasonable length (max 64 chars)
        if session_id.len() > 64 {
            return Err(eyre!("Session ID too long: {} chars", session_id.len()));
        }

        if session_id.is_empty() {
            return Err(eyre!("Session ID cannot be empty"));
        }

        if !session_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(eyre!(
                "Session ID contains invalid characters. Only alphanumeric and underscore allowed."
            ));
        }

        Ok(())
    }

    /// Create new analyzer with custom configuration
    pub fn with_config(pool: PgPool, config: CascadeAnalyzerConfig) -> Self {
        Self { pool, config }
    }

    /// Analyze cascades for a set of events to be modified
    pub async fn analyze_cascades(&self, event_ids: &[Ulid]) -> Result<CascadeAnalysis> {
        info!(
            "Analyzing cascades for {} events (timeout: {:?})",
            event_ids.len(),
            self.config.timeout
        );

        // Generate unique session ID for this analysis
        let session_id = sinex_core::types::ulid::Ulid::new().to_string();

        // Wrap the entire transaction in a timeout to prevent indefinite holds
        let timeout_duration = self.config.timeout;
        let analysis_future = async {
            // Start a transaction for the entire analysis
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| db_error(e, "begin cascade analysis transaction"))?;

            // Execute analysis within transaction
            let result = self
                .analyze_cascades_in_transaction(&mut tx, event_ids, &session_id)
                .await;

            // Commit or rollback based on result
            match result {
                Ok(analysis) => {
                    tx.commit()
                        .await
                        .map_err(|e| db_error(e, "commit cascade analysis transaction"))?;
                    Ok(analysis)
                }
                Err(e) => {
                    // Rollback automatically happens on drop, but be explicit
                    if let Err(rollback_err) = tx.rollback().await {
                        warn!(
                            "Failed to rollback cascade analysis transaction: {}",
                            rollback_err
                        );
                    }
                    Err(e)
                }
            }
        };

        match tokio::time::timeout(timeout_duration, analysis_future).await {
            Ok(result) => result,
            Err(_elapsed) => {
                warn!(
                    timeout_secs = timeout_duration.as_secs(),
                    event_count = event_ids.len(),
                    session_id = %session_id,
                    "Cascade analysis exceeded timeout - transaction aborted"
                );
                Err(eyre!(
                    "Cascade analysis timeout after {:?} (analyzed {} events). \
                    Consider increasing SINEX_CASCADE_TIMEOUT_SECS or reducing max_depth.",
                    timeout_duration,
                    event_ids.len()
                ))
            }
        }
    }

    /// Internal method to perform analysis within a transaction
    async fn analyze_cascades_in_transaction(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        event_ids: &[Ulid],
        session_id: &str,
    ) -> Result<CascadeAnalysis> {
        // Create temp table with unique name (transaction-scoped)
        let temp_table = self.create_temp_tables_tx(tx, session_id).await?;

        // Populate with initial events
        self.populate_initial_events_tx(tx, &temp_table, event_ids)
            .await?;

        // Build dependency graph iteratively
        let max_depth = self.build_dependency_graph_tx(tx, &temp_table).await?;

        // Calculate statistics
        let depth_histogram = self.calculate_depth_histogram_tx(tx, &temp_table).await?;
        let total_affected = self.count_affected_events_tx(tx, &temp_table).await?;

        // Find integrity violations
        let integrity_violations = self.find_integrity_violations_tx(tx, &temp_table).await?;

        // Detect circular dependencies
        let circular_dependencies = self
            .detect_circular_dependencies_tx(tx, &temp_table)
            .await?;

        // Clean up temp tables (within transaction)
        self.cleanup_temp_tables_tx(tx, &temp_table).await?;

        // Estimate memory usage
        let memory_estimate = total_affected * 256; // Rough estimate: 256 bytes per event

        Ok(CascadeAnalysis {
            max_depth,
            depth_histogram,
            integrity_violations,
            total_affected,
            circular_dependencies,
            memory_estimate,
        })
    }

    /// Create temporary tables for analysis (transaction version)
    async fn create_temp_tables_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        session_id: &str,
    ) -> Result<String> {
        // Validate session_id to prevent SQL injection
        Self::validate_session_id(session_id)?;
        let mut repo = EventRepositoryTx::new(tx);
        let table_name = repo
            .prepare_cascade_session(session_id, true)
            .await
            .map_err(|e| eyre!("prepare cascade session failed: {e}"))?;
        debug!("Created temp table: {}", table_name);
        Ok(table_name)
    }

    /// Populate initial events to analyze (transaction version)
    async fn populate_initial_events_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table_name: &str,
        event_ids: &[Ulid],
    ) -> Result<()> {
        if event_ids.is_empty() {
            return Ok(());
        }

        let mut repo = EventRepositoryTx::new(tx);
        repo.populate_cascade_roots(table_name, event_ids)
            .await
            .map_err(|e| eyre!("populate cascade roots failed: {e}"))?;
        debug!("Populated {} initial events", event_ids.len());
        Ok(())
    }

    /// Build dependency graph using iterative deepening (transaction version)
    async fn build_dependency_graph_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table_name: &str,
    ) -> Result<usize> {
        let mut repo = EventRepositoryTx::new(tx);
        let depth = repo
            .expand_cascade(table_name, self.config.max_depth as i32)
            .await
            .map_err(|e| eyre!("expand cascade graph failed: {e}"))?;

        Ok(depth)
    }

    /// Calculate depth histogram (transaction version)
    async fn calculate_depth_histogram_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table_name: &str,
    ) -> Result<HashMap<usize, usize>> {
        let mut repo = EventRepositoryTx::new(tx);
        let rows = repo
            .cascade_depth_histogram(table_name)
            .await
            .map_err(|e| eyre!("cascade depth histogram failed: {e}"))?;

        let mut histogram = HashMap::new();
        for (depth, count) in rows {
            histogram.insert(depth as usize, count as usize);
        }

        Ok(histogram)
    }

    /// Count affected events (transaction version)
    async fn count_affected_events_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table_name: &str,
    ) -> Result<usize> {
        let mut repo = EventRepositoryTx::new(tx);
        let count = repo
            .cascade_node_count(table_name)
            .await
            .map_err(|e| eyre!("count cascade nodes failed: {e}"))?;
        Ok(count as usize)
    }

    /// Find integrity violations (transaction version)
    async fn find_integrity_violations_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table_name: &str,
    ) -> Result<Vec<IntegrityViolation>> {
        let mut repo = EventRepositoryTx::new(tx);
        let rows = repo
            .cascade_integrity_violations(table_name, 100)
            .await
            .map_err(|e| eyre!("find cascade integrity violations failed: {e}"))?;

        let mut violations = Vec::new();
        for (live_id, archived_id) in rows {
            violations.push(IntegrityViolation {
                archived_event_id: archived_id,
                live_event_id: live_id,
                violation_type: ViolationType::LiveToArchived,
                severity: Severity::Critical,
            });
        }

        if !violations.is_empty() {
            warn!("Found {} integrity violations", violations.len());
        }

        Ok(violations)
    }

    /// Detect circular dependencies (transaction version)
    async fn detect_circular_dependencies_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table_name: &str,
    ) -> Result<Vec<CircularDependency>> {
        let quoted_table = Self::quote_identifier(table_name);
        // For now, use a simple SQL approach to find potential cycles
        // In production, would implement proper Tarjan's algorithm
        let max_cycle_depth = self.config.max_depth.max(1);
        let query = format!(
            r#"
            WITH RECURSIVE cycle_check AS (
                SELECT 
                    id,
                    parent_ids,
                    ARRAY[id] as path,
                    FALSE as has_cycle
                FROM {0}
                WHERE depth = 0
                
                UNION ALL
                
                SELECT 
                    t.id,
                    t.parent_ids,
                    cc.path || t.id,
                    t.id = ANY(cc.path) as has_cycle
                FROM {0} t
                JOIN cycle_check cc ON t.id = ANY(cc.parent_ids)
                WHERE NOT cc.has_cycle
                AND array_length(cc.path, 1) < {1}
            )
            SELECT (path)::uuid[] AS path
            FROM cycle_check
            WHERE has_cycle
            LIMIT 10
            "#,
            quoted_table, max_cycle_depth
        );

        let rows = sqlx::query_as::<_, (Vec<Uuid>,)>(&query)
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| db_error(e, "detect circular dependencies"))?;

        let mut cycles = Vec::new();
        for (path,) in rows {
            let converted: Vec<Ulid> = path.into_iter().map(Ulid::from_uuid).collect();
            cycles.push(CircularDependency {
                cycle: converted,
                is_strong: true, // Conservative assumption
            });
        }

        if !cycles.is_empty() {
            warn!("Found {} circular dependencies", cycles.len());
        }

        Ok(cycles)
    }

    /// Clean up temp tables (transaction version)
    async fn cleanup_temp_tables_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table_name: &str,
    ) -> Result<()> {
        let mut repo = EventRepositoryTx::new(tx);
        repo.cleanup_cascade_session(table_name)
            .await
            .map_err(|e| eyre!("cleanup cascade session failed: {e}"))?;
        Ok(())
    }

    /// Plan safe execution order for cascade operations
    pub async fn plan_cascade_order(&self, event_ids: &[Ulid]) -> Result<Vec<Ulid>> {
        // Perform topological sort to get safe execution order
        // Events with no dependencies first, then their children, etc.

        info!("Planning cascade order for {} events", event_ids.len());

        // Build dependency map
        let mut dependencies: HashMap<Ulid, Vec<Ulid>> = HashMap::new();
        let mut in_degree: HashMap<Ulid, usize> = HashMap::new();

        for &event_id in event_ids {
            in_degree.insert(event_id, 0);
            dependencies.insert(event_id, Vec::new());
        }

        // Query dependencies - need to use raw query due to ULID type limitations
        use sqlx::Row;
        let rows = sqlx::query(
            r#"
            SELECT 
                id as event_id,
                CASE
                    WHEN source_event_ids IS NULL THEN NULL
                    ELSE ARRAY(SELECT ulid_to_uuid(elem) FROM unnest(source_event_ids) AS elem)
                END as source_event_ids
            FROM core.events
            WHERE id = ANY($1::ulid[])
            "#,
        )
        .bind(event_ids.to_uuid_vec())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| db_error(e, "plan cascade order - query dependencies"))?;

        // Build graph
        for row in rows {
            let event_id: Ulid = row.get("event_id");
            let source_ids: Option<Vec<Uuid>> = row.get("source_event_ids");

            if let Some(source_ids) = source_ids {
                for source_id in source_ids {
                    let source_id = Ulid::from_uuid(source_id);
                    if event_ids.contains(&source_id) {
                        record_dependency(&mut dependencies, &mut in_degree, source_id, event_id);
                    }
                }
            }
        }

        // Topological sort using Kahn's algorithm
        let mut queue: VecDeque<Ulid> = VecDeque::new();
        let mut result = Vec::new();

        // Start with nodes that have no dependencies
        for (&event_id, &degree) in &in_degree {
            if degree == 0 {
                queue.push_back(event_id);
            }
        }

        while let Some(event_id) = queue.pop_front() {
            result.push(event_id);

            // Process children
            if let Some(children) = dependencies.get(&event_id) {
                for &child_id in children {
                    if let Some(degree) = in_degree.get_mut(&child_id) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push_back(child_id);
                        }
                    }
                }
            }
        }

        // Check for cycles
        if result.len() != event_ids.len() {
            return Err(eyre!(
                "Circular dependencies detected: processed {} of {} events",
                result.len(),
                event_ids.len()
            ));
        }

        // Reverse to get deletion order (children before parents)
        result.reverse();

        info!("Planned cascade order for {} events", result.len());
        Ok(result)
    }
}

fn record_dependency(
    dependencies: &mut HashMap<Ulid, Vec<Ulid>>,
    in_degree: &mut HashMap<Ulid, usize>,
    source_id: Ulid,
    event_id: Ulid,
) {
    dependencies.entry(source_id).or_default().push(event_id);
    *in_degree.entry(event_id).or_insert(0) += 1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use sinex_test_utils::{sinex_test, TestContext};

    #[sinex_test]
    fn session_id_validation_enforces_length() -> TestResult<()> {
        assert!(StreamingCascadeAnalyzer::validate_session_id(&"a".repeat(64)).is_ok());
        assert!(StreamingCascadeAnalyzer::validate_session_id(&"a".repeat(65)).is_err());
        Ok(())
    }

    #[sinex_test]
    fn session_id_validation_rejects_invalid_chars() -> TestResult<()> {
        assert!(StreamingCascadeAnalyzer::validate_session_id("valid_session_1").is_ok());
        assert!(StreamingCascadeAnalyzer::validate_session_id("invalid-session").is_err());
        Ok(())
    }

    #[sinex_test]
    fn record_dependency_inserts_missing_keys() -> TestResult<()> {
        let mut dependencies = HashMap::new();
        let mut in_degree = HashMap::new();
        let source_id = Ulid::new();
        let event_id = Ulid::new();

        record_dependency(&mut dependencies, &mut in_degree, source_id, event_id);

        assert_eq!(dependencies.get(&source_id), Some(&vec![event_id]));
        assert_eq!(in_degree.get(&event_id), Some(&1));
        Ok(())
    }

    #[sinex_test]
    async fn cascade_order_detects_cycles(ctx: TestContext) -> TestResult<()> {
        let analyzer = StreamingCascadeAnalyzer::new(ctx.pool.clone());
        let now = Utc::now();
        let payload = json!({});

        let a = Ulid::new();
        let b = Ulid::new();
        let c = Ulid::new();
        let cycle_links = vec![(a, vec![b]), (b, vec![c]), (c, vec![a])];

        for (event_id, parents) in &cycle_links {
            let parents_uuid: Vec<Uuid> = parents.iter().map(|id| id.to_uuid()).collect();
            sqlx::query(
                "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids) \
                 VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid[]::ulid[])",
            )
            .bind(event_id.to_uuid())
            .bind("cascade-test")
            .bind("cascade.test")
            .bind("test-host")
            .bind(payload.clone())
            .bind(now)
            .bind(parents_uuid)
            .execute(&ctx.pool)
            .await?;
        }

        let err = analyzer
            .plan_cascade_order(&[a, b, c])
            .await
            .expect_err("cycle should be detected in cascade ordering");
        assert!(
            err.to_string().contains("Circular dependencies"),
            "unexpected error: {err}"
        );

        Ok(())
    }
}
