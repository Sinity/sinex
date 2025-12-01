#![doc = include_str!("../docs/cascade_analyzer.md")]

use color_eyre::eyre::{eyre, Result};
use serde::{Deserialize, Serialize};
use sinex_core::db::query_helpers::{db_error, UlidArrayExt};
use sinex_core::db::repositories::{DbPoolExt, EventRepositoryTx};
use sinex_core::types::ulid::Ulid;
use sqlx::PgPool;
use std::collections::{HashMap, VecDeque};
use tracing::{debug, info, warn};
use uuid::Uuid;

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
}

impl Default for CascadeAnalyzerConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            max_depth: 100,
            include_weak_dependencies: false,
            memory_limit_bytes: Some(1024 * 1024 * 1024), // 1GB default
        }
    }
}

/// Memory-efficient cascade analyzer using streaming algorithms
pub struct StreamingCascadeAnalyzer {
    pool: PgPool,
    config: CascadeAnalyzerConfig,
}

#[allow(dead_code)]
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
        info!("Analyzing cascades for {} events", event_ids.len());

        // Generate unique session ID for this analysis
        let session_id = sinex_core::types::ulid::Ulid::new().to_string();

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

    /// Create temporary tables for analysis  
    async fn create_temp_tables(&self, session_id: &str) -> Result<String> {
        // Validate session_id to prevent SQL injection
        Self::validate_session_id(session_id)?;

        let table_name = self
            .pool
            .events()
            .prepare_cascade_session(session_id, false)
            .await
            .map_err(|e| eyre!("prepare cascade session failed: {e}"))?;
        debug!("Created temporary table {}", table_name);
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

    /// Populate initial events to analyze
    async fn populate_initial_events(&self, table_name: &str, event_ids: &[Ulid]) -> Result<()> {
        self.pool
            .events()
            .populate_cascade_roots(table_name, event_ids)
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

    /// Build dependency graph using iterative deepening
    async fn build_dependency_graph(&self, table_name: &str) -> Result<usize> {
        let depth = self
            .pool
            .events()
            .expand_cascade(table_name, self.config.max_depth as i32)
            .await
            .map_err(|e| eyre!("expand cascade graph failed: {e}"))?;

        info!("Built dependency graph with max depth {}", depth);
        Ok(depth)
    }

    /// Calculate histogram of cascade depths
    async fn calculate_depth_histogram(&self, table_name: &str) -> Result<HashMap<usize, usize>> {
        let rows = self
            .pool
            .events()
            .cascade_depth_histogram(table_name)
            .await
            .map_err(|e| eyre!("cascade depth histogram failed: {e}"))?;

        let mut histogram = HashMap::new();
        for (depth, count) in rows {
            histogram.insert(depth as usize, count as usize);
        }

        Ok(histogram)
    }

    /// Count total affected events
    async fn count_affected_events(&self, table_name: &str) -> Result<usize> {
        let count = self
            .pool
            .events()
            .cascade_node_count(table_name)
            .await
            .map_err(|e| eyre!("count cascade nodes failed: {e}"))?;
        Ok(count as usize)
    }

    /// Find integrity violations
    async fn find_integrity_violations(&self, table_name: &str) -> Result<Vec<IntegrityViolation>> {
        let rows = self
            .pool
            .events()
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

    /// Detect circular dependencies using Tarjan's algorithm
    async fn detect_circular_dependencies(
        &self,
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
                FROM {}
                WHERE depth = 0
                
                UNION ALL
                
                SELECT 
                    t.id,
                    t.parent_ids,
                    cc.path || t.id,
                    t.id = ANY(cc.path) as has_cycle
                FROM {} t
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
            .fetch_all(&self.pool)
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

    /// Clean up temporary tables
    async fn cleanup_temp_tables(&self, table_name: &str) -> Result<()> {
        self.pool
            .events()
            .cleanup_cascade_session(table_name)
            .await
            .map_err(|e| eyre!("cleanup cascade session failed: {e}"))?;
        debug!("Cleaned up temporary table {}", table_name);
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
                source_event_ids
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
            let source_ids: Option<Vec<Ulid>> = row.get("source_event_ids");

            if let Some(source_ids) = source_ids {
                for source_id in source_ids {
                    if event_ids.contains(&source_id) {
                        dependencies.get_mut(&source_id).unwrap().push(event_id);
                        *in_degree.get_mut(&event_id).unwrap() += 1;
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

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::sinex_test;

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
}
