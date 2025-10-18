#![doc = include_str!("../doc/cascade_analyzer.md")]

use chrono::Utc;
use color_eyre::eyre::{eyre, Result};
use serde::{Deserialize, Serialize};
use sinex_core::db::query_helpers::{db_error, UlidArrayExt};
use sinex_core::types::ulid::Ulid;
use sqlx::PgPool;
use std::collections::{HashMap, VecDeque};
use tracing::{debug, info, warn};

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
        let session_id = format!("{}", Utc::now().timestamp_nanos_opt().unwrap_or_default());

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

        let table_name = format!("cascade_analysis_{}", session_id);

        // Use PostgreSQL's quote_ident() to safely handle the table name
        let create_table_sql = r#"
            SELECT 'CREATE TEMP TABLE ' || quote_ident($1) || ' (
                id ULID PRIMARY KEY,
                depth INTEGER NOT NULL DEFAULT 0,
                parent_ids ULID[] DEFAULT ''{}''::ULID[],
                processed BOOLEAN DEFAULT FALSE
            ) ON COMMIT DROP' AS sql
        "#;

        let sql_result = sqlx::query_scalar::<_, String>(create_table_sql)
            .bind(&table_name)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| db_error(e, "build safe table creation SQL"))?;

        sqlx::query(&sql_result)
            .execute(&mut **tx)
            .await
            .map_err(|e| {
                tracing::error!("Failed to create temp table {}: {}", table_name, e);
                db_error(e, "create temp cascade tables")
            })?;

        debug!("Created temp table: {}", table_name);
        Ok(table_name)
    }

    /// Create temporary tables for analysis  
    async fn create_temp_tables(&self, session_id: &str) -> Result<String> {
        // Validate session_id to prevent SQL injection
        Self::validate_session_id(session_id)?;

        // Generate unique table name for this session
        let table_name = format!("cascade_analysis_{}", session_id);

        // Use PostgreSQL's quote_ident() to safely handle table and index names
        let create_table_sql = r#"
            SELECT 'CREATE TEMPORARY TABLE IF NOT EXISTS ' || quote_ident($1) || ' (
                id ULID PRIMARY KEY,
                depth INT NOT NULL DEFAULT 0,
                parent_ids ULID[],
                child_ids ULID[],
                is_archived BOOLEAN DEFAULT FALSE,
                is_live BOOLEAN DEFAULT TRUE,
                processed BOOLEAN DEFAULT FALSE
            )' AS sql
        "#;

        let table_sql = sqlx::query_scalar::<_, String>(create_table_sql)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| db_error(e, "build safe table creation SQL"))?;

        let create_indexes_sql = r#"
            SELECT 'CREATE INDEX IF NOT EXISTS ' || quote_ident('idx_' || $1 || '_depth') || 
                   ' ON ' || quote_ident($1) || ' (depth); ' ||
                   'CREATE INDEX IF NOT EXISTS ' || quote_ident('idx_' || $1 || '_processed') || 
                   ' ON ' || quote_ident($1) || ' (processed)' AS sql
        "#;

        let indexes_sql = sqlx::query_scalar::<_, String>(create_indexes_sql)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| db_error(e, "build safe index creation SQL"))?;

        // Execute table creation
        sqlx::query(&table_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                tracing::error!("Failed to create temp table {}: {}", table_name, e);
                db_error(e, "create temp cascade table")
            })?;

        // Execute index creation
        sqlx::query(&indexes_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                tracing::error!("Failed to create indexes for table {}: {}", table_name, e);
                db_error(e, "create temp cascade indexes")
            })?;

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

        let values: Vec<String> = event_ids
            .iter()
            .map(|id| format!("('{}'::ulid, 0)", id))
            .collect();

        let insert_sql = format!(
            "INSERT INTO {} (id, depth) VALUES {} ON CONFLICT DO NOTHING",
            table_name,
            values.join(",")
        );

        sqlx::query(&insert_sql)
            .execute(&mut **tx)
            .await
            .map_err(|e| db_error(e, "populate initial events"))?;

        debug!("Populated {} initial events", event_ids.len());
        Ok(())
    }

    /// Populate initial events to analyze
    async fn populate_initial_events(&self, table_name: &str, event_ids: &[Ulid]) -> Result<()> {
        let query = format!(
            r#"
            INSERT INTO {} (id, depth, parent_ids, is_archived)
            SELECT 
                e.event_id,
                0,
                e.source_event_ids,
                FALSE
            FROM core.events e
            WHERE e.event_id = ANY($1::ulid[])
            "#,
            table_name
        );

        sqlx::query(&query)
            .bind(event_ids.to_uuid_vec())
            .execute(&self.pool)
            .await
            .map_err(|e| db_error(e, "populate initial events"))?;

        debug!("Populated {} initial events", event_ids.len());
        Ok(())
    }

    /// Build dependency graph using iterative deepening (transaction version)
    async fn build_dependency_graph_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table_name: &str,
    ) -> Result<usize> {
        let mut current_depth = 0;
        let max_depth = self.config.max_depth;
        let batch_size = self.config.batch_size;

        loop {
            // Process events in batches to avoid memory issues
            let mut total_inserted = 0;
            let mut batch_offset = 0;

            loop {
                // Find children of current depth events in batches
                let query = format!(
                    r#"
                    WITH current_level AS (
                        SELECT id, parent_ids
                        FROM {}
                        WHERE depth = $1 AND NOT processed
                        LIMIT $2 OFFSET $3
                    ),
                    children AS (
                        SELECT DISTINCT e.event_id, e.source_event_ids as parent_ids
                        FROM core.events e
                        JOIN current_level cl ON e.source_event_ids && ARRAY[cl.id]
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {} t WHERE t.id = e.event_id
                        )
                        LIMIT $2
                    )
                    INSERT INTO {} (id, depth, parent_ids)
                    SELECT event_id, $4, parent_ids
                    FROM children
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id
                    "#,
                    table_name, table_name, table_name
                );

                let inserted = sqlx::query(&query)
                    .bind(current_depth as i32)
                    .bind(batch_size as i32)
                    .bind(batch_offset as i32)
                    .bind((current_depth + 1) as i32)
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(|e| db_error(e, "build dependency graph - insert children"))?;

                let batch_count = inserted.len();
                total_inserted += batch_count;

                if batch_count < batch_size {
                    // No more events at this offset
                    break;
                }

                batch_offset += batch_size;

                // Check memory limit if configured
                if let Some(memory_limit) = self.config.memory_limit_bytes {
                    // Estimate memory usage (rough calculation)
                    let estimated_rows = self.count_affected_events_tx(tx, table_name).await?;
                    let estimated_memory = estimated_rows * 64; // ~64 bytes per row estimate

                    if estimated_memory > memory_limit {
                        warn!(
                            "Memory limit reached: {} bytes (limit: {} bytes)",
                            estimated_memory, memory_limit
                        );
                        return Err(eyre!("Memory limit exceeded during graph building"));
                    }
                }
            }

            // Mark current depth as processed
            let update_query = format!(
                "UPDATE {} SET processed = true WHERE depth = $1",
                table_name
            );
            sqlx::query(&update_query)
                .bind(current_depth as i32)
                .execute(&mut **tx)
                .await
                .map_err(|e| db_error(e, "build dependency graph - mark processed"))?;

            if total_inserted == 0 || current_depth >= max_depth {
                break;
            }

            current_depth += 1;
            debug!(
                "Processed depth {}, inserted {} new events",
                current_depth - 1,
                total_inserted
            );
        }

        Ok(current_depth)
    }

    /// Calculate depth histogram (transaction version)
    async fn calculate_depth_histogram_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table_name: &str,
    ) -> Result<HashMap<usize, usize>> {
        let query = format!(
            r#"
            SELECT depth, COUNT(*) as count
            FROM {}
            GROUP BY depth
            ORDER BY depth
            "#,
            table_name
        );

        let rows = sqlx::query_as::<_, (i32, i64)>(&query)
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| db_error(e, "calculate depth histogram"))?;

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
        // Use safe identifier quoting for table name
        let query_sql = r#"
            SELECT 'SELECT COUNT(*) FROM ' || quote_ident($1) AS sql
        "#;

        let safe_query = sqlx::query_scalar::<_, String>(query_sql)
            .bind(table_name)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| db_error(e, "build safe count query"))?;

        let row = sqlx::query_scalar::<_, i64>(&safe_query)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| db_error(e, "count affected events"))?;

        Ok(row as usize)
    }

    /// Find integrity violations (transaction version)
    async fn find_integrity_violations_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table_name: &str,
    ) -> Result<Vec<IntegrityViolation>> {
        // Find live events that would reference archived events
        let query = format!(
            r#"
            WITH archived_set AS (
                SELECT id FROM {} WHERE depth = 0
            ),
            violations AS (
                SELECT 
                    e.event_id as live_event_id,
                    unnest(e.source_event_ids) as archived_event_id
                FROM core.events e
                WHERE e.source_event_ids && (SELECT array_agg(id) FROM archived_set)
                AND e.event_id NOT IN (SELECT id FROM {})
            )
            SELECT DISTINCT live_event_id, archived_event_id
            FROM violations
            LIMIT 100
            "#,
            table_name, table_name
        );

        let rows = sqlx::query_as::<_, (Ulid, Ulid)>(&query)
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| db_error(e, "find integrity violations"))?;

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
        // For now, use a simple SQL approach to find potential cycles
        // In production, would implement proper Tarjan's algorithm
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
                AND array_length(cc.path, 1) < 10
            )
            SELECT path
            FROM cycle_check
            WHERE has_cycle
            LIMIT 10
            "#,
            table_name, table_name
        );

        let rows = sqlx::query_as::<_, (Vec<Ulid>,)>(&query)
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| db_error(e, "detect circular dependencies"))?;

        let mut cycles = Vec::new();
        for (path,) in rows {
            cycles.push(CircularDependency {
                cycle: path,
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
        // Temp tables created with ON COMMIT DROP will auto-cleanup
        // But we can explicitly drop if needed
        let drop_sql_query = r#"
            SELECT 'DROP TABLE IF EXISTS ' || quote_ident($1) AS sql
        "#;

        let safe_drop_sql = sqlx::query_scalar::<_, String>(drop_sql_query)
            .bind(table_name)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| db_error(e, "build safe drop table SQL"))?;

        sqlx::query(&safe_drop_sql)
            .execute(&mut **tx)
            .await
            .map_err(|e| db_error(e, "cleanup temp tables"))?;
        Ok(())
    }

    /// Build dependency graph using iterative deepening
    async fn build_dependency_graph(&self, table_name: &str) -> Result<usize> {
        let mut current_depth = 0;
        let max_depth = self.config.max_depth;
        let batch_size = self.config.batch_size;

        loop {
            // Process events in batches to avoid memory issues
            let mut total_inserted = 0;
            let mut batch_offset = 0;

            loop {
                // Find children of current depth events in batches
                let query = format!(
                    r#"
                    WITH current_level AS (
                        SELECT id, parent_ids
                        FROM {}
                        WHERE depth = $1 AND NOT processed
                        LIMIT $2 OFFSET $3
                    ),
                    children AS (
                        SELECT DISTINCT e.event_id, e.source_event_ids as parent_ids
                        FROM core.events e
                        JOIN current_level cl ON e.source_event_ids && ARRAY[cl.id]
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {} t WHERE t.id = e.event_id
                        )
                        LIMIT $2
                    )
                    INSERT INTO {} (id, depth, parent_ids)
                    SELECT event_id, $4, parent_ids
                    FROM children
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id
                    "#,
                    table_name, table_name, table_name
                );

                let inserted = sqlx::query(&query)
                    .bind(current_depth as i32)
                    .bind(batch_size as i32)
                    .bind(batch_offset as i32)
                    .bind((current_depth + 1) as i32)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|e| db_error(e, "build dependency graph - insert children"))?;

                let batch_count = inserted.len();
                total_inserted += batch_count;

                if batch_count < batch_size {
                    // No more events at this offset
                    break;
                }

                batch_offset += batch_size;

                // Check memory limit if configured
                if let Some(memory_limit) = self.config.memory_limit_bytes {
                    let estimated_memory = self.estimate_memory_usage(table_name).await?;
                    if estimated_memory > memory_limit {
                        warn!(
                            "Memory limit exceeded: {} > {}",
                            estimated_memory, memory_limit
                        );
                        return Err(eyre!("Analysis would exceed memory limit"));
                    }
                }
            }

            // Mark current depth as processed
            let update_query = format!(
                "UPDATE {} SET processed = TRUE WHERE depth = $1",
                table_name
            );
            sqlx::query(&update_query)
                .bind(current_depth as i32)
                .execute(&self.pool)
                .await
                .map_err(|e| db_error(e, "build dependency graph - mark processed"))?;

            if total_inserted == 0 || current_depth >= max_depth {
                break;
            }

            current_depth += 1;
            debug!(
                "Processed depth {} with batch size {}, found {} children",
                current_depth, batch_size, total_inserted
            );
        }

        info!("Built dependency graph with max depth {}", current_depth);
        Ok(current_depth)
    }

    /// Estimate memory usage of the temp table
    async fn estimate_memory_usage(&self, table_name: &str) -> Result<usize> {
        let query = format!(
            r#"
            SELECT COUNT(*) as count,
                   AVG(octet_length(id::text) + 
                       COALESCE(array_length(parent_ids, 1) * 16, 0) + 
                       8) as avg_row_size
            FROM {}
            "#,
            table_name
        );

        let result = sqlx::query_as::<_, (i64, Option<f64>)>(&query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| db_error(e, "estimate memory usage"))?;

        let (count, avg_size) = result;
        let estimated_bytes = (count as f64 * avg_size.unwrap_or(100.0)) as usize;

        Ok(estimated_bytes)
    }

    /// Calculate histogram of cascade depths
    async fn calculate_depth_histogram(&self, table_name: &str) -> Result<HashMap<usize, usize>> {
        let query = format!(
            r#"
            SELECT depth, COUNT(*) as count
            FROM {}
            GROUP BY depth
            ORDER BY depth
            "#,
            table_name
        );

        let rows = sqlx::query_as::<_, (i32, i64)>(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| db_error(e, "calculate depth histogram"))?;

        let mut histogram = HashMap::new();
        for (depth, count) in rows {
            histogram.insert(depth as usize, count as usize);
        }

        Ok(histogram)
    }

    /// Count total affected events
    async fn count_affected_events(&self, table_name: &str) -> Result<usize> {
        // Use safe identifier quoting for table name
        let query_sql = r#"
            SELECT 'SELECT COUNT(*) FROM ' || quote_ident($1) AS sql
        "#;

        let safe_query = sqlx::query_scalar::<_, String>(query_sql)
            .bind(table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| db_error(e, "build safe count query"))?;

        let row = sqlx::query_scalar::<_, i64>(&safe_query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| db_error(e, "count affected events"))?;

        Ok(row as usize)
    }

    /// Find integrity violations
    async fn find_integrity_violations(&self, table_name: &str) -> Result<Vec<IntegrityViolation>> {
        // Find live events that would reference archived events
        let query = format!(
            r#"
            WITH archived_set AS (
                SELECT id FROM {} WHERE depth = 0
            ),
            violations AS (
                SELECT 
                    e.event_id as live_event_id,
                    unnest(e.source_event_ids) as archived_event_id
                FROM core.events e
                WHERE e.source_event_ids && (SELECT array_agg(id) FROM archived_set)
                AND e.event_id NOT IN (SELECT id FROM {})
            )
            SELECT DISTINCT live_event_id, archived_event_id
            FROM violations
            LIMIT 100
            "#,
            table_name, table_name
        );

        let rows = sqlx::query_as::<_, (Ulid, Ulid)>(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| db_error(e, "find integrity violations"))?;

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
        // For now, use a simple SQL approach to find potential cycles
        // In production, would implement proper Tarjan's algorithm
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
                AND array_length(cc.path, 1) < 10
            )
            SELECT path
            FROM cycle_check
            WHERE has_cycle
            LIMIT 10
            "#,
            table_name, table_name
        );

        let rows = sqlx::query_as::<_, (Vec<Ulid>,)>(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| db_error(e, "detect circular dependencies"))?;

        let mut cycles = Vec::new();
        for (path,) in rows {
            cycles.push(CircularDependency {
                cycle: path,
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
        // Use safe identifier quoting for table name
        let drop_sql_query = r#"
            SELECT 'DROP TABLE IF EXISTS ' || quote_ident($1) AS sql
        "#;

        let safe_drop_sql = sqlx::query_scalar::<_, String>(drop_sql_query)
            .bind(table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| db_error(e, "build safe drop table SQL"))?;

        sqlx::query(&safe_drop_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| db_error(e, "cleanup temp tables"))?;

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
                event_id,
                source_event_ids
            FROM core.events
            WHERE event_id = ANY($1::ulid[])
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

    #[tokio::test]
    async fn test_cascade_analysis_structure() {
        let analysis = CascadeAnalysis {
            max_depth: 5,
            depth_histogram: HashMap::from([(0, 10), (1, 20), (2, 15)]),
            integrity_violations: vec![],
            total_affected: 45,
            circular_dependencies: vec![],
            memory_estimate: 11520,
        };

        assert_eq!(analysis.max_depth, 5);
        assert_eq!(analysis.total_affected, 45);
        assert_eq!(analysis.depth_histogram.get(&1), Some(&20));
    }

    #[test]
    fn test_violation_types() {
        let violation = IntegrityViolation {
            archived_event_id: Ulid::new(),
            live_event_id: Ulid::new(),
            violation_type: ViolationType::LiveToArchived,
            severity: Severity::Critical,
        };

        match violation.violation_type {
            ViolationType::LiveToArchived => assert!(true),
            _ => panic!("Wrong violation type"),
        }

        match violation.severity {
            Severity::Critical => assert!(true),
            _ => panic!("Wrong severity"),
        }
    }
}
