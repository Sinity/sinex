use axum::{extract::State, http::StatusCode, response::Json, routing::get, Router};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use sinex_core::{CoreError, ErrorContext, JsonValue};
use sinex_db::DbPool;
use std::collections::HashMap;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

#[derive(Debug, Serialize, Deserialize)]
pub struct ComponentStatus {
    pub status: String,
    pub last_heartbeat: chrono::DateTime<chrono::Utc>,
    pub uptime_seconds: i64,
    pub memory_usage_mb: i32,
    pub events_processed_last_minute: i32,
    pub binary_version: String,
    pub git_hash: String,
    pub time_since_last_heartbeat_seconds: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SystemHealthResponse {
    pub overall_status: String,
    pub components: HashMap<String, ComponentStatus>,
    pub system_summary: SystemSummary,
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SystemSummary {
    pub healthy_components: u32,
    pub degraded_components: u32,
    pub failed_components: u32,
    pub total_components: u32,
    pub missing_components: u32, // Expected but not reporting
}

/// Health check endpoint - returns overall system health
async fn get_system_health(
    State(pool): State<Arc<DbPool>>,
) -> Result<Json<SystemHealthResponse>, StatusCode> {
    let cutoff = Utc::now() - Duration::minutes(3);

    // Get latest heartbeat events from both legacy process heartbeats and satellite heartbeats via journald
    let heartbeats = match sqlx::query!(
        r#"
        WITH satellite_heartbeats AS (
            SELECT DISTINCT ON ((payload->>'message')::jsonb->'fields'->>'service_name')
                (payload->>'message')::jsonb->'fields'->>'service_name' as component_name,
                ts_ingest as timestamp,
                (payload->>'message')::jsonb->'fields'->>'status' as status,
                ((payload->>'message')::jsonb->'fields'->>'uptime_seconds')::bigint as uptime_seconds,
                ((payload->>'message')::jsonb->'fields'->>'memory_usage_mb')::integer as memory_usage_mb,
                ((payload->>'message')::jsonb->'fields'->>'events_processed')::integer as events_processed_last_minute,
                (payload->>'message')::jsonb->'fields'->>'version' as binary_version,
                (payload->>'message')::jsonb->'fields'->>'git_hash' as git_hash
            FROM raw.events
            WHERE source = 'journald'
              AND event_type = 'entry.written'
              AND payload->>'syslog_identifier' LIKE 'sinex-%'
              AND (payload->>'message')::jsonb->>'message' = 'heartbeat'
              AND ts_ingest > $1
            ORDER BY (payload->>'message')::jsonb->'fields'->>'service_name', ts_ingest DESC
        ),
        process_heartbeats AS (
            SELECT DISTINCT ON (payload->>'process_name')
                payload->>'process_name' as component_name,
                ts_ingest as timestamp,
                payload->>'health_status' as status,
                (payload->>'uptime_seconds')::bigint as uptime_seconds,
                (payload->>'memory_mb')::integer as memory_usage_mb,
                (payload->>'events_processed')::integer as events_processed_last_minute,
                payload->>'version' as binary_version,
                'unknown' as git_hash
            FROM raw.events
            WHERE source = 'sinex.process'
              AND event_type = 'process.heartbeat'
              AND ts_ingest > $1
            ORDER BY payload->>'process_name', ts_ingest DESC
        )
        SELECT * FROM satellite_heartbeats
        UNION ALL
        SELECT * FROM process_heartbeats
        "#,
        cutoff
    )
    .fetch_all(pool.as_ref())
    .await
    {
        Ok(heartbeats) => heartbeats,
        Err(e) => {
            let error_context = ErrorContext::new(CoreError::Database(format!(
                "Failed to fetch heartbeats: {}",
                e
            )))
            .with_operation("get_system_health")
            .with_context("table", "raw.events")
            .with_context("cutoff_time", cutoff.to_rfc3339())
            .with_context("query_type", "fetch_recent_process_heartbeats")
            .with_context(
                "suggestion",
                "Check database connectivity and process.heartbeat events in raw.events table",
            )
            .build();

            error!(
                error = %error_context,
                cutoff_time = %cutoff,
                operation = "get_system_health",
                "Database query failed while fetching process heartbeat events"
            );

            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let mut components = HashMap::new();
    let mut healthy_count = 0;
    let mut degraded_count = 0;
    let mut failed_count = 0;

    let now = Utc::now();

    for hb in heartbeats {
        let time_since_heartbeat = (now - hb.timestamp.unwrap_or_else(Utc::now))
            .num_seconds();

        let status_str = hb.status.unwrap_or_else(|| "unknown".to_string());
        let component_name = hb.component_name.unwrap_or_else(|| "unknown".to_string());
        
        let status = ComponentStatus {
            status: status_str.clone(),
            last_heartbeat: hb.timestamp.unwrap_or_else(Utc::now),
            uptime_seconds: hb.uptime_seconds.unwrap_or(0),
            memory_usage_mb: hb.memory_usage_mb.unwrap_or(0),
            events_processed_last_minute: hb.events_processed_last_minute.unwrap_or(0),
            binary_version: hb.binary_version.unwrap_or_else(|| "unknown".to_string()),
            git_hash: hb.git_hash.unwrap_or_else(|| "unknown".to_string()),
            time_since_last_heartbeat_seconds: time_since_heartbeat,
        };

        match status_str.as_str() {
            "healthy" | "PASS" => healthy_count += 1,
            "degraded" | "WARNING" => degraded_count += 1,
            "failed" | "FAIL" => failed_count += 1,
            _ => failed_count += 1, // Unknown status treated as failed
        }

        components.insert(component_name, status);
    }

    let total_count = components.len() as u32;

    // Determine overall status
    let overall_status = if total_count == 0 {
        "unknown"
    } else if failed_count > 0 {
        "failed"
    } else if degraded_count > 0 {
        "degraded"
    } else {
        "healthy"
    };

    let system_summary = SystemSummary {
        healthy_components: healthy_count,
        degraded_components: degraded_count,
        failed_components: failed_count,
        total_components: total_count,
        missing_components: 0, // Requires expected components registry - not implemented
    };

    Ok(Json(SystemHealthResponse {
        overall_status: overall_status.to_string(),
        components,
        system_summary,
        last_updated: now,
    }))
}

/// Simple liveness check
async fn health_check() -> Json<JsonValue> {
    Json(serde_json::json!({
        "status": "ok",
        "service": "sinex-health-aggregator",
        "timestamp": Utc::now()
    }))
}

/// Get detailed component information
async fn get_component_details(
    State(pool): State<Arc<DbPool>>,
    axum::extract::Path(component_name): axum::extract::Path<String>,
) -> Result<Json<JsonValue>, StatusCode> {
    // Get recent heartbeat events for this component from both sources (last 10)
    let heartbeats = match sqlx::query!(
        r#"
        WITH satellite_heartbeats AS (
            SELECT ts_ingest as timestamp,
                   (payload->>'message')::jsonb->'fields'->>'status' as status,
                   ((payload->>'message')::jsonb->'fields'->>'uptime_seconds')::bigint as uptime_seconds,
                   ((payload->>'message')::jsonb->'fields'->>'memory_usage_mb')::integer as memory_usage_mb,
                   ((payload->>'message')::jsonb->'fields'->>'cpu_usage_percent')::real as cpu_usage_percent,
                   ((payload->>'message')::jsonb->'fields'->>'events_processed')::integer as events_processed_last_minute,
                   ((payload->>'message')::jsonb->'fields'->>'errors_count')::integer as errors_last_hour,
                   (payload->>'message')::jsonb->'fields'->>'last_error_message' as last_error_message,
                   (payload->>'message')::jsonb->'fields'->>'version' as binary_version,
                   (payload->>'message')::jsonb->'fields'->>'git_hash' as git_hash
            FROM raw.events
            WHERE source = 'journald'
              AND event_type = 'entry.written'
              AND payload->>'syslog_identifier' = $1
              AND (payload->>'message')::jsonb->>'message' = 'heartbeat'
            ORDER BY ts_ingest DESC
            LIMIT 10
        ),
        process_heartbeats AS (
            SELECT ts_ingest as timestamp,
                   payload->>'health_status' as status,
                   (payload->>'uptime_seconds')::bigint as uptime_seconds,
                   (payload->>'memory_mb')::integer as memory_usage_mb,
                   (payload->>'cpu_percent')::real as cpu_usage_percent,
                   (payload->>'events_processed')::integer as events_processed_last_minute,
                   (payload->>'errors_count')::integer as errors_last_hour,
                   null as last_error_message,
                   payload->>'version' as binary_version,
                   'unknown' as git_hash
            FROM raw.events
            WHERE source = 'sinex.process'
              AND event_type = 'process.heartbeat'
              AND payload->>'process_name' = $1
            ORDER BY ts_ingest DESC
            LIMIT 10
        )
        SELECT * FROM satellite_heartbeats
        UNION ALL
        SELECT * FROM process_heartbeats
        ORDER BY timestamp DESC
        LIMIT 10
        "#,
        component_name
    )
    .fetch_all(pool.as_ref())
    .await
    {
        Ok(heartbeats) => heartbeats,
        Err(e) => {
            error!("Failed to fetch component details: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    if heartbeats.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    Ok(Json(serde_json::json!({
        "component_name": component_name,
        "recent_heartbeats": heartbeats.into_iter().map(|hb| {
            serde_json::json!({
                "timestamp": hb.timestamp,
                "status": hb.status,
                "uptime_seconds": hb.uptime_seconds,
                "memory_usage_mb": hb.memory_usage_mb,
                "cpu_usage_percent": hb.cpu_usage_percent,
                "events_processed_last_minute": hb.events_processed_last_minute,
                "errors_last_hour": hb.errors_last_hour,
                "last_error_message": hb.last_error_message,
                "binary_version": hb.binary_version,
                "git_hash": hb.git_hash
            })
        }).collect::<Vec<_>>(),
        "last_updated": Utc::now()
    })))
}

/// Get list of all known components
async fn list_components(State(pool): State<Arc<DbPool>>) -> Result<Json<JsonValue>, StatusCode> {
    let components = match sqlx::query!(
        r#"
        WITH satellite_components AS (
            SELECT DISTINCT (payload->>'message')::jsonb->'fields'->>'service_name' as component_name,
                   MAX(ts_ingest) as last_seen
            FROM raw.events
            WHERE source = 'journald'
              AND event_type = 'entry.written'
              AND payload->>'syslog_identifier' LIKE 'sinex-%'
              AND (payload->>'message')::jsonb->>'message' = 'heartbeat'
            GROUP BY (payload->>'message')::jsonb->'fields'->>'service_name'
        ),
        process_components AS (
            SELECT DISTINCT payload->>'process_name' as component_name,
                   MAX(ts_ingest) as last_seen
            FROM raw.events
            WHERE source = 'sinex.process'
              AND event_type = 'process.heartbeat'
            GROUP BY payload->>'process_name'
        )
        SELECT * FROM satellite_components
        UNION ALL
        SELECT * FROM process_components
        ORDER BY component_name
        "#
    )
    .fetch_all(pool.as_ref())
    .await
    {
        Ok(components) => components,
        Err(e) => {
            error!("Failed to list components: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let total_count = components.len();
    Ok(Json(serde_json::json!({
        "components": components.into_iter().map(|c| {
            serde_json::json!({
                "name": c.component_name.unwrap_or_else(|| "unknown".to_string()),
                "last_seen": c.last_seen
            })
        }).collect::<Vec<_>>(),
        "total_count": total_count
    })))
}

/// Get monitoring alerts - silent sources and resource exhaustion
async fn get_monitoring_alerts(
    State(pool): State<Arc<DbPool>>,
) -> Result<Json<JsonValue>, StatusCode> {
    // Look for recent silent source events
    let silent_sources = match sqlx::query!(
        r#"
        SELECT payload
        FROM raw.events
        WHERE source = 'sinex.monitoring.sources'
          AND event_type = 'sources_silent'
          AND ts_ingest > NOW() - INTERVAL '1 hour'
        ORDER BY ts_ingest DESC
        LIMIT 10
        "#
    )
    .fetch_all(pool.as_ref())
    .await
    {
        Ok(events) => events,
        Err(e) => {
            error!("Failed to fetch silent source events: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // Look for recent resource exhaustion events
    let resource_alerts = match sqlx::query!(
        r#"
        SELECT payload
        FROM raw.events
        WHERE source = 'sinex.monitoring.resources'
          AND event_type = 'resource_exhaustion'
          AND ts_ingest > NOW() - INTERVAL '1 hour'
        ORDER BY ts_ingest DESC
        LIMIT 10
        "#
    )
    .fetch_all(pool.as_ref())
    .await
    {
        Ok(events) => events,
        Err(e) => {
            error!("Failed to fetch resource exhaustion events: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // Look for schema validation failures
    let schema_failures = match sqlx::query!(
        r#"
        SELECT COUNT(*) as failure_count,
               array_agg(DISTINCT source) as failing_sources
        FROM raw.events
        WHERE source LIKE '%error%' OR event_type LIKE '%failed%' OR event_type LIKE '%error%'
          AND ts_ingest > NOW() - INTERVAL '1 hour'
        "#
    )
    .fetch_one(pool.as_ref())
    .await
    {
        Ok(result) => result,
        Err(e) => {
            error!("Failed to fetch schema failure events: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    Ok(Json(serde_json::json!({
        "timestamp": Utc::now(),
        "alerts": {
            "silent_sources": silent_sources.into_iter().map(|e| e.payload).collect::<Vec<_>>(),
            "resource_exhaustion": resource_alerts.into_iter().map(|e| e.payload).collect::<Vec<_>>(),
            "schema_failures": {
                "count_last_hour": schema_failures.failure_count.unwrap_or(0),
                "failing_sources": schema_failures.failing_sources.unwrap_or_default()
            }
        }
    })))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("sinex_health_aggregator=info,info")
        .init();

    // Get database URL from environment
    let database_url = std::env::var("DATABASE_URL").map_err(|_| {
        anyhow::anyhow!("DATABASE_URL environment variable is required but not set")
    })?;

    // Connect to database
    let pool = Arc::new(sinex_db::create_pool(&database_url).await?);

    // Test database connection
    if let Err(e) = sqlx::query("SELECT 1").execute(pool.as_ref()).await {
        error!("Failed to connect to database: {}", e);
        return Err(e.into());
    }

    info!("Connected to database successfully");

    // Build application routes
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/system", get(get_system_health))
        .route("/components", get(list_components))
        .route("/components/:component_name", get(get_component_details))
        .route("/alerts", get(get_monitoring_alerts))
        .layer(CorsLayer::permissive())
        .with_state(pool);

    // Get port from environment or use default
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8082".to_string())
        .parse::<u16>()
        .unwrap_or(8082);

    let bind_addr = format!("0.0.0.0:{}", port);

    info!(
        "🏥 Health aggregation service starting on http://{}",
        bind_addr
    );
    info!("📊 Available endpoints:");
    info!("  GET /health          - Service liveness check");
    info!("  GET /system          - Overall system health");
    info!("  GET /components      - List all components");
    info!("  GET /components/:name - Component details");
    info!("  GET /alerts          - Monitoring alerts (silent sources, resource exhaustion)");

    // Start the server
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
