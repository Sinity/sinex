use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
// use sinex_core::{ComponentHeartbeat, SystemHealth};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing::{info, error};

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
    State(pool): State<Arc<PgPool>>,
) -> Result<Json<SystemHealthResponse>, StatusCode> {
    let cutoff = Utc::now() - Duration::minutes(3);
    
    // Get latest heartbeat for each component
    let heartbeats = match sqlx::query!(
        r#"
        SELECT DISTINCT ON (component_name)
            component_name,
            timestamp,
            status,
            uptime_seconds,
            memory_usage_mb,
            events_processed_last_minute,
            binary_version,
            git_hash
        FROM component_heartbeats
        WHERE timestamp > $1
        ORDER BY component_name, timestamp DESC
        "#,
        cutoff
    )
    .fetch_all(pool.as_ref())
    .await
    {
        Ok(heartbeats) => heartbeats,
        Err(e) => {
            error!("Failed to fetch heartbeats: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };
    
    let mut components = HashMap::new();
    let mut healthy_count = 0;
    let mut degraded_count = 0;
    let mut failed_count = 0;
    
    let now = Utc::now();
    
    for hb in heartbeats {
        let time_since_heartbeat = (now - hb.timestamp.unwrap_or_else(Utc::now)).num_seconds();
        
        let status = ComponentStatus {
            status: hb.status.clone(),
            last_heartbeat: hb.timestamp.unwrap_or_else(Utc::now),
            uptime_seconds: hb.uptime_seconds.unwrap_or(0),
            memory_usage_mb: hb.memory_usage_mb.unwrap_or(0),
            events_processed_last_minute: hb.events_processed_last_minute.unwrap_or(0),
            binary_version: hb.binary_version.unwrap_or_else(|| "unknown".to_string()),
            git_hash: hb.git_hash.unwrap_or_else(|| "unknown".to_string()),
            time_since_last_heartbeat_seconds: time_since_heartbeat,
        };
        
        match hb.status.as_str() {
            "healthy" => healthy_count += 1,
            "degraded" => degraded_count += 1,
            "failed" => failed_count += 1,
            _ => failed_count += 1, // Unknown status treated as failed
        }
        
        components.insert(hb.component_name, status);
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
        missing_components: 0, // TODO: Compare against expected components
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
    State(pool): State<Arc<PgPool>>,
    axum::extract::Path(component_name): axum::extract::Path<String>,
) -> Result<Json<JsonValue>, StatusCode> {
    // Get recent heartbeats for this component (last 10)
    let heartbeats = match sqlx::query!(
        r#"
        SELECT timestamp, status, uptime_seconds, memory_usage_mb,
               cpu_usage_percent, events_processed_last_minute, 
               errors_last_hour, last_error_message, binary_version, git_hash
        FROM component_heartbeats
        WHERE component_name = $1
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
async fn list_components(
    State(pool): State<Arc<PgPool>>,
) -> Result<Json<JsonValue>, StatusCode> {
    let components = match sqlx::query!(
        r#"
        SELECT DISTINCT component_name,
               MAX(timestamp) as last_seen
        FROM component_heartbeats
        GROUP BY component_name
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
                "name": c.component_name,
                "last_seen": c.last_seen
            })
        }).collect::<Vec<_>>(),
        "total_count": total_count
    })))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("sinex_health_aggregator=info,info")
        .init();
    
    // Get database URL from environment
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL environment variable required");
    
    // Connect to database
    let pool = Arc::new(
        sqlx::PgPool::connect(&database_url).await?
    );
    
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
        .layer(CorsLayer::permissive())
        .with_state(pool);
    
    // Get port from environment or use default
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8082".to_string())
        .parse::<u16>()
        .unwrap_or(8082);
    
    let bind_addr = format!("0.0.0.0:{}", port);
    
    info!("🏥 Health aggregation service starting on http://{}", bind_addr);
    info!("📊 Available endpoints:");
    info!("  GET /health          - Service liveness check");
    info!("  GET /system          - Overall system health");
    info!("  GET /components      - List all components");
    info!("  GET /components/:name - Component details");
    
    // Start the server
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    axum::serve(listener, app).await?;
    
    Ok(())
}