use anyhow::Result;
use chrono::{DateTime, Utc};
use hyprland::event_listener::EventListener;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
struct Event {
    id: Uuid,
    source: String,
    ts_ingest: DateTime<Utc>,
    payload: serde_json::Value,
    provenance: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("hyprland_ingestor=debug".parse()?),
        )
        .init();

    info!("Starting Hyprland ingestor...");

    // Database connection
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://localhost/exocortex".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    info!("Connected to database");

    // Test connection
    let row = sqlx::query("SELECT version()")
        .fetch_one(&pool)
        .await?;
    let version: String = row.get(0);
    info!("PostgreSQL version: {}", version);

    // Start event listener
    let mut listener = EventListener::new();

    listener.add_workspace_change_handler(|id| {
        info!("Workspace changed to: {}", id);
    });

    listener.add_active_window_change_handler(|data| {
        info!("Active window changed: {:?}", data);
    });

    listener.add_fullscreen_state_change_handler(|state| {
        info!("Fullscreen state changed: {}", state);
    });

    // Start listening in a separate task
    let pool_clone = pool.clone();
    tokio::spawn(async move {
        if let Err(e) = listen_and_ingest(listener, pool_clone).await {
            error!("Event listener error: {}", e);
        }
    });

    // Keep main thread alive
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    Ok(())
}

async fn listen_and_ingest(mut listener: EventListener, pool: PgPool) -> Result<()> {
    listener.start_listener_async().await?;
    Ok(())
}

async fn ingest_event(
    pool: &PgPool,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<()> {
    let event = Event {
        id: Uuid::new_v4(),
        source: "hyprland".to_string(),
        ts_ingest: Utc::now(),
        payload: json!({
            "type": event_type,
            "data": payload,
        }),
        provenance: json!({
            "ingestor_version": env!("CARGO_PKG_VERSION"),
            "hostname": std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()),
        }),
    };

    sqlx::query(
        r#"
        INSERT INTO raw.events (id, source, ts_ingest, payload, provenance)
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(&event.id)
    .bind(&event.source)
    .bind(&event.ts_ingest)
    .bind(&event.payload)
    .bind(&event.provenance)
    .execute(pool)
    .await?;

    Ok(())
}