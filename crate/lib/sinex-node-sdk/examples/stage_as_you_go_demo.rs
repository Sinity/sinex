//! Stage-as-You-Go Demonstration
//!
//! This example demonstrates how to use the Stage-as-You-Go pattern
//! to process large content while providing immediate feedback via events.
//!
//! It shows:
//! 1. Setting up `StageAsYouGoContext`
//! 2. Using `MaterialBuilder` to create source materials
//! 3. Emitting events with provenance linked to materials
//! 4. Finalizing materials
//!
//! Run with: cargo run --example `stage_as_you_go_demo`

use color_eyre::eyre::Result;
use serde_json::json;
use sinex_db::models::Event;
use sinex_node_sdk::NodeResult;
use sinex_node_sdk::acquisition_manager::AcquisitionManager;
use sinex_node_sdk::stage_as_you_go::{StageAsYouGoContext, StageAsYouGoNode, StageAsYouGoResult};
use sinex_primitives::events::LogLinePayload;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

/// Mock node to demonstrate Stage-as-You-Go
pub struct DemoLogNode {
    context: StageAsYouGoContext,
}

impl DemoLogNode {
    #[must_use]
    pub fn new(context: StageAsYouGoContext) -> Self {
        Self { context }
    }
}

impl StageAsYouGoNode for DemoLogNode {
    async fn process_with_staging(
        &mut self,
        content: &[u8],
        source_uri: Option<&str>,
        metadata: serde_json::Value,
    ) -> NodeResult<StageAsYouGoResult> {
        let start_time = std::time::Instant::now();
        let mut event_ids = Vec::new();

        println!("Step 1: Register in-flight source material");
        // This uses MaterialBuilder internally
        let material_id = self
            .context
            .register_in_flight("demo_log", source_uri, metadata)
            .await?;

        println!("  -> Registered material: {material_id}");

        println!("Step 2: Process content and emit events");
        let content_str = String::from_utf8_lossy(content);

        for (i, line) in content_str.lines().enumerate() {
            println!("  -> Processing line {}: {}", i + 1, line);

            // Create event payload
            let payload = LogLinePayload {
                line: line.to_string(),
                line_number: (i + 1) as u64,
                log_source: "demo".to_string(),
                log_file: source_uri.unwrap_or("unknown").to_string(),
                offset_start: 0, // Simplified for demo
                offset_end: line.len() as i64,
                source_material_id: material_id.to_string(),
            };

            let event = Event::new(
                payload,
                // Provenance will be overwritten/augmented by context
                sinex_primitives::Provenance::from_synthesis(std::iter::once(
                    sinex_primitives::events::EventId::from_uuid(Uuid::now_v7()),
                ))
                .expect("non-empty iterator yields synthesis provenance"),
            );

            // Emit with provenance linked to material
            let event_id = self
                .context
                .emit_event_with_provenance(
                    event.to_json_event()?,
                    material_id,
                    Some(0), // timestamps/offsets would be real in prod
                    Some(line.len() as i64),
                )
                .await?;

            event_ids.push(event_id.to_string());
        }

        println!("Step 3: Finalize source material");
        self.context
            .finalize_source_material(material_id, content, Some("text/plain"), Some("utf-8"))
            .await?;

        println!("  -> Finalized material");

        Ok(StageAsYouGoResult {
            source_material_id: material_id,
            event_ids,
            bytes_processed: content.len(),
            duration: start_time.elapsed(),
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Stage-as-You-Go Demo");
    println!("====================");

    // 1. Setup minimal dependencies
    // In a real app, these come from NodeHandles or runtime
    // For demo, we'll try to connect to local NATS or fail gracefully

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    println!("Connecting to NATS at {nats_url}...");

    let client = match async_nats::connect(&nats_url).await {
        Ok(c) => c,
        Err(e) => {
            println!("Could not connect to NATS: {e}");
            println!("Skipping live execution, but code compilation is verified.");
            return Ok(());
        }
    };

    // 2. Initialize AcquisitionManager
    let manager = Arc::new(AcquisitionManager::with_defaults(client, "demo-source"));

    // 3. Initialize StageAsYouGoContext
    // We use a mock channel for events since we don't have a full event emitter loop here
    let (tx, _rx) = mpsc::channel(100);
    let context = StageAsYouGoContext::from_sender(
        manager, tx,   // events go here
        true, // dry_run = true (don't actually publish events to NATS streams for demo)
    );

    // 4. Run the node
    let mut node = DemoLogNode::new(context);
    let content = b"Log line 1\nLog line 2\nLog line 3";

    match node
        .process_with_staging(
            content,
            Some("demo.log"),
            json!({"custom_field": "demo_value"}),
        )
        .await
    {
        Ok(result) => {
            println!("\nSuccess!");
            println!("{result:#?}");
        }
        Err(e) => {
            println!("\nProcessing failed (expected if NATS streams not set up): {e}");
            println!("Ensure JetStream streams 'SOURCE_MATERIAL_*' exist.");
        }
    }

    Ok(())
}
