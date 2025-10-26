//! Material Assembler for consuming material slices from NATS JetStream.
//!
//! Consumes source_material.begin/slices/end messages, assembles slices into
//! complete files, verifies hashes, writes to git-annex, and updates the
//! source material registry.

use async_nats::{jetstream, Client as NatsClient};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::Deserialize;
use sinex_core::{
    db::{DbPool, DbPoolExt},
    environment::SinexEnvironment,
    types::Ulid,
    Id, JsonValue, SourceMaterialRecord,
};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::{IngestdResult, SinexError};

/// Message from source_material.begin
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct MaterialBeginMessage {
    material_id: String,
    material_kind: String,
    source_identifier: String,
    metadata: JsonValue,
    started_at: String,
}

/// Message from source_material.end
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct MaterialEndMessage {
    material_id: String,
    ended_at: String,
    content_hash: String,
    total_slices: usize,
    total_size_bytes: i64,
}

/// Assembler state for a single material being assembled
#[derive(Debug)]
#[allow(dead_code)]
struct AssemblerState {
    material_id: Ulid,
    temp_path: PathBuf,
    temp_file: Option<File>,
    expected_offset: i64,
    slice_count: usize,
    out_of_order_buffer: BTreeMap<i64, Vec<u8>>,
    started_at: DateTime<Utc>,
    hasher: blake3::Hasher,
}

impl AssemblerState {
    fn new(material_id: Ulid, temp_path: PathBuf) -> Self {
        Self {
            material_id,
            temp_path,
            temp_file: None,
            expected_offset: 0,
            slice_count: 0,
            out_of_order_buffer: BTreeMap::new(),
            started_at: Utc::now(),
            hasher: blake3::Hasher::new(),
        }
    }

    /// Write slice data at given offset
    async fn write_slice(&mut self, offset: i64, data: Vec<u8>) -> IngestdResult<()> {
        if offset == self.expected_offset {
            // In-order slice - write immediately
            if let Some(ref mut file) = self.temp_file {
                file.write_all(&data).await?;
            }
            self.hasher.update(&data);
            self.expected_offset += data.len() as i64;
            self.slice_count += 1;

            // Check if we can flush any buffered slices
            self.flush_buffered_slices().await?;
        } else if offset > self.expected_offset {
            // Out-of-order slice - buffer it
            debug!(
                material_id = %self.material_id,
                offset,
                expected = self.expected_offset,
                "Buffering out-of-order slice"
            );
            self.out_of_order_buffer.insert(offset, data);
        } else {
            // Duplicate or overlapping slice - skip
            warn!(
                material_id = %self.material_id,
                offset,
                expected = self.expected_offset,
                "Skipping duplicate or overlapping slice"
            );
        }

        Ok(())
    }

    /// Flush any buffered slices that are now in order
    async fn flush_buffered_slices(&mut self) -> IngestdResult<()> {
        while let Some((&offset, _)) = self.out_of_order_buffer.first_key_value() {
            if offset == self.expected_offset {
                let data = self.out_of_order_buffer.remove(&offset).unwrap();
                if let Some(ref mut file) = self.temp_file {
                    file.write_all(&data).await?;
                }
                self.hasher.update(&data);
                self.expected_offset += data.len() as i64;
                self.slice_count += 1;
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Verify final hash matches expected
    fn verify_hash(&self, expected_hash: &str) -> bool {
        let actual_hash = self.hasher.finalize().to_hex();
        actual_hash.as_str() == expected_hash
    }
}

/// Material assembler service
pub struct MaterialAssembler {
    js: jetstream::Context,
    pool: DbPool,
    env: SinexEnvironment,
    assembler_state: Arc<RwLock<HashMap<Ulid, AssemblerState>>>,
    annex_path: PathBuf,
}

impl MaterialAssembler {
    pub fn new(nats_client: NatsClient, pool: DbPool, annex_path: PathBuf) -> Self {
        let js = jetstream::new(nats_client);
        let env = sinex_core::environment().clone();

        Self {
            js,
            pool,
            env,
            assembler_state: Arc::new(RwLock::new(HashMap::new())),
            annex_path,
        }
    }

    /// Bootstrap JetStream streams for materials
    async fn bootstrap_streams(&self) -> IngestdResult<()> {
        info!("Bootstrapping material streams");

        // source_material.begin stream
        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: self.env.nats_subject("source_material_begin"),
                subjects: vec![self.env.nats_subject("source_material.begin")],
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network(format!("Failed to create begin stream: {}", e)))?;

        // source_material.slices stream - operational buffer for material assembly
        // 7 days retention to allow for delayed assembly and replay
        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: self.env.nats_subject("source_material_slices"),
                subjects: vec![self.env.nats_subject("source_material.slices.>")],
                storage: jetstream::stream::StorageType::File,
                max_age: tokio::time::Duration::from_secs(7 * 24 * 60 * 60), // 7 days (operational buffer)
                max_message_size: 512 * 1024,                                // 512KB max slice size
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network(format!("Failed to create slices stream: {}", e)))?;

        // source_material.end stream
        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: self.env.nats_subject("source_material_end"),
                subjects: vec![self.env.nats_subject("source_material.end")],
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network(format!("Failed to create end stream: {}", e)))?;

        info!("Material streams bootstrapped successfully");
        Ok(())
    }

    pub async fn run(self) -> IngestdResult<()> {
        info!("Starting Material Assembler");

        // Bootstrap streams
        self.bootstrap_streams().await?;

        // Spawn consumers for each message type
        let begin_handle = self.spawn_begin_consumer();
        let slices_handle = self.spawn_slices_consumer();
        let end_handle = self.spawn_end_consumer();

        // Wait for all consumers
        tokio::select! {
            result = begin_handle => {
                error!("Begin consumer exited: {:?}", result);
            }
            result = slices_handle => {
                error!("Slices consumer exited: {:?}", result);
            }
            result = end_handle => {
                error!("End consumer exited: {:?}", result);
            }
        }

        Ok(())
    }

    /// Spawn consumer for source_material.begin messages
    fn spawn_begin_consumer(&self) -> tokio::task::JoinHandle<IngestdResult<()>> {
        let js = self.js.clone();
        let env = self.env.clone();
        let state = self.assembler_state.clone();

        tokio::spawn(async move {
            let stream_name = env.nats_subject("source_material_begin");
            let stream = js
                .get_stream(&stream_name)
                .await
                .map_err(|e| SinexError::network(format!("Failed to get begin stream: {}", e)))?;

            let consumer = stream
                .get_or_create_consumer(
                    "ingestd_material_begin",
                    jetstream::consumer::pull::Config {
                        durable_name: Some("ingestd_material_begin".to_string()),
                        ack_policy: jetstream::consumer::AckPolicy::Explicit,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| SinexError::network(format!("Failed to create consumer: {}", e)))?;

            loop {
                let mut messages = consumer
                    .batch()
                    .max_messages(10)
                    .messages()
                    .await
                    .map_err(|e| SinexError::network(format!("Failed to fetch messages: {}", e)))?;

                while let Some(msg) = messages.next().await {
                    let msg = match msg {
                        Ok(m) => m,
                        Err(e) => {
                            error!("Error receiving begin message: {}", e);
                            continue;
                        }
                    };

                    match serde_json::from_slice::<MaterialBeginMessage>(&msg.payload) {
                        Ok(begin_msg) => {
                            let material_id: Ulid = begin_msg.material_id.parse().unwrap();

                            // Create temp file
                            let temp_dir = std::env::temp_dir();
                            let temp_path =
                                temp_dir.join(format!("sinex_assemble_{}.tmp", material_id));

                            let mut assembler_state =
                                AssemblerState::new(material_id, temp_path.clone());
                            assembler_state.temp_file =
                                Some(File::create(&temp_path).await.map_err(|e| {
                                    SinexError::io(format!("Failed to create temp file: {}", e))
                                })?);

                            state.write().await.insert(material_id, assembler_state);

                            info!(material_id = %material_id, "Initialized material assembler");
                            msg.ack().await.map_err(|e| {
                                SinexError::network(format!("Failed to ack: {}", e))
                            })?;
                        }
                        Err(e) => {
                            error!("Failed to parse begin message: {}", e);
                            msg.ack().await.map_err(|e| {
                                SinexError::network(format!("Failed to ack: {}", e))
                            })?;
                        }
                    }
                }
            }
        })
    }

    /// Spawn consumer for source_material.slices.* messages
    fn spawn_slices_consumer(&self) -> tokio::task::JoinHandle<IngestdResult<()>> {
        let js = self.js.clone();
        let env = self.env.clone();
        let state = self.assembler_state.clone();

        tokio::spawn(async move {
            let stream_name = env.nats_subject("source_material_slices");
            let stream = js
                .get_stream(&stream_name)
                .await
                .map_err(|e| SinexError::network(format!("Failed to get slices stream: {}", e)))?;

            let consumer = stream
                .get_or_create_consumer(
                    "ingestd_material_slices",
                    jetstream::consumer::pull::Config {
                        durable_name: Some("ingestd_material_slices".to_string()),
                        ack_policy: jetstream::consumer::AckPolicy::Explicit,
                        max_ack_pending: 1000,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| SinexError::network(format!("Failed to create consumer: {}", e)))?;

            loop {
                let mut messages = consumer
                    .batch()
                    .max_messages(100)
                    .messages()
                    .await
                    .map_err(|e| SinexError::network(format!("Failed to fetch messages: {}", e)))?;

                while let Some(msg) = messages.next().await {
                    let msg = match msg {
                        Ok(m) => m,
                        Err(e) => {
                            error!("Error receiving slice message: {}", e);
                            continue;
                        }
                    };

                    // Extract headers
                    let headers = msg.headers.as_ref();
                    let offset: i64 = headers
                        .and_then(|h| h.get("Offset"))
                        .and_then(|v| v.as_str().parse().ok())
                        .unwrap_or(0);

                    // Extract material_id from subject
                    let subject = msg.subject.to_string();
                    let material_id_str = subject.split('.').last().unwrap_or("");
                    let material_id: Ulid = match material_id_str.parse() {
                        Ok(id) => id,
                        Err(_) => {
                            error!("Invalid material_id in subject: {}", subject);
                            msg.ack().await.ok();
                            continue;
                        }
                    };

                    // Write slice to assembler
                    let mut state_guard = state.write().await;
                    if let Some(assembler_state) = state_guard.get_mut(&material_id) {
                        if let Err(e) = assembler_state
                            .write_slice(offset, msg.payload.to_vec())
                            .await
                        {
                            error!(material_id = %material_id, "Failed to write slice: {}", e);
                        }
                    } else {
                        warn!(material_id = %material_id, "No assembler state for material");
                    }

                    msg.ack().await.ok();
                }
            }
        })
    }

    /// Spawn consumer for source_material.end messages
    fn spawn_end_consumer(&self) -> tokio::task::JoinHandle<IngestdResult<()>> {
        let js = self.js.clone();
        let env = self.env.clone();
        let state = self.assembler_state.clone();
        let pool = self.pool.clone();
        let _annex_path = self.annex_path.clone();

        tokio::spawn(async move {
            let stream_name = env.nats_subject("source_material_end");
            let stream = js
                .get_stream(&stream_name)
                .await
                .map_err(|e| SinexError::network(format!("Failed to get end stream: {}", e)))?;

            let consumer = stream
                .get_or_create_consumer(
                    "ingestd_material_end",
                    jetstream::consumer::pull::Config {
                        durable_name: Some("ingestd_material_end".to_string()),
                        ack_policy: jetstream::consumer::AckPolicy::Explicit,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| SinexError::network(format!("Failed to create consumer: {}", e)))?;

            loop {
                let mut messages = consumer
                    .batch()
                    .max_messages(10)
                    .messages()
                    .await
                    .map_err(|e| SinexError::network(format!("Failed to fetch messages: {}", e)))?;

                while let Some(msg) = messages.next().await {
                    let msg = match msg {
                        Ok(m) => m,
                        Err(e) => {
                            error!("Error receiving end message: {}", e);
                            continue;
                        }
                    };

                    match serde_json::from_slice::<MaterialEndMessage>(&msg.payload) {
                        Ok(end_msg) => {
                            let material_id: Ulid = end_msg.material_id.parse().unwrap();

                            // Finalize material
                            let mut state_guard = state.write().await;
                            if let Some(mut assembler_state) = state_guard.remove(&material_id) {
                                // Flush and close temp file
                                if let Some(mut file) = assembler_state.temp_file.take() {
                                    file.flush().await.ok();
                                }

                                // Verify hash
                                if !assembler_state.verify_hash(&end_msg.content_hash) {
                                    error!(
                                        material_id = %material_id,
                                        "Hash mismatch! Material corrupted"
                                    );
                                    // TODO: Route to DLQ
                                    msg.ack().await.ok();
                                    continue;
                                }

                                // Move to git-annex (placeholder - would need actual annex integration)
                                info!(
                                    material_id = %material_id,
                                    slices = assembler_state.slice_count,
                                    bytes = assembler_state.expected_offset,
                                    "Material assembly complete"
                                );

                                // Update database - mark as completed
                                let repo = pool.source_materials();
                                let id: Id<SourceMaterialRecord> = Id::from_ulid(material_id);
                                if let Err(e) = repo
                                    .finalize_in_flight(
                                        id,
                                        None,
                                        None,
                                        None,
                                        Some(assembler_state.expected_offset),
                                    )
                                    .await
                                {
                                    error!(material_id = %material_id, "Failed to finalize: {}", e);
                                }

                                // Clean up temp file
                                if let Err(e) =
                                    tokio::fs::remove_file(&assembler_state.temp_path).await
                                {
                                    warn!("Failed to remove temp file: {}", e);
                                }

                                msg.ack().await.ok();
                            } else {
                                warn!(material_id = %material_id, "No assembler state for material end");
                                msg.ack().await.ok();
                            }
                        }
                        Err(e) => {
                            error!("Failed to parse end message: {}", e);
                            msg.ack().await.ok();
                        }
                    }
                }
            }
        })
    }
}
