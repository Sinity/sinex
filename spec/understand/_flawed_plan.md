Write an explanation for the agent who wrote this, what they misunderstand
---------------------


# Comprehensive Implementation Plan: Sinex Missing Features

After deep analysis and iterative thinking, here's a meticulous implementation plan for the missing features in the Sinex architecture.

## 1. Enable Ingestor Scanning of Historical Blobs

### Core Architecture Changes

#### A. Extend StreamProcessorContext with Blob Access

```rust
// In sinex-satellite-sdk/src/stream_processor.rs
impl StreamProcessorContext {
    /// Read blob content from source_material_registry
    pub async fn read_blob(&self, blob_id: Ulid) -> SatelliteResult<Vec<u8>> {
        // Query source_material_registry for checksum
        let row = sqlx::query!(
            "SELECT checksum FROM raw.source_material_registry WHERE blob_id = $1",
            blob_id.to_uuid()
        )
        .fetch_one(&self.db_pool)
        .await?;

        // Use git-annex via command to retrieve content
        let annex_path = std::env::var("SINEX_ANNEX_PATH")
            .unwrap_or_else(|_| "/realm/data/sinex-annex/sinex-blobs".to_string());

        // Find the blob file by checksum
        let output = std::process::Command::new("git-annex")
            .args(&["find", "--include", &format!("*{}*", row.checksum)])
            .current_dir(&annex_path)
            .output()?;

        let blob_path = String::from_utf8(output.stdout)?
            .lines()
            .next()
            .ok_or_else(|| SatelliteError::General(anyhow!("Blob not found in annex")))?;

        // Ensure content is available locally
        std::process::Command::new("git-annex")
            .args(&["get", &blob_path])
            .current_dir(&annex_path)
            .status()?;

        // Read the content
        let full_path = PathBuf::from(&annex_path).join(&blob_path);
        Ok(tokio::fs::read(full_path).await?)
    }

    /// Read blob content in chunks for large files
    pub async fn read_blob_chunked(
        &self,
        blob_id: Ulid,
        chunk_size: usize,
        mut callback: impl FnMut(&[u8]) -> SatelliteResult<()>
    ) -> SatelliteResult<()> {
        // Similar to above but read in chunks
        // This prevents OOM for large blobs
    }
}
```

#### B. Modify Satellite Scan Methods

```rust
// In each satellite's unified_processor.rs
impl StatefulStreamProcessor for FsWatcher {
    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        // Check if this is a blob scan
        if let Some(blob_id_str) = args.config.get("blob_id").and_then(|v| v.as_str()) {
            let blob_id = Ulid::from_str(blob_id_str)?;
            return self.scan_blob(blob_id, args).await;
        }

        // Regular filesystem scan
        self.scan_filesystem(from, until, args).await
    }

    async fn scan_blob(&mut self, blob_id: Ulid, args: ScanArgs) -> SatelliteResult<ScanReport> {
        info!("Scanning historical blob: {}", blob_id);
        let start = Instant::now();
        let mut events_emitted = 0;

        // Read blob metadata
        let blob_info = sqlx::query!(
            "SELECT source_identifier, source_material_format FROM raw.source_material_registry WHERE blob_id = $1",
            blob_id.to_uuid()
        )
        .fetch_one(&self.context.as_ref().unwrap().db_pool)
        .await?;

        // Process based on format
        match blob_info.source_material_format.as_str() {
            "tar" | "tar.gz" => {
                // Extract and process tar archive
                self.process_tar_blob(blob_id).await?
            }
            "log" | "txt" => {
                // Process as line-delimited log
                self.process_log_blob(blob_id).await?
            }
            _ => {
                // Process as single file
                self.process_single_file_blob(blob_id).await?
            }
        }

        Ok(ScanReport {
            events_processed: events_emitted,
            duration: start.elapsed(),
            final_checkpoint: Checkpoint::None,
            time_range: None,
            processor_stats: HashMap::new(),
            successful_targets: vec![blob_id.to_string()],
            failed_targets: HashMap::new(),
            warnings: vec![],
        })
    }
}
```

#### C. Gateway RPC Method for Triggering Scans

```rust
// In sinex-gateway/src/handlers/coordinator.rs
pub async fn trigger_ingestor_scan(
    services: &ServiceContainer,
    params: Value,
) -> Result<Value> {
    let ingestor = params.get("ingestor")
        .and_then(|v| v.as_str())
        .ok_or("Missing ingestor parameter")?;
    let blob_id = params.get("blob_id")
        .and_then(|v| v.as_str())
        .ok_or("Missing blob_id parameter")?;
    let operation_id = Ulid::new();

    // Log operation start
    sqlx::query!(
        "SELECT core.start_operation($1, $2, $3::jsonb)",
        operation_id.to_uuid(),
        "coordinator.scan",
        json!({
            "ingestor": ingestor,
            "blob_id": blob_id,
            "triggered_by": "exo-replay"
        })
    )
    .execute(&services.db_pool)
    .await?;

    // Launch satellite process with systemd
    let service_name = format!("sinex-{}-scan@{}.service", ingestor, blob_id);
    let output = Command::new("systemctl")
        .args(&["--user", "start", &service_name])
        .output()
        .await?;

    if !output.status.success() {
        return Err(anyhow!("Failed to start scan service: {}",
            String::from_utf8_lossy(&output.stderr)));
    }

    Ok(json!({
        "operation_id": operation_id.to_string(),
        "service": service_name,
        "status": "started"
    }))
}
```

#### D. Systemd Template Unit

```ini
# /etc/systemd/user/sinex-fs-watcher-scan@.service
[Unit]
Description=Sinex FS Watcher Scan for blob %i
After=sinex-ingestd.service

[Service]
Type=oneshot
Environment="DATABASE_URL=postgresql:///sinex"
Environment="SINEX_ANNEX_PATH=/realm/data/sinex-annex/sinex-blobs"
ExecStart=/usr/bin/sinex-fs-watcher \
    --service-name "fs-watcher-scan-%i" \
    scan \
    --from none \
    --until snapshot \
    --targets '{"blob_id": "%i"}'
```

#### E. Modified exo replay Command

```python
# In cli/exo.py
def replay_ingestor(ctx, ingestor: str, blob_id: str, dry_run: bool, force: bool):
    """Replay an ingestor on a historical blob."""

    # Step 1: Validate blob exists
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT blob_id, source_identifier, user_comment,
                       staged_at, processing_status
                FROM raw.source_material_registry
                WHERE blob_id = %s::uuid
            """, (blob_id,))
            blob_info = cur.fetchone()

            if not blob_info:
                console.print(f"[red]Error: Blob {blob_id} not found[/red]")
                return

            if blob_info['processing_status'] != 'staged':
                console.print(f"[yellow]Warning: Blob status is '{blob_info['processing_status']}'[/yellow]")

    # Step 2: Find existing events from this blob
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) as count,
                       MIN(ts_orig) as first_event,
                       MAX(ts_orig) as last_event
                FROM core.events
                WHERE source_material_id = %s::uuid
            """, (blob_id,))
            existing = cur.fetchone()

    # Step 3: Display impact analysis
    console.print(f"\n[bold]Blob Replay Impact Analysis[/bold]")
    console.print(f"Blob ID: [yellow]{blob_id}[/yellow]")
    console.print(f"Source: [cyan]{blob_info['source_identifier']}[/cyan]")
    console.print(f"Comment: {blob_info['user_comment'] or 'None'}")
    console.print(f"Staged: {blob_info['staged_at']}")
    console.print(f"\nExisting events: [red]{existing['count']}[/red]")
    if existing['count'] > 0:
        console.print(f"Time range: {existing['first_event']} to {existing['last_event']}")

    if dry_run:
        console.print("\n[yellow]DRY RUN - no changes will be made[/yellow]")
        return

    # Step 4: Confirmation
    if not force and existing['count'] > 0:
        if not click.confirm(f"\nArchive {existing['count']} existing events and re-scan?"):
            console.print("[yellow]Replay cancelled[/yellow]")
            return

    # Step 5: Archive existing events
    if existing['count'] > 0:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("BEGIN")
                try:
                    # Set archive metadata
                    cur.execute("""
                        SELECT raw.set_archive_metadata(%s, %s, NULL)
                    """, ['exo-replay-ingestor', f'Replay {ingestor} on blob {blob_id}'])

                    # Archive events with this source_material_id
                    cur.execute("""
                        DELETE FROM core.events
                        WHERE source_material_id = %s::uuid
                    """, (blob_id,))
                    archived_count = cur.rowcount

                    cur.execute("COMMIT")
                    console.print(f"Archived [red]{archived_count}[/red] events")
                except Exception as e:
                    cur.execute("ROLLBACK")
                    raise

    # Step 6: Trigger scan via gateway
    try:
        response = rpc_call("coordinator.trigger_ingestor_scan", {
            "ingestor": ingestor,
            "blob_id": blob_id
        })

        operation_id = response.get('operation_id')
        console.print(f"\n[green]✓ Scan started[/green]")
        console.print(f"Operation ID: {operation_id}")
        console.print(f"Service: {response.get('service')}")

        # Step 7: Monitor completion (optional)
        if not ctx.obj.get('async_mode'):
            console.print("\nMonitoring scan progress...")
            monitor_scan_completion(operation_id)

    except SinexRPCError as e:
        console.print(f"[red]Failed to trigger scan: {e}[/red]")
        return
```

## 2. Implement Stage-as-you-go Pattern with Crash Resilience

### A. Add Blob Management to StreamProcessorContext

```rust
// In sinex-satellite-sdk/src/stream_processor.rs
pub struct StreamProcessorContext {
    // ... existing fields ...

    /// Current in-flight blob for stage-as-you-go
    pub current_blob_id: Option<Ulid>,

    /// Temporary file for buffering raw data
    pub current_blob_file: Option<tokio::fs::File>,

    /// Current offset in blob file
    pub current_blob_offset: u64,

    /// Blob rotation interval
    pub blob_rotation_interval: Duration,

    /// Last blob rotation time
    pub last_blob_rotation: Instant,
}

impl StreamProcessorContext {
    /// Create new in-flight source material record
    pub async fn start_new_blob(&mut self) -> SatelliteResult<()> {
        let blob_id = Ulid::new();
        let batch_id = Ulid::new();
        let source_id = format!("{}-realtime-{}",
            self.service_name,
            chrono::Utc::now().format("%Y%m%d-%H%M%S")
        );

        // Create in-flight record in database
        sqlx::query!(
            r#"INSERT INTO raw.source_material_registry
               (blob_id, checksum, stage_batch_id, source_identifier,
                staged_on_host, staged_by_user, processing_status,
                timing_info_type, source_material_format)
               VALUES ($1, 'PENDING', $2, $3, $4, $5, 'sensing', 'intrinsic', 'raw')"#,
            blob_id.to_uuid(),
            batch_id.to_uuid(),
            source_id,
            self.host,
            "sinex-system",
        )
        .execute(&self.db_pool)
        .await?;

        // Create temporary file
        let temp_path = self.work_dir.join(format!("blob_{}.tmp", blob_id));
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&temp_path)
            .await?;

        self.current_blob_id = Some(blob_id);
        self.current_blob_file = Some(file);
        self.current_blob_offset = 0;
        self.last_blob_rotation = Instant::now();

        info!("Started new in-flight blob: {}", blob_id);
        Ok(())
    }

    /// Finalize current blob and add to git-annex
    pub async fn finalize_current_blob(&mut self) -> SatelliteResult<()> {
        if let (Some(blob_id), Some(mut file)) =
            (self.current_blob_id.take(), self.current_blob_file.take()) {

            // Flush and sync file
            file.sync_all().await?;
            drop(file);

            let temp_path = self.work_dir.join(format!("blob_{}.tmp", blob_id));

            // Calculate checksum
            let checksum = calculate_blake3(&temp_path).await?;

            // Add to git-annex
            let annex_path = std::env::var("SINEX_ANNEX_PATH")
                .unwrap_or_else(|_| "/realm/data/sinex-annex/sinex-blobs".to_string());

            let final_name = format!("realtime_{}_{}.blob",
                self.service_name,
                blob_id
            );
            let final_path = PathBuf::from(&annex_path).join(&final_name);

            tokio::fs::rename(&temp_path, &final_path).await?;

            // Add to git-annex
            let output = Command::new("git-annex")
                .args(&["add", &final_name])
                .current_dir(&annex_path)
                .output()
                .await?;

            if !output.status.success() {
                return Err(SatelliteError::General(anyhow!(
                    "Failed to add blob to git-annex: {}",
                    String::from_utf8_lossy(&output.stderr)
                )));
            }

            // Update source_material_registry
            let file_size = tokio::fs::metadata(&final_path).await?.len() as i64;

            sqlx::query!(
                r#"UPDATE raw.source_material_registry
                   SET checksum = $2,
                       processing_status = 'completed',
                       source_size = $3,
                       end_time = NOW()
                   WHERE blob_id = $1"#,
                blob_id.to_uuid(),
                checksum,
                file_size
            )
            .execute(&self.db_pool)
            .await?;

            // Commit to git
            Command::new("git")
                .args(&["commit", "-m", &format!("Auto-stage realtime blob {}", blob_id)])
                .current_dir(&annex_path)
                .status()
                .await?;

            info!("Finalized blob {} with checksum {}", blob_id, checksum);
        }

        Ok(())
    }

    /// Emit event with automatic source material tracking
    pub async fn emit_event_with_provenance(
        &mut self,
        mut event: RawEvent,
        raw_data: Option<&[u8]>
    ) -> SatelliteResult<()> {
        // Ensure we have an active blob
        if self.current_blob_id.is_none() {
            self.start_new_blob().await?;
        }

        // Check if we need to rotate
        if self.last_blob_rotation.elapsed() > self.blob_rotation_interval {
            self.finalize_current_blob().await?;
            self.start_new_blob().await?;
        }

        // Set source material fields
        if let Some(blob_id) = self.current_blob_id {
            event.source_material_id = Some(blob_id);
            event.anchor_byte = Some(self.current_blob_offset as i64);

            // Write raw data to blob file if provided
            if let Some(data) = raw_data {
                if let Some(ref mut file) = self.current_blob_file {
                    file.write_all(data).await?;
                    file.write_all(b"\n").await?; // Add delimiter

                    event.source_material_offset_start = Some(self.current_blob_offset as i64);
                    self.current_blob_offset += data.len() as u64 + 1;
                    event.source_material_offset_end = Some(self.current_blob_offset as i64);
                }
            }
        }

        // Send event through normal channel
        self.emit_event(event).await
    }
}
```

### B. Modify Service Runner to Handle Blob Lifecycle

```rust
// In sinex-satellite-sdk/src/stream_processor.rs
impl<T: StatefulStreamProcessor + 'static> StreamProcessorRunner<T> {
    pub async fn run_service(&mut self) -> SatelliteResult<()> {
        // ... existing startup code ...

        // Start initial blob for stage-as-you-go
        if self.processor.capabilities().supports_stage_as_you_go {
            self.context.as_mut().unwrap().start_new_blob().await?;

            // Start blob rotation task
            let ctx = self.context.as_ref().unwrap();
            let event_buffer = ctx.event_buffer.clone();
            let db_pool = ctx.db_pool.clone();
            let service_name = ctx.service_name.clone();

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(300)); // 5 minutes
                loop {
                    interval.tick().await;
                    // Trigger blob rotation
                    // This would send a signal to the main processing loop
                }
            });
        }

        // ... rest of service logic ...

        // On shutdown, finalize current blob
        if self.processor.capabilities().supports_stage_as_you_go {
            self.context.as_mut().unwrap().finalize_current_blob().await?;
        }

        Ok(())
    }
}
```

## 3. PKM Markdown Decomposer with Event Triggering

### A. Emit Event from exo blob stage

```python
# In cli/exo.py - modify blob_stage function
def blob_stage(file_path: str, source_id: str, comment: Optional[str], tags: Optional[str], annex_repo: str):
    # ... existing staging logic ...

    # After successful staging, emit an event
    if operation_id:
        # Create staging event to trigger automata
        staging_event = {
            "source": "exo.blob.stage",
            "event_type": "source_material.staged",
            "host": hostname,
            "payload": {
                "blob_id": str(blob_id),
                "source_identifier": source_id,
                "checksum": blake3_hash,
                "size_bytes": source_size,
                "mime_type": detect_mime_type(file_path),
                "user_tags": parsed_tags,
                "stage_batch_id": str(stage_batch_id)
            }
        }

        # Submit via direct DB insert (since exo doesn't use ingestd)
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO core.events (
                        event_id, source, event_type, host, ts_orig,
                        payload, source_material_id
                    ) VALUES (
                        gen_ulid(), %s, %s, %s, NOW(), %s::jsonb, %s
                    )
                """, (
                    staging_event["source"],
                    staging_event["event_type"],
                    staging_event["host"],
                    json.dumps(staging_event["payload"]),
                    blob_id  # Link event to the blob it announces
                ))

                # Also publish to Redis for automata
                redis_client = redis.Redis.from_url(os.environ.get('SINEX_REDIS_URL', 'redis://localhost:6379'))
                redis_client.xadd(
                    'sinex:streams:hotlog',
                    {
                        'event_type': staging_event["event_type"],
                        'source': staging_event["source"],
                        'data': json.dumps(staging_event)
                    }
                )
```

### B. Create PKM Markdown Decomposer Automaton

```rust
// New file: crate/sinex-pkm-markdown-automaton/src/lib.rs
use async_trait::async_trait;
use serde_json::{json, Value};
use sinex_satellite_sdk::{
    EventFilter, HotlogAutomaton, HotlogAutomatonContext, HotlogAutomatonEvent,
    ProcessingResult, SatelliteError, SatelliteResult,
};
use sinex_events::RawEvent;
use sinex_ulid::Ulid;
use std::str::FromStr;
use tracing::{debug, info, warn};
use pulldown_cmark::{Parser, Event, Tag};
use regex::Regex;

pub struct PkmMarkdownDecomposer {
    context: Option<HotlogAutomatonContext>,
    entity_pattern: Regex,
    relation_pattern: Regex,
}

impl PkmMarkdownDecomposer {
    pub fn new() -> Self {
        Self {
            context: None,
            // [[Entity:Type]] pattern
            entity_pattern: Regex::new(r"\[\[([^:]+):([^\]]+)\]\]").unwrap(),
            // [[Entity1->relates_to->Entity2]] pattern
            relation_pattern: Regex::new(r"\[\[([^-]+)->([^-]+)->([^\]]+)\]\]").unwrap(),
        }
    }

    async fn decompose_markdown(&self, content: &str, blob_id: Ulid) -> SatelliteResult<()> {
        let ctx = self.context.as_ref().unwrap();
        let parser = Parser::new(content);
        let mut current_section = String::new();
        let mut prose_blocks = Vec::new();
        let mut current_prose = String::new();

        for event in parser {
            match event {
                Event::Start(Tag::Heading(level)) => {
                    // Save current prose block if any
                    if !current_prose.trim().is_empty() {
                        prose_blocks.push((current_section.clone(), current_prose.clone()));
                        current_prose.clear();
                    }
                }
                Event::Text(text) => {
                    // Extract entities from text
                    for cap in self.entity_pattern.captures_iter(&text) {
                        let entity_name = &cap[1];
                        let entity_type = &cap[2];

                        let event = RawEvent::new(
                            "pkm.markdown.decomposer",
                            "pkm.entity.discovered",
                            json!({
                                "name": entity_name,
                                "type": entity_type,
                                "context": current_section,
                                "source_material_id": blob_id,
                            })
                        );

                        ctx.emit_synthesis_event(event).await?;
                    }

                    // Extract relations
                    for cap in self.relation_pattern.captures_iter(&text) {
                        let from_entity = &cap[1];
                        let relation_type = &cap[2];
                        let to_entity = &cap[3];

                        let event = RawEvent::new(
                            "pkm.markdown.decomposer",
                            "pkm.relation.discovered",
                            json!({
                                "from": from_entity,
                                "relation": relation_type,
                                "to": to_entity,
                                "context": current_section,
                                "source_material_id": blob_id,
                            })
                        );

                        ctx.emit_synthesis_event(event).await?;
                    }

                    current_prose.push_str(&text);
                }
                Event::Code(code) => {
                    // Emit code blocks as separate entities
                    let event = RawEvent::new(
                        "pkm.markdown.decomposer",
                        "pkm.code.block",
                        json!({
                            "code": code.to_string(),
                            "context": current_section,
                            "source_material_id": blob_id,
                        })
                    );

                    ctx.emit_synthesis_event(event).await?;
                }
                _ => {}
            }
        }

        // Emit prose blocks
        for (section, prose) in prose_blocks {
            let event = RawEvent::new(
                "pkm.markdown.decomposer",
                "pkm.prose.block",
                json!({
                    "section": section,
                    "content": prose,
                    "source_material_id": blob_id,
                })
            );

            ctx.emit_synthesis_event(event).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl HotlogAutomaton for PkmMarkdownDecomposer {
    fn automaton_name(&self) -> &str {
        "pkm-markdown-decomposer"
    }

    async fn initialize(&mut self, ctx: HotlogAutomatonContext) -> SatelliteResult<()> {
        info!("Initializing PKM markdown decomposer");
        self.context = Some(ctx);
        Ok(())
    }

    fn event_filters(&self) -> Vec<EventFilter> {
        vec![
            EventFilter::new(
                Some("exo.blob.stage".to_string()),
                Some("source_material.staged".to_string())
            )
        ]
    }

    async fn process_event(
        &mut self,
        event: HotlogAutomatonEvent,
    ) -> SatelliteResult<ProcessingResult> {
        let source_id = event.event.payload
            .get("source_identifier")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Only process if it's marked as PKM markdown
        if !source_id.contains("pkm") && !source_id.contains("markdown") {
            return Ok(ProcessingResult::Skip {
                reason: "Not PKM markdown content".to_string()
            });
        }

        let blob_id_str = event.event.payload
            .get("blob_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SatelliteError::General(anyhow!("Missing blob_id")))?;

        let blob_id = Ulid::from_str(blob_id_str)?;

        info!("Processing PKM markdown blob: {}", blob_id);

        // Read blob content
        let ctx = self.context.as_ref().unwrap();
        let content = ctx.read_blob(blob_id).await?;
        let markdown = String::from_utf8(content)
            .map_err(|e| SatelliteError::General(anyhow!("Invalid UTF-8: {}", e)))?;

        // Decompose and emit events
        self.decompose_markdown(&markdown, blob_id).await?;

        Ok(ProcessingResult::Success { checkpoint_data: None })
    }
}
```

## 4. Development/Production Environment Isolation

### A. Create Environment Configuration

```rust
// New file: crate/sinex-config/src/environment.rs
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinexEnvironment {
    Development,
    Production,
}

impl SinexEnvironment {
    pub fn from_env() -> Self {
        match std::env::var("SINEX_ENVIRONMENT").as_deref() {
            Ok("production") | Ok("prod") => Self::Production,
            _ => Self::Development,
        }
    }

    pub fn socket_path(&self) -> &'static str {
        match self {
            Self::Development => "/tmp/sinex-dev/ingest.sock",
            Self::Production => "/run/sinex/ingest.sock",
        }
    }

    pub fn redis_stream_prefix(&self) -> &'static str {
        match self {
            Self::Development => "sinex-dev",
            Self::Production => "sinex",
        }
    }

    pub fn database_name(&self) -> &'static str {
        match self {
            Self::Development => "sinex_dev",
            Self::Production => "sinex",
        }
    }

    pub fn annex_path(&self) -> &'static str {
        match self {
            Self::Development => "/tmp/sinex-dev/annex",
            Self::Production => "/realm/data/sinex-annex/sinex-blobs",
        }
    }

    pub fn work_dir(&self) -> &'static str {
        match self {
            Self::Development => "/tmp/sinex-dev/work",
            Self::Production => "/var/lib/sinex",
        }
    }
}
```

### B. Update Configuration Loading

```rust
// In sinex-satellite-sdk/src/config.rs
impl SatelliteConfig {
    pub fn load_from_env(service_name: &str) -> Self {
        let env = SinexEnvironment::from_env();

        Self {
            service_name: service_name.to_string(),
            log_level: std::env::var("SINEX_LOG_LEVEL")
                .unwrap_or_else(|_| default_log_level()),
            ingest_socket_path: std::env::var("SINEX_INGEST_SOCKET")
                .unwrap_or_else(|_| env.socket_path().to_string()),
            redis_url: std::env::var("SINEX_REDIS_URL")
                .unwrap_or_else(|_| format!("redis://localhost:6379/{}",
                    if env == SinexEnvironment::Development { "1" } else { "0" }
                )),
            database_url: std::env::var("DATABASE_URL").ok(),
            // ... other fields with environment-specific defaults
        }
    }
}
```

### C. Update Redis Stream Names

```rust
// In sinex-ingestd/src/service.rs
async fn batch_publish_to_redis(
    client: &RedisClient,
    config: &IngestdConfig,
    events: &[RawEvent],
) -> IngestdResult<()> {
    let env = SinexEnvironment::from_env();
    let stream_name = format!("{}:streams:hotlog", env.redis_stream_prefix());

    // ... rest of implementation using environment-specific stream name
}
```

## 5. Comprehensive Testing Strategy

### A. Unit Tests for Blob Access

```rust
// In sinex-satellite-sdk/src/stream_processor.rs
#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::TestContext;

    #[sinex_test]
    async fn test_blob_reading(ctx: TestContext) {
        // Stage a test blob
        let blob_content = b"test log line 1\ntest log line 2\n";
        let blob_id = ctx.stage_test_blob(blob_content, "test-logs").await?;

        // Create processor context
        let processor_ctx = StreamProcessorContext::test_context(ctx.db_pool.clone());

        // Test reading
        let content = processor_ctx.read_blob(blob_id).await?;
        assert_eq!(content, blob_content);
    }

    #[sinex_test]
    async fn test_chunked_blob_reading(ctx: TestContext) {
        // Create large test blob
        let mut large_content = Vec::new();
        for i in 0..10000 {
            large_content.extend_from_slice(format!("Line {}\n", i).as_bytes());
        }

        let blob_id = ctx.stage_test_blob(&large_content, "large-test").await?;

        // Test chunked reading
        let processor_ctx = StreamProcessorContext::test_context(ctx.db_pool.clone());
        let mut total_read = 0;

        processor_ctx.read_blob_chunked(blob_id, 8192, |chunk| {
            total_read += chunk.len();
            Ok(())
        }).await?;

        assert_eq!(total_read, large_content.len());
    }
}
```

### B. Integration Tests for Blob Scanning

```rust
// In test/integration/blob_scanning.rs
#[sinex_test]
async fn test_fs_watcher_blob_scan(ctx: TestContext) {
    // Create test log content
    let log_content = r#"
2024-01-15 10:00:00 INFO Starting application
2024-01-15 10:00:01 ERROR Failed to connect to database
2024-01-15 10:00:02 INFO Retrying connection
"#;

    // Stage blob
    let blob_id = ctx.stage_test_blob(log_content.as_bytes(), "test-logs").await?;

    // Create fs-watcher
    let mut fs_watcher = FsWatcher::new();
    fs_watcher.initialize(ctx.create_processor_context("fs-watcher-test")).await?;

    // Scan the blob
    let args = ScanArgs {
        config: HashMap::from([
            ("blob_id".to_string(), json!(blob_id.to_string()))
        ]),
        ..Default::default()
    };

    let report = fs_watcher.scan(
        Checkpoint::None,
        TimeHorizon::Snapshot,
        args
    ).await?;

    // Verify events were created
    assert_eq!(report.events_processed, 3);

    // Check events in database
    let events = ctx.get_events_for_blob(blob_id).await?;
    assert_eq!(events.len(), 3);

    // Verify source material linkage
    for event in &events {
        assert_eq!(event.source_material_id, Some(blob_id));
        assert!(event.anchor_byte.is_some());
        assert!(event.source_material_offset_start.is_some());
        assert!(event.source_material_offset_end.is_some());
    }
}
```

### C. End-to-End Replay Test

```rust
#[sinex_test]
async fn test_replay_workflow(ctx: TestContext) {
    // Stage a blob
    let content = "test content for replay";
    let blob_id = ctx.stage_blob_via_cli(content, "test-replay").await?;

    // Run initial scan
    ctx.run_exo_command(&["replay", "--ingestor", "fs-watcher", "--blob", &blob_id.to_string()])
        .await?;

    // Get initial events
    let initial_events = ctx.get_events_for_blob(blob_id).await?;
    let initial_count = initial_events.len();

    // Run replay
    ctx.run_exo_command(&["replay", "--ingestor", "fs-watcher", "--blob", &blob_id.to_string(), "--force"])
        .await?;

    // Verify old events were archived
    let archived = ctx.get_archived_events_for_blob(blob_id).await?;
    assert_eq!(archived.len(), initial_count);

    // Verify new events were created
    let new_events = ctx.get_events_for_blob(blob_id).await?;
    assert_eq!(new_events.len(), initial_count);

    // Verify events have same source material but different IDs
    for (old, new) in initial_events.iter().zip(new_events.iter()) {
        assert_ne!(old.event_id, new.event_id);
        assert_eq!(old.source_material_id, new.source_material_id);
        assert_eq!(old.anchor_byte, new.anchor_byte);
    }
}
```
