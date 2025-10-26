# Future Event Pipeline Design

> **Operational note (2025-10-23)**  
> JetStream ingestion is canonical (`docs/way.md`). Any sensd/gRPC references here are historical context.


*Created: 2025-01-21*

This document preserves the valuable concepts from the pipeline.rs implementation and outlines how a future event processing pipeline could enhance Sinex after the core provenance infrastructure is complete.

> **Historical notice (2025-07-24)**  
> Examples reference a Redis-backed transport layer; adapt them to JetStream when revisiting the pipeline work.

## Overview

The event pipeline pattern provides a composable, stage-based approach to event processing. While not immediately necessary, it could become valuable for:

1. **Post-provenance processing** in ingestd
2. **Complex event synthesis** in automata
3. **Declarative transformations** (SQL-as-Automaton)

## Core Concepts

### Pipeline Architecture

```rust
pub struct EventPipeline {
    stages: Vec<Box<dyn PipelineStage>>,
    context: PipelineContext,
    metrics: Arc<PipelineMetrics>,
}

pub struct PipelineContext {
    pub db_pool: DbPool,
    pub redis: Option<RedisClient>,
    pub metadata: HashMap<String, Value>,
    pub checkpoint: Option<Checkpoint>,
}
```

### Stage Result Pattern

The pipeline uses a result enum for flow control:

```rust
pub enum StageResult<T> {
    Continue(T),           // Process normally
    ContinueBatch(Vec<T>), // Output multiple items
    Skip,                  // Skip remaining stages
    Retry(T),             // Retry this stage
    Error(Error),         // Stop pipeline
}
```

### PipelineStage Trait

```rust
#[async_trait]
pub trait PipelineStage: Send + Sync {
    type Input: Send;
    type Output: Send;
    
    async fn process(
        &self, 
        input: Self::Input, 
        ctx: &mut PipelineContext
    ) -> Result<StageResult<Self::Output>>;
    
    fn name(&self) -> &str;
    
    fn metrics(&self) -> HashMap<String, Value> {
        HashMap::new()
    }
}
```

## Use Cases

### 1. Ingestd Event Processing

After events have proper provenance from StreamingIngestorFramework:

```rust
let pipeline = EventPipeline::new(config)
    .add_stage(SchemaValidationStage::new(&schema_registry))
    .add_stage(ProvenanceValidationStage::new())
    .add_stage(DeduplicationStage::new(&dedup_cache))
    .add_stage(EnrichmentStage::new()) // Add ts_ingest, host
    .add_stage(BatchingStage::new(1000))
    .add_stage(StorageStage::new(&db_pool))
    .add_stage(RedisPublishStage::new(&redis_client));

pipeline.process_stream(event_stream).await?;
```

### 2. Automaton Synthesis Pipeline

For complex multi-step processing:

```rust
let synthesis_pipeline = EventPipeline::new(config)
    .add_stage(FilterStage::new(|e| e.event_type == "file.created"))
    .add_stage(WindowingStage::new(Duration::from_secs(60)))
    .add_stage(AggregationStage::new())
    .add_stage(SynthesisStage::new()) // Sets source_event_ids
    .add_stage(EmitStage::new(&context));
```

### 3. Declarative SQL Transformations

For the future SQL-as-Automaton:

```rust
let sql_pipeline = EventPipeline::new(config)
    .add_stage(SqlTransformStage::from_file("filter_commands.sql"))
    .add_stage(SqlTransformStage::from_file("aggregate_by_user.sql"))
    .add_stage(SqlTransformStage::from_file("create_summary.sql"));
```

## Key Features from Original Implementation

### 1. Stage History Tracking

Each event maintains its processing history:

```rust
pub struct StagedEvent {
    pub event: Event,
    pub stage_history: Vec<StageResult>,
    pub created_at: Instant,
}

pub struct StageResult {
    pub stage_name: String,
    pub duration: Duration,
    pub success: bool,
    pub error: Option<String>,
}
```

### 2. Built-in Metrics

The pipeline automatically tracks:
- Stage execution times
- Success/failure rates  
- Throughput metrics
- Stage-specific custom metrics

### 3. Error Context

Errors include full context about which stage failed and why.

## Stream Processing Requirements

For production use, the pipeline needs:

1. **Async Streaming**: Process streams of events, not just single events
2. **Backpressure**: Handle flow control when downstream stages are slower
3. **Batching**: Process events in efficient batches
4. **Checkpoint Integration**: Resume from last checkpoint after crashes
5. **Parallel Execution**: Run independent stages concurrently where possible

## Example Stage Implementations

### ValidationStage

```rust
pub struct ValidationStage {
    schema_registry: Arc<SchemaRegistry>
}

#[async_trait]
impl PipelineStage for ValidationStage {
    type Input = Event;
    type Output = Event;
    
    async fn process(
        &self, 
        event: Event, 
        _ctx: &mut PipelineContext
    ) -> Result<StageResult<Event>> {
        // Validate event has required fields
        if event.source.as_str().is_empty() {
            return Err(SinexError::validation("Event source cannot be empty"));
        }
        
        // Validate against schema if present
        if let Some(schema_id) = &event.payload_schema_id {
            self.schema_registry.validate(&event.payload, schema_id)?;
        }
        
        Ok(StageResult::Continue(event))
    }
    
    fn name(&self) -> &str {
        "validation"
    }
}
```

### BatchingStage

```rust
pub struct BatchingStage {
    batch_size: usize,
    buffer: Arc<Mutex<Vec<Event>>>,
}

#[async_trait]
impl PipelineStage for BatchingStage {
    type Input = Event;
    type Output = Vec<Event>;
    
    async fn process(
        &self, 
        event: Event, 
        _ctx: &mut PipelineContext
    ) -> Result<StageResult<Vec<Event>>> {
        let mut buffer = self.buffer.lock().await;
        buffer.push(event);
        
        if buffer.len() >= self.batch_size {
            let batch = std::mem::take(&mut *buffer);
            Ok(StageResult::Continue(batch))
        } else {
            Ok(StageResult::Skip)
        }
    }
    
    fn name(&self) -> &str {
        "batching"
    }
}
```

## Integration Points

### With StreamingIngestorFramework

The pipeline operates AFTER provenance is established:

```
[Raw Bytes] → [StreamingIngestorFramework] → [Events with provenance] → [EventPipeline]
```

### With Repository Pattern

Stages use repositories for database operations:

```rust
pub struct StorageStage {
    db_pool: DbPool,
}

impl StorageStage {
    async fn store_batch(&self, events: Vec<Event>) -> Result<()> {
        let repo = self.db_pool.events();
        repo.insert_batch(events).await?;
        Ok(())
    }
}
```

### With Telemetry

Pipeline metrics integrate with sinex-telemetry:

```rust
impl EventPipeline {
    pub fn emit_metrics(&self) {
        for (stage_name, metrics) in self.collect_metrics() {
            telemetry::emit_event(
                "pipeline.stage.metrics",
                json!({
                    "stage": stage_name,
                    "processed": metrics.processed,
                    "errors": metrics.errors,
                    "avg_duration_ms": metrics.avg_duration.as_millis(),
                })
            );
        }
    }
}
```

## Future Considerations

1. **Dynamic Pipeline Construction**: Load pipeline definitions from configuration
2. **Pipeline Templates**: Pre-built pipelines for common patterns
3. **Visual Pipeline Editor**: GUI for constructing pipelines
4. **Pipeline Testing**: Framework for testing individual stages
5. **Performance Optimization**: Parallel stage execution where possible

## Conclusion

The event pipeline pattern provides a clean abstraction for multi-stage event processing. While not immediately needed, it would be valuable once:

1. The core provenance infrastructure is complete
2. We have more complex processing requirements
3. We want to enable declarative event transformations

The pattern complements rather than replaces the StreamingIngestorFramework, operating at a different layer of the architecture.
