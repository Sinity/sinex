# EventSource Database Access Issue

## Problem Statement
The EventSource trait doesn't provide database access, forcing sources to either:
1. Work without state persistence (losing functionality)
2. Create their own database connections (architectural violation)

## Evidence
1. **Clipboard Source**: Cannot use BlobManager for proper blob storage, has TODO comment
2. **Atuin Source**: Creates its own PostgreSQL connection to query `raw.events` table (lines 170-190)
3. **Git-annex Integration**: Can't store blob metadata without database access

## Current EventSource Trait
```rust
pub trait EventSource: Send + Sync + 'static {
    type Config: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;
    const SOURCE_NAME: &'static str;
    
    async fn initialize(config: Self::Config) -> Result<Self> where Self: Sized;
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()>;
    async fn shutdown(&mut self) -> Result<()> { Ok(()) }
}
```

## Proposed Solution
Add a context parameter to EventSource methods:

```rust
pub struct EventSourceContext {
    pub db_pool: Option<PgPool>,
    pub blob_manager: Option<Arc<BlobManager>>,
    pub config: serde_json::Value, // Source-specific config
}

pub trait EventSource: Send + Sync + 'static {
    const SOURCE_NAME: &'static str;
    
    async fn initialize(ctx: EventSourceContext) -> Result<Self> where Self: Sized;
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()>;
    async fn shutdown(&mut self) -> Result<()> { Ok(()) }
}
```

## Impact
This change would:
1. Allow proper state management (last processed timestamps, deduplication)
2. Enable use of BlobManager for large content
3. Remove need for ad-hoc database connections
4. Maintain single connection pool
5. Enable proper transaction boundaries

## Migration Path
1. Update EventSource trait
2. Create EventSourceContext in collector
3. Update all EventSource implementations
4. Remove workarounds (env var access, direct connections)