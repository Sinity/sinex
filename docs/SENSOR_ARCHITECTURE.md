# Sensor Architecture: Preventing Common Mistakes

Note: This is an enforcement summary. For end‑to‑end architecture and data flow, see `docs/architecture/IngestionArchitecture_And_TelemetrySources.md` and `docs/architecture/system-overview.md`.

## The Golden Rule: Only sensd Captures Source Material

**⚠️ CRITICAL: Satellites must NEVER directly capture source material!**

## Why This Matters

The Sinex architecture enforces a strict separation of concerns:

1. **sensd** - The ONLY component that captures source material
2. **Satellites** - Process material that sensd has already captured
3. **Automata** - Process events to create synthesized events

## Common Mistake: Satellites Acting as Sensors

### ❌ WRONG: Satellite Directly Capturing Data

```rust
// DON'T DO THIS IN A SATELLITE!
impl DesktopSatellite {
    async fn monitor_clipboard(&self) {
        let clipboard_content = clipboard.get_text(); // ❌ Direct capture!
        let event = Event::new(...);  // ❌ Creating event without source material!
        self.ingest(event).await;
    }
}
```

### ✅ CORRECT: Satellite Processing sensd's Material

```rust
// This is the correct pattern
impl DesktopSatellite {
    async fn process_material(&self, material_id: Ulid, data: &[u8]) {
        // Process material that sensd already captured
        let event = Event::from_material(
            material_id,
            offset_start,
            offset_end,
            // ... event details
        );
        self.ingest(event).await;
    }
}
```

## Type-Level Enforcement

The SDK provides compile-time guards to prevent mistakes:

### 1. SensorCapability - Only sensd Can Have This

```rust
// This type is private and cannot be created by satellites
pub struct SensorCapability<T> {
    _private: Private, // Prevents external construction
}
```

### 2. MaterialConsumer Trait - What Satellites Should Implement

```rust
impl MaterialConsumer for MySatellite {
    // Can only process already-captured material
    async fn process_material_slice(&self, material_id: Ulid, data: &[u8]) {
        // Process the material, don't capture it
    }
}
```

### 3. Compile-Time Checks

```rust
// Use this macro to ensure your component isn't acting as a sensor
ensure_not_sensor!(my_satellite);
```

## Architecture Diagram

```
External Sources          sensd Layer              Satellite Layer
================    ===================    =======================

Clipboard ──┐
            │
Hyprland ───┼──────► sensd (Sensor) ──────► MaterialSliceStream
            │             │                           │
Terminal ───┘             │                           ▼
                          ▼                    Satellites (Processors)
                  Source Material                     │
                    Registry                          ▼
                                                  Events with
                                                Material Provenance
```

## Red Flags in Code Review

Watch for these patterns that indicate a satellite is incorrectly acting as a sensor:

### 🚨 Direct External API Access
```rust
// RED FLAG!
use tokio::net::UnixStream;
let socket = UnixStream::connect("/tmp/hypr/socket2").await?;
```

### 🚨 File System Watching
```rust
// RED FLAG!
use notify::{Watcher, RecursiveMode};
let watcher = notify::recommended_watcher()?;
```

### 🚨 Database Queries
```rust
// RED FLAG!
sqlx::query!("SELECT * FROM external_database").fetch_all(&pool).await?;
```

### 🚨 Event Creation Without Provenance
```rust
// RED FLAG!
let event = RawEvent::new(source, event_type, payload); // No material reference!
```

## The Correct Flow

1. **External Source** produces data
2. **sensd sensor module** captures it as Source Material
3. **Temporal Ledger** records precise timing
4. **MaterialSliceStream** makes it available
5. **Satellite** processes the material
6. **Event** created with `Provenance::Material`

## Enforcement Checklist

- [ ] No direct external API connections in satellites
- [ ] No file system watchers in satellites  
- [ ] No database queries to external DBs in satellites
- [ ] All events have either Material or Synthesis provenance
- [ ] Satellites only implement `MaterialConsumer`, never `SensorOperation`
- [ ] Only sensd has `SensorCapability` types

## Examples of Proper Separation

### sensd Sensor Module (CORRECT)
```rust
// In sensd - this is where sensor code belongs
pub struct ClipboardSensor {
    capability: SensorCapability<Clipboard>, // Only sensd can have this
}

impl ClipboardSensor {
    pub async fn capture(&self) -> Result<()> {
        let content = clipboard.get_text(); // ✅ Direct capture in sensd
        self.store_material(content).await?;
        Ok(())
    }
}
```

### Satellite Processor (CORRECT)
```rust
// In satellite - only processes what sensd captured
pub struct DesktopProcessor;

impl MaterialConsumer for DesktopProcessor {
    async fn process_material_slice(&self, material_id: Ulid, data: &[u8]) {
        // Parse the clipboard content from material
        let clipboard_data: ClipboardData = serde_json::from_slice(data)?;
        
        // Create event with proper material provenance
        let event = Event::from_material(
            material_id,
            // ...
        );
        
        Ok(vec![event])
    }
}
```

## Migration Guide

If you have a satellite that's currently acting as a sensor:

1. **Identify sensor behavior** - Look for direct external connections
2. **Extract to sensd module** - Move capture logic to a sensd sensor
3. **Convert to MaterialConsumer** - Make satellite process material instead
4. **Add provenance** - Use `Event::from_material()` factory
5. **Remove dependencies** - Remove notify, database clients, etc.

## Testing Your Implementation

```rust
#[test]
fn satellite_is_not_sensor() {
    let satellite = MySatellite::new();
    
    // This should compile
    let _not_sensor = satellite.verify_not_sensor();
    
    // This should NOT compile (good!)
    // let _cap = SensorCapability::<MySatellite>::new();
}
```

## Summary

Remember: **Satellites are consumers, not producers of source material!**

- ✅ sensd captures
- ✅ Satellites process  
- ❌ Satellites capture (NEVER DO THIS!)

When in doubt, ask: "Am I connecting directly to an external source?" If yes, that code belongs in sensd, not in a satellite.
