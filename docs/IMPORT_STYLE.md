# Import Style Guide

This guide documents the import patterns and conventions used throughout the Sinex codebase after namespace verbosity refactoring.

## Core Patterns

### Prelude vs Explicit Imports

**Use prelude for test files and when you need 5+ types from a crate:**
```rust
use sinex_test_utils::prelude::*;    // Tests: always use prelude
use sinex_satellite_sdk::prelude::*; // When using many SDK types
```

**Use explicit imports for production code with few dependencies:**
```rust
use sinex_satellite_sdk::{CheckpointManager, StatefulStreamProcessor, TimeHorizon};
use sinex_core::{EventSource, EventType, RawEvent, Ulid};
```

### Import Block Organization

**Standard order (separated by blank lines):**
```rust
// 1. Standard library
use std::collections::HashMap;
use std::sync::Arc;

// 2. External dependencies (alphabetical)
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

// 3. Sinex crates (by dependency order: core → satellite-sdk → services → app)
use sinex_core::{EventType, RawEvent, Ulid};
use sinex_satellite_sdk::{StatefulStreamProcessor, CheckpointManager};
use sinex_test_utils::prelude::*;

// 4. Local modules
use crate::config::ProcessorConfig;
use crate::errors::ProcessorError;
```

## Re-export Patterns

### Crate-level Re-exports (lib.rs)
```rust
// Group by functional area
pub use checkpoint::{CheckpointManager, CheckpointState};
pub use config::{AutomatonConfig, EventSourceConfig, SatelliteConfig};
pub use grpc_client::{BatchResult, GrpcClientConfig, IngestClient};

// Common error and result types
pub use crate::{SatelliteError, SatelliteResult};
```

### Prelude Module Structure
```rust
//! Prelude module for convenient imports
//!
//! ```rust
//! use sinex_satellite_sdk::prelude::*;
//! ```

// Core traits and types
pub use crate::{StatefulStreamProcessor, CheckpointManager};

// Configuration types  
pub use crate::{SatelliteConfig, EventSourceConfig};

// Error handling
pub use crate::{SatelliteError, SatelliteResult};
```

## Type Alias Conventions

**Use type aliases for complex generic types:**
```rust
pub type SatelliteResult<T> = Result<T, SatelliteError>;
pub type EventId = Id<Event>;
pub type SourceId = Id<EventSource>;
```

**Avoid aliases for simple types:**
```rust
// Don't do this
pub type EventPayload = serde_json::Value;

// Just use the type directly
use serde_json::Value;
```

## Module-specific Patterns

### Database Modules
```rust
use sinex_core::db::{models::RawEvent, repositories::DbPoolExt};
use sinex_core::{EventType, Ulid}; // Re-exported types
```

### Test Modules
```rust
use sinex_test_utils::prelude::*;  // Always use prelude
use color_eyre::eyre::Result;      // Standard error type
```

### Satellite Implementations  
```rust
use sinex_satellite_sdk::{
    StatefulStreamProcessor, CheckpointManager, IngestClient,
    SatelliteConfig, TimeHorizon, ProcessorType,
};
// Avoid deep module paths like sinex_satellite_sdk::stream_processor::StatefulStreamProcessor
```

## When to Use Each Pattern

### Use `prelude::*` when:
- Writing tests (always use test-utils prelude)
- Using 5+ types from the same crate
- The types are commonly used together
- Developing rapidly and import ergonomics matter

### Use explicit imports when:
- Writing production code with clear dependencies
- Only using 1-3 types from a crate
- Type conflicts might occur
- Code will be maintained long-term

### Use crate re-exports when:
- Types are commonly used together across the codebase
- You want to hide internal module structure
- Creating a stable public API
- Reducing import verbosity is important

## Anti-patterns to Avoid

**Deep module nesting:**
```rust
// Don't do this
use sinex_satellite_sdk::stream_processor::context::StreamProcessorContext;

// Do this instead (via re-export)
use sinex_satellite_sdk::StreamProcessorContext;
```

**Mixing prelude with explicit imports from same crate:**
```rust
// Don't do this
use sinex_satellite_sdk::prelude::*;
use sinex_satellite_sdk::SomeSpecificType;

// Choose one approach per crate
```

**Unnecessary type aliases:**
```rust
// Don't create aliases for standard types
pub type MyString = String;
pub type MyVec<T> = Vec<T>;
```