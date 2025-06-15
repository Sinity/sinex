# sinex-promo-worker

This crate provides both a library and binary for managing event promotion in Sinex.

## Library Components

### PromotionRouter

Routes events to agents based on their subscription manifests:

```rust
use sinex_promo_worker::{PromotionRouter, get_active_manifests};

let manifests = get_active_manifests(&pool).await?;
let router = PromotionRouter::from_manifests(manifests);

// Determine which agents should process an event
let target_agents = router.route_event(&event);
```

### EventScanner

Scans for new events that need promotion:

```rust
use sinex_promo_worker::{EventScanner, ScannerConfig};

let config = ScannerConfig {
    batch_size: 1000,
    initial_lookback: chrono::Duration::hours(24),
    process_historical: false,
};

let mut scanner = EventScanner::new(config);
let new_events = scanner.scan_new_events(&pool).await?;
```

### create_promotion_entries

Creates promotion queue entries for events:

```rust
use sinex_promo_worker::create_promotion_entries;

let count = create_promotion_entries(&pool, events, &router).await?;
```

## Binary Modes

The binary can run in two modes:

### Scanner Mode

Continuously scans for new events and creates promotion queue entries:

```bash
sinex-promo-worker --scanner-mode
```

### Worker Mode

Processes promotion queue entries for a specific agent:

```bash
sinex-promo-worker --agent-name my-agent
```

## Testing

The library provides comprehensive unit tests for the routing logic that don't require database access. Integration tests can be enabled with proper database setup.