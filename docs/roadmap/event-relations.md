# Event Relations and Traceability System

## Overview

A comprehensive event relationship system is planned to enable tracking causality, workflows, and correlations between events. This system would support both automatic relationship discovery and manual annotation.

## Motivation

Event relationships are crucial for:
- **Workflow Reconstruction**: Understanding sequences of related actions
- **Root Cause Analysis**: Tracing effects back to causes
- **Context Discovery**: Finding related events across different sources
- **Pattern Recognition**: Identifying recurring event sequences

## Proposed Schema

### Event Relations Table

```sql
CREATE TABLE IF NOT EXISTS core.event_relations (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    from_event_id ULID NOT NULL REFERENCES core.events(id),
    to_event_id ULID NOT NULL REFERENCES core.events(id),
    relation_type TEXT NOT NULL,
    confidence FLOAT NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    detection_source TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT NOT NULL,
    
    CONSTRAINT unique_event_relation 
        UNIQUE (from_event_id, to_event_id, relation_type)
);

-- Indexes for efficient traversal
CREATE INDEX idx_event_relations_from ON core.event_relations(from_event_id);
CREATE INDEX idx_event_relations_to ON core.event_relations(to_event_id);
CREATE INDEX idx_event_relations_type ON core.event_relations(relation_type);
```

### Event Clusters Table

```sql
CREATE TABLE IF NOT EXISTS core.event_clusters (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    cluster_name TEXT NOT NULL,
    cluster_type TEXT NOT NULL,
    description TEXT,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.event_cluster_members (
    cluster_id ULID NOT NULL REFERENCES core.event_clusters(id),
    event_id ULID NOT NULL REFERENCES core.events(id),
    role TEXT,
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    added_by TEXT NOT NULL,
    
    PRIMARY KEY (cluster_id, event_id)
);
```

## Relationship Types

### Causal Relationships
- `causes`: Direct causation (file change → test failure)
- `triggers`: Indirect causation (commit → CI build)
- `enables`: Prerequisite relationship

### Temporal Relationships
- `precedes`: Simple time ordering
- `follows`: Reverse time ordering
- `concurrent_with`: Overlapping time periods

### Contextual Relationships
- `references`: Mentions or links to
- `derived_from`: Data transformation
- `part_of`: Hierarchical containment

### Workflow Relationships
- `workflow_step`: Sequential workflow stages
- `retry_of`: Retry attempts
- `alternative_to`: Alternative approaches

## Detection Sources

### Automatic Detection
```rust
// Temporal proximity detector
detect_temporal_relationships(
    window: Duration::minutes(5),
    confidence_threshold: 0.7,
);

// Content-based detector
detect_content_relationships(
    similarity_threshold: 0.8,
    fields: vec!["cwd", "file_path"],
);

// Explicit reference detector
detect_reference_relationships(
    patterns: vec![r"event:(\w+)", r"#(\d+)"],
);
```

### Manual Annotation
```rust
// User-provided relationships
EventRelationService::create_relation(
    from_event_id,
    to_event_id,
    RelationType::Causes,
    confidence: 1.0,
    created_by: "user",
)?;
```

### ML-Based Discovery
```rust
// Pattern learning from confirmed relationships
train_relationship_model(
    confirmed_relations: Vec<EventRelation>,
    features: vec!["event_type", "time_delta", "shared_entities"],
);
```

## Query Patterns

### Trace Forward (Effects)
```sql
WITH RECURSIVE event_effects AS (
    SELECT * FROM core.events WHERE id = ?
    UNION ALL
    SELECT e.* 
    FROM core.events e
    JOIN core.event_relations r ON e.id = r.to_event_id
    JOIN event_effects ee ON r.from_event_id = ee.id
    WHERE r.relation_type IN ('causes', 'triggers')
)
SELECT * FROM event_effects;
```

### Trace Backward (Causes)
```sql
WITH RECURSIVE event_causes AS (
    SELECT * FROM core.events WHERE id = ?
    UNION ALL
    SELECT e.*
    FROM core.events e
    JOIN core.event_relations r ON e.id = r.from_event_id
    JOIN event_causes ec ON r.to_event_id = ec.id
    WHERE r.relation_type IN ('causes', 'triggers')
)
SELECT * FROM event_causes;
```

## Clustering Strategies

### Workflow Clustering
- Group events by shared workflow ID
- Identify workflow boundaries
- Label workflow types

### Session Clustering
- Group by temporal proximity
- Consider user/process boundaries
- Handle concurrent sessions

### Topic Clustering
- Group by content similarity
- Use embeddings for semantic clustering
- Cross-source topic correlation

## Implementation Priorities

1. **Phase 1**: Core schema and manual relationships
2. **Phase 2**: Temporal and reference detectors
3. **Phase 3**: Clustering and workflow detection
4. **Phase 4**: ML-based discovery
5. **Phase 5**: Performance optimization with materialized views

## Benefits

- **Powerful Queries**: "Show all events caused by this commit"
- **Impact Analysis**: "What would be affected if this file changes?"
- **Debugging**: "Trace this error back to its root cause"
- **Insights**: "Discover common workflow patterns"