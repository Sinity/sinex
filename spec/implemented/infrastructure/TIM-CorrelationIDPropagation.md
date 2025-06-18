# TIM-EventRelations: Event Traceability System

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 85% (Core relationship tables and clustering implemented, advanced pattern recognition pending)
**Dependencies**: PostgreSQL, ULID generation, raw.events table
**Blocks**: Context-aware suggestions, workflow reconstruction, cross-source correlation

## MVP Specification
- Core event_relations table with typed relationships
- Event clustering system for workflow grouping
- Basic temporal and causal relationship detection
- Confidence scoring for relationship certainty
- Recursive query support for chain traversal

## Enhanced Features
- ML-driven pattern recognition for automatic relationship discovery
- Advanced workflow classification and labeling
- Cross-domain event correlation
- Performance-optimized materialized views
- User annotation and feedback integration

## Implementation Checklist
- [x] Core relationship schema (event_relations, event_clusters, event_cluster_members)
- [x] Basic relationship types (causal, temporal, contextual, hierarchical)
- [x] Manual relationship creation API
- [x] Cluster management functions
- [x] Recursive query patterns
- [x] Performance indexes
- [ ] Automatic relationship discovery agents
- [ ] ML pattern recognition
- [ ] Materialized view optimization
- [ ] User annotation interface

*   **Relevant Tables:** `core.event_relations`, `core.event_clusters`, `core.event_cluster_members`
*   **Vision Document Reference:** Part I.3, Principle 4 (Context is Continuous) - implemented via post-hoc event linking

This TIM describes how traceability and relationships between events are established in the Sinex system. Events are linked after ingestion through explicit relationships and clustering mechanisms.

## 1. Event Relations System Architecture

The Sinex system establishes relationships between events through explicit database tables that capture different types of connections:

### Core Tables

*   **`core.event_relations`:** Direct relationships between pairs of events
    *   `from_event_id` → `to_event_id` with typed `relation_type`
    *   Confidence scores (0.0-1.0) for relationship certainty
    *   Detection source (`temporal_analysis`, `user_annotation`, `causal_inference`)
    *   Metadata JSON for additional context

*   **`core.event_clusters`:** Higher-level groupings of related events
    *   Named clusters with types (`session`, `workflow`, `project`, `incident`)
    *   Time boundaries and descriptive summaries
    *   Flexible metadata for domain-specific information

*   **`core.event_cluster_members`:** Many-to-many event-cluster associations
    *   Optional roles (`start`, `end`, `key_event`) within clusters
    *   Timestamp tracking for membership changes

### Relationship Types

*   **Causal:** `caused_by`, `triggered_by`, `resulted_in`
*   **Temporal:** `followed_by`, `preceded_by`, `concurrent_with`
*   **Contextual:** `related_to`, `part_of`, `references`
*   **Hierarchical:** `parent_of`, `child_of`, `subsumes`

## 2. Event Linking Mechanisms

### Automatic Discovery

*   **Temporal Analysis:** Events within configurable time windows get `followed_by` relations
*   **Causal Inference:** Pattern recognition identifies likely cause-effect relationships
*   **Content Analysis:** Similar payloads or shared artifacts create `related_to` connections
*   **Session Boundaries:** Terminal/application sessions automatically cluster related events

### Manual Annotation

*   **User Relations:** Direct user creation of event relationships through interfaces
*   **Cluster Creation:** Manual grouping of events into meaningful workflows or projects
*   **Metadata Enhancement:** Adding context, corrections, or importance markers

### Agent-Driven Detection

*   **Pattern Recognition:** ML agents identify recurring relationship patterns
*   **Domain-Specific Logic:** Specialized agents for development workflows, research sessions, etc.
*   **Cross-Source Correlation:** Linking events from different ingestion sources

## 3. Implementation Patterns

### Database Queries for Event Relationships

```sql
-- Find all events that followed a specific event
SELECT e2.* FROM raw.events e2
JOIN core.event_relations r ON r.to_event_id = e2.id
WHERE r.from_event_id = $1 AND r.relation_type = 'followed_by';

-- Get complete workflow chain from starting event
WITH RECURSIVE workflow_chain AS (
  SELECT id, source, event_type, ts_ingest, 0 as depth
  FROM raw.events WHERE id = $1
  UNION ALL
  SELECT e.id, e.source, e.event_type, e.ts_ingest, w.depth + 1
  FROM raw.events e
  JOIN core.event_relations r ON r.to_event_id = e.id
  JOIN workflow_chain w ON w.id = r.from_event_id
  WHERE r.relation_type IN ('followed_by', 'caused_by')
)
SELECT * FROM workflow_chain ORDER BY depth, ts_ingest;

-- Find events in the same cluster
SELECT e.* FROM raw.events e
JOIN core.event_cluster_members ecm ON ecm.event_id = e.id
WHERE ecm.cluster_id = $1
ORDER BY e.ts_ingest;
```

### Programmatic Relationship Creation

```rust
// Create explicit event relationship
async fn create_event_relation(
    pool: &PgPool,
    from_id: Ulid,
    to_id: Ulid,
    relation_type: &str,
    confidence: f64,
    detected_by: &str,
    metadata: Value,
) -> Result<Ulid> {
    let relation_id = Ulid::new();
    sqlx::query!(
        "INSERT INTO core.event_relations 
         (id, from_event_id, to_event_id, relation_type, confidence, detected_by, metadata)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
        relation_id.as_uuid(),
        from_id.as_uuid(),
        to_id.as_uuid(),
        relation_type,
        confidence,
        detected_by,
        metadata
    ).execute(pool).await?;
    Ok(relation_id)
}

// Create event cluster for related activities
async fn create_event_cluster(
    pool: &PgPool,
    name: &str,
    cluster_type: &str,
    event_ids: &[Ulid],
    metadata: Value,
) -> Result<Ulid> {
    let cluster_id = Ulid::new();
    let time_start = get_earliest_event_time(pool, event_ids).await?;
    let time_end = get_latest_event_time(pool, event_ids).await?;
    
    // Create cluster
    sqlx::query!(
        "INSERT INTO core.event_clusters 
         (id, name, cluster_type, time_start, time_end, metadata)
         VALUES ($1, $2, $3, $4, $5, $6)",
        cluster_id.as_uuid(),
        name,
        cluster_type,
        time_start,
        time_end,
        metadata
    ).execute(pool).await?;
    
    // Add members
    for event_id in event_ids {
        sqlx::query!(
            "INSERT INTO core.event_cluster_members (cluster_id, event_id)
             VALUES ($1, $2)",
            cluster_id.as_uuid(),
            event_id.as_uuid()
        ).execute(pool).await?;
    }
    
    Ok(cluster_id)
}
```

## 4. Performance Considerations and Optimization

### Relationship Discovery Performance

*   **Temporal Analysis:** Windowed queries over time ranges minimize overhead
*   **Batch Processing:** Relations discovered in batches rather than real-time per event
*   **Confidence Thresholds:** Only store relationships above configurable confidence levels
*   **Index Strategy:** Optimized indexes on event timestamps and relation types

### Storage Efficiency

*   **Relationship Pruning:** Automatic cleanup of low-confidence or superseded relations
*   **Clustering Granularity:** Balance between granular tracking and storage overhead
*   **Metadata Compression:** JSON metadata stored efficiently for frequently accessed patterns

### Query Optimization

*   **Materialized Views:** Pre-computed relationship paths for common queries
*   **Graph Traversal Limits:** Bounded recursive queries to prevent runaway costs
*   **Caching Strategy:** Hot relationship data cached for interactive queries

## 5. Use Cases and Query Patterns

### Development Workflow Tracking

```sql
-- Find debugging session for specific bug
SELECT ec.*, array_agg(e.event_type ORDER BY e.ts_ingest) as activity_flow
FROM core.event_clusters ec
JOIN core.event_cluster_members ecm ON ecm.cluster_id = ec.id
JOIN raw.events e ON e.id = ecm.event_id
WHERE ec.cluster_type = 'debugging_session'
  AND ec.metadata->>'bug_id' = 'BUG-123'
GROUP BY ec.id;
```

### Research Session Reconstruction

```sql
-- Trace research flow from initial query to final notes
WITH research_chain AS (
  SELECT e.*, 0 as step FROM raw.events e 
  WHERE e.event_type = 'search.executed' 
    AND e.payload->>'query' ILIKE '%authentication%'
  UNION ALL
  SELECT e2.*, rc.step + 1 FROM raw.events e2
  JOIN core.event_relations r ON r.to_event_id = e2.id
  JOIN research_chain rc ON rc.id = r.from_event_id
  WHERE r.relation_type IN ('led_to', 'informed_by')
    AND rc.step < 10
)
SELECT * FROM research_chain ORDER BY step, ts_ingest;
```

### Context-Aware Suggestions

```sql
-- Find similar past activities for current context
SELECT COUNT(*) as frequency, er.relation_type, e2.event_type
FROM core.event_relations er
JOIN raw.events e1 ON e1.id = er.from_event_id
JOIN raw.events e2 ON e2.id = er.to_event_id
WHERE e1.event_type = $current_event_type
  AND e1.source = $current_source
GROUP BY er.relation_type, e2.event_type
ORDER BY frequency DESC;
```

