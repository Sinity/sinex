# Universal Tagging System Design

## Overview

A comprehensive tagging system is planned for Sinex to enable flexible organization and discovery of events, entities, and artifacts. While not yet implemented, this system would provide hierarchical tags, aliases, and polymorphic associations.

## Motivation

Tags are essential for:
- User-driven organization of captured data
- Cross-cutting categorization beyond entity types
- Status tracking (e.g., `status.reviewed`, `status.todo`)
- Project association (e.g., `project.sinex`, `project.client-work`)
- Topic classification (e.g., `topic.ai.llm`, `topic.security`)

## Proposed Schema

### Core Tags Table

```sql
CREATE TABLE IF NOT EXISTS core.tags (
    tag_id         ULID PRIMARY KEY DEFAULT gen_ulid(),
    tag_name       TEXT UNIQUE NOT NULL, 
    -- Canonical tag name using dot notation for hierarchy
    -- Examples: "project.sinex.docs", "status.in-progress", "topic.rust.async"
    
    description    TEXT,
    parent_tag_id  ULID REFERENCES core.tags(tag_id) ON DELETE SET NULL,
    aliases        TEXT[], -- Alternative names: ["AI"] for "artificial-intelligence"
    properties     JSONB,  -- UI hints: color, icon, special behaviors
    
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for efficient queries
CREATE INDEX idx_tags_name ON core.tags(tag_name);
CREATE INDEX idx_tags_parent ON core.tags(parent_tag_id);
CREATE GIN INDEX idx_tags_aliases ON core.tags USING gin(aliases);
```

### Polymorphic Tag Associations

```sql
-- Generic tagging junction table
CREATE TABLE IF NOT EXISTS core.tagged_items (
    id             ULID PRIMARY KEY DEFAULT gen_ulid(),
    tag_id         ULID NOT NULL REFERENCES core.tags(tag_id),
    
    -- Polymorphic reference
    taggable_type  TEXT NOT NULL, -- 'event', 'entity', 'blob', etc.
    taggable_id    ULID NOT NULL,
    
    -- Optional metadata
    confidence     FLOAT CHECK (confidence >= 0 AND confidence <= 1),
    tagged_by      TEXT, -- User or automaton name
    tagged_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    UNIQUE(tag_id, taggable_type, taggable_id)
);

-- Indexes for each taggable type
CREATE INDEX idx_tagged_items_events 
ON core.tagged_items(taggable_id) 
WHERE taggable_type = 'event';

CREATE INDEX idx_tagged_items_entities 
ON core.tagged_items(taggable_id) 
WHERE taggable_type = 'entity';
```

## Integration Patterns

### Manual Tagging
```rust
// Tag an event
TagService::tag_item(
    tag_name: "status.reviewed",
    item_type: TaggableType::Event,
    item_id: event_id,
    tagged_by: "user",
)?;
```

### Automated Tagging
```rust
// Automaton assigns tags with confidence
TagService::tag_item_with_confidence(
    tag_name: "topic.rust.error-handling",
    item_type: TaggableType::Event,
    item_id: event_id,
    confidence: 0.85,
    tagged_by: "code-analyzer-automaton",
)?;
```

### Tag Queries
```rust
// Find all events with a tag
let events = TagService::find_items_by_tag(
    tag_name: "project.sinex",
    item_type: TaggableType::Event,
)?;

// Find all tags for an item
let tags = TagService::get_item_tags(
    item_type: TaggableType::Entity,
    item_id: entity_id,
)?;
```

## Current Workaround

Until the tagging system is implemented, tags are stored in JSON metadata:
- Events: `payload.tags` array
- Entities: `metadata.tags` array
- PKM annotations: `metadata.tags` array

This allows tag-like functionality but lacks:
- Centralized tag management
- Hierarchical relationships
- Efficient tag-based queries
- Tag normalization and deduplication

## Implementation Priority

The tagging system is a high-value feature that would benefit from implementation because:
1. Many other systems already expect tags in metadata
2. Users need ways to organize captured data
3. Automata could provide intelligent auto-tagging
4. Cross-cutting queries become much more powerful

## Migration Path

When implemented:
1. Create tag tables with initial seed data
2. Extract existing tags from JSON metadata
3. Normalize and deduplicate tag names
4. Create polymorphic associations
5. Update services to use tag tables
6. Maintain backward compatibility with metadata.tags