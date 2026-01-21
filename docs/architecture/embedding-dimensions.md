# Embedding Dimensions - Design Documentation

## Current State

The Sinex schema uses fixed-dimension vector columns for embeddings:

```sql
-- event_embeddings table
embedding vector(1536)  -- Hardcoded to OpenAI ada-002 dimensions
```

## Problem

**Dimension Lock-in**: Changing embedding models requires schema migration, making it difficult to:

- Test different embedding models
- Upgrade to new model versions
- Support multiple models simultaneously

## Solutions

### Option 1: Dynamic Vector Column (Simplest)

Remove dimension constraint entirely:

```sql
ALTER TABLE event_embeddings 
  ALTER COLUMN embedding TYPE vector;
```

**Pros:**

- No migrations needed for model changes
- Flexible for experimentation

**Cons:**

- No schema validation
- Potential for mixed dimensions in same table
- Slightly larger storage overhead

### Option 2: Model-Specific Tables (Recommended)

Create separate tables per model:

```sql
CREATE TABLE event_embeddings_openai_ada002 (
    event_id ulid PRIMARY KEY,
    embedding vector(1536),
    created_at timestamptz DEFAULT now()
);

CREATE TABLE event_embeddings_openai_v3_large (
    event_id ulid PRIMARY KEY,
    embedding vector(3072),
    created_at timestamptz DEFAULT now()
);
```

**Pros:**

- Clear model separation
- Schema validation per model
- Easy to compare models
- Can run A/B tests

**Cons:**

- More tables to manage
- Requires migration for new models

### Option 3: Metadata-Driven (Most Flexible)

Store dimension in metadata:

```sql
CREATE TABLE event_embeddings (
    event_id ulid PRIMARY KEY,
    model_name text NOT NULL,
    dimension int NOT NULL,
    embedding vector,  -- Dynamic
    created_at timestamptz DEFAULT now()
);

CREATE INDEX idx_embeddings_model ON event_embeddings(model_name);
```

**Pros:**

- Single table for all models
- Queryable by model
- Clear provenance

**Cons:**

- Most complex
- Requires application-level dimension tracking

## Recommendation

**Use Option 2 (Model-Specific Tables)** for production:

1. Create naming convention: `event_embeddings_{model}_{version}`
2. Add migration template for new models
3. Update embedding service to route to correct table
4. Keep old tables for backward compatibility

**Use Option 1 (Dynamic)** for development/experimentation.

## Implementation

See `sinex-schema/src/schema/embeddings.rs` for current schema.

To add a new embedding model:

1. Create migration:

   ```bash
   sea-orm-cli migrate generate add_embeddings_{model}
   ```

2. Define table with appropriate dimensions

3. Update embedding service configuration

## See Also

- pgvector documentation: <https://github.com/pgvector/pgvector>
- Current schema: `crate/lib/sinex-schema/src/schema/embeddings.rs`
