# Vector Embeddings and Semantic Search

**Status**: Designed, not implemented
**Implementation**: 0% (Design complete, implementation not started)
**Priority**: High
**Dependencies**: PostgreSQL pgvector extension, SentenceTransformers library, model downloads
**Blocks**: Semantic search, content similarity, LLM context augmentation

## Overview

Vector embeddings transform text into dense vector representations, capturing semantic meaning. They are crucial for semantic search, similarity detection, and providing context to LLM agents. This design prioritizes local, CPU-efficient models for privacy and offline operation.

## Technical Specification

### Model Selection for Local Deployment

Balanced for performance, speed, resource requirements, and licensing:

**Recommended Models**:
- **Primary Choice**: BAAI General Embedding (BGE) - `bge-base-en-v1.5`
  - 109M params, 768 dimensions, good MTEB performance, CPU-feasible
- **CPU-Optimized Alternative**: Microsoft E5 - `e5-base-v2`
- **SentenceTransformers Options**:
  - `all-MiniLM-L6-v2`: 22M params, 384 dims, very fast, moderate accuracy
  - `all-mpnet-base-v2`: 110M params, 768 dims, better accuracy
- **GTE Models**: `gte-base-en-v1.5` or `gte-small` (highly regarded on MTEB)

### Performance Optimization

**INT8 Quantization for CPU**:
- ~2.3x faster inference
- ~4x smaller model memory
- 95-99% accuracy retention
- Tools: ctransformers (GGUF), ONNX Runtime, Intel Neural Compressor

**Intel OpenVINO Acceleration**:
- Additional 3-5x speedup on Intel hardware
- Requires model conversion to OpenVINO IR format

## Database Schema Design

**Note**: The `km.concepts` table already has an `embedding vector(1536)` column implemented
(see `/migrations/00000000000004_create_knowledge_management.sql`). The tables below are
additional infrastructure for comprehensive embedding support.

### artifact_embeddings Table

Stores embeddings for chunks/summaries of `core.artifact_contents`:

```sql
CREATE TABLE IF NOT EXISTS artifact_embeddings (
    content_id              ULID NOT NULL REFERENCES core.artifact_contents(content_id) ON DELETE CASCADE,
    embedding_name          TEXT NOT NULL, -- e.g., "text_chunk_0001", "title_v1"
    model_name              TEXT NOT NULL, -- e.g., "all-MiniLM-L6-v2_local_v1"
    model_dimension         INT NOT NULL,
    embedding_vector        VECTOR,        -- pgvector type
    input_text_hash_blake3  TEXT NULLABLE, -- BLAKE3 hash of embedded text
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (content_id, embedding_name, model_name)
);

-- HNSW index for fast similarity search (per ADR-005)
CREATE INDEX idx_artifact_embeddings_hnsw_cosine ON artifact_embeddings
  USING hnsw (embedding_vector vector_cosine_ops)
  WITH (m = 16, ef_construction = 64);
```

### event_embeddings Table

For direct `core.events` payload embeddings:

```sql
CREATE TABLE IF NOT EXISTS event_embeddings (
    event_id                ULID NOT NULL REFERENCES core.events(id) ON DELETE CASCADE,
    embedding_name          TEXT NOT NULL,
    jsonpath_to_text        TEXT NULLABLE, -- JSONPath expression used
    model_name              TEXT NOT NULL,
    model_dimension         INT NOT NULL,
    embedding_vector        VECTOR,
    input_text_hash_blake3  TEXT NULLABLE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, embedding_name, model_name)
);
```

### embedding_cache Table

Deduplicates embedding generation for identical text:

```sql
CREATE TABLE IF NOT EXISTS embedding_cache (
    input_text_hash_blake3  TEXT NOT NULL,
    model_name              TEXT NOT NULL,
    model_dimension         INT NOT NULL,
    embedding_vector        VECTOR NOT NULL,
    first_generated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_accessed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    access_count            BIGINT NOT NULL DEFAULT 1,
    PRIMARY KEY (input_text_hash_blake3, model_name)
);
```

## Implementation Architecture

### Embedding Agent

A dedicated agent for generating embeddings with:

**Target Content**:
- Text from `core.artifact_contents` (PKM notes, web archives)
- Selected textual fields from `core.events.payload`
- Living Document segments

**Processing Pipeline**:
1. Fetch unembedded content
2. Apply chunking strategy
3. Check embedding cache
4. Generate new embeddings as needed
5. Store in appropriate tables
6. Update cache

### Chunking Strategies

**Initial Approach**: Fixed-size character chunks with overlap
- Configurable chunk size (e.g., 1000 chars)
- Configurable overlap (e.g., 100 chars)
- Sequential naming: `text_chunk_0001`, `text_chunk_0002`

**Future Enhancements**:
- Semantic chunking based on logical units
- Sentence-based chunking with NLTK/spaCy
- Markdown structure-aware chunking
- LLM-aided semantic segmentation

## Implementation Plan

### Phase 1: Core Infrastructure
- [ ] Install pgvector extension in migrations
- [ ] Create embedding tables schema
- [ ] Implement basic embedding agent
- [ ] Set up SentenceTransformers integration
- [ ] Create text chunking pipeline

### Phase 2: Batch Processing
- [ ] Backfill script for existing content
- [ ] Parallel processing optimization
- [ ] Progress tracking and resumption
- [ ] Performance benchmarking

### Phase 3: Real-time Pipeline
- [ ] Event-driven embedding generation
- [ ] Redis Streams integration
- [ ] Cache management strategies
- [ ] Model versioning system

### Phase 4: Advanced Features
- [ ] Multiple model support
- [ ] Fine-tuning on user data
- [ ] Multilingual embeddings
- [ ] GPU acceleration support

## Integration Points

- **Knowledge Graph**: Embeddings for entity similarity
- **Search Interface**: Semantic search capabilities
- **LLM Integration**: Context retrieval for AI agents
- **Query System**: Enhanced with vector similarity

## Performance Targets

- **Throughput**: 100+ documents/minute on CPU
- **Latency**: <100ms for single document embedding
- **Storage**: ~3KB per 768-dim embedding
- **Cache Hit Rate**: >80% for common content

## Future Considerations

- **Model Evolution**: Strategy for updating embeddings with new models
- **Hybrid Search**: Combining keyword and semantic search
- **Cross-lingual Search**: Multilingual embedding models
- **Compression**: Dimensionality reduction techniques for storage optimization