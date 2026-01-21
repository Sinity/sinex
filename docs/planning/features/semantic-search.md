# Semantic Search & Knowledge Discovery

**Status**: Designed, not implemented
**Implementation**: ~5% (Database schema exists, core implementation needed)
**Priority**: High
**Dependencies**: pgvector, pg_trgm, SentenceTransformers, NLP libraries

## Overview

This document covers Sinex's semantic search stack: vector embeddings, hybrid search, entity resolution, and GPU acceleration for scale. These features work together to enable intelligent content discovery, similarity matching, and knowledge graph construction.

```
┌─────────────────────────────────────────────────────────────────┐
│                    SEMANTIC SEARCH STACK                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────┐    ┌──────────────────────┐          │
│  │   Vector Embeddings  │───▶│    Hybrid Search     │          │
│  │   (Foundation)       │    │   (Vector + FTS)     │          │
│  └──────────────────────┘    └──────────────────────┘          │
│           │                           │                         │
│           ↓                           ↓                         │
│  ┌──────────────────────┐    ┌──────────────────────┐          │
│  │  Entity Resolution   │    │   GPU Acceleration   │          │
│  │  (Knowledge Graph)   │    │   (Scale)            │          │
│  └──────────────────────┘    └──────────────────────┘          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

| Priority | Feature | Status | Dependency |
|----------|---------|--------|------------|
| 1 | Vector Embeddings | 0% | pgvector, SentenceTransformers |
| 2 | Hybrid Search | 0% | Embeddings, FTS |
| 3 | Entity Resolution | 15% | Embeddings, pg_trgm |
| 4 | GPU Acceleration | 0% | Embeddings (at scale) |

---

# Part 1: Vector Embeddings

Vector embeddings transform text into dense vector representations, capturing semantic meaning. They are crucial for semantic search, similarity detection, and providing context to LLM agents. This design prioritizes local, CPU-efficient models for privacy and offline operation.

## Model Selection for Local Deployment

Balanced for performance, speed, resource requirements, and licensing:

**Recommended Models**:
- **Primary Choice**: BAAI General Embedding (BGE) - `bge-base-en-v1.5`
  - 109M params, 768 dimensions, good MTEB performance, CPU-feasible
- **CPU-Optimized Alternative**: Microsoft E5 - `e5-base-v2`
- **SentenceTransformers Options**:
  - `all-MiniLM-L6-v2`: 22M params, 384 dims, very fast, moderate accuracy
  - `all-mpnet-base-v2`: 110M params, 768 dims, better accuracy
- **GTE Models**: `gte-base-en-v1.5` or `gte-small` (highly regarded on MTEB)

## Performance Optimization

**INT8 Quantization for CPU**:
- ~2.3x faster inference
- ~4x smaller model memory
- 95-99% accuracy retention
- Tools: ctransformers (GGUF), ONNX Runtime, Intel Neural Compressor

**Intel OpenVINO Acceleration**:
- Additional 3-5x speedup on Intel hardware
- Requires model conversion to OpenVINO IR format

## Database Schema

### embedding_cache Table

Deduplicates embedding generation for identical text:

```sql
CREATE TABLE IF NOT EXISTS core.embedding_cache (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    text_hash TEXT NOT NULL,
    embedding_model_id ulid NOT NULL REFERENCES core.embedding_models(id),
    embedding vector(1536) NOT NULL,
    text_sample TEXT,
    use_count INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_text_model_embedding UNIQUE(text_hash, embedding_model_id)
);

CREATE INDEX idx_embedding_cache_hash ON core.embedding_cache(text_hash);
CREATE INDEX idx_embedding_cache_lru ON core.embedding_cache(last_used_at);
CREATE INDEX idx_embedding_cache_vector ON core.embedding_cache
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);
```

### artifact_embeddings Table

Stores embeddings for chunks/summaries of `core.artifact_contents`:

```sql
CREATE TABLE IF NOT EXISTS artifact_embeddings (
    content_id              ULID NOT NULL REFERENCES core.artifact_contents(content_id) ON DELETE CASCADE,
    embedding_name          TEXT NOT NULL,
    model_name              TEXT NOT NULL,
    model_dimension         INT NOT NULL,
    embedding_vector        VECTOR,
    input_text_hash_blake3  TEXT NULLABLE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (content_id, embedding_name, model_name)
);

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
    jsonpath_to_text        TEXT NULLABLE,
    model_name              TEXT NOT NULL,
    model_dimension         INT NOT NULL,
    embedding_vector        VECTOR,
    input_text_hash_blake3  TEXT NULLABLE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, embedding_name, model_name)
);
```

## Architectural Decisions

### IVFFlat for Vector Indexes (ADR-005)
We use IVFFlat over HNSW for pgvector indexes because:
- **Faster build times**: Important for development iteration
- **Lower memory usage**: More efficient for our scale
- **Good enough recall**: With proper tuning of lists/probes

Trade-offs:
- Requires periodic reindexing if data distribution changes
- Need to tune probes parameter for query speed vs recall
- May switch to HNSW later if query patterns demand it

### CPU-based pgvector for Scale (ADR-007)
We chose to stay with pgvector on CPU rather than external GPU vector DBs because:
- **Simplicity**: No additional services to deploy or manage
- **Unified data**: Embeddings live with their metadata
- **Good enough performance**: ~1800 QPS at 91% recall on 50M vectors
- **Cost-effective**: Leverages existing PostgreSQL hardware

## Embedding Agent Architecture

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

**Chunking Strategies**:
- Fixed-size character chunks with overlap (initial)
- Semantic chunking based on logical units (future)
- Markdown structure-aware chunking (future)

## Performance Targets

- **Throughput**: 100+ documents/minute on CPU
- **Latency**: <100ms for single document embedding
- **Storage**: ~3KB per 768-dim embedding
- **Cache Hit Rate**: >80% for common content

---

# Part 2: Hybrid Search (Vector + Full-Text)

Hybrid search combines semantic vector search (via pgvector) and traditional keyword-based full-text search (FTS) to provide more comprehensive and relevant results than either method alone. Results are combined using Reciprocal Rank Fusion (RRF).

## pgvector Implementation

**HNSW Indexing Strategy** (per ADR-005):
```sql
CREATE INDEX idx_artifact_embeddings_hnsw_cosine ON artifact_embeddings
  USING hnsw (embedding_vector vector_cosine_ops)
  WITH (m = 16, ef_construction = 64);
```

**HNSW Parameters**:
- `m`: Connections per node (16-32 for balance)
- `ef_construction`: Build quality (64-200)
- `ef_search`: Query-time candidate list size

## Metadata Filtering with ANN Search

**Challenges**:
- Post-filtering can hurt recall if matches aren't in initial K results
- Need to balance vector similarity with metadata constraints

**Best Practices**:
1. Increase K for ANN queries when filtering
2. Use higher `ef_search` with metadata filters
3. Leverage pgvector native filtering in recent versions

**Example Query**:
```sql
SELECT
    ae.content_id,
    ae.embedding_name,
    (1 - (ae.embedding_vector <=> $1)) AS similarity
FROM artifact_embeddings ae
JOIN core.artifact_contents cac ON ae.content_id = cac.content_id
JOIN core.artifacts ca ON cac.artifact_id = ca.artifact_id
WHERE
    ae.model_name = $2
    AND ca.artifact_type = 'pkm_note'
ORDER BY (ae.embedding_vector <=> $1) ASC
LIMIT 100;
```

## PostgreSQL Full-Text Search

**Schema Additions**:
```sql
ALTER TABLE core.artifact_contents
ADD COLUMN content_text_tsvector tsvector
GENERATED ALWAYS AS (to_tsvector('english', coalesce(content_text, ''))) STORED;

CREATE INDEX idx_artifact_contents_fts_gin ON core.artifact_contents
USING GIN (content_text_tsvector);
```

**Ranking Functions**:
- `ts_rank`: Term frequency based
- `ts_rank_cd`: Cover density (term proximity) - often preferred

## Reciprocal Rank Fusion (RRF)

**Algorithm**:
```
RRF_Score(document) = Σ (1 / (k + rank_i(document)))
```
- `rank_i`: Document rank in search system i
- `k`: Constant (typically 60) to dampen lower ranks

**Benefits**:
- No score normalization needed
- Robust to different scoring scales
- Simple to implement and tune

## Handling Chunked Embeddings

For documents split into chunks:
```sql
vector_chunk_scores AS (
    SELECT
        content_id,
        (1 - (embedding_vector <=> query_embedding)) as similarity,
        ROW_NUMBER() OVER (
            PARTITION BY content_id
            ORDER BY (embedding_vector <=> query_embedding) ASC
        ) as chunk_rank
    FROM artifact_embeddings
),
vector_document_scores AS (
    SELECT
        content_id,
        MAX(similarity) as best_similarity
    FROM vector_chunk_scores
    WHERE chunk_rank = 1
    GROUP BY content_id
)
```

## Search API Design

**Query Interface**:
```json
{
  "query": "user search terms",
  "filters": {
    "type": "pkm_note",
    "date_range": {...}
  },
  "limit": 20,
  "search_type": "hybrid" | "vector" | "keyword"
}
```

**Response Format**:
```json
{
  "results": [{
    "id": "content_id",
    "title": "Document Title",
    "snippet": "...matching text...",
    "scores": {
      "hybrid": 0.85,
      "vector": 0.92,
      "keyword": 0.78
    }
  }],
  "total": 150,
  "query_time_ms": 24
}
```

## Performance Targets

For 1M document corpus on commodity hardware:
- **p50 latency**: ~24ms
- **p99 latency**: ~85ms
- **Throughput**: 100+ queries/second
- **Index build time**: 3-5 mins (GIN), 25-35 mins (HNSW)

---

# Part 3: Entity Resolution and Knowledge Graph

Entity Resolution (ER) is crucial for building the Sinex Knowledge Graph (`core.entities`, `core.entity_relations`). It transforms unstructured text from PKM notes, web archives, and event payloads into structured, interconnected knowledge.

## PostgreSQL pg_trgm for Fuzzy Matching

Fast, in-database fuzzy string matching using trigrams:

**Key Functions**:
- `similarity(text1, text2)`: Returns float (0-1) similarity
- `text1 % text2`: True if similarity > threshold (default 0.3)
- `word_similarity(text1, text2)`: Based on common trigrams within words
- Distance operators: `<->` (trigram distance) for k-NN search

**Recommended Thresholds**:
- Person Names: `similarity >= 0.6`
- Organizations: `similarity >= 0.4`

**Required Indexes**:
```sql
CREATE INDEX idx_core_entities_label_trgm_gin ON core.entities
  USING GIN (canonical_label gin_trgm_ops);
CREATE INDEX idx_core_entities_aliases_trgm_gin ON core.entities
  USING GIN (aliases gin_trgm_ops);
```

Performance: 50-200ms on ~1M entity records with GIN indexes.

## Advanced Fuzzy Matching (fuzzystrmatch)

Supplements pg_trgm for re-ranking candidates:

**Algorithms**:
- Levenshtein Distance: Edit distance for typo tolerance
- Phonetic Algorithms:
  - soundex(text)
  - metaphone(text, max_output_len)
  - dmetaphone(text) - Double Metaphone

## Machine Learning Approaches

### spaCy Integration
- Pre-trained NER models for entity extraction
- Rule-based matching (Matcher, PhraseMatcher)
- EntityLinker component for Sinex KB

**Workflow**:
1. Process text with spaCy NER pipeline → extract mentions
2. Generate candidates from `core.entities` using pg_trgm
3. Use EntityLinker or custom re-ranking model
4. Select best candidate above confidence threshold

### Transformer Models
- Hugging Face NER models (BERT, RoBERTa based)
- Bi-Encoder for candidate retrieval
- Cross-Encoder for accurate re-ranking

## Blocking Strategies for Scalability

Reduce comparison space by >95% for >1M entities:

- **Exact Match**: On normalized first word or prefix
- **Phonetic Blocking**: Group by Soundex/Metaphone codes
- **Sorted Neighborhood Method (SNM)**: Compare only within fixed window
- **Canopy Clustering**: Fast approximate similarity for loose clusters
- **Locality Sensitive Hashing (LSH)**: Hash for similar entities in same bucket

## Entity Resolution Agent Architecture

### Processing Pipeline

1. **NER Phase**: Extract mentions using spaCy or Transformers, identify entity type
2. **Blocking/Candidate Generation**: Retrieve candidates using pg_trgm, filter by type
3. **Scoring/Re-ranking**: Advanced fuzzy similarity, contextual embeddings, cross-encoder
4. **Linking Decision**:
   - **High confidence (>0.9)**: Auto-link
   - **Ambiguous (0.7-0.85)**: Send to review queue
   - **No good candidates**: Propose new entity

### Event Integration

The agent logs all actions as events:
- `knowledge_graph.relation_created` - Entity linked
- `sinex.agent.entity_resolution_processed` - Processing complete
- Updates to `core.entity_relations` table

## Human Review Workflow

Essential for high data quality:

- Ambiguous mentions flagged for review
- UI presents mention in context with top candidates
- Reviewer actions: Confirm, Reject, Create new entity, Merge duplicates
- Decision tracking for audit trail
- Active learning from human decisions

## Performance Targets

- **Throughput**: 1000+ entities/second for batch processing
- **Latency**: <500ms for single entity resolution
- **Accuracy**: >90% precision at >85% recall
- **Scale**: Handle 10M+ entities in knowledge base

---

# Part 4: GPU-Accelerated Vector Search

GPU acceleration becomes necessary when the dataset grows to very large scales (>10-50 million vectors), where CPU-based pgvector performance becomes a bottleneck.

## MVP Specification
- External vector database deployment (Milvus or Qdrant)
- GPU-accelerated HNSW or CAGRA indexes
- Hybrid query routing between PostgreSQL and GPU vector DB
- Basic data synchronization from pgvector to GPU database
- Performance monitoring and benchmarking

## Enhanced Features
- Real-time CDC-based synchronization via Debezium/Kafka
- Distributed GPU cluster support for extreme scale
- Advanced quantization techniques (FP16/INT8)
- Multi-GPU sharding and replication
- Automatic failover and load balancing
- Cost optimization with spot instances

## Implementation Strategy

### Phase 1: Foundation
- Deploy Milvus/Qdrant with single GPU support
- Implement batch synchronization from pgvector
- Create hybrid query router
- Benchmark performance vs pgvector baseline

### Phase 2: Real-time Sync
- Set up Debezium CDC pipeline
- Implement dual-write pattern for new embeddings
- Add monitoring and alerting
- Validate data consistency

### Phase 3: Scale & Optimize
- Multi-GPU cluster configuration
- Implement quantization strategies
- Add caching layers
- Optimize for cost with spot instances

## Technical Requirements
- NVIDIA GPU with 16GB+ VRAM (minimum)
- CUDA toolkit and drivers
- Docker/Kubernetes for deployment
- Fast NVMe storage for indexes
- High-bandwidth networking for cluster mode

## Performance Targets
- 50x speedup over CPU HNSW for large datasets
- Sub-10ms query latency at 100M+ vectors
- 10,000+ QPS with proper sharding
- 95%+ recall accuracy with quantization

## Cost Considerations
- Becomes cost-effective at >10-50M vectors
- ~63% cost reduction vs scaled CPU at 100M vectors
- GPU instances: g5.xlarge for medium, p4d for large scale
- Consider spot instances for batch operations

## Migration Path
1. Set up GPU vector database alongside pgvector
2. Bulk load existing embeddings
3. Implement synchronization mechanism
4. Gradually shift query traffic with feature flags
5. Monitor and validate results
6. Complete cutover with fallback plan

---

# Implementation Roadmap

## Phase 1: Embeddings Core Infrastructure
- [ ] Install pgvector extension in migrations
- [ ] Create embedding tables schema
- [ ] Implement basic embedding agent
- [ ] Set up SentenceTransformers integration
- [ ] Create text chunking pipeline
- [ ] Backfill script for existing content

## Phase 2: Full-Text Search & Hybrid
- [ ] Add tsvector columns to text tables
- [ ] Create GIN indexes for FTS
- [ ] Implement RRF algorithm
- [ ] Create hybrid search function
- [ ] Handle edge cases (missing embeddings, etc.)

## Phase 3: Entity Resolution
- [ ] Install pg_trgm and fuzzystrmatch extensions
- [ ] Create fuzzy matching indexes
- [ ] spaCy pipeline setup and NER integration
- [ ] Blocking strategy implementation
- [ ] Confidence scoring system
- [ ] Human review queue tables and API

## Phase 4: Scale & Advanced Features
- [ ] GPU vector database evaluation (if >10M vectors)
- [ ] Query expansion and rewriting
- [ ] Personalized ranking
- [ ] Learning to Rank ML models
- [ ] Multi-modal search (text, images, audio)

---

# Database Dependencies

```sql
-- Required extensions
CREATE EXTENSION IF NOT EXISTS vector;           -- pgvector
CREATE EXTENSION IF NOT EXISTS pg_trgm;          -- Fuzzy matching
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;    -- Phonetic matching

-- Core tables
core.events              -- Event embeddings
core.entities            -- Resolved entities
core.entity_relations    -- Knowledge graph edges
artifact_embeddings      -- Content embeddings
event_embeddings         -- Event payload embeddings
embedding_cache          -- Deduplication cache
```

## See Also

- LLM orchestration: [llm-orchestration.md](./llm-orchestration.md)
- Vision features: [../../vision/feature-status.md](../../vision/feature-status.md)
