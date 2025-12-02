# Hybrid Search (Vector + Full-Text) in PostgreSQL

**Status**: Designed, not implemented
**Implementation**: 0% (Design complete, implementation not started)
**Priority**: High
**Dependencies**: PostgreSQL pgvector extension, FTS configuration, embedding models, RRF algorithm
**Blocks**: Semantic search, content discovery, query processing, knowledge retrieval

## Overview

Hybrid search combines the strengths of semantic vector search (via pgvector) and traditional keyword-based full-text search (FTS) to provide more comprehensive and relevant results than either method alone. Results are combined using Reciprocal Rank Fusion (RRF) for optimal ranking.

## Technical Specification

### pgvector Implementation

**Extension Setup**:
```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

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

### Metadata Filtering with ANN Search

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

### PostgreSQL Full-Text Search

**Core Components**:
- `to_tsvector('english', text)`: Convert text to searchable tokens
- `plainto_tsquery('english', query)`: Convert query to search terms
- `websearch_to_tsquery`: More flexible query parsing

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

### Reciprocal Rank Fusion (RRF)

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

## Implementation Architecture

### Hybrid Search Function

Key components of the SQL function:

1. **FTS Results CTE**:
   - Query tsvector columns
   - Rank using ts_rank_cd
   - Assign FTS rank values

2. **Vector Results CTE**:
   - Query embedding vectors
   - Handle chunked documents (aggregate best chunk)
   - Assign vector rank values

3. **RRF Combination**:
   - FULL OUTER JOIN results
   - Calculate hybrid scores
   - Order by combined ranking

### Handling Chunked Embeddings

For documents split into chunks:
```sql
-- Find best matching chunk per document
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
-- Aggregate to document level
vector_document_scores AS (
    SELECT
        content_id,
        MAX(similarity) as best_similarity
    FROM vector_chunk_scores
    WHERE chunk_rank = 1
    GROUP BY content_id
)
```

## Implementation Plan

### Phase 1: FTS Infrastructure
- [ ] Add tsvector columns to text tables
- [ ] Create GIN indexes for FTS
- [ ] Implement basic keyword search
- [ ] Configure language analyzers

### Phase 2: Vector Search Integration
- [ ] Ensure pgvector is properly configured
- [ ] Create vector similarity functions
- [ ] Handle chunked document aggregation
- [ ] Optimize HNSW parameters

### Phase 3: Hybrid Search
- [ ] Implement RRF algorithm
- [ ] Create hybrid search function
- [ ] Handle edge cases (missing embeddings, etc.)
- [ ] Add metadata filtering support

### Phase 4: Performance Optimization
- [ ] Benchmark different configurations
- [ ] Tune index parameters
- [ ] Implement caching strategies
- [ ] Add search analytics

### Phase 5: Advanced Features
- [ ] Query expansion and rewriting
- [ ] Personalized ranking
- [ ] Multi-modal search
- [ ] Search suggestions

## Performance Targets

For 1M document corpus on commodity hardware:
- **p50 latency**: ~24ms
- **p99 latency**: ~85ms
- **Throughput**: 100+ queries/second
- **Index build time**: 3-5 mins (GIN), 25-35 mins (HNSW)

## Zero-Downtime Index Rotation

For production deployments:
```sql
-- Build new index without blocking
CREATE INDEX CONCURRENTLY new_index_name ON table ...;

-- Atomic swap
BEGIN;
DROP INDEX IF EXISTS old_index_name;
ALTER INDEX new_index_name RENAME TO old_index_name;
COMMIT;
```

## Search API Design

### Query Interface
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

### Response Format
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

## Future Enhancements

- **Learning to Rank**: ML models for result ranking
- **Query Understanding**: Intent detection and entity recognition
- **Faceted Search**: Dynamic filtering and aggregations
- **Federated Search**: Combine results from multiple sources
- **Real-time Indexing**: Stream processing for immediate searchability