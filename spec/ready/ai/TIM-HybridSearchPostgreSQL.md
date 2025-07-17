# TIM-HybridSearchPostgreSQL: Hybrid Search (Vector + Full-Text) in PostgreSQL

## Status Dashboard
**Maturity Level**: L2 - Ready for Implementation
**Implementation**: 0% (Design complete, implementation not started)
**Dependencies**: PostgreSQL pgvector extension, FTS configuration, embedding models, RRF algorithm
**Blocks**: Semantic search, content discovery, query processing, knowledge retrieval

## MVP Specification
- pgvector extension setup and vector storage
- PostgreSQL full-text search (FTS) configuration
- Basic hybrid search combining vector and keyword results
- Reciprocal Rank Fusion (RRF) for result combination
- Simple search API interface

## Enhanced Features
- Advanced ranking algorithms and tuning
- Query expansion and rewriting
- Search result personalization
- Performance optimization with specialized indexes
- Advanced analytics and search insights
- Multi-modal search support

## Implementation Checklist
- [ ] pgvector extension setup
- [ ] FTS index configuration
- [ ] Vector similarity search functions
- [ ] Keyword search optimization
- [ ] RRF ranking implementation
- [ ] Combined search query interface
- [ ] Performance benchmarking
- [ ] Search relevance tuning
- [ ] API endpoint development

*   **Relevant ADRs:** `[ADR-005-VectorIndexTypePgvector.md](docs/adr/ADR-005-VectorIndexTypePgvector.md)` (HNSW for `pgvector`), `[ADR-007-LargeScaleVectorSearchStrategy.md](docs/adr/ADR-007-LargeScaleVectorSearchStrategy.md)` (`pgvector` CPU first)
*   **Original UG Context:** Section 17
*   **Vision Document Reference:** Part V.2

This TIM details the implementation of a hybrid search system within PostgreSQL, combining semantic vector search (via `pgvector`) and traditional keyword-based full-text search (FTS), with results combined using Reciprocal Rank Fusion (RRF).

## 1. Rationale Summary

Hybrid search leverages the strengths of both semantic (meaning-based) and lexical (keyword-based) search to improve retrieval relevance and recall, providing more comprehensive results than either method alone.

## 2. `pgvector` Implementation Details [UG Sec 17.1]

### 2.1. Extension Installation

```sql
CREATE EXTENSION IF NOT EXISTS vector; -- Run once per database by superuser
```
(Ensure `pgvector` PostgreSQL extension is installed on the system, e.g., via NixOS package `pkgs.postgresql_16Packages.pgvector`).

### 2.2. Indexing Strategies (HNSW as per ADR-005)

*   **HNSW (Hierarchical Navigable Small World):** Preferred for balance of query speed, recall, and dynamic data handling.
*   **DDL Example for HNSW on `artifact_embeddings.embedding_vector`:**
    (Assumes `embedding_vector` column is of type `VECTOR(dimension)`)
    ```sql
    -- From TIM-EmbeddingGenerationModels.md
    -- CREATE INDEX IF NOT EXISTS idx_artifact_embeddings_hnsw_cosine ON artifact_embeddings
    --   USING hnsw (embedding_vector vector_cosine_ops)
    --   WITH (m = 16, ef_construction = 64);
    -- For event_embeddings:
    -- CREATE INDEX IF NOT EXISTS idx_event_embeddings_hnsw_cosine ON event_embeddings
    --   USING hnsw (embedding_vector vector_cosine_ops)
    --   WITH (m = 16, ef_construction = 64);
    ```
*   **HNSW Parameters:**
    *   `m`: Connections per node (default 16). Higher `m` (e.g., 24-32) -> better recall, larger/slower index build.
    *   `ef_construction`: Candidate list size during build (default 64). Higher `ef_construction` (e.g., 100-200) -> better index quality, much slower build.
*   **Query-Time Parameter `ef_search`:**
    *   Controls candidate list size during search. Higher `ef_search` -> better recall, slower query.
    *   Set per session: `SET hnsw.ef_search = 100;` (Default is often `ef_construction` or a fraction of it).
    *   Increase `ef_search` when metadata filters are also applied to improve recall of filtered ANN results.

### 2.3. Metadata Filtering with ANN Search [UG Sec 17.1.2, SA1]

Combining ANN search with `WHERE` clause metadata filters (e.g., `WHERE model_name = '...' AND artifact_type = 'pkm_note'`).

*   **Challenge:** Standard post-filtering (ANN top-K first, then filter) can hurt recall if true matches satisfying metadata are not in the initial K.
*   **Mitigation/Best Practice:**
    1.  **Increase K for ANN:** Fetch more candidates from ANN (e.g., `LIMIT 200` for vector search part) before metadata filtering in the application or outer SQL query.
    2.  **Increase `hnsw.ef_search`:** When filters are present, use a higher `ef_search` value to make the HNSW search more exhaustive.
    3.  **`pgvector` Native Filtering (Recent Versions):** Newer `pgvector` versions (e.g., 0.5.0+) may offer improved support for pushing filters into the index scan for HNSW/IVFFlat, improving recall. Check specific `pgvector` documentation for planner behavior with metadata column indexes.
*   **Query Example (Conceptual - vector search part with metadata filter):**
    ```sql
    -- Assuming p_query_embedding and p_model_name are parameters
    -- This is just the vector search part, RRF combines it with FTS later.
    SELECT
        ae.content_id,
        ae.embedding_name,
        (1 - (ae.embedding_vector <=> $1)) AS similarity -- $1 is p_query_embedding
    FROM artifact_embeddings ae
    JOIN core.artifact_contents cac ON ae.content_id = cac.content_id
    JOIN core.artifacts ca ON cac.artifact_id = ca.artifact_id
    WHERE
        ae.model_name = $2 -- $2 is p_model_name
        AND ca.artifact_type = 'pkm_note' -- Example metadata filter on related table
        -- AND (ae.embedding_vector <=> $1) < 0.6 -- Optional: pre-filter by distance threshold
    ORDER BY (ae.embedding_vector <=> $1) ASC -- ASC for distance, DESC for similarity
    LIMIT 100; -- Fetch enough candidates for RRF
    ```

## 3. PostgreSQL Full-Text Search (FTS) [UG Sec 17.1.3, OR3, CR3]

For keyword-based search, approximating BM25-like ranking.

### 3.1. `tsvector` and `tsquery`

*   `to_tsvector('english', text_content)`: Converts text to `tsvector` (normalized lexemes).
*   `plainto_tsquery('english', query_string)`: Converts query to `tsquery` (ANDs terms).
*   `websearch_to_tsquery('english', query_string)`: More flexible query parsing (supports OR, `-` for NOT, quotes).
*   Match operator: `tsvector_column @@ tsquery_object`.

### 3.2. Generated `tsvector` Column and GIN Index

*   **Add to `core.artifact_contents` (or other text tables):**
    ```sql
    ALTER TABLE core.artifact_contents
    ADD COLUMN IF NOT EXISTS content_text_tsvector tsvector
    GENERATED ALWAYS AS (to_tsvector('english', coalesce(content_text, ''))) STORED;

    -- Index this generated column
    CREATE INDEX IF NOT EXISTS idx_artifact_contents_fts_gin ON core.artifact_contents
    USING GIN (content_text_tsvector);
    -- GIN fastupdate=on (default) is generally fine for dynamic tables.
    -- Consider fastupdate=off for append-heavy tables if index size/query speed is paramount over insert speed.
    ```
*   Similar `tsvector` columns and GIN indexes can be created for `core.events.payload` (if indexing specific JSONB text fields) or `core.entities` (for `canonical_label`, `description`).

### 3.3. Ranking Functions

*   `ts_rank(tsvector, tsquery [, normalization_options])`: Term frequency based.
*   `ts_rank_cd(tsvector, tsquery [, normalization_options])`: Cover density based (proximity of terms). Often preferred.

## 4. Reciprocal Rank Fusion (RRF) for Combining Scores [UG Sec 17.2]

### 4.1. Algorithm and Rationale [OR3, CR3, SA4]

Combines ranked lists from FTS and vector search.
`RRF_Score(document_d) = Σ (1 / (k + rank_i(d)))`
*   `rank_i(d)`: Rank of `d` in results from search system `i`.
*   `k`: Constant (e.g., 60). Dampens impact of lower ranks.
*   Benefits: Simple, no score normalization/weight tuning needed, robust.

### 4.2. SQL Implementation (Hybrid Search Function)

The function `hybrid_search_exocortex_artifacts` (provided in UG Sec 17.2.2 / `openai_sinex_6.md` Sec 5) implements this. Key aspects:
*   Takes user keyword query (`p_search_text`) and query embedding (`p_query_embedding`).
*   CTE for FTS results: Queries `core.artifact_contents.content_text_tsvector`, ranks using `ts_rank_cd`, assigns `fts_rank_val`.
*   CTE for vector results: Queries `artifact_embeddings.embedding_vector` against `p_query_embedding` (using `<=>` operator), ranks by distance/similarity, assigns `vector_rank_val`.
    *   **Important for Chunked Embeddings:** If multiple embedding chunks exist per `core.artifact_contents.content_id` (as is typical), the `vector_results` CTE must first determine the "best" matching chunk for each `content_id` (e.g., `MAX(similarity)` or `MIN(distance)` grouped by `content_id`) and then rank these unique `content_id`s. The UG example needs refinement for this to correctly apply RRF at the document level rather than chunk level.
        *   **Revised Vector Results Logic (Conceptual for document-level RRF):**
            ```sql
            -- Inside hybrid_search_exocortex_artifacts function
            -- ...
            vector_chunk_scores AS (
                SELECT
                    ae.content_id, -- content_id of the original document/text
                    (1 - (ae.embedding_vector <=> p_query_embedding)) as chunk_similarity,
                    -- Keep other relevant fields if needed from ae
                    ROW_NUMBER() OVER (PARTITION BY ae.content_id ORDER BY (ae.embedding_vector <=> p_query_embedding) ASC) as chunk_rank_within_content
                FROM artifact_embeddings ae
                WHERE ae.model_name = p_target_embedding_model_name
                  -- Optional: ca.artifact_type = 'pkm_note' -- If pre-filtering by type for vector search
            ),
            vector_document_scores AS (
                SELECT
                    vcs.content_id,
                    MAX(vcs.chunk_similarity) as best_chunk_similarity -- Aggregate: best similarity for this document
                FROM vector_chunk_scores vcs
                WHERE vcs.chunk_rank_within_content = 1 -- Or aggregate if multiple chunks contribute
                GROUP BY vcs.content_id
            ),
            vector_results AS (
                SELECT
                    vds.content_id,
                    cac.artifact_id, -- Join to get artifact_id
                    ca.current_title,
                    substring(cac.content_text from 1 for 250) as generated_snippet,
                    vds.best_chunk_similarity as vector_similarity, -- This is the score to rank by
                    ROW_NUMBER() OVER (ORDER BY vds.best_chunk_similarity DESC) as vector_rank_val
                FROM vector_document_scores vds
                JOIN core.artifact_contents cac ON vds.content_id = cac.content_id
                JOIN core.artifacts ca ON cac.artifact_id = ca.artifact_id
                ORDER BY vector_similarity DESC
                LIMIT p_vector_limit
            )
            -- ... then FULL OUTER JOIN fts_results with this revised vector_results on content_id or artifact_id
            ```
*   Main query `FULL OUTER JOIN`s FTS and vector results on `content_id` (or `artifact_id`), calculates `r_hybrid_score` using RRF formula, orders by it.

## 5. Performance Benchmarks [UG Sec 17.3, CR3]

*   Target for 1M document corpus (PKM/web markdowns) on commodity hardware:
    *   p50 latency: ~24ms
    *   p99 latency: ~85ms
*   These demonstrate feasibility of responsive hybrid search with PostgreSQL.

## 6. Zero-Downtime Index Rotation [UG Sec 17.4, CR3]

For rebuilding FTS GIN or `pgvector` HNSW/IVFFlat indexes without downtime.
1.  `CREATE INDEX CONCURRENTLY new_index_name ON my_table ...;`
    *   Builds new index without blocking writes. Longer, more resource intensive.
    *   Check `pgvector` docs for `CONCURRENTLY` support for specific index types (HNSW build is somewhat incremental by nature).
2.  Once built and validated:
    ```sql
    BEGIN;
    DROP INDEX IF EXISTS old_index_name; -- Brief lock
    ALTER INDEX new_index_name RENAME TO old_index_name; -- Fast metadata op
    COMMIT;
    ```
*   Build Times (1M docs, CR3 hardware):
    *   GIN FTS index: 3-5 mins (`CONCURRENTLY`).
    *   HNSW (`pgvector`): 25-35 mins (standard build, check if `CONCURRENTLY` supported/needed).

