# TIM-EmbeddingGenerationModels: Embedding Generation, Models, Storage

*   **Relevant ADR:** `[ADR-005-VectorIndexTypePgvector.md](docs/adr/ADR-005-VectorIndexTypePgvector.md)` (HNSW for `pgvector`), `[ADR-007-LargeScaleVectorSearchStrategy.md](docs/adr/ADR-007-LargeScaleVectorSearchStrategy.md)` (`pgvector` CPU first)
*   **Original UG Context:** Section 16
*   **Vision Document Reference:** Part III.3.6

This TIM details the strategy for generating vector embeddings for textual content within the Exocortex, including model selection, hardware acceleration, backfill process, table schemas, and chunking strategies.

## 1. Rationale Summary

Embeddings transform text into dense vector representations, capturing semantic meaning. They are crucial for semantic search, similarity detection, and providing context to LLM agents. The Exocortex prioritizes local, CPU-efficient models initially.

## 2. Model Selection for Local Deployment [UG Sec 16.1, SR1, SA1]

Balance performance (retrieval accuracy), speed, resource requirements (size, RAM/VRAM), and licensing. Models runnable on CPU (with quantization) are prioritized for local-first Exocortex.

*   **Recommended Models (as per SR1, SA1, `openai_sinex_6.md` Sec 5):**
    *   **Primary Balanced Choice (SR1): BAAI General Embedding (BGE) - `bge-base-en-v1.5`**
        *   Params: ~109M, Dimensions: 768. Good MTEB performance, feasible for local CPU.
    *   **Alternative CPU-Optimized (SR1): Microsoft E5 - `e5-base-v2`**
    *   **SentenceTransformers (SBERT) - General Purpose:**
        *   `all-MiniLM-L6-v2`: ~22M params, 384 dims. Very fast, moderate accuracy. Good for resource-constrained or initial prototyping. (Used in Python backfill example).
        *   `all-mpnet-base-v2`: ~110M params, 768 dims. Better accuracy than MiniLM.
    *   **GTE (Alibaba DAMO): `gte-base-en-v1.5`** or `gte-small`. Highly regarded on MTEB.
*   **Evaluation Benchmark:** MTEB (Massive Text Embedding Benchmark) is the primary reference for comparing model quality.
*   **Considerations for Exocortex:**
    *   **Fine-tuning [SA1]:** Future: Fine-tune a smaller base model on user's own PKM/Exocortex data for domain-specific relevancy.
    *   **Dimensionality [SA1]:** Higher dimensions (768+) vs. lower (384) impacts storage/compute.
    *   **Multilinguality:** If needed, select multilingual models (e.g., `paraphrase-multilingual-mpnet-base-v2`).

## 3. INT8 Quantization and Hardware Acceleration [UG Sec 16.2, 16.3, SR1]

*   **INT8 Quantization (for CPU Deployment) [SR1]:**
    *   Benefits: ~2.3x faster inference, ~4x smaller model memory. Accuracy typically 95-99% of FP32.
    *   Tools: `ctransformers` (GGUF), ONNX Runtime, Intel Neural Compressor, Hugging Face Optimum.
*   **Intel OpenVINO (for Intel CPU/iGPU Acceleration) [SR1]:**
    *   Can provide additional 3-5x speedup. Requires converting model to OpenVINO IR format.

## 4. Embedding Agent and Backfill Strategy [UG Sec 16.4, SA4, `openai_sinex_6.md` Sec 5]

An `EmbeddingAgent` (or batch script) generates embeddings for new and existing content.

*   **Target Content:**
    *   Text from `core.artifact_contents` (PKM notes, web archive Markdown).
    *   Selected textual fields from `raw.events.payload`.
    *   Segments from the Living Document.
*   **Python/SentenceTransformers Batch Backfill Script (Conceptual Core from UG Sec 16.4):**
    This script fetches text from `core.artifact_contents` that hasn't been embedded with a specific model, chunks it, generates embeddings, and stores them in `artifact_embeddings` and `embedding_cache`.

    ```python
    import os
    import psycopg2
    from psycopg2.extras import execute_values # For batch inserts
    from pgvector.psycopg2 import register_vector # For handling pgvector type
    from sentence_transformers import SentenceTransformer
    import hashlib # Using hashlib for BLAKE3-like hashing if blake3 lib not available
    # from blake3 import blake3 # Preferred: from 'blake3' PyPI package

    # --- Configuration ---
    DB_CONNECT_STRING = os.getenv("DATABASE_URL", "postgresql://sinex_user:dev_password@localhost:5432/sinex_db")
    MODEL_PATH_OR_NAME = os.getenv("EMBEDDING_MODEL_PATH", "all-MiniLM-L6-v2")
    MODEL_NAME_FOR_DB = os.getenv("EMBEDDING_MODEL_DB_NAME", "all-MiniLM-L6-v2_local_v1") # Version your model usage
    MODEL_DIMENSION = int(os.getenv("EMBEDDING_MODEL_DIMENSION", 384)) # Must match model
    BATCH_SIZE_ITEMS = int(os.getenv("EMBEDDING_BATCH_SIZE_ITEMS", 10)) # Number of artifact_contents items
    BATCH_SIZE_CHUNKS_MODEL = int(os.getenv("EMBEDDING_BATCH_SIZE_CHUNKS_MODEL", 32)) # Chunks for model.encode
    MIN_TEXT_LENGTH_CHARS = int(os.getenv("EMBEDDING_MIN_TEXT_LENGTH_CHARS", 25))
    CHUNK_SIZE_CHARS = int(os.getenv("EMBEDDING_CHUNK_SIZE_CHARS", 1000))
    CHUNK_OVERLAP_CHARS = int(os.getenv("EMBEDDING_CHUNK_OVERLAP_CHARS", 100))

    def get_text_chunks(text: str, chunk_size: int, overlap: int) -> list[tuple[str, int, int]]:
        """Chunks text, returns list of (chunk_text, chunk_index, start_char_offset)."""
        chunks = []
        start_idx = 0
        chunk_seq = 0
        while start_idx < len(text):
            end_idx = min(start_idx + chunk_size, len(text))
            chunks.append((text[start_idx:end_idx], chunk_seq, start_idx))
            chunk_seq += 1
            if end_idx == len(text):
                break
            start_idx += (chunk_size - overlap)
            if start_idx >= len(text): # Avoid tiny last chunk due to large overlap
                 # Check if the remaining part is substantial enough, or merge with previous if too small
                if len(text) - start_idx < (chunk_size * 0.2) and chunks: # e.g. less than 20% of chunk_size
                    # Append to previous chunk if it makes sense or just break
                    # For simplicity here, we might create a small final chunk or break
                    break
                # If we decide to make a small final chunk, ensure it doesn't re-process same text
                # This simple chunker might create small overlaps if not careful with the loop condition.
                # A more robust chunker from LangChain or similar might be better for complex texts.
        return chunks

    def compute_blake3_hash(text_content: str) -> str:
        # Use actual blake3 if available, otherwise fallback for example
        # return blake3(text_content.encode('utf-8')).hexdigest()
        return hashlib.sha256(text_content.encode('utf-8')).hexdigest() # Example fallback

    def main_embedding_backfill():
        print(f"Starting embedding backfill for model: {MODEL_NAME_FOR_DB} ({MODEL_PATH_OR_NAME})")
        try:
            model = SentenceTransformer(MODEL_PATH_OR_NAME)
            # Verify model dimension matches configuration
            test_embedding = model.encode("test")
            if test_embedding.shape[0] != MODEL_DIMENSION:
                raise ValueError(f"Model dimension mismatch: Configured {MODEL_DIMENSION}, Model output {test_embedding.shape[0]}")
        except Exception as e:
            print(f"Error loading SentenceTransformer model '{MODEL_PATH_OR_NAME}': {e}")
            return

        conn = None
        try:
            conn = psycopg2.connect(DB_CONNECT_STRING)
            register_vector(conn) # Initialize pgvector type handling for psycopg2 connection
            print("Database connection successful.")

            with conn.cursor() as cursor:
                while True:
                    # Fetch artifact_contents items that need embedding for this model
                    # This query identifies content_ids that do not have an entry in artifact_embeddings
                    # for the specific MODEL_NAME_FOR_DB.
                    cursor.execute(f"""
                        SELECT cac.content_id, cac.content_text
                        FROM core.artifact_contents cac
                        WHERE cac.content_text IS NOT NULL AND LENGTH(cac.content_text) >= %s
                          AND NOT EXISTS (
                            SELECT 1 FROM artifact_embeddings ae
                            WHERE ae.content_id = cac.content_id AND ae.model_name = %s
                          )
                        ORDER BY cac.captured_at_ts_orig ASC -- Process older content first, or DESC for newer
                        LIMIT %s;
                    """, (MIN_TEXT_LENGTH_CHARS, MODEL_NAME_FOR_DB, BATCH_SIZE_ITEMS))
                    
                    rows_to_process = cursor.fetchall()
                    if not rows_to_process:
                        print("No new artifact content to embed for this model. Backfill complete or caught up.")
                        break

                    print(f"Fetched {len(rows_to_process)} artifact_contents items for embedding.")
                    
                    all_embeddings_to_insert_db = [] # Tuples for execute_values into artifact_embeddings
                    all_cache_entries_to_insert_db = [] # Tuples for execute_values into embedding_cache
                    
                    texts_for_model_batch = [] # List of text chunks to send to model.encode()
                    chunk_metadata_for_db_linking = [] # List of (content_id, embedding_name, input_text_hash)

                    for content_id_str, text_content in rows_to_process:
                        if text_content is None or len(text_content.strip()) < MIN_TEXT_LENGTH_CHARS:
                            # Mark as processed_empty or similar to avoid re-fetching if it's persistently problematic
                            # For now, just skip
                            continue

                        chunks = get_text_chunks(text_content, CHUNK_SIZE_CHARS, CHUNK_OVERLAP_CHARS)
                        if not chunks:
                            continue

                        for chunk_text, chunk_idx, _start_offset_chars in chunks:
                            if len(chunk_text.strip()) < MIN_TEXT_LENGTH_CHARS:
                                continue
                            
                            input_text_hash = compute_blake3_hash(chunk_text)
                            embedding_name = f"text_chunk_{chunk_idx:04d}" # e.g., text_chunk_0000

                            # Check embedding_cache
                            cursor.execute("""
                                SELECT embedding_vector FROM embedding_cache
                                WHERE input_text_hash_blake3 = %s AND model_name = %s;
                            """, (input_text_hash, MODEL_NAME_FOR_DB))
                            cached_result = cursor.fetchone()

                            if cached_result:
                                cached_vector = cached_result[0]
                                all_embeddings_to_insert_db.append((
                                    content_id_str, embedding_name, MODEL_NAME_FOR_DB, MODEL_DIMENSION,
                                    cached_vector, input_text_hash
                                ))
                            else:
                                texts_for_model_batch.append(chunk_text)
                                chunk_metadata_for_db_linking.append((content_id_str, embedding_name, input_text_hash))
                    
                    if texts_for_model_batch:
                        print(f"Encoding {len(texts_for_model_batch)} new text chunks with model {MODEL_PATH_OR_NAME}...")
                        generated_embedding_vectors_np = model.encode(texts_for_model_batch, batch_size=BATCH_SIZE_CHUNKS_MODEL, show_progress_bar=False)
                        
                        for i, vec_np_array in enumerate(generated_embedding_vectors_np):
                            content_id_for_chunk, embedding_name_for_chunk, input_text_hash_for_chunk = chunk_metadata_for_db_linking[i]
                            embedding_vector_list = vec_np_array.tolist() # Convert numpy array to list for pgvector
                            
                            all_embeddings_to_insert_db.append((
                                content_id_for_chunk, embedding_name_for_chunk, MODEL_NAME_FOR_DB, MODEL_DIMENSION,
                                embedding_vector_list, input_text_hash_for_chunk
                            ))
                            all_cache_entries_to_insert_db.append((
                                input_text_hash_for_chunk, MODEL_NAME_FOR_DB, MODEL_DIMENSION, embedding_vector_list
                            ))

                    # Upsert embeddings into artifact_embeddings
                    if all_embeddings_to_insert_db:
                        execute_values(
                            cursor,
                            """
                            INSERT INTO artifact_embeddings
                                (content_id, embedding_name, model_name, model_dimension, embedding_vector, input_text_hash_blake3)
                            VALUES %s
                            ON CONFLICT (content_id, embedding_name, model_name) DO UPDATE SET
                                embedding_vector = EXCLUDED.embedding_vector,
                                input_text_hash_blake3 = EXCLUDED.input_text_hash_blake3,
                                created_at = NOW();
                            """,
                            all_embeddings_to_insert_db,
                            page_size=len(all_embeddings_to_insert_db) # Upsert all in one go for this batch
                        )
                        print(f"Upserted {len(all_embeddings_to_insert_db)} embeddings into artifact_embeddings.")

                    # Insert new embeddings into embedding_cache
                    if all_cache_entries_to_insert_db:
                        execute_values(
                            cursor,
                            """
                            INSERT INTO embedding_cache
                                (input_text_hash_blake3, model_name, model_dimension, embedding_vector)
                            VALUES %s
                            ON CONFLICT (input_text_hash_blake3, model_name) DO NOTHING; -- Don't update if hash+model already exists
                            """,
                            all_cache_entries_to_insert_db,
                            page_size=len(all_cache_entries_to_insert_db)
                        )
                        print(f"Attempted insert of {len(all_cache_entries_to_insert_db)} entries into embedding_cache.")
                    
                    conn.commit()
                    print(f"Batch committed. Processed {len(rows_to_process)} source items.")

                    if len(rows_to_process) < BATCH_SIZE_ITEMS:
                        print("Processed less than a full batch of items, likely caught up for now.")
                        break 
        
        except psycopg2.Error as db_err:
            if conn: conn.rollback()
            print(f"Database error during embedding backfill: {db_err}")
        except Exception as e:
            if conn: conn.rollback()
            print(f"General error during embedding backfill: {e}")
        finally:
            if conn:
                conn.close()
                print("Database connection closed.")

    // if __name__ == "__main__":
    //     main_embedding_backfill()
    ```
*   **Scheduling:** Run this script as a periodic job (systemd timer or cron). See UG Sec 16.4 / `openai_sinex_6.md` Sec 5 for NixOS systemd timer example.

## 5. Embedding Table Schemas [UG Sec 16.5, Primary Document Part III.3.6 & App A]

### 5.1. `artifact_embeddings`

Stores embeddings for chunks/summaries of `core.artifact_contents`.
```sql
CREATE TABLE IF NOT EXISTS artifact_embeddings (
    content_id              ULID NOT NULL REFERENCES core.artifact_contents(content_id) ON DELETE CASCADE,
    embedding_name          TEXT NOT NULL, -- e.g., "text_chunk_0001", "title_v1", "summary_short_v1.2"
    model_name              TEXT NOT NULL, -- e.g., "all-MiniLM-L6-v2_local_v1", "openai_text-embedding-3-small_api_v1"
    model_dimension         INT NOT NULL,
    embedding_vector        VECTOR,        -- Using pgvector type, dimension matches model_dimension
    input_text_hash_blake3  TEXT NULLABLE, -- BLAKE3 hash of the exact text chunk that was embedded
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (content_id, embedding_name, model_name)
);
COMMENT ON TABLE artifact_embeddings IS 'Vector embeddings for textual content from core_artifact_contents.';
-- Index for ANN search (HNSW chosen per ADR-005)
CREATE INDEX IF NOT EXISTS idx_artifact_embeddings_hnsw_cosine ON artifact_embeddings
  USING hnsw (embedding_vector vector_cosine_ops) -- Or vector_l2_ops if model optimized for L2
  WITH (m = 16, ef_construction = 64); -- Adjust m, ef_construction based on benchmarks
-- Other useful indexes
CREATE INDEX IF NOT EXISTS idx_artifact_embeddings_model_name ON artifact_embeddings (model_name, content_id);
CREATE INDEX IF NOT EXISTS idx_artifact_embeddings_input_hash ON artifact_embeddings (input_text_hash_blake3, model_name) WHERE input_text_hash_blake3 IS NOT NULL;
```

### 5.2. `event_embeddings` (For Direct `raw.events` Payloads)

Stores embeddings for selected textual fields directly from `raw.events.payload`.
```sql
CREATE TABLE IF NOT EXISTS event_embeddings (
    event_id                ULID NOT NULL REFERENCES raw.events(id) ON DELETE CASCADE,
    embedding_name          TEXT NOT NULL, -- e.g., "payload_field_description_chunk_0001", "payload_summary_v1"
    jsonpath_to_text        TEXT NULLABLE, -- JSONPath expression used to extract text from raw.events.payload
    model_name              TEXT NOT NULL,
    model_dimension         INT NOT NULL,
    embedding_vector        VECTOR,
    input_text_hash_blake3  TEXT NULLABLE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, embedding_name, model_name)
);
COMMENT ON TABLE event_embeddings IS 'Vector embeddings for textual content extracted directly from raw.events payloads.';
CREATE INDEX IF NOT EXISTS idx_event_embeddings_hnsw_cosine ON event_embeddings
  USING hnsw (embedding_vector vector_cosine_ops)
  WITH (m = 16, ef_construction = 64);
CREATE INDEX IF NOT EXISTS idx_event_embeddings_model_name ON event_embeddings (model_name, event_id);
CREATE INDEX IF NOT EXISTS idx_event_embeddings_input_hash ON event_embeddings (input_text_hash_blake3, model_name) WHERE input_text_hash_blake3 IS NOT NULL;
```

### 5.3. `embedding_cache`

Deduplicates embedding generation for identical text chunks with the same model.
```sql
CREATE TABLE IF NOT EXISTS embedding_cache (
    input_text_hash_blake3  TEXT NOT NULL,     -- BLAKE3 hash of the exact input text string
    model_name              TEXT NOT NULL,     -- Model used for this cached embedding
    model_dimension         INT NOT NULL,      -- Dimension of the cached vector
    embedding_vector        VECTOR NOT NULL,   -- The cached vector (matching model_dimension)
    first_generated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_accessed_at        TIMESTAMPTZ NOT NULL DEFAULT now(), -- For LRU cache eviction if needed
    access_count            BIGINT NOT NULL DEFAULT 1,
    PRIMARY KEY (input_text_hash_blake3, model_name)
);
COMMENT ON TABLE embedding_cache IS 'Cache for generated embeddings to avoid re-computing for identical text+model.';
-- Trigger to update last_accessed_at on read (might be too much overhead, app-level update might be better)
-- CREATE TRIGGER trg_embedding_cache_update_last_accessed
-- AFTER SELECT ON embedding_cache FOR EACH ROW
-- WHEN (OLD.last_accessed_at < NOW() - INTERVAL '1 hour') -- Only update if accessed recently after a while
-- EXECUTE FUNCTION update_last_accessed_timestamp_func(); -- Needs custom function
```
*Note: Automatically updating `last_accessed_at` via trigger on `SELECT` can be very high overhead. It's often better managed by the application updating it less frequently or if an LRU policy on this table is strictly needed.*

## 6. Chunking Strategies for Long Texts [UG Sec 16.6, Primary Document III.3.6]

Essential because most embedding models have input sequence length limits.

*   **Initial/Simple: Fixed-Size Character or Token Chunks with Overlap:**
    *   Implemented in Python backfill script (`get_text_chunks` function).
    *   Parameters: `CHUNK_SIZE_CHARS`, `CHUNK_OVERLAP_CHARS`.
    *   `embedding_name` in `artifact_embeddings` / `event_embeddings` includes chunk index (e.g., `text_chunk_0001`).
*   **Future/Advanced: Semantic Chunking:**
    *   Divide text based on logical semantic units (paragraphs, sentences, sections).
    *   Methods: Recursive Character Text Splitter (LangChain concept), sentence-based chunking (NLTK/spaCy), Markdown structure chunking (by headings), LLM-aided semantic chunking.
    *   Aims for more coherent chunk embeddings.
*   **Metadata for Chunks:** Store original document ID, chunk sequence number, start/end offsets, surrounding context (e.g., headings) with each chunk's embedding. This is crucial for reconstituting context from retrieved chunks. This metadata can go into `artifact_embeddings.embedding_name` (if encoded) or a separate JSONB column if more structure is needed per embedding.

