# ADR-005: Vector Index Type for `pgvector`

*   **Status:** Implemented
*   **Date:** 2024-03-11
*   **Implementation Date:** 2025-07-17
*   **Context & Problem Statement:**
    The Exocortex will use the `pgvector` extension in PostgreSQL for storing and searching vector embeddings (e.g., in `artifact_embeddings`). `pgvector` supports several index types for Approximate Nearest Neighbor (ANN) search. The choice of index type significantly impacts query speed, recall accuracy, build time, memory usage, and how well the index handles dynamic data (inserts/updates/deletes). The two primary contenders are HNSW (Hierarchical Navigable Small World) and IVFFlat (Inverted File with Flat Compression).

*   **Discussed Options:**

    1.  **HNSW (Hierarchical Navigable Small World):**
        *   **Description:** A graph-based ANN index. Nodes in the graph are data points (vectors), and edges connect nearest neighbors. Search involves traversing this graph. It's a multi-layered graph where upper layers have longer links for faster coarse searching, and lower layers have shorter links for fine-grained searching.
        *   **Pros:**
            *   **High Recall & Query Speed:** Generally offers excellent recall and fast query performance, often outperforming IVFFlat in balanced scenarios [SR1].
            *   **Good for Dynamic Data:** Handles insertions of new vectors relatively well without requiring full index rebuilds to maintain performance. Deletions are also supported.
            *   **Tunable Parameters:** `m` (connections per node) and `ef_construction` (candidate list size during build) control index quality/build time. `ef_search` (candidate list size during query) controls query speed/recall trade-off.
        *   **Cons:**
            *   **Longer Build Times:** Typically takes significantly longer to build an HNSW index compared to IVFFlat [SR1].
            *   **Higher Memory Usage:** HNSW indexes tend to consume more RAM than IVFFlat for the same dataset [SR1]. The index often needs to be largely in memory for good performance.
            *   **Parameter Tuning:** Requires some experimentation with `m`, `ef_construction`, and `ef_search` to find optimal settings for a given dataset and workload.

    2.  **IVFFlat (Inverted File with Flat Compression):**
        *   **Description:** An inverted file index. During build, vectors are clustered using k-means into `lists` (partitions/clusters). The centroids of these clusters are stored. A query vector is first compared to cluster centroids to find the `probes` (number of) nearest clusters. Then, an exact search is performed only within the vectors belonging to those probed clusters. "Flat" means no quantization/compression is applied to the vectors within the lists (unlike IVFADC or IVFPQ).
        *   **Pros:**
            *   **Faster Build Times:** Generally faster to build than HNSW, especially if `lists` is not excessively large.
            *   **Lower Memory Usage:** Typically more memory-efficient than HNSW.
            *   **Simple Concept:** Easier to understand conceptually.
        *   **Cons:**
            *   **Recall/Speed Trade-off via `probes`:** Query performance and recall are highly dependent on the `probes` parameter. Low `probes` is fast but low recall; high `probes` is slower but better recall. Finding the right balance can be tricky.
            *   **Performance Degradation with Dynamic Data:** Index quality can degrade over time with many insertions if the initial k-means clustering (centroids) is not updated. May require periodic full re-indexing on highly dynamic datasets to maintain optimal performance.
            *   **Parameter Tuning (`lists`):** The number of `lists` needs to be chosen carefully based on dataset size (e.g., `N/1000` or `sqrt(N)` [SA1]). Poor choice can significantly impact performance.
            *   **Curse of Dimensionality:** Can suffer more in very high-dimensional spaces compared to HNSW [SA1].

*   **Decision:**
    **HNSW (Hierarchical Navigable Small World)** will be the default and recommended index type for vector embeddings stored using `pgvector` in the Exocortex (e.g., on `artifact_embeddings.embedding_vector`).
    *   Initial recommended HNSW parameters (tunable based on benchmarks with Exocortex data):
        *   `m = 16` (or up to `24` [CR3] if recall needs are very high and memory allows)
        *   `ef_construction = 64` (or up to `200` [CR3] for higher build quality if build time is not critical)
    *   Query-time `ef_search` will be tuned based on desired recall/latency trade-off (e.g., default `40`, increased when metadata filters are applied as per SA1).

*   **Rationale for Decision:**
    1.  **Balanced Performance for Target Workload:** For the Exocortex, which will have dynamically growing embedding sets and requires good recall for semantic search, HNSW generally offers a better overall balance of query speed, recall accuracy, and ability to handle incremental data changes without frequent full rebuilds [SR1, SA1].
    2.  **Recall Priority:** High recall is important for semantic search in a personal knowledge base to ensure relevant items are not missed. HNSW typically provides better recall at reasonable query speeds compared to IVFFlat with a small number of probes.
    3.  **Handling Dynamic Data:** The Exocortex will continuously ingest new content and generate new embeddings. HNSW's ability to gracefully handle insertions is a significant advantage over IVFFlat, which might require periodic, potentially disruptive, re-indexing to maintain cluster quality.
    4.  **Mature and Improving `pgvector` Support:** HNSW is a well-supported and actively developed index type in `pgvector`.

*   **Consequences:**
    *   Longer initial build times for HNSW indexes on large existing datasets compared to IVFFlat. This is primarily a one-time or infrequent cost.
    *   Higher memory footprint for HNSW indexes. System RAM for PostgreSQL must be provisioned accordingly.
    *   Careful selection and potential tuning of `m`, `ef_construction`, and `ef_search` parameters will be needed as the dataset grows and query patterns emerge.
    *   IVFFlat remains an option for specific niche use cases within the Exocortex if its characteristics (e.g., extremely fast build for a static dataset segment, very low memory for a less critical index) are specifically required, but HNSW is the default.

