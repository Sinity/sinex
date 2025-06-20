# TIM-EntityResolutionTechniques: Entity Resolution in Exocortex

## Status Dashboard
**Maturity Level**: L2 - Ready for Implementation
**Implementation**: 15% (Database schema exists, algorithm implementation needed)
**Dependencies**: PostgreSQL pg_trgm extension, entity tables, NLP libraries, embedding models
**Blocks**: Knowledge graph construction, entity linking, content understanding, automated relationships

## MVP Specification
- Named Entity Recognition (NER) for common entity types
- Fuzzy string matching using PostgreSQL pg_trgm
- Basic entity linking to canonical entries
- Simple disambiguation and confidence scoring
- Integration with knowledge graph tables

## Enhanced Features
- Advanced NER models and custom entity types
- Sophisticated disambiguation algorithms
- Cross-reference validation and relationship inference
- Temporal entity resolution and tracking
- Multi-modal entity extraction (text, images, audio)
- Active learning and user feedback integration

## Implementation Checklist
- [ ] PostgreSQL pg_trgm extension setup
- [ ] NER pipeline implementation
- [ ] Fuzzy matching and candidate generation
- [ ] Entity linking and disambiguation algorithms
- [ ] Integration with core.entities tables
- [ ] Confidence scoring and validation
- [ ] Performance optimization for large datasets
- [ ] Cross-reference relationship inference
- [ ] User feedback and correction mechanisms

*   **Relevant ADR:** (N/A directly, implements core knowledge graph building)
*   **Original UG Context:** Section 19
*   **Vision Document Reference:** Part I.3 Principle 6 (NER implied), Part III.3.5 (Knowledge Graph)

This TIM details techniques for Entity Resolution (ER) within the Exocortex, which involves Named Entity Recognition (NER - identifying mentions of entities like persons, organizations, projects, topics in text) and Entity Linking (EL - disambiguating these mentions and linking them to canonical entries in `core_entities`).

## 1. Rationale Summary

Entity Resolution is crucial for building the Exocortex Knowledge Graph (`core_entities`, `core_entity_relations`). It transforms unstructured text (from PKM notes, web archives, event payloads) into structured, interconnected knowledge by identifying and linking named entities. This enables richer contextual queries, automated linking, and deeper understanding.

## 2. PostgreSQL `pg_trgm` for Fuzzy Candidate Matching [UG Sec 19.1, CR4]

Provides fast, in-database fuzzy string matching using trigrams. Used as a primary method for generating candidate entities from `core_entities` that match a textual mention.

*   **Extension:** `CREATE EXTENSION IF NOT EXISTS pg_trgm;`
*   **Key Functions/Operators:**
    *   `similarity(text1, text2)`: Returns float (0-1) similarity.
    *   `text1 % text2`: True if `similarity > threshold` (default 0.3, set via `SET pg_trgm.similarity_threshold = X;`).
    *   `word_similarity(text1, text2)`: Based on common trigrams within words.
    *   Distance operators: `<->` (trigram distance), `<<->>` (word trigram distance). Use with `ORDER BY ... LIMIT N` for k-NN search.
*   **Recommended Thresholds for Candidate Generation [CR4]:**
    *   Person Names: `similarity(mention, entity.canonical_label) >= 0.6`
    *   Organizations: `similarity(mention, entity.canonical_label) >= 0.4`
    *   (These need tuning based on data.)
*   **Indexing (GIN with `gin_trgm_ops`):** Essential for performance on `core_entities.canonical_label` and `core_entities.aliases`.
    ```sql
    -- On core_entities table
    -- CREATE INDEX IF NOT EXISTS idx_core_entities_label_trgm_gin ON core.entities
    --   USING GIN (canonical_label gin_trgm_ops);
    -- CREATE INDEX IF NOT EXISTS idx_core_entities_aliases_trgm_gin ON core.entities
    --   USING GIN (aliases gin_trgm_ops); -- If aliases is TEXT[]
    ```
*   **Performance [CR4]:** 50-200ms on ~1M entity records with GIN indexes.

## 3. Advanced Fuzzy Matching Algorithms (`fuzzystrmatch` extension) [UG Sec 19.2, CR4]

Supplements `pg_trgm` for re-ranking candidates or as part of a composite similarity score.
*   **Extension:** `CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;`
*   **Algorithms:**
    *   **Levenshtein Distance:** `levenshtein(text1, text2)` (edit distance). Good for typos.
    *   **Phonetic Algorithms:**
        *   `soundex(text)`
        *   `metaphone(text, max_output_len)`
        *   `dmetaphone(text)`, `dmetaphone_alt(text)` (Double Metaphone)
        *   Use: Pre-filter candidates by matching phonetic codes, or include phonetic similarity in scoring.

## 4. Machine Learning Approaches for NER and EL [UG Sec 19.3, CR5]

ML models leverage contextual information for more accurate NER and EL.

### 4.1. spaCy

*   Python NLP library with pre-trained NER models.
*   Rule-based matching (`Matcher`, `PhraseMatcher`).
*   `EntityLinker` component (can be trained/configured for Exocortex `core_entities` KB).
*   **Workflow (Conceptual for Exocortex ER Agent):**
    1.  Process input text (e.g., from `core_artifact_contents`) with spaCy NER pipeline -> extracts mentions (text, offsets, entity type label like `PERSON`, `ORG`).
    2.  For each mention:
        a.  Generate candidates from `core_entities` using `pg_trgm` (filtered by predicted entity type) and/or vector similarity on `core_entities.embedding_vector` (if available, embedding the mention's context).
        b.  Use spaCy `EntityLinker` (if trained on `core_entities` as KB) or a custom re-ranking model to score candidates based on mention-candidate similarity and context-candidate coherence.
    3.  Select best candidate above confidence threshold.

### 4.2. Hugging Face Transformers

*   Access to many pre-trained NER models (BERT, RoBERTa based) and sentence embedding models.
*   **NER:** Use `transformers` pipeline for NER (e.g., `pipeline("ner", model="dbmdz/bert-large-cased-finetuned-conll03-english")`).
*   **Candidate Retrieval (Bi-Encoder):**
    1.  Embed context around mention (e.g., sentence containing mention) using SentenceTransformer model (e.g., `all-MiniLM-L6-v2`).
    2.  Embed `canonical_label` + `description` for candidate entities from `core_entities`.
    3.  Rank candidates by cosine similarity.
*   **Re-ranking (Cross-Encoder):**
    1.  Take top N candidates from bi-encoder/`pg_trgm`.
    2.  Use a cross-encoder model (e.g., `cross-encoder/ms-marco-MiniLM-L-6-v2`). Input is `(mention_context_sentence, candidate_entity_description_or_label)`. Output is a more accurate similarity score. Slower but better for final re-ranking.

## 5. Blocking Strategies for Scalability (Candidate Generation) [UG Sec 19.4, CR4, CR5]

Crucial first step to reduce comparison space for large `core_entities` KBs. Can reduce comparisons by >95% for >1M entities [CR4].

*   **Techniques:**
    *   **Exact Match on Indexed Key Fields [CR4]:** E.g., match on normalized first word or prefix.
    *   **Phonetic Blocking [CR4]:** Group `core_entities` by Soundex/Metaphone codes. Compare mention only against entities in same phonetic bucket(s).
    *   **Sorted Neighborhood Method (SNM) [CR5]:**
        1.  Create "blocking key" (e.g., first 5 chars normalized name + Soundex).
        2.  Sort `core_entities` by this key.
        3.  For new mention, compare only against entities within a fixed window around its position in sorted list.
    *   **Canopy Clustering [CR5]:** Use fast approximate similarity (e.g., Jaccard on q-grams) to create loose "canopies" (clusters). Compare mention only against entities in canopies it falls into.
    *   **Locality Sensitive Hashing (LSH):** Hash entities (e.g., MinHash on shingles) so similar entities likely map to same LSH bucket.

## 6. Distributed Processing (Spark, Flink) for Batch ER [UG Sec 19.5, CR5]

For very large-scale *batch* ER (initial KB population, bulk import, global re-resolution).
*   **Apache Spark:** Distributed batch processing. Can implement blocking and pairwise similarity. Libraries like **Splink** provide probabilistic record linkage on Spark.
*   **Apache Flink:** Distributed stream processing. For *real-time/streaming* ER of incoming mentions against large, dynamic `core_entities`.

## 7. Human Review Workflows and Active Learning [UG Sec 19.6, CR4, CR5]

Essential for high data quality.
*   **Resolution Queue / Review Interface:**
    *   Mentions unresolved with high confidence (e.g., scores in ambiguous range 0.7-0.85 [CR4], multiple high-score candidates, no good candidates) are flagged for human review.
    *   UI presents mention in context, top N candidates. Reviewer can: confirm match, reject all, create new entity, merge candidates.
    *   A `core.entity_resolution_review_queue` table might store these.
*   **Decision Tracking & Audit Trail [CR4]:** Log all human review decisions.
*   **Active Learning [CR5]:** Human review decisions are labeled data. Use to:
    *   Fine-tune ML models (re-rankers, NER classifiers).
    *   Adjust similarity thresholds.
    *   Improve blocking rules.
    *   Prioritize items for review where system is least confident (uncertainty sampling).

## 8. Exocortex Entity Resolution Agent (`agent/entity_resolver`)

*   **Triggers:** Consumes new textual content (from `core_artifact_contents`, specific `raw.events.payload` fields).
*   **Pipeline:**
    1.  **NER:** Extract mentions (spaCy or Transformers model).
    2.  **Blocking/Candidate Generation:** For each mention, retrieve candidate entities from `core_entities` using `pg_trgm` (filtered by predicted entity type) and/or other blocking strategies (phonetic, LSH if `core_entities` is very large). If `core_entities` has embeddings, also use vector similarity for candidates.
    3.  **Scoring/Re-ranking:** Score candidates using:
        *   Advanced fuzzy string similarity (Levenshtein on normalized labels).
        *   Contextual similarity (embedding of mention context vs. candidate entity embedding/description, possibly with cross-encoder for top few).
        *   Heuristics (e.g., popularity of entity, prior links).
    4.  **Linking/Decision:**
        *   If a single candidate scores above a high confidence threshold (e.g., >0.9): Automatically link.
            *   Create/update `core_entity_relations` entry (e.g., type `mentions_entity` linking source `artifact_id` or `event_id` to `target_entity_id`).
            *   Log `knowledge_graph.relation_created` event.
        *   If scores are ambiguous or no candidate above low threshold: Send to human review queue.
        *   If mention refers to a clearly new entity (no plausible candidates, high novelty score from NER): Propose creating new entry in `core_entities` (possibly via review queue).
*   **Eventification:** The agent logs its actions (links created, entities proposed, items sent to review) as `sinex.agent.entity_resolution_processed` events.

