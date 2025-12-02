# Entity Resolution and Knowledge Graph Construction

**Status**: Designed, not implemented
**Implementation**: 15% (Database schema exists, algorithm implementation needed)
**Priority**: High
**Dependencies**: PostgreSQL pg_trgm extension, entity tables, NLP libraries, embedding models
**Blocks**: Knowledge graph construction, entity linking, content understanding, automated relationships

## Overview

Entity Resolution (ER) is crucial for building the Sinex Knowledge Graph (`core.entities`, `core.entity_relations`). It transforms unstructured text from PKM notes, web archives, and event payloads into structured, interconnected knowledge by identifying and linking named entities. This enables richer contextual queries, automated linking, and deeper understanding.

## Technical Specification

### PostgreSQL pg_trgm for Fuzzy Matching

Fast, in-database fuzzy string matching using trigrams:

**Extension Setup**: `CREATE EXTENSION IF NOT EXISTS pg_trgm;`

**Key Functions**:
- `similarity(text1, text2)`: Returns float (0-1) similarity
- `text1 % text2`: True if similarity > threshold (default 0.3)
- `word_similarity(text1, text2)`: Based on common trigrams within words
- Distance operators: `<->` (trigram distance) for k-NN search

**Recommended Thresholds**:
- Person Names: `similarity >= 0.6`
- Organizations: `similarity >= 0.4`
- Requires tuning based on data

**Required Indexes**:
```sql
CREATE INDEX idx_core_entities_label_trgm_gin ON core.entities
  USING GIN (canonical_label gin_trgm_ops);
CREATE INDEX idx_core_entities_aliases_trgm_gin ON core.entities
  USING GIN (aliases gin_trgm_ops);
```

Performance: 50-200ms on ~1M entity records with GIN indexes.

### Advanced Fuzzy Matching (fuzzystrmatch)

Supplements pg_trgm for re-ranking candidates:

**Extension**: `CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;`

**Algorithms**:
- Levenshtein Distance: Edit distance for typo tolerance
- Phonetic Algorithms:
  - soundex(text)
  - metaphone(text, max_output_len)
  - dmetaphone(text) - Double Metaphone

### Machine Learning Approaches

#### spaCy Integration
- Pre-trained NER models for entity extraction
- Rule-based matching (Matcher, PhraseMatcher)
- EntityLinker component for Sinex KB

**Workflow**:
1. Process text with spaCy NER pipeline → extract mentions
2. Generate candidates from `core.entities` using pg_trgm
3. Use EntityLinker or custom re-ranking model
4. Select best candidate above confidence threshold

#### Transformer Models
- Hugging Face NER models (BERT, RoBERTa based)
- Bi-Encoder for candidate retrieval
- Cross-Encoder for accurate re-ranking

### Blocking Strategies for Scalability

Reduce comparison space by >95% for >1M entities:

**Techniques**:
- **Exact Match**: On normalized first word or prefix
- **Phonetic Blocking**: Group by Soundex/Metaphone codes
- **Sorted Neighborhood Method (SNM)**:
  - Create blocking key (e.g., first 5 chars + Soundex)
  - Sort entities by key
  - Compare only within fixed window
- **Canopy Clustering**: Fast approximate similarity for loose clusters
- **Locality Sensitive Hashing (LSH)**: Hash for similar entities in same bucket

## Entity Resolution Agent Architecture

### Processing Pipeline

1. **NER Phase**:
   - Extract mentions using spaCy or Transformers
   - Identify entity type (PERSON, ORG, etc.)

2. **Blocking/Candidate Generation**:
   - Retrieve candidates using pg_trgm
   - Filter by predicted entity type
   - Apply blocking strategies for large KBs
   - Use vector similarity if embeddings available

3. **Scoring/Re-ranking**:
   - Advanced fuzzy string similarity
   - Contextual similarity (embeddings)
   - Cross-encoder for top candidates
   - Heuristics (entity popularity, prior links)

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

### Review Queue Interface
- Ambiguous mentions flagged for review
- UI presents mention in context with top candidates
- Reviewer actions:
  - Confirm match
  - Reject all candidates
  - Create new entity
  - Merge duplicate entities

### Decision Tracking
- Log all review decisions for audit trail
- Track reviewer actions in `entity_resolution_review_queue` table
- Use decisions for active learning

### Active Learning
Human decisions provide labeled data for:
- Fine-tuning ML models
- Adjusting similarity thresholds
- Improving blocking rules
- Uncertainty sampling for review prioritization

## Implementation Plan

### Phase 1: Basic Infrastructure
- [ ] Install pg_trgm and fuzzystrmatch extensions
- [ ] Create fuzzy matching indexes
- [ ] Implement basic string similarity matching
- [ ] Simple entity linking pipeline

### Phase 2: NER Integration
- [ ] spaCy pipeline setup
- [ ] NER model selection and configuration
- [ ] Entity type filtering
- [ ] Mention extraction from events

### Phase 3: Advanced Matching
- [ ] Blocking strategy implementation
- [ ] Phonetic matching integration
- [ ] Confidence scoring system
- [ ] Candidate ranking algorithms

### Phase 4: Human Review System
- [ ] Review queue tables and API
- [ ] Web interface for entity review
- [ ] Decision tracking system
- [ ] Active learning feedback loop

### Phase 5: Scale Optimization
- [ ] Distributed processing for batch ER
- [ ] LSH implementation for large KBs
- [ ] Performance benchmarking
- [ ] Real-time streaming ER

## Performance Targets

- **Throughput**: 1000+ entities/second for batch processing
- **Latency**: <500ms for single entity resolution
- **Accuracy**: >90% precision at >85% recall
- **Scale**: Handle 10M+ entities in knowledge base

## Future Enhancements

### Advanced Features
- Multi-modal entity extraction (text, images, audio)
- Cross-lingual entity resolution
- Temporal entity tracking (name changes over time)
- Relationship inference from co-occurrence

### Integration Points
- **Embeddings**: Use semantic similarity for candidate ranking
- **LLM Integration**: Entity disambiguation with language models
- **Knowledge Graph**: Automatic relationship discovery
- **Search**: Entity-aware query understanding