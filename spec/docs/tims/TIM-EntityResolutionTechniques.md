# TIM - Entity Resolution Techniques

**Category**: AI Processing  
**Maturity Level**: L2 - Ready for Implementation  
**Implementation Status**: 15% - Database Foundation Present  

## Status Dashboard

### MVP Specification
- [ ] Person entity extraction from events (0%)
- [ ] File/document entity linking (0%)
- [ ] Project/workspace entity identification (0%)
- [ ] Basic entity deduplication algorithms (0%)
- [ ] Entity relationship graph construction (5% - tables exist)

### Enhanced Features  
- [ ] Cross-application entity correlation (0%)
- [ ] Temporal entity evolution tracking (0%)
- [ ] Confidence scoring for entity matches (0%)
- [ ] Machine learning-based entity resolution (0%)
- [ ] Entity lifecycle and state management (0%)

### Implementation Checklist
- [ ] Create `EntityResolutionWorker` in sinex-worker
- [ ] Implement person name extraction from text events
- [ ] Add file path canonicalization and linking
- [ ] Create project/workspace detection algorithms
- [ ] Implement fuzzy matching for entity deduplication
- [ ] Add entity relationship inference
- [ ] Create entity confidence scoring system
- [ ] Integrate with embedding similarity search
- [ ] Add entity temporal tracking
- [ ] Implement entity merge/split operations

## Overview

Entity Resolution identifies, links, and tracks real-world entities (people, files, projects, applications) across the diverse event stream captured by Sinex. This creates a unified knowledge graph that enables sophisticated queries and analysis of user activities and relationships.

## Current Implementation Status

**Verification against codebase:**
- ✅ **Database Infrastructure**: Entity tables exist in migration `20250103120013_create_event_relations_and_annotations.sql`
- ✅ **Knowledge Management**: Tables exist in `20250103120011_create_knowledge_management_tables.sql`
- ✅ **AI Infrastructure**: LLM and embedding tables support entity processing
- ✅ **Vector Search**: pgvector enabled for similarity-based entity matching
- ❌ **Entity Worker**: No entity resolution worker implementation found
- ❌ **Entity Extraction**: No entity extraction logic in existing workers
- ❌ **Resolution Algorithms**: No entity deduplication or matching algorithms

## Motivation

Entity resolution enables powerful capabilities:
- "Show me all activities related to person X"
- "What files are associated with project Y?"
- "Who have I collaborated with on this document?"
- "Track the evolution of this project over time"
- Cross-application workflow understanding

## Technical Requirements

### Core Components

1. **EntityResolutionWorker**
   - Process events for entity extraction
   - Apply resolution algorithms for deduplication
   - Update entity relationship graph
   - Manage entity confidence scores and lifecycle

2. **Entity Extractors**
   - Person name extraction from various text sources
   - File/document canonical path resolution
   - Project/workspace identification from filesystem patterns
   - Application entity recognition

3. **Resolution Algorithms**
   - Fuzzy string matching for name variations
   - Vector similarity for semantic matching
   - Temporal co-occurrence analysis
   - Rule-based entity linking

### Integration Points

- **File System Events**: Extract file entities and project structures
- **Terminal Events**: Extract command entities and user behavior patterns
- **Window Manager Events**: Application and workspace entities
- **Text Content**: Person names, organizations, project references
- **Vector Embeddings**: Semantic similarity for entity matching

## Implementation Architecture

### Worker Structure
```rust
pub struct EntityResolutionWorker {
    pool: PgPool,
    entity_extractors: Vec<Box<dyn EntityExtractor>>,
    resolution_engine: ResolutionEngine,
    confidence_threshold: f64,
}

#[async_trait]
pub trait EntityExtractor: Send + Sync {
    async fn extract_entities(&self, event: &RawEvent) -> Result<Vec<ExtractedEntity>>;
    fn entity_types(&self) -> &[EntityType];
}

pub struct ResolutionEngine {
    fuzzy_matcher: FuzzyMatcher,
    vector_store: VectorStore,
    temporal_analyzer: TemporalAnalyzer,
}
```

### Entity Types
```rust
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum EntityType {
    Person,
    File,
    Directory,
    Project,
    Application,
    Repository,
    Branch,
    Issue,
    PullRequest,
    Organization,
    Meeting,
    Document,
}

#[derive(Serialize, Deserialize)]
pub struct ExtractedEntity {
    pub entity_type: EntityType,
    pub raw_identifier: String,
    pub canonical_name: Option<String>,
    pub confidence: f64,
    pub context: HashMap<String, Value>,
    pub source_event_id: Ulid,
    pub extraction_method: String,
}

#[derive(Serialize, Deserialize)]
pub struct ResolvedEntity {
    pub id: Ulid,
    pub entity_type: EntityType,
    pub canonical_name: String,
    pub alternative_names: Vec<String>,
    pub attributes: HashMap<String, Value>,
    pub first_seen: OffsetDateTime,
    pub last_seen: OffsetDateTime,
    pub confidence_score: f64,
    pub merge_history: Vec<EntityMerge>,
}
```

## Entity Extraction Strategies

### Person Entities
```rust
pub struct PersonExtractor {
    name_patterns: Vec<Regex>,
    context_analyzers: Vec<ContextAnalyzer>,
}

impl PersonExtractor {
    // Extract from email signatures, git commits, chat messages
    // Use context clues: @mentions, email addresses, signatures
    // Handle name variations: "John Smith", "J. Smith", "john.smith@company.com"
}
```

### File/Document Entities
```rust
pub struct FileEntityExtractor {
    path_canonicalizer: PathCanonicalizer,
    project_detector: ProjectDetector,
}

impl FileEntityExtractor {
    // Canonicalize paths: resolve symlinks, relative paths
    // Detect project boundaries: git repos, package.json, etc.
    // Link files to projects and track renames/moves
}
```

### Project/Workspace Entities
```rust
pub struct ProjectExtractor {
    workspace_detectors: Vec<WorkspaceDetector>,
    project_markers: HashMap<String, ProjectType>,
}

impl ProjectExtractor {
    // Detect project types: git repos, cargo projects, npm packages
    // Identify workspace boundaries and hierarchies
    // Track project evolution and relationships
}
```

## Resolution Algorithms

### Fuzzy String Matching
```rust
pub struct FuzzyMatcher {
    pub fn similarity_score(&self, s1: &str, s2: &str) -> f64;
    pub fn is_likely_match(&self, s1: &str, s2: &str, threshold: f64) -> bool;
}

// Algorithms:
// - Levenshtein distance for typos
// - Jaro-Winkler for name variations  
// - Token-based matching for reordered names
// - Phonetic matching (Soundex, Metaphone)
```

### Vector Similarity Matching
```rust
pub struct VectorMatcher {
    pub async fn find_similar_entities(
        &self, 
        entity: &ExtractedEntity, 
        threshold: f64
    ) -> Result<Vec<(ResolvedEntity, f64)>>;
}

// Use existing vector embeddings infrastructure
// Generate embeddings for entity names and contexts
// Find semantically similar entities across time
```

### Temporal Co-occurrence Analysis
```rust
pub struct TemporalAnalyzer {
    pub fn analyze_co_occurrence(
        &self,
        entities: &[ExtractedEntity],
        time_window: Duration,
    ) -> Vec<EntityRelationship>;
}

// Entities mentioned together are likely related
// Temporal patterns indicate entity relationships
// Co-occurrence confidence scoring
```

## Configuration

### Basic Configuration
```toml
[entity_resolution]
enabled = true
processing_batch_size = 100
confidence_threshold = 0.7
max_alternatives_per_entity = 10

[entity_resolution.extractors]
enable_person_extraction = true
enable_file_extraction = true
enable_project_extraction = true
enable_application_extraction = true

[entity_resolution.fuzzy_matching]
person_name_threshold = 0.8
file_path_threshold = 0.9
project_name_threshold = 0.75
enable_phonetic_matching = true

[entity_resolution.temporal_analysis]
co_occurrence_window_hours = 24
relationship_confidence_threshold = 0.6
max_relationships_per_entity = 50

[entity_resolution.privacy]
anonymize_person_names = false
exclude_sensitive_paths = ["/home/user/.ssh", "/home/user/.gnupg"]
redact_email_addresses = false
```

## Database Integration

### Existing Schema Usage
```sql
-- Leverage existing tables from migrations:

-- core.entities (from 20250103120013)
-- Stores resolved entities with canonical names

-- core.entity_relations (from 20250103120013)  
-- Tracks relationships between entities

-- core.event_annotations (from 20250103120013)
-- Links events to identified entities

-- core.artifact_embeddings (from 20250103120012)
-- Vector embeddings for semantic matching
```

### Entity Resolution Jobs
```sql
CREATE TABLE IF NOT EXISTS core.entity_resolution_jobs (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    event_id ulid NOT NULL REFERENCES raw.events(id),
    status TEXT NOT NULL DEFAULT 'pending' 
        CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    extracted_entities JSONB,
    resolved_entities JSONB,
    confidence_scores JSONB,
    processing_started_at TIMESTAMPTZ,
    processing_completed_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

## Privacy Considerations

### Entity Privacy Levels
- **Public**: Project names, public repositories, open source contributors
- **Internal**: Company colleagues, shared documents, team projects
- **Personal**: Family names, personal files, private communications
- **Sensitive**: Health records, financial information, legal documents

### Privacy Controls
- Configurable anonymization of person names
- Entity extraction exclusion lists
- Redaction of sensitive attribute values
- Opt-out mechanisms for specific entity types

### Data Minimization
- Store only necessary attributes for resolution
- Time-based expiration of low-confidence entities
- Aggregation over detailed tracking where possible

## Performance Considerations

### Processing Efficiency
- Batch processing of events for entity extraction
- Incremental updates to entity graph
- Caching of resolution decisions
- Priority processing for high-confidence extractions

### Scalability Patterns
- Parallel processing of independent entity extractions
- Distributed resolution across multiple workers
- Hierarchical entity clustering for large datasets
- Lazy loading of entity relationships

### Resource Management
- Memory-efficient fuzzy matching algorithms
- Disk-based storage for large entity graphs
- Connection pooling for database operations
- Monitoring of processing queue lengths

## Testing Strategy

### Unit Tests
- Entity extraction algorithms for various text formats
- Fuzzy matching accuracy across name variations
- Vector similarity matching validation
- Confidence scoring consistency

### Integration Tests
- End-to-end entity resolution pipeline
- Cross-entity-type relationship inference
- Database persistence and retrieval
- Worker queue processing validation

### System Tests
- Large-scale entity resolution across historical data
- Performance testing with realistic event volumes
- Accuracy validation against ground truth datasets
- Privacy compliance verification

## Success Metrics

### Accuracy Metrics
- >85% precision for person name extraction
- >90% recall for file entity identification  
- >80% accuracy for entity deduplication
- <5% false positive rate for entity relationships

### Performance Metrics
- <10 seconds processing time per 1000 events
- <500MB memory usage for resolution worker
- >99% availability for entity resolution service
- <1 hour lag for entity graph updates

### Quality Metrics
- Consistent confidence scoring across entity types
- Stable entity identifiers over time
- Meaningful relationship detection without noise
- Effective handling of entity evolution and changes

## Dependencies

### System Requirements
- **PostgreSQL with pgvector**: Vector similarity search
- **Sufficient RAM**: For in-memory entity caches and fuzzy matching
- **Disk Storage**: Entity graph persistence and processing queues

### Rust Crates
- `fuzzy-matcher` - Fuzzy string matching algorithms
- `regex` - Pattern matching for entity extraction
- `phonetics` - Phonetic matching algorithms
- `temporal-collections` - Time-based entity analysis

### External Resources
- Person name databases for validation
- Common project naming patterns
- File type and extension mappings
- Organization and company name lists

## Future Enhancements

### Advanced Resolution
- Machine learning models for entity classification
- Graph neural networks for relationship inference
- Active learning for resolution accuracy improvement
- Federated entity resolution across multiple Sinex instances

### Domain-Specific Extensions
- Academic entity resolution (papers, authors, institutions)
- Software development entities (APIs, libraries, frameworks)
- Geographic entity resolution (locations, addresses)
- Business entity resolution (customers, vendors, projects)

### Integration Opportunities
- Knowledge graph visualization and exploration
- Entity-based search and filtering interfaces
- Cross-temporal entity evolution analysis
- Entity-driven workflow and productivity insights

## References

- [Entity Resolution Survey](https://dl.acm.org/doi/10.1145/1667062.1667072)
- [Fuzzy String Matching Algorithms](https://ieeexplore.ieee.org/document/4016511)
- [Knowledge Graph Entity Resolution](https://arxiv.org/abs/1909.06181)
- [Temporal Entity Linking](https://www.aclweb.org/anthology/D19-1049/)