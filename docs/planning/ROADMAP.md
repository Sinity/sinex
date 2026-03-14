# Sinex Long-Term Roadmap

## Core Vision Documents

These documents contain detailed brainstorming and speculative designs for major, unimplemented features. They are preserved here to ensure the project's long-term vision is not lost.

- [**Semantic Desktop Stream**](../vision/semantic-desktop-stream.md): A detailed exploration of how Sinex could build a real-time, semantic understanding of the user's context.
- [**Multi-Device Sync Architecture**](../vision/multi-device-sync-architecture.md): A deep dive into the challenges and potential solutions for synchronizing Sinex data across multiple devices.
- [**Project Target State**](../vision/project-target-state.md): A high-level planning document outlining the ultimate goals and target state of the project.
- [**Emergent Insights and Speculative Extensions**](../vision/emergent-insights-and-extensions.md): A collection of brainstorming and speculative ideas for future features and enhancements.


See also: Explore experience milestones in [explore-ux-roadmap.md](./explore-ux-roadmap.md).

## Exploratory Directions Worth Keeping In Mind

These are plausible future directions, but not execution-ready plans:

- **Datasette / Dogsheep interoperability**: useful mainly as a portability and exploration
  story, not as a replacement for the primary PostgreSQL-backed system. The viable angle is
  export/import tooling or an optional exploration surface, not a parallel canonical storage
  model.
- **Rapid assembly estimates**: the general lesson is that browser capture, embeddings, and
  similar additions are mechanically approachable once the core pipeline is stable, but old
  line-count estimates are not durable planning artifacts and should not drive sequencing.

## Far Future Optimizations

These enhancements require significant effort and should only be considered after core refactoring is complete and performance bottlenecks are identified through real-world usage.

### pgrx - PostgreSQL Extensions

**When to consider**: Only if query performance becomes a demonstrable bottleneck

Potential use cases:
- Custom aggregation functions for event analytics
- Graph traversal algorithms for provenance queries
- Pattern matching across event sequences
- Custom index types optimized for event queries

**Required evidence before implementation**:
- Benchmark data showing query bottlenecks
- Specific queries that can't be optimized with standard PostgreSQL
- Cost/benefit analysis vs. alternative solutions

### roaring-rs - Compressed Bitmaps

**When to consider**: When tracking millions of event IDs in memory

Example use case - checkpoint tracking:
```rust
// Current: HashSet<Id<Event>>
let mut processed: HashSet<Id<Event>> = HashSet::new();

// With roaring: Compressed bitmap
let mut processed = RoaringBitmap::new();
processed.insert(event_id.to_u64());
```

**Required evidence before implementation**:
- Memory profiling showing HashSet overhead
- Actual workloads with millions of IDs
- Performance benchmarks comparing approaches

### zerocopy - Binary Serialization

**When to consider**: If JSON serialization becomes a bottleneck

Potential benefits:
- Zero-copy deserialization
- Direct memory mapping
- Reduced allocation overhead

**Trade-offs**:
- Loss of human-readable format
- More complex debugging
- Version alignment challenges

### Advanced Analytics Integration

#### hydroflow - Dataflow Programming
- Alternative to imperative automata
- Formal verification possibilities
- Requires complete architecture rethink

#### Machine Learning Libraries
- linfa for classical ML (clustering, classification)
- candle/tch-rs for neural networks
- Local embedding generation

**When to consider**: After establishing clear use cases and data patterns

## Speculative Features

### 3D Visualization with Bevy
- Knowledge graph visualization
- Real-time activity "digital twin"
- Requires significant UI/UX design work

### Distributed Sinex
- Multi-machine event capture
- Federated query capabilities
- Consensus and synchronization challenges
