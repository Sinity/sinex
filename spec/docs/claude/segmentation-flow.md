# Activity Segmentation: Candidate/Final Flow

## Overview

The activity segmentation system resolves potentially overlapping activity periods from multiple agents into a clean, non-conflicting timeline of user activity segments.

## Data Flow

### 1. Candidate Generation

Agents emit `activity.segment_candidate` events with:
- `start`/`end` timestamps 
- `label` (activity description)
- `algo` (algorithm/agent identifier)
- `confidence` score
- `metadata` (source-specific details)

**Characteristics:**
- Multiple agents can emit overlapping candidates
- Same time period may have conflicting labels
- Different granularities (fine vs coarse segments)

### 2. Resolution Process

A dedicated resolver agent processes candidates to emit `activity.segment_final` events:

**Conflict Resolution:**
- Overlapping segments: higher confidence wins
- Tie-breaking: algorithm priority, recency, specificity
- Merge compatible adjacent segments

**Output Fields:**
- `start`/`end` (resolved boundaries)
- `label` (chosen activity)
- `confidence` (derived from candidates)
- `derived_from[]` (array of candidate IDs)
- `supersedes[]` (previous finals this replaces)

### 3. Timeline Reconstruction

UI/query layer uses `activity.segment_final` events:
- Ordered by `start` timestamp
- Non-overlapping guarantees
- Provenance via `derived_from` links
- Change history via `supersedes` chains

## Benefits

1. **Clean Timeline**: UI shows resolved, non-conflicting segments
2. **Agent Independence**: Each agent focuses on its domain expertise  
3. **Transparency**: Full provenance from candidates to finals
4. **Incremental**: New candidates can trigger re-resolution
5. **Versioning**: Historical timeline states preserved

## Example

```
Candidates:
- Agent A: 09:00-10:00 "coding" (conf: 0.8)
- Agent B: 09:30-10:30 "meeting" (conf: 0.9) 
- Agent C: 09:45-10:15 "video-call" (conf: 0.7)

Resolution:
- Final 1: 09:00-09:30 "coding" (from A)
- Final 2: 09:30-10:30 "meeting" (from B, supersedes C)
```

This provides a clean separation between detection (candidates) and resolution (finals) while maintaining full audit trails.