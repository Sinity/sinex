# Competitive Landscape Analysis

> Extracted from analysis sessions (Jan 2026). This is a point-in-time snapshot.

## The Category: Personal Cognitive Infrastructure

Sinex occupies a category that doesn't have a widely-recognized name yet. It's not:
- **Productivity software** — focused on task management, not comprehensive capture
- **Surveillance/lifelogging** — designed for personal sovereignty, not external monitoring
- **Backup software** — captures events and meaning, not just files
- **PKM tools** (Obsidian, Roam) — these are authoring-first, not capture-first

The closest academic precedent is **Gordon Bell's MyLifeBits** (Microsoft Research, 2001) — a research project that captured all digital artifacts but predated modern LLMs that make retrieval tractable.

## Commercial Landscape (as of late 2025)

| Product | Approach | Limitations |
|---------|----------|-------------|
| **Limitless (ex-Rewind.ai)** | Cloud-based screenshot capture | Acquired by Meta (Dec 2025), shutting down apps. Validates cloud-dependent privacy tradeoffs. |
| **Microsoft Recall** | On-device screenshot capture + search | Requires $1000+ Copilot+ hardware with specific NPUs. Screenshots only, not events. |
| **Apple Intelligence** | On-device ML summaries | Walled garden, limited scope, no event sourcing. |
| **ActivityWatch** | Open-source time tracking | Logs metadata only, not comprehensive event capture with provenance. |
| **Screenpipe** | OCR-based capture | No event sourcing, no provenance chain, screenshots not events. |

## What Makes Event Sourcing Different

Most capture tools store **snapshots** (screenshots, file states). Sinex stores **events with provenance**:

1. **Temporal queries** — "What was I doing December 15th at 3pm?" answered from the event log
2. **Replay through new models** — Old events can be re-processed with improved automata
3. **Complete auditability** — Every synthesis traces back to source material bytes
4. **Provenance XOR** — Every event has material OR synthesis provenance, never both or neither

## The Gap

| What Exists | What's Missing | Sinex Approach |
|-------------|----------------|----------------|
| Cloud AI memory | Local-first AI memory | Core architecture: local-first, event-sourced |
| Screenshot capture | Event-sourced capture | Provenance-tracked event stream |
| Document search | System-wide capture | Multi-node constellation |
| Time tracking | Semantic understanding | Automata + future LLM integration |
| PKM organization | Automatic capture | Nodes capture without manual authoring |

## Key Insight

The commercial landscape has validated the thesis: cloud-dependent, venture-backed AI memory products face fundamental conflicts between privacy and business models. Local-first event sourcing avoids this trap.

---

*This analysis is ephemeral — competitive landscape changes rapidly. For architectural decisions, see `../current/architecture/`.*
