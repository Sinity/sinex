# Core Types and Constants

Foundational types, error handling, and constants used throughout the Sinex ecosystem.

## Core Philosophy: Deep Oneness and Auditable Metacognition

Sinex architecture is guided by four pillars:

### 1. Deep Oneness

- One event stream (`core.events`) – no separation between raw and synthesis.
- One processing primitive (`StatefulStreamProcessor`) – all components are stream processors.
- One data lifecycle (Stage → Replay → Synthesis → Curation → Action).

### 2. Declarative Core

- Logic as data, not code.
- Behaviour described through configuration and patterns.
- Imperative code reserved for inherently complex operations.

### 3. Human-in-the-Loop

- Faithful recording of messy reality without premature cleverness.
- Automate resolution where possible, rely on human judgment when necessary.
- Users remain final arbiters of meaning through curation.

### 4. Auditable Metacognition

- Data provenance via `source_event_ids` chains.
- Intent provenance via `core.operations_log`.
- The system remembers not just facts but why it changed its mind.

## Sentient Archive Vision

Sinex implements a “sentient archive” — a system that not only captures but understands and
participates in the user’s digital experience. Through its satellite constellation architecture and
deep philosophical principles, Sinex augments human cognition outside the mind.
