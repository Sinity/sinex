----

> **Historical note:** Legacy vision document predating the JetStream-only rollout. Concepts like sensd/gRPC/outbox remain here for reference; see `docs/way.md` for current architecture.

**Sinnix Exocortex: The Sentient Archive (Definitive Document v2.1)**
=====================================================================

**Table of Contents (Conceptual - will be reflected in document structure)**
---

* **Foreword: The Imperative of Cognitive Sovereignty**
* **Part I: The Exocortex Covenant – Manifesto, Principles, and Human Context**
  * 1.1. Against Digital Oblivion: A Manifesto for Personal Data Continuity
  * 1.2. The Exocortex Pledge: Core Visionary Commitments
  * 1.3. Foundational Principles of the Sentient Archive
  * 1.4. Human Context: Designing for Real Minds
  * 1.5. Confronting the Developer's Existential Loop: Why Build in the Age of AI?
* **Part II: The Core Cognitive Habitats – Living Document & Personal Knowledge Management**
  * 2.1. The Living Document: Your Active Cognitive Workspace
    * 2.1.1. Philosophy & Concept: Beyond Static Notes – An Externalized, Persistent Working Memory
    * 2.1.2. Core Architecture & Mechanics: An Event-Sourced, Agent-Driven System
    * 2.1.3. Interaction Model & User Experience
    * 2.1.4. Data Model, Representation, and Persistence
  * 2.2. PKM Reimagined: Notes, Web Archives, and Media as Native Exocortex Artifacts
    * 2.2.1. Philosophy: Unifying Curated Knowledge with the Event Stream – Breaking Down Silos
    * 2.2.2. Markdown Note Integration (Focus on Neovim Workflow)
    * 2.2.3. Web Page Archiving as Rich PKM Artifacts
    * 2.2.4. Media & Blob Integration within PKM (Nayuki-Inspired Content-Addressing & Universal Tagging)
* **Part III: The Architecture of Awareness – Building the Universal Substrate**
  * 3.1. The Canonical Event Substrate: The Immutable Heart of the Exocortex
    * 3.1.1. Philosophy Revisited: The Universal Log, Append-Only Truth, Auditability, Replayability
    * 3.1.2. Technology Stack & Rationale (PostgreSQL, TimescaleDB, ULIDs)
    * 3.1.3. Core Schema `raw.events` - Unified and Refined
    * 3.1.4. The Schema Registry (`sinex_schemas.event_payload_schemas` & `sinex_schemas.agent_manifests`)
    * 3.1.5. Indexing Strategy for Performance and Queryability
  * 3.2. The Sensory Network: The Universal Ingestion Layer
    * 3.2.0. Philosophy of Ingestion: Layered Fidelity, Redundancy, Ambient Capture, Minimal Source-Side Processing, Direct Ingestion Patterns
    * 3.2.1. Ingestor Management & Common Patterns (Systemd, Idempotency, DLQs, Standardized Output, Health Monitoring)
    * 3.2.2. Detailed Ingestor Domain Breakdowns:
      * A. Compositor & Direct Input (Hyprland, evdev, Keyboard, Mouse)
      * B. Application Semantics (Browser, Neovim, Terminal (Kitty, asciinema), AT-SPI2)
      * C. System & Environment (Journald Bridge, Filesystem, System Sensors)
      * D. Audio/Visual Streams (PipeWire, Speech-to-Text, OCR)
      * E. Mobile, Wearable & IoT Context
      * F. Meta-Cognitive & Subjective Ingestion
  * 3.3. The Structuring Engine: From Raw Signals to Actionable Knowledge
    * 3.3.1. Philosophy: Emergent Order, Lossless Transformation, Traceable Lineage, Retroactive Processing
    * 3.3.2. Promotion Pipelines: Mechanism & Orchestration
    * 3.3.3. Domain Table Design Principles
    * 3.3.4. Enrichment Processes (Layered and Agent-Driven: Tagging, Embedding, NER, Semantic Hashing, Summarization)
    * 3.3.5. The Knowledge Graph as an Emergent Semantic Network
    * **3.3.6. Embedding Generation and Semantic Indexing (Future Phase Foundation):**
  * 3.4. Blob Management with Git-Annex: The Physical Archive for Non-Textual and Original Source Content
    * 3.4.1. Philosophy: Content-Addressing for Integrity, Deduplication, Location Independence. Metadata in DB, Content in Annex.
    * 3.4.2. `core_blobs` Table: Central Metadata Registry for Annexed Content
    * 3.4.3. Integration Workflow with Exocortex Events & Artifacts
    * 3.4.4. Accessing Blobs from Exocortex UI/Agents
    * 3.4.5. Benefits & Management of Git-Annex
  * 3.5. The Semantic Desktop Stream: Synthesizing Context for Advanced Agency
* **Part IV: The Agentic Ecosystem – Automation, Intelligence, and Partnership**
  * 4.1. The Agent Framework: Orchestrating Distributed Intelligence
    * 4.1.1. Philosophy of Agentic Design
    * 4.1.2. Agent Registry (`sinex_schemas.agent_manifests`)
    * 4.1.3. Systemd Integration & Lifecycle Management
    * 4.1.4. Communication & Data Flow Patterns (Event-Driven, Agent DLQs)
  * 4.2. LLMs in the Exocortex: Roles, Integration, and Meta-Programming
    * 4.2.1. Diverse Roles of LLMs
    * 4.2.2. Model Management & Access (`core_llm_models` Registry)
    * 4.2.3. Prompt Engineering & Management (`core_prompts`)
    * 4.2.4. Cost Tracking and Budgeting (`sinex.agent.llm_api_call` events)
  * 4.3. Archetypal Agents and Their Capabilities
    * 4.3.1. Task-Oriented & Proactive Agents
    * 4.3.2. Analytical & Retrospective Agents
    * 4.3.3. Integration & Synchronization Agents
    * 4.3.4. Meta-Reflective & System Maintenance Agents
* **Part V: The Bridge to Self – Interaction, Query, Feedback, and Self-Modeling**
  * 5.1. UI/UX Philosophy & Primary Interaction Channels
    * 5.1.2. Neovim Plugin: The Power-User Cockpit
    * 5.1.3. `exo` CLI: The Scriptable Backbone
    * 5.1.4. Dashboards (Grafana; Web UI - Future)
    * 5.1.5. Inbox Workflow as a Core Interaction Pattern
  * 5.2. The Art of the Query: Unlocking the Sentient Archive
    * 5.2.1. Query Capabilities & Language (SQL, Simplified `exo` Syntax, Hybrid)
    * 5.2.2. Query Cookbook: Practical Examples
  * 5.3. Weaving Understanding: Event Relations & Narrative Construction
    * 5.3.1. Explicit Event & Artifact Relations (`event_relations`, `core_entity_relations`, `core_artifact_links`)
    * 5.3.2. Agent-Driven Narrativization (`meta.narrative_generated` events)
    * **5.3.3. Generic Event Annotations (`event_annotations`): Layering User and Agent Insights**
  * 5.4. Cognitive Feedback Loops & Instrumented Self-Modeling
    * 5.4.1. Surfacing Patterns & Anomalies
    * 5.4.2. Intentional Tracking & Goal Alignment
    * 5.4.3. The Exocortex as a Mirror for Self-Understanding and Experimentation
    * 5.5. Modeling User Context: Derived Semantic Layers for Sessions, Intents, and Composite Actions
* **Part VI: Sustaining the Covenant – System Integrity, Evolution, and the Path Forward**
  * 6.1. Meta-Observability: The System Observing Itself – A First-Class Data Stream
    * 6.1.1. Philosophy: All Exocortex Operational Data *is* Exocortex Data
    * 6.1.2. Key Metrics & Events Captured
    * 6.1.3. Ingestion of Meta-Observability Data
    * 6.1.4. Utilization for Self-Management and User Awareness
  * 6.2. Security, Privacy, and Data Sovereignty: Protecting the Cognitive Core
    * 6.2.1. Access Control & Authentication
    * 6.2.2. Encryption
    * 6.2.3. Consent & Control for Sensitive Data
    * 6.2.4. Data Export and Deletion
  * 6.3. Backup, Disaster Recovery, and Data Integrity: Ensuring Permanence
    * 6.3.1. PostgreSQL Backup Strategy
    * 6.3.2. Git-Annex Backup Strategy
    * 6.3.3. NixOS Configuration Backup
    * 6.3.4. Disaster Recovery Plan
    * 6.3.5. Data Integrity Checks
  * 6.4. Performance, Scalability, and Schema Evolution: Growing Gracefully
    * 6.4.1. Database Performance Tuning & Management
    * 6.4.2. Agent & Ingestion Scalability
    * 6.4.3. Schema Evolution Strategy (Formal Migrations Post-Phase 2.5)
  * 6.5. Federation and Multi-Device Coherence: The Distributed Exocortex (Future Vision)
    * 6.5.1. Core Principles for Federation
    * 6.5.2. Technical Enablers Already in Place
    * 6.5.3. Synchronization Mechanisms (Speculative)
    * 6.5.4. Challenges
  * 6.6. The Journey: MVP, Phased Implementation, and Open Horizons
    * 6.6.1. Recap of the Minimum Viable Exocortex (MVP)
    * 6.6.2. Current Phase (Post-Phase 2.5): Deepened Core Capture & Foundational Tooling
    * 6.6.3. Next Phase (Phase 3): Deepening Semantic Capture & User Interaction
    * 6.6.4. Subsequent Phases (Illustrative)
    * 6.6.5. Friction-Driven Prioritization
  * 6.7. Open Horizons & The Spirit of Continuous Evolution
  * Concluding Call to Action: Building Your Sentient Archive – An Ongoing Commitment to Self-Authorship.
* **Appendices (Conceptual List)**
  * A. SQL Data Definition Language (DDL) for Core Tables
  * B. Canonical Event `source` Identifiers & Core Payload Schemas (JSON Schema Examples)
  * C. Example Agent Manifests & Key LLM Prompt Templates
  * D. `exo` CLI Command Reference (Generated)
  * E. Glossary of Key Exocortex Terms
  * F. Security Threat Model & Mitigation Details
  * G. Backup and Recovery Detailed Procedures
* Essay 1: The Exocortex as a Laboratory for Self: A Guide to Personal Experimentation
* Essay 2: The Accidental Philosopher: Emergent Insights from a Universal Personal Archive
* Essay 3: The Poetics of Data: Finding Narrative and Meaning in Personal Event Streams

**Sinnix Exocortex: The Sentient Archive (Definitive Document v2.1)**
=====================================================================

**Foreword: The Imperative of Cognitive Sovereignty**
---

We stand at a peculiar juncture in human history. Our digital tools grant us unprecedented access to information, communication, and computational power, yet this abundance often engenders a profound and pervasive sense of fragmentation. The lived texture of our daily experience—the fleeting thoughts, the crucial insights, the context of our decisions, the emotional undercurrents of our work—is scattered across a constellation of ephemeral applications, proprietary silos, and rapidly decaying local caches. We generate more data about ourselves than ever before, yet we *remember* less, *understand* less of our own cognitive trails, and feel increasingly alienated from the very digital environments designed to augment us. This is the crisis of digital amnesia, a quiet erosion of personal continuity and intellectual self-possession.

The consequences are not trivial. When our working memory is perpetually externalized into systems that neither we nor our tools can intelligently query or connect, our capacity for deep work diminishes. When the provenance of our ideas is lost, our ability to build upon them, to learn from error, or to discern genuine signal from noise is compromised. When our digital footprint is primarily a resource to be mined by others rather than a coherent archive for our own reflection and growth, we risk becoming passive consumers of our own lives, rather than active authors. The need is not for more apps, faster processors, or larger storage, but for a new *kind* of personal infrastructure: one that is user-owned, deeply intelligible, and designed from the ground up to serve as a faithful, extensible, and empowering substrate for human cognition.

The Sinex Exocortex is conceived as a direct, radical response to this imperative. It is not another note-taking application, a second brain built on rigid methodologies, or a mere productivity dashboard. It is an ambitious endeavor to construct a **cognitive habitat**: a persistent, universally capturing, and intelligently structured digital environment that mirrors, supports, and augments the user's own mind. It seeks to transform the digital realm from a source of distraction and fragmentation into a coherent, queryable, and deeply personal extension of self. The **"sentience"** of this archive is not artificial general intelligence, but rather an emergent property of its comprehensive awareness of the user's context, its capacity for proactive, relevant assistance via its agentic ecosystem, and its ability to reflect patterns and insights that resonate deeply with the user's own understanding, fostering a feeling of genuine cognitive partnership. A "cognitive habitat," in this sense, is an environment that actively shapes and is shaped by thought, offering rich affordances for diverse cognitive modes—deep work, playful exploration, structured planning, and quiet reflection—all within a framework that prioritizes the user's autonomy and well-being.

The promise of the Exocortex is threefold: to restore **agency** by placing the user in absolute control of their data and its interpretation; to cultivate **insight** by making the patterns, connections, and causal chains within their experience visible and navigable; and to enable **intentional evolution** by providing a substrate for self-modeling, feedback, and the conscious design of one's own cognitive workflows. This document lays out the philosophy, architecture, and vision for such a system—a sentient archive for a sovereign mind, built not just for utility, but also as a platform for **playful self-discovery and joyful tinkering** with the very tools that shape our thought.

---

**Part I: The Exocortex Covenant – Manifesto, Principles, and Human Context**
---

**1.1. Against Digital Oblivion: A Manifesto for Personal Data Continuity**
---

We are navigating an epoch defined by an unprecedented deluge of digital interactions, a constant stream of information that shapes our thoughts, guides our actions, and mediates our connections. Yet, paradoxically, this era of hyper-connectivity and computational plenty is often characterized by a pervasive digital amnesia. The intricate context of our work—the specific browser tab that sparked an idea, the sequence of commands that solved a problem, the subtle shift in focus that preceded a breakthrough, the very *why* behind our digital endeavors—evaporates with alarming speed, lost to closed application silos, aggressive log rotations, or the simple limitations of human memory. This is more than an inconvenience; it is a fundamental impediment to cumulative learning, deep reflection, and robust self-understanding. To live a significant portion of our lives through digital media without a means to reliably recall, connect, and build upon that experience is to condemn ourselves to a state of perpetual cognitive groundhog day.

The Sinex Exocortex issues a direct challenge to this acceptance of digital oblivion. It is not merely a tool but a philosophical stance, an assertion that our digital experiences are as integral to our personal narratives as our physical ones and deserve the same—if not greater—diligence in preservation and accessibility. The Exocortex is conceived as an **"anti-forgetting machine,"** a system architected from its core for the lossless, comprehensive, and contextually rich capture of the user's entire digital life. The moral imperative is to restore a sense of continuity and ownership over our digital selves; if our interactions are increasingly digital, our memory must become equally so, but on our own terms. The cognitive imperative is to transform this captured history from a passive archive into an active substrate for learning, enabling us to trace the lineage of our ideas, understand the antecedents of our successes and failures, and build with the full weight of our past experience. This is an act of deliberate memory construction, forging a foundation for a more conscious, coherent, and self-determined digital existence.

**1.2. The Exocortex Pledge: Core Visionary Commitments**
---

The design, development, and ongoing evolution of the Sinex Exocortex are anchored by a set of inviolable pledges to its user:

* **Pledge 1: To Capture Comprehensively and Losslessly.** The Exocortex commits to striving for the capture of every potentially significant digital trace and subjective marker, from the most granular hardware interrupt to the highest-level strategic insight. Data will be ingested in its rawest, most complete form available, preserving all original detail. Multi-modal and redundant capture strategies will be employed wherever feasible to enhance fidelity, ensure resilience against individual sensor failure, and provide richer data for subsequent correlation.
* **Pledge 2: To Structure Meaningfully and Emergently.** The Exocortex commits to the principle that data structure must serve genuine understanding and user utility, not dictate or limit the scope of capture. Schemas will be flexible, versioned, and designed to evolve in response to observed usage patterns and the user's evolving needs. The system will facilitate the gradual, often AI-assisted, emergence of order, connections, and semantic richness from initially unstructured or semi-structured data, always ensuring that the raw, original events remain inviolate and available for future reinterpretation with new tools or understanding.
* **Pledge 3: To Empower User Agency Unconditionally.** The Exocortex commits to ensuring that the user remains the absolute sovereign of their own data and the system that manages it. All captured data, system configurations, agent behaviors, and data processing pipelines will be transparent, inspectable, and, in principle, modifiable by the user. The system will provide powerful tools for automation and insight generation, but it will never impose actions, coerce behavior, or operate opaquely against the user's explicit will or implicit understanding. Extensibility and "hackability" are core design features, not afterthoughts.
* **Pledge 4: To Evolve Continuously and Transparently Through Iteration.** The Exocortex commits to being a living system, co-evolving with its user, their changing needs, and the broader technological landscape. Development will be relentlessly iterative, prioritizing the delivery of tangible value by addressing personally-felt friction points. All significant changes to the system's architecture, core principles, or agentic behaviors will be documented, reasoned, and communicated transparently.

**1.3. Foundational Principles of the Sentient Archive**

These pledges are made concrete through a set of foundational design principles that permeate every layer of the Exocortex:

* **Principle 1: Universal Capture is Primary.**
    The default operational stance of the Exocortex is to *capture*. If a signal related to the user's digital experience or declared internal state can be instrumented or logged, it should be, at the highest fidelity reasonably attainable. This encompasses the philosophy of losslessness (preferring raw data over summarized or transformed data at ingest), redundancy as strategic depth (e.g., capturing keystrokes at the hardware level via `evdev`, at the compositor level via a Hyprland ingestor, and at the application level via a Neovim plugin, allowing for cross-validation and context fusion), and multi-modality as default (integrating text, audio, visual, sensor, and interaction event streams into a unified temporal framework). "Capture everything" is not a hyperbole but a design target approached asymptotically.

* **Principle 2: Structure is Emergent.**
    The Exocortex explicitly rejects the imposition of rigid, comprehensive, and premature schemas at the point of data ingestion. Raw data flows into the `raw.events` table with minimal structural assumptions (typically a flexible JSONB payload). Typed domain tables, knowledge graph connections, semantic tags, and other forms of higher-level organization are created *downstream* through promotion and enrichment pipelines. This "schema-on-demand" or "schema-on-query" approach, coupled with the guaranteed integrity of the raw event log, ensures that the system can adapt to new data types, evolving analytical needs, or future AI capabilities without requiring destructive migrations of historical data. Meaning is iteratively refined and discovered, not preordained.

* **Principle 3: Agency is Sovereign.**
    The user is, and must always remain, the ultimate authority and beneficiary of their Exocortex. This principle manifests as radical transparency (all data is queryable by the user, all agent logic inspectable), universal hackability (the system is built on open standards and configurable components, with NixOS providing a declarative foundation for modification), user control over automation (agents suggest and assist but rarely act irrevocently without consent, especially in early stages), and a strict avoidance of "black box" mechanisms that obscure decision-making or data flows. Data ownership is absolute and local-first by default.

* **Principle 4: Context is Continuous.**
    Isolated data points offer limited insight; the true power of the Exocortex lies in its ability to capture and reconstruct the rich, continuous *context* surrounding any event or artifact. This is achieved through:
  * *Universal Timestamping:* Rigorous application of `ts_ingest` (system time of DB insertion) and `ts_orig` (source-native timestamp) to all events.
  * *Global Identifiers:* Use of ULIDs for all primary keys (events, notes, entities, blobs, etc.) allows for unambiguous referencing across the entire system. ULIDs are ideally generated client-side by ingestors for events that might be queued offline (e.g., from mobile) to ensure global uniqueness before they hit the database, which can then enforce the PK constraint. For always-online ingestors, database-side generation via `DEFAULT generate_ulid()` is simpler but requires online connectivity for ID assignment.
  * *Explicit Linking:* Mechanisms like `parent_id` (rarely used directly on `raw.events`, more for specific derived structures) and structured links in `event_relations` or `core_entity_relations` allow for the explicit modeling of causal, temporal, or semantic relationships.
  * *Rich Provenance:* The top-level `host`, `ingestor_version`, and `payload_schema_id` fields in `raw.events`, supplemented by a dedicated `_provenance` key within the `payload` JSONB (containing details like `script_hash`, `input_file_path`, `agent_id_if_generated`, `retry_count`, `original_event_id_if_correction`), meticulously track the origin of data. A crucial component of this provenance is the **`correlation_id` (UUID/TEXT)**, also ideally within `payload._provenance`. This ID is generated at the *initiation point* of a complex, multi-step user interaction or workflow (e.g., by a Neovim command, a browser extension action, or the start of an `exo` CLI process) and is *propagated* to all subsequent events generated as part of that single logical operation, regardless of their `source` or `host`. This enables the reconstruction of complete, multi-event user tasks ("user stories") and provides deep context for analysis (e.g., "show all terminal commands, file edits, and web lookups related to `correlation_id` 'research_bug_fix_XYZ'").
  * *Temporal and Causal Coherence:* Agents and query interfaces are designed to surface data not just as isolated items but as parts of temporally coherent sessions or causally linked chains of events.

* **Principle 5: Feedback is Fuel.**
    The Exocortex is designed to be a profoundly reflexive system. It is not merely a passive recipient of data but an active participant in a feedback loop with the user, aimed at both personal and systemic improvement. This manifests as:
  * *System as Mirror:* Dashboards, queries, and agent-generated narratives provide the user with insights into their own patterns of activity, focus, productivity, friction, and even emotional states.
  * *Self-Modeling Substrate:* The Exocortex provides the raw material and the tools for the user to conduct explicit self-experiments, track habits, and model their own cognitive and behavioral dynamics.
  * *Iterative Improvement Through Use:* The very act of using the Exocortex, and encountering points of friction or missing functionality, generates signals (which can themselves be logged as `meta.friction_report` events) that drive its next iteration of development.
  * *Friction as Actionable Signal:* Any difficulty, annoyance, or inefficiency experienced by the user in their workflows (whether Exocortex-related or general digital work) is treated as a high-priority candidate for instrumentation, analysis, and potential agent-driven automation or support. The system aims to learn from its (and its user's) "mistakes," potentially employing **"friction mapping"** as an active diagnostic process where UI or agents help visualize clusters of logged friction to guide process redesign.

* **Principle 6: Meta-Cognition and Subjective Experience are First-Class Data.**
    A true cognitive augmentation system cannot ignore the internal landscape of its user. The Exocortex elevates subjective experience and meta-cognitive processes to the status of first-class, eventified data. This includes:
  * *Intentions and Plans:* Explicitly logging goals, project plans, and strategic shifts (`planning.milestone`, `meta.intention.created`).
  * *Friction and Blockages:* Capturing moments of difficulty, confusion, procrastination, or aversion (`meta.friction_logged`).
  * *Insights and Breakthroughs:* Recording "aha!" moments, solutions found, and key learnings (`meta.insight_captured`).
  * *Emotional and Affective States:* Allowing for the logging of mood, energy levels, or other subjective states (`subjective.mood_reported`), and correlating these with other activities.
  * *Narratives and Reflections:* Supporting the creation (manual or agent-assisted) of summaries and stories about personal journeys, projects, or periods of time (`meta.narrative_generated`).
    By integrating this "internal" data stream, the Exocortex can provide much deeper and more personalized context and support than systems that only track external actions.

* **Principle 7: Ethical Alignment & Anti-Coercion (Explicit Anti-Goals).** The Exocortex is fundamentally a tool for user empowerment and self-authorship. It therefore actively resists becoming:
  * *A Coercive Taskmaster:* It does not enforce productivity metrics, prescribe "correct" workflows, or induce guilt through comparison or judgment. Its aim is support, not surveillance for the sake of optimization against external norms. This includes avoiding features that could inadvertently lead to unhealthy self-instrumentation overload or attention fragmentation.
  * *An Opaque Algorithmic Controller:* All significant agentic decisions or data transformations must be inspectable, with their reasoning (e.g., prompts used, data considered) accessible to the user. There are no "black box" AIs making unexplainable alterations to the user's cognitive landscape.
  * *A Source of Anxiety or Overload:* While aiming for comprehensive capture, the UI/UX layers must provide effective filtering, summarization, and progressive disclosure to prevent overwhelming the user. The system should reduce cognitive load, not add to it through excessive self-monitoring demands.
  * *A Replacement for Human Judgment or Critical Thought:* Agents augment and assist, but the user remains the final arbiter of meaning, truth, and action. The Exocortex is a co-pilot, not an autopilot for life.
  * *A System that Fosters Unhealthy Dependency:* While providing powerful support, the design should encourage the user's own skill development and intrinsic motivation, rather than creating a crutch that atrophies natural cognitive abilities.

---

**1.4. Human Context: Designing for Real Minds – Neurodiversity, Executive Function, and Cognitive Augmentation in the Sinex Exocortex**

The ambition of the Sinex Exocortex extends beyond the mere technological feat of comprehensive data capture and intelligent retrieval. At its heart, it is a system designed *for humans*—with a profound acknowledgment of the diverse ways human minds experience, process, and interact with the digital world. Traditional software design often implicitly targets a mythical "average user," inadvertently creating friction and barriers for those whose cognitive landscapes differ. The Exocortex, in contrast, embraces cognitive diversity not as a set of deficits to be remediated, but as a spectrum of strengths, perspectives, and needs that technology can uniquely support and empower. This commitment is woven into its architecture, its interaction paradigms, and its overarching philosophy of augmenting agency and fostering cognitive well-being.

* **1.4.1. Introduction: Beyond the "Average User" – Embracing Cognitive Diversity as a Design Imperative**

The digital environments we inhabit are rarely neutral; they embody assumptions about how attention is allocated, how memory functions, how tasks are initiated and sustained, and how information is best structured. For many, these assumptions align well enough with their innate cognitive styles. For others, particularly individuals who identify as neurodivergent—such as those with Attention-Deficit/Hyperactivity Disorder (ADHD) or on the Autism Spectrum Condition (ASC)—or those who experience significant challenges with executive functions, standard digital tools can often feel like ill-fitting garments, inducing frustration, inefficiency, and a sense of being perpetually out of sync.

The Sinex Exocortex approaches this not as a problem of "fixing" the user, but of designing a more adaptable, accommodating, and ultimately empowering *cognitive habitat*. It recognizes that neurodiversity brings unique strengths—intense focus, pattern recognition, creative associative thinking, systemic thinking—alongside distinct challenges. The goal is to provide an infrastructure that minimizes the cognitive tax imposed by common digital friction points while providing scaffolds that allow individuals to leverage their inherent strengths more effectively. Similarly, executive functions—the set of mental skills that include working memory, flexible thinking, and self-control—are crucial for navigating complex tasks and achieving long-term goals. While these functions vary across all individuals, challenges in this domain are a near-universal experience in the face_of modern information overload. The Exocortex aims to provide a robust externalized support system for these vital cognitive processes, benefiting all users but offering particularly profound leverage for those who find them a consistent bottleneck.

* **1.4.2. ADHD (Attention-Deficit/Hyperactivity Disorder) & The Exocortex: Scaffolding Focus, Memory, and Action**

The lived experience of ADHD is often characterized by a dynamic interplay of intense focus, rapid attentional shifts, and challenges with working memory, task initiation, and sustained effort on non-novel tasks. The digital world, with its constant notifications and hyperlinked rabbit holes, can be both a playground for the ADHD mind's associative strengths and a minefield for its vulnerabilities. The Exocortex is designed to act as a cognitive prosthesis and a supportive partner, addressing core ADHD-related challenges directly:

* **Augmenting Working Memory & Combating "Out of Sight, Out of Mind":**
    The hallmark of ADHD working memory is often its limited capacity for holding multiple, non-stimulating pieces of information concurrently, and the rapid decay of items not currently in the attentional foreground. The Exocortex counters this through its principle of **universal, frictionless capture**. Every fleeting idea jotted in the Living Document, every URL visited, every command typed, every snippet copied to the clipboard is preserved in the `raw.events` substrate. The Living Document, in particular, with its stream-of-consciousness input modality (typed or voice-to-text via global hotkeys), serves as an infinitely patient external buffer. *A user, deep in a coding task, can capture a tangential thought about a different project ("LD: Remember to email Jane about API docs") without breaking flow, knowing it's safely stored, timestamped, and retrievable later via query or an "Inbox" review.* This offloads the immense cognitive burden of trying to "not forget."

* **Enhancing Object & Task Permanence:**
    For tasks, ideas, or browser tabs not immediately visible, their existence can fade from active awareness. The Exocortex makes these "objects" persistent and easily resurfaced. Structured tasks extracted from the Living Document or manually logged (as `core_artifacts` of type `task_item`) can be queried by project, due date, or status. `core_artifacts` representing PKM notes or web archives maintain their presence. *An agent, for instance, could be configured to periodically surface (via a Neovim popup or a `sinex.system.suggestion_created` event) open tasks related to the currently active project context (inferred from Hyprland window titles or Git repository), gently nudging them back into awareness without being intrusive.*

* **Lowering Activation Energy & Supporting Task Initiation/Resumption:**
    The "wall of awful" often encountered when facing a new or complex task is a significant hurdle. The Exocortex aims to lower this barrier:
  * *Minimal Capture Effort:* Global hotkeys for logging `meta.friction_logged` ("Ugh, can't start this"), `meta.intention.created` ("Okay, I WILL work on X for 20 mins"), or quick voice notes to the Living Document reduce the friction of externalizing the initial struggle or commitment.
  * *Agent-Assisted Task Breakdown:* An LLM agent, when prompted with a large goal described in the Living Document ("LD: Plan out the Exocortex Phase 3 implementation"), can help break it down into smaller, more manageable, and less intimidating sub-tasks, creating `artifact.todo.created` events for each.
  * *Contextual Retrieval for Resumption:* After an interruption or context switch, querying `exo find --source neovim_plugin --event_type file_saved --payload_contains '{"project_root":"/path/to/ProjectX"}' --since "yesterday" --limit 5` immediately shows the last files worked on. Similarly for terminal commands or browser tabs related to that project. This drastically reduces the "where was I?" re-orientation cost.

* **Temporal Scaffolding & Combating Time Blindness:**
    The subjective experience of time can be altered in ADHD. The Exocortex provides an objective record:
  * All events are rigorously timestamped (`ts_orig`, `ts_ingest`). Queries and dashboards can visualize actual time spent on specific applications (from `domain_hyprland.focus_changes`), projects (by correlating activity with project tags), or even specific tasks (if `task.started`/`task.stopped` meta-events are logged).
  * This data can be used for personal reflection ("I thought I only spent an hour on web browsing, but it was three") and to calibrate future time estimation for planning. Agents could even be developed to integrate with Pomodoro-like timers, logging `activity_segment.identified` events for focus sprints.

* **Managing Distraction & Leveraging Hyperfocus:**
  * Logging `meta.friction_logged` with `perceived_cause: "distraction_internal"` or `"distraction_external"` can help identify patterns and triggers.
  * The Exocortex can mark periods of deep work as `activity_segment.identified` with `segment_type: "hyperfocus_on_X"`. Analyzing what preceded these states (sleep, nutrition from `physio.*` logs, type of task, lack of interruptions from `mobile.notification.received`) might reveal conditions conducive to achieving them.
  * Gentle, user-configurable agentic "nudges" (e.g., a subtle desktop notification if activity has significantly strayed from a declared `meta.intention.created` for a prolonged period) could offer a non-judgmental re-orientation prompt.

* **Building Emotional Self-Awareness for Task Engagement:**
    The interplay between emotional state and executive function is critical. By logging `subjective.mood_reported`, `meta.activation_energy_shift`, or `meta.friction_logged` (especially when friction is emotional, like task aversion), and correlating these with tasks, projects, or times of day, the user can gain insight into their personal motivation cycles and develop strategies for navigating them more effectively.

* **1.4.3. Autism Spectrum Condition (ASC) & The Exocortex: Structuring Clarity, Routine, and Semantic Depth**

Individuals on the Autism Spectrum often possess unique cognitive strengths, such as exceptional pattern recognition, a capacity for intense focus on areas of interest (often termed "special interests" or monotropism), and a preference for logical, systematic thinking. They may also experience challenges related to sensory sensitivities, executive functioning differences (particularly around initiation and switching), and a need for clear, unambiguous communication and predictable environments. The Exocortex is designed to be a highly customizable and structured habitat that can cater to these characteristics:

* **User-Defined Structure, Predictability, and Routine:**
    ASC often involves a preference for order and a dislike of unexpected change or ambiguity. The Exocortex provides:
  * *Explicit Data Models & Schema Registry:* The `sinex_schemas.event_payload_schemas` and `sinex_schemas.agent_manifests` allow the user to understand (and even define) the precise structure of data and the behavior of system components. This transparency reduces uncertainty.
  * *Declarative Configuration (NixOS):* The entire system environment, from OS packages to agent configurations, is managed declaratively, ensuring predictability and reproducibility. There are no hidden or implicit system state changes.
  * *Customizable Agentic Workflows:* Agents can be configured with very specific, rule-based triggers and actions. The user can design highly predictable automation routines for data processing, PKM organization, or task management that align with their preferred methods.
  * *Consistent UI Paradigms:* While extensible, core UI interactions (e.g., in Neovim via Telescope, `exo` CLI command structure) are designed for consistency.

* **Deep Support for Special Interests & Monotropic Focus:**
    The Exocortex can become an unparalleled tool for individuals to pursue their special interests with depth and rigor:
  * *Comprehensive Information Aggregation:* All notes, web archives, PDFs, code snippets, data files, and relevant events related to an interest can be meticulously captured, tagged (using the `core_tags` system for fine-grained categorization), and interlinked.
  * *Semantic Depth & Knowledge Graph:* The system's ability to extract entities (`core_entities`) and relationships (`core_entity_relations`) from captured content allows for the construction of rich, detailed knowledge graphs specific to the user's interests. This can support deep analysis and discovery of novel connections.
  * *Frictionless Capture for Uninterrupted Flow:* Quick capture methods ensure that insights or new data points related to an interest can be logged without significantly disrupting a state of deep focus.

* **Managing Information Flow & Sensory Input:**
    Sensory or information overload can be a significant challenge. The Exocortex aims to provide tools for managing this:
  * *User-Configurable Information Density:* UIs are designed to be customizable. Neovim, as a text-based interface, can be made highly minimalist. Dashboards can be tailored to show only essential information. The `exo` CLI allows precise filtering of output.
  * *Controlled Notification System:* The `NotificationDispatcher` agent can be configured to batch, filter, delay, or even silence non-critical system notifications based on user-defined rules or current "focus mode" (which could be logged as a `meta.focus_session.started` event).
  * *Structured "Inbox Workflow":* (As detailed in Part V.1.5) Provides a controlled point for processing new information, preventing a constant, overwhelming influx. Items can be triaged and dealt with systematically.

* **Explicit Semantics & Literal Data Representation:**
    A preference for clear, unambiguous information is supported by:
  * *Raw Data Integrity:* The principle of storing raw, unaltered event payloads ensures there's always a ground truth.
  * *Explicit Schemas & Metadata:* `description` fields in `core_tags`, `core_entities`, schema definitions, and agent manifests allow for explicit articulation of meaning and purpose.
  * *LLM Prompting for Clarity:* When interacting with LLMs, prompts can be engineered to request literal interpretations, structured outputs (like JSON), and explicit reasoning steps, aligning with a preference for precision.

* **Leveraging Systemizing Strengths & Pattern Recognition:**
    The Exocortex itself is a complex system, and its transparent, hackable nature can be inherently engaging for individuals with strong systemizing tendencies:
  * *Data Analysis & Querying:* The ability to write precise SQL queries, explore data patterns, and build custom dashboards can be a powerful outlet for analytical skills.
  * *Workflow Optimization:* Designing and refining agentic workflows, data promotion pipelines, or personal automation scripts can become a satisfying endeavor.
  * *Schema Design & Knowledge Modeling:* Contributing to the definition of event schemas, entity types, or relationship ontologies for personal use allows deep engagement with the system's structure.

* **1.4.4. Executive Function Support: A Universal Augmentation Layer**

Executive functions (EF) are the brain's self-management system, encompassing a suite of cognitive processes crucial for goal-directed behavior. Challenges in EF are common and can impact anyone, though they are often a defining feature for neurodivergent individuals. The Exocortex is architected to provide robust, externalized scaffolding for these functions:

* **Planning & Organization:**
  * *Exocortex Tool:* The Living Document serves as an ideal space for brainstorming, mind-mapping, and outlining complex projects. LLM agents can assist in breaking down large goals into hierarchical sub-tasks.
  * *Structured Representation:* These plans are then formalized as `planning.goal.defined` and `planning.milestone_defined` events, or as `core_artifacts` of type `task_item` or `project_plan`, often with explicit dependencies logged in `core_entity_relations`. PKM notes (`core_artifacts` type `pkm_note`) can be linked as supporting material.
  * *Benefit:* Offloads the mental effort of holding complex plans in mind; provides a clear, queryable structure for projects.

* **Task Initiation & Prioritization:**
  * *Exocortex Tool:* Agentic reminders for upcoming or overdue `task_item`s. The "Inbox Workflow" helps triage and prioritize new tasks. The `exo` CLI can surface tasks based on priority, due date, or project.
  * *Contextual Cues:* When switching to a project context (e.g., opening a project-specific Neovim session), relevant open tasks can be automatically displayed.
  * *Logging Activation Energy:* Manually logging `meta.activation_energy_shift` helps track and understand personal patterns of motivation for different types of tasks.
  * *Benefit:* Reduces the "activation energy" barrier to starting tasks; helps maintain momentum.

* **Working Memory & Information Management:**
  * *Exocortex Tool:* Universal capture ensures no piece of information encountered or generated is lost. The `raw.events` table, `core_artifact_contents` (for PKM/web), and `core_blobs` (for files) act as an infallible external memory.
  * *Living Document:* Serves as an active, evolving scratchpad, offloading the need to juggle multiple ideas or pieces of information mentally.
  * *Contextual UI Panels:* Neovim sidebars showing related notes, events, or tasks while working on a specific item.
  * *Benefit:* Frees internal working memory for processing and problem-solving, rather than mere retention.

* **Time Management & Temporal Awareness:**
  * *Exocortex Tool:* Precise timestamps (`ts_orig`, `ts_ingest`) on all events. Dashboards (Grafana) visualizing time spent on different applications, projects, or task types (derived from event stream analysis).
  * *Integration with CalendarSyncAgent:* Correlating Exocortex activity with scheduled appointments.
  * *Benefit:* Provides objective data on time allocation, aiding in planning, identifying time sinks, and combating "time blindness."

* **Self-Monitoring & Progress Tracking:**
  * *Exocortex Tool:* Querying `task_item` statuses, `planning.milestone_defined` completion, or frequency of `meta.insight_captured` vs. `meta.friction_logged` for a given project.
  * *Agent-Generated Narratives:* Weekly or project-based summaries (`meta.narrative_generated`) provide a qualitative overview of progress and challenges.
  * *Benefit:* Allows for objective assessment of progress, identification of bottlenecks, and adjustment of strategies.

* **Cognitive Flexibility & Adaptability:**
  * *Exocortex Tool:* The ability to quickly search and retrieve context from past projects or related problem domains when tackling new or shifting tasks.
  * *Knowledge Graph:* Exploring links in `core_entity_relations` can reveal alternative perspectives or related concepts.
  * *Living Document's Malleability:* Allows for easy restructuring of plans and ideas as new information comes to light.
  * *Benefit:* Supports smoother transitions between tasks and mental models; facilitates creative recombination of ideas.

* **Inhibition & Emotional Control (Indirect Support via Awareness):**
  * *Exocortex Tool:* Logging `subjective.mood_reported`, `meta.friction_logged` (especially noting emotional triggers like "frustration" or "anxiety"), and `meta.activation_energy_shift`.
  * *Correlation:* Agents or user queries can correlate these subjective states with specific activities, times of day, or external events (e.g., "Do I log more friction events when working on backend tasks after 3 PM?").
  * *Benefit:* While not directly controlling impulses, this data builds self-awareness of emotional patterns and their impact on work, enabling the user to develop more effective coping strategies or to proactively structure their environment to minimize triggers.

By providing this comprehensive layer of externalized support for executive functions, the Exocortex aims to reduce the cognitive overhead of daily digital life, allowing users to direct more of their mental energy towards their core goals and creative endeavors.

* **1.4.5. Beyond Deficit Remediation: Augmenting Strengths & Fostering Well-being**

It is crucial to emphasize that the Exocortex's engagement with neurodiversity and executive function is **not solely about remediating perceived deficits**. It is equally, if not more importantly, about **amplifying existing cognitive strengths** and fostering overall **cognitive well-being**.

* For instance, the intense, sustained focus often associated with ASC or ADHD hyperfocus can be channeled into building and curating an unparalleled personal knowledge base within the Exocortex. The system's capacity for deep information aggregation, semantic linking, and powerful querying can transform a special interest from a hobby into a domain of profound personal expertise and discovery.
* The pattern-recognition abilities often heightened in autistic individuals can be applied to analyzing their own Exocortex data, uncovering subtle correlations or systemic insights that might elude others. The system's "hackability" makes it a perfect environment for such analytical exploration.
* For individuals who thrive on novelty and associative thinking (common in ADHD), the Exocortex's ability to surface unexpected connections between disparate pieces of information through semantic search or graph traversal can be a powerful catalyst for creativity and innovation.

Ultimately, the system aims to reduce the cognitive toil associated with merely *managing* information, context, and tasks. By offloading these burdens to a reliable external substrate, it seeks to free up the user's mental resources for what humans do best: creative problem-solving, deep learning, imaginative exploration, and the pursuit of intrinsically motivating goals. The focus is on enabling **sustainable engagement** and **meaningful accomplishment**, as defined by the user themselves, rather than chasing externally imposed productivity metrics. The "joyful hacking" and "playful self-discovery" aspects, inherent in building and customizing such a deeply personal system, are considered integral to this vision of cognitive well-being—the Exocortex as a personalized sandbox for exploring how one thinks, learns, and creates most effectively.

* **1.4.6. Iterative Co-evolution with the User: A Personalized Cognitive Partnership**

The Sinex Exocortex is not conceived as a static tool prescribed for a particular cognitive profile or set of challenges. It is, by design, a **dynamic system that iteratively co-evolves with its user.** The very act of using the Exocortex generates data about the user's unique cognitive style, their specific friction points, their preferred workflows, and their evolving goals. This data, in turn, informs both how the user customizes the system and how its agentic capabilities might be tuned or extended.

* **Feedback Loops for System Adaptation:** User interactions with agents (e.g., correcting an LLM's summary, re-tagging an artifact, dismissing a suggestion) are logged as `sinex.agent.llm_output_feedback` or similar meta-events. Meta-agents can analyze this feedback to refine prompts, adjust agent parameters, or even suggest new types of automation better suited to the user's observed patterns.
* **User as Co-Designer:** The principle of hackability means the user is not just a consumer of the Exocortex but an active participant in its ongoing design. As their understanding of their own needs deepens through interaction with the system, they can script new queries, develop custom agentic behaviors, or even propose changes to core data schemas that better reflect their personal ontology.
* **Evolving Support for Evolving Needs:** As the user's projects, interests, and even cognitive strategies change over time, the Exocortex is designed to adapt alongside them. New ingestors can be added, old ones retired. Tagging schemes can be refactored. The focus of the Living Document can shift. This ongoing adaptation is central to its long-term value as a truly personal cognitive augmentation, ensuring it remains relevant and supportive throughout different phases of the user's life and work.

This commitment to iterative co-evolution ensures that the Exocortex remains not just a tool, but a genuine cognitive partner, increasingly attuned and responsive to the individual it serves.

---

**1.5. Confronting the Developer's Existential Loop: Why Build in the Age of AI?**

The contemporary technologist, particularly one embarking on ambitious personal infrastructure projects, faces a persistent and potentially demoralizing question: Why invest months or years meticulously crafting a system when emerging AI, especially Large Language Models, promises to achieve similar outcomes—or even generate the system itself—with exponentially less effort in the near future? This "developer's existential loop" (the cycle of "I should build X" -> "But AI will do X better/faster" -> "But I need X now" -> "But building it takes as long as waiting for AI" -> paralysis) is a rational response to an unprecedented rate of technological acceleration.

The Sinex Exocortex project is undertaken not in ignorance of this loop, but as a considered, strategic engagement with it. The rationale for building *now* rests on several pillars:

* **The Enduring Value of Irreproducible Personal Historical Context: Your Data, Your Moat.**
    Future AIs will be incredibly powerful at processing and generating information based on their training data and the prompts they are given. However, they cannot retroactively create or perfectly infer the rich, nuanced, idiosyncratic history of *your* specific digital life—your unique sequence of actions, thoughts, mistakes, insights, and contextual shifts. The Exocortex, by committing to universal capture *starting today*, builds an irreplaceable longitudinal dataset. This dataset is not just a passive archive; it becomes the ultimate high-fidelity training corpus for any *future, truly personalized* AI assistant or cognitive partner you might choose to employ. The ability to trace a single logical user interaction (e.g., "researching API X for Project Y") across *dozens* of disparate raw events—browser visits, note edits, terminal commands, LLM queries, even pauses for thought logged as meta-events—via a shared `payload._provenance.correlation_id` provides a level of granular, interconnected context that generic models will never possess for your unique history. It is this deep, causal understanding of *your* process that forms an unassailable asset.

* **The Unsimulable Nature of Situated Expertise Gained Through Building: Learning by Doing.**
    The process of designing, implementing, debugging, and iteratively refining a system as deeply intertwined with one's own cognitive processes as the Exocortex yields a form of "maker's knowledge." This situated expertise—an intimate understanding of one's own workflow bottlenecks, data idiosyncrasies, and the subtle interplay between tools and thought—cannot be acquired by simply prompting a generic model to generate a solution. This deep understanding is itself a powerful cognitive asset, enabling more effective use of any tool, AI-generated or otherwise.

* **The Power of Immediate Utility and Incremental Value vs. Waiting for Hypothetical AI Futures.**
    The Exocortex is designed for iterative, friction-driven development. Each component, from the simplest ingestor to a sophisticated analytical agent, is intended to solve a *current, felt problem* or unlock *immediate cognitive leverage*. Even a partially implemented Exocortex, capturing just a few key data streams and offering basic query capabilities, can provide significant daily utility. This contrasts sharply with deferring all action in anticipation of a future AI panacea that may arrive later than expected, may not perfectly align with individual needs, or may come with unacceptable trade-offs in terms of ownership or privacy.

* **The Exocortex as a Unique, High-Fidelity Substrate *for* and *in Partnership with* Personalized AI: Preparing for Symbiosis.**
    Rather than viewing advanced AI as an alternative that renders personal infrastructure obsolete, the Exocortex positions itself as the *ideal foundation* for a more profound and effective human-AI cognitive partnership. Future LLMs will be far more powerful when operating on a rich, structured, contextually-linked, and historically deep dataset of an individual's own making, rather than just generic web data or a shallow snapshot of current application states. The Exocortex aims to provide precisely this substrate, enabling AIs to offer truly personalized insights, suggestions, and automation that are grounded in the user's actual, longitudinal experience.

Building the Sinex Exocortex, therefore, is not an act of naive resistance against technological tides, but a pragmatic and forward-looking strategy. It is an investment in personal data sovereignty, a method for cultivating deep operational and self-knowledge, and a way to ensure that the inevitable integration of more powerful AI into our lives happens on our own terms, leveraging our own unique histories for genuinely personalized augmentation. It is a commitment to authoring one's own cognitive future.

---

**Part II: The Core Cognitive Habitats – Living Document & Personal Knowledge Management**

The Exocortex is conceived not merely as a passive archive of digital traces but as an active, dynamic environment engineered to support and augment the user's cognitive processes. At the heart of this environment are two deeply integrated "cognitive habitats": the Living Document, which serves as an externalized working memory and a real-time partner for evolving thought, and a reimagined Personal Knowledge Management (PKM) system that transforms curated knowledge from static files into living, interconnected components of the broader Exocortex. These are not disparate applications but rather two primary modalities through which the user interacts with, shapes, and draws insight from the unified data substrate. They represent the Exocortex's commitment to facilitating the entire lifecycle of knowledge work, from the initial spark of a fleeting idea to the construction of enduring, interconnected understanding.

**2.1. The Living Document: Your Active Cognitive Workspace**

* **2.1.1. Philosophy & Concept: Beyond Static Notes – An Externalized, Persistent Working Memory**

Traditional digital tools for thought often force a premature commitment to structure or fall short in capturing the fluid, associative, and often messy reality of active cognition. The Living Document within the Sinex Exocortex is designed to transcend these limitations. It is conceptualized as an **externalized, persistent working memory**—a dynamic and responsive digital surface that acts as a direct extension of the user's mind. Its fundamental purpose is to provide a frictionless space for the capture of stream-of-consciousness thought, the iterative development of plans and drafts, the tracking of active mental states (including intentions, open loops, and evolving ideas), and the bridging of this initially unstructured input with the Exocortex's capacity for structured artifact generation and knowledge linking.

The Living Document is not a static repository to be manually curated in the manner of conventional notes or documents. Instead, it is an **interactive "thinking environment,"** an AI-augmented partner that actively participates in the cognitive process. It aims to capture the delicate dance of ideation, where thoughts are "parked" without fear of being lost to attentional shifts, explored without the immediate pressure of formal organization, and gradually shaped, refined, and interconnected with intelligent assistance from the system's agentic capabilities. It is a space designed to lower the activation energy for externalizing thought, thereby freeing internal cognitive resources for higher-order reasoning and creativity.

* **2.1.2. Core Architecture & Mechanics: An Event-Sourced, Agent-Driven System**

The dynamism and resilience of the Living Document are rooted in an **event-sourced architecture**, with its intelligence and adaptability provided by a modular **LLM node-graph processing pipeline.**

At its foundation, all modifications to the Living Document—whether initiated by direct user input, explicit commands, or autonomous agent actions—are meticulously recorded as immutable `livingdoc.delta` events within the Exocortex's canonical `raw.events` table. These deltas are not merely textual diffs; they can represent a range of semantic operations, from simple text appends and JSON Patches (RFC 6902) for structural changes, to custom operations like `{op: "link_node_to_event", node_id: "X", target_event_id: "Y"}` or `{op: "extract_as_task", source_node_id: "A", task_title: "..."}`. This event-sourcing ensures complete auditability of the Living Document's evolution, enables robust version history and rollback capabilities, and allows for the replayability of its development for debugging, analysis, or state reconstruction.

The processing of input and the ongoing evolution of the Living Document are orchestrated by a configurable chain of LLM-powered and traditional code-based agents, conceptualized as an LLM Node Graph:

First, the **Input Ingestion & Segmentation** stage handles the diverse forms of input. This includes typed text from Neovim, the `exo` CLI, or future graphical UIs; real-time voice transcripts captured via the PipeWire ingestor and processed by local Speech-to-Text models like Whisper.cpp; content pasted from the clipboard; and explicit links or transclusions of other Exocortex artifacts (existing PKM notes, specific raw events, web archives, or media blobs). Sophisticated LLMs or robust heuristic parsers then work to segment this raw, often continuous input stream into meaningful "thought units," "utterances," or "semantic segments," each potentially receiving a unique internal identifier for tracking.

Next, the **Delta Engine**, embodied by a core "Living Doc Manager" LLM agent, takes center stage. This agent consumes the segmented thought units, considering the current relevant state of the Living Document (e.g., the currently focused section, related nodes in its graph structure, or recent contextual events from the wider Exocortex). Its primary function is to intelligently determine how the Living Document should evolve in response to the new input and to generate the corresponding `livingdoc.delta` events. This involves not just appending new text, but also potentially modifying existing nodes, creating new structural relationships (like parent-child or sibling links), or applying formatting and metadata. The Delta Engine is guided by user intent—whether explicitly stated through commands embedded in the input or inferred by the LLM based on the content and context—as well as by pre-defined heuristics for maintaining coherence and usability.

Parallel to and downstream from the Delta Engine, **Artifact Extraction & Promotion** agents work to identify and elevate structured information from the often free-form content of the Living Document. Specialized LLM nodes, or dedicated agents subscribing to `livingdoc.delta` events, scan new and modified content for patterns indicative of actionable items (TODOs, reminders), declarative statements (claims, hypotheses), questions requiring further research, definitions of new concepts or entities, project outlines, or meeting notes. Upon identification, these agents emit new, distinct `sinex.artifact.*` events (e.g., `sinex.artifact.todo_created`, `sinex.artifact.claim_stated`, `sinex.artifact.project_defined`) into `raw.events`. These events contain the extracted structured data and maintain strong provenance links (e.g., via `related_ids` within `payload._provenance` or specific fields in their `payload`) back to the precise segment or node(s) within the Living Document from which they originated. These `sinex.artifact.*` events are then available for promotion into dedicated domain tables (or as `core_entities` nodes) for structured querying and management.

Simultaneously, **Knowledge Graph Integration** occurs. Named Entity Recognition (NER) agents continuously process the content of the Living Document and its extracted artifacts. Identified entities—such as people, organizations, projects, technical terms, or even recurring concepts—are resolved against (or used to create new entries in) the `core_entities` table. Relationships between these entities, whether explicitly stated in the Living Document (e.g., "Project X depends on API Y") or inferred by LLMs (e.g., "Thought A seems to contradict Claim B"), are used to create or update links in the `core_entity_relations` table. This process dynamically weaves the evolving content of the Living Document directly into the Exocortex-wide knowledge graph, making its insights and connections accessible from any other part of the system.

Finally, for **Versioning and Snapshots**, the complete history of the Living Document is preserved through the immutable stream of `livingdoc.delta` events. This allows for theoretically perfect reconstruction of any past state. For performance and practical access, periodic snapshots of the "rendered" state of the Living Document (or significant, frequently accessed portions) can be generated. These snapshots might take the form of a structured JSON tree representing the node graph, or a compiled Markdown document. They would be stored as versioned blobs in git-annex, with a corresponding `sinex.livingdoc.snapshot_created` event recorded in `raw.events` that references the snapshot's annex_key and the ULID of the last `livingdoc.delta` event incorporated into that snapshot.

* **2.1.3. Interaction Model & User Experience:**

The user's engagement with the Living Document is designed to be fluid, intuitive, and empowering, supporting a range of cognitive styles and tasks.

A primary characteristic is **frictionless, multi-modal input.** Capture is paramount. Global hotkeys, accessible from any application (configured via desktop environment or tools like `sxhkd` for Neovim focus, and system-wide for a dedicated quick-capture UI), allow the user to instantly append typed thoughts or activate real-time voice-to-text dictation directly into a designated "inbox" section of the Living Document or the currently active context. Clipboard content can be pasted, with agents attempting to automatically identify source and context, such as the URL if from a browser, or the file path if from a file manager.

The system supports both **explicit commands and implicit structuring.** The user can seamlessly intersperse natural language commands within their input stream—for instance, "LD: Make this a sub-point of the previous thought on PyLibX auth," or "LD: Summarize my notes on ProjectOmega from today," or even "LD: Create a task: Follow up with Jane Doe regarding API discussion." An intent recognition layer, likely a specialized LLM or a robust rule-based parser, identifies these commands and routes them to the appropriate Living Doc Manager functions or other designated agents. In the absence of such explicit commands, the Delta Engine LLM defaults to inferring appropriate structure and connections based on the content, its relation to existing document nodes, and the broader Exocortex context.

A crucial aspect of the user experience is **agentic partnership and proactive assistance.** Exocortex agents are designed to be more than passive recipients of information. They can, for example, proactively suggest the refactoring of Living Document sections if they detect emerging themes or redundancies, offering to group related paragraphs or create new headings. Agents may also offer to link new entries to existing notes, tasks, or web archives within the Exocortex, based on semantic similarity, keyword matches, or temporal proximity. Furthermore, they can surface potential contradictions or inconsistencies, either within the Living Document itself or between its content and other data stored in the Exocortex. If user input is ambiguous, agents might pose clarifying questions, such as "Are you referring to Project X or Project Y here?" All such proactive interventions are designed to be non-intrusive, typically requiring user confirmation (especially in the early stages of system use or for significant changes), and their assertiveness can be configured by the user. The user can also *teach* agents by correcting their suggestions or providing explicit feedback (e.g., via an `:ExoAgentFeedback` command or UI element), which is logged as a `sinex.agent.llm_output_feedback` event and can be used by meta-agents to refine future agent behavior or prompts.

Finally, the system is built for **multi-modal input fusion.** It can gracefully handle and attempt to intelligently interleave inputs from various sources arriving in close succession. For example, a typed sentence, followed by a dictated paragraph elaborating on that sentence, followed by a pasted URL and a reference to a screenshot (which itself becomes a blob linked to the Living Document), can all be woven by the Living Doc Manager and associated agents into a single, coherent, and contextually rich segment of the Living Document, with agents helping to establish and record the relationships between these disparate pieces of information.

* **2.1.4. Data Model, Representation, and Persistence:**

The internal structure of the Living Document must be sufficiently flexible to accommodate its dynamic, multi-faceted nature, while its persistence strategy ensures integrity and reconstructibility.

Its **internal logical structure** is best conceived as a graph of nodes. Each node within this graph can represent various units of thought or organization, such as a paragraph of text, an individual list item, an extracted structured artifact (like a task or a claim), an embedded image or a reference to an external Exocortex blob, or a section header that groups child nodes. Every node possesses a unique ULID, its primary content (which could be plain text, rich text like Markdown, or structured JSON for specific artifact types), associated metadata (timestamps, authoring agent or user, tags specific to that node), and explicit links to other nodes (parent-child for hierarchy, sibling for sequence, "related-to" for associative connections, "depends-on" for task dependencies, etc.). This graph model allows for non-linear organization, transclusion of content, and the representation of complex interdependencies between ideas.

Regarding **persistence**, the canonical state of the Living Document is fundamentally derived from the immutable **event stream of `livingdoc.delta` events** stored in the `raw.events` table. This adherence to event sourcing is the ultimate guarantee of truth and history. For practical performance in loading, displaying, and querying the current state of the Living Document (or frequently accessed portions thereof), a "current rendered state" or a cache is maintained. This might take the form of a dedicated PostgreSQL table, perhaps named `livingdoc_nodes_current_state`, with columns for `node_id ULID`, `parent_id ULID`, `node_type TEXT`, `content_text_or_jsonb`, `metadata_jsonb`, `sequence_order_within_parent INT`, and other relevant structured fields. Alternatively, for very large or complex document states, snapshots of the rendered document (e.g., as a structured JSON tree representing the node graph, or a compiled Markdown document) could be periodically generated and stored as versioned blobs in git-annex. These snapshots would be referenced by a `sinex.livingdoc.snapshot_created` event in `raw.events`, which includes the snapshot's annex_key and the ULID of the last `livingdoc.delta` event incorporated into that snapshot. This cached or snapshotted state is always considered a derivative and is fully reconstructible from the authoritative delta event stream.

The system supports multiple **rendered views** of the Living Document, tailored to different interaction needs. This includes clean, human-readable Markdown for export, simple editing, or interoperability with other tools. An interactive outliner view, available within Neovim and planned for future Web UIs, would allow for easy navigation and manipulation of the hierarchical structure. Furthermore, a canvas-style graph visualization, similar to that seen in tools like Obsidian, could display nodes and their explicit connections, offering a powerful way to explore the associative landscape of the user's thoughts.

**2.2. PKM Reimagined: Notes, Web Archives, and Media as Native Exocortex Artifacts**

The Sinnix Exocortex aims to unify existing Personal Knowledge Management (PKM) practices with its core event-driven, database-centric architecture. This transforms static files and isolated knowledge silos into dynamic, interconnected, and eventified artifacts, fully integrated into the user's broader digital experience.

* **2.2.1. Philosophy: Unifying Curated Knowledge with the Event Stream – Breaking Down Silos**

Traditional PKM systems, often based on folders of Markdown files or proprietary database formats, excel at curating explicit knowledge but frequently become disconnected from the live, dynamic stream of daily digital activity. The Exocortex philosophy is to **eventify PKM artifacts**, treating user-authored notes, annotated web page archives, and curated media not as separate entities but as first-class citizens of the same underlying data substrate that holds transient events like keystrokes or window focus changes. This deep integration allows for unprecedented levels of cross-correlation between curated knowledge and real-time activity, contextual retrieval of notes based on surrounding events, and the application of the Exocortex's agentic and AI-driven capabilities to the user's entire body of personal knowledge. The familiar filesystem, particularly for Markdown notes, becomes a convenient and optionally synchronized *view* or *cache*, while the Exocortex database, through the `core_artifacts` and `core_artifact_contents` tables, serves as the canonical, versioned, and richly linked source of truth.

* **2.2.2. Markdown Note Integration (Focus on Neovim Workflow):**

This component is critical for ensuring that established and efficient Markdown-based note-taking workflows, especially those centered around Neovim, are not only preserved but significantly enhanced by Exocortex integration.

The **initial import and onboarding** process involves an agent that scans the user's existing PKM vault(s). For each Markdown file discovered:

1. A stable `artifact_id` (ULID) is generated for the conceptual note, often based on a hash of its persistent relative path within the vault to maintain identity across sessions. This ID is stored in the `core_artifacts` table with `artifact_type='pkm_note'` and `canonical_identifier` set to its relative path. `created_at_ts_orig` is populated from file system mtime or frontmatter.
2. The file's full Markdown content is read, and its BLAKE3 hash (`content_hash_blake3`) is computed.
3. This content is stored in the `core_artifact_contents` table (as `content_text` with `content_format='text/markdown'`), linked to the `artifact_id`, with the `content_hash_blake3`, and the file's modification timestamp as `captured_at_ts_orig`.
4. The `core_artifacts` entry is updated to point to this `content_id` as its `current_content_text_id`.
5. Metadata (title, tags, aliases, etc.) is parsed from YAML frontmatter or inferred (e.g., title from filename/H1). This populates fields in `core_artifacts` (like `current_title`, `tags`) or its `properties` JSONB (a field to be added to `core_artifacts` for arbitrary key-value metadata).
6. All Wikilinks (`[[Target Note Title]]` or `[[target_ulid_if_known]]`) and standard Markdown links are parsed.
7. An initial `sinex.pkm.note_imported` event is emitted to `raw.events`. Its payload includes the `artifact_id`, `content_id` (of the imported version), extracted metadata, parsed outgoing links, and original filesystem path.

A robust **bi-directional sync mechanism** maintains coherence between the canonical database state and the filesystem view used by Neovim:

* **Neovim Save → Database:** A `BufWritePost` autocmd in Neovim (or an external filesystem watcher like `inotifywait`) triggers a sync agent. This agent reads the saved Markdown file, computes its new `content_hash_blake3`.
  * If the new hash differs from the `content_hash_blake3` associated with the `current_content_text_id` of the corresponding `artifact_id` in `core_artifacts`:
        1. A new entry is made in `core_artifact_contents` with the new content, new hash, and current timestamp.
        2. The `core_artifacts` entry for this note is updated: `current_content_text_id` now points to the new `content_id`, and `last_event_ts_orig` (and a conceptual `updated_at` field if added) is updated.
        3. A `sinex.pkm.note_updated` event is emitted to `raw.events`, payload including `artifact_id`, old `content_id`/hash, new `content_id`/hash, and ideally a diff/patch.
        4. Tags and Wikilinks are re-parsed from the new content, and relevant link tables (`core_artifact_links`, `core_entity_relations`) are updated.
* **Database Change → Filesystem:** An agent monitors `core_artifact_contents` for new versions linked to `pkm_note` artifacts that didn't originate from a local file sync. It retrieves the Markdown content for the new `content_id` and overwrites the corresponding file on disk.
* **Conflict Resolution:** If both file and database record have changed independently, a `sinex.pkm.sync_conflict` event is generated. The Neovim UI presents diffs and options for manual merge or version selection.

**Link management** uses a `core_artifact_links` table (`link_id ULID PK`, `source_artifact_id ULID FK core_artifacts`, `target_identifier_text TEXT`, `resolved_target_artifact_id ULID FK core_artifacts NULLABLE`, `link_type TEXT`, `context_snippet TEXT`). An agent resolves `target_identifier_text` to `resolved_target_artifact_id`.

**Neovim integration** provides Telescope pickers (searching `core_artifacts` by title, tags, FTS on `core_artifact_contents`, semantic similarity on `artifact_embeddings`), `gf` for link navigation (querying `core_artifact_links`), dynamic backlink panels, and tag/title completion.

* **2.2.3. Web Page Archiving as Rich PKM Artifacts:**

Web content is elevated to first-class PKM artifacts.
The **capture workflow** starts with a `sinex.web.capture_request` event (from browser extension, Raindrop.io sync, or manual `exo` command).
An **Archiving Agent** then:

1. Creates/updates a `core_artifacts` entry (`artifact_type='webpage_archive'`, `canonical_identifier`=normalized URL).
2. Fetches full HTML (via headless browser for fidelity, using user session) and stores as a `core_blobs` entry (git-annexed, `blob_type="html_original_archive"`).
3. Extracts main text (Trafilatura/Jina Reader) and converts to Markdown. This Markdown is stored as another `core_blobs` entry (`blob_type="markdown_web_extract"`).
4. Creates a `core_artifact_contents` entry for the Markdown version, linking to the `artifact_id` and storing the `content_hash_blake3` of the Markdown blob, `capture_method`, and the `source_blob_hash_blake3` of the original HTML blob.
5. Updates `core_artifacts` with extracted title, metadata, and points `current_content_text_id` to the new Markdown `content_id`.
6. The Markdown content is queued for embedding in `artifact_embeddings`.
7. Outgoing links from the Markdown are parsed into `core_artifact_links`.
8. Emits a `sinex.web.page_archived` event (payload includes `artifact_id`, Markdown `content_id`, URL, blob hashes).
**Deduplication and Versioning:** Re-archiving a URL compares the new Markdown's `content_hash_blake3`. Identical content isn't re-stored; different content creates a new `core_artifact_contents` version.

* **2.2.4. Media & Blob Integration within PKM (Nayuki-Inspired Content-Addressing & Universal Tagging):**

All PKM attachments (images, PDFs, audio, datasets) are managed via **content-addressing with git-annex** and integrated into the universal tagging system.
**Markdown notes reference blobs** using an `annex_key:<git_annex_key_with_suffix>` URI. Neovim/UIs resolve this via `core_blobs` to display or open.
The **Universal Tagging System** (`core_tags`, `core_tag_aliases`, `artifact_tags` join table: `target_object_id ULID`, `target_object_type TEXT` ('core_artifact_content', 'core_blob', 'raw_event', 'core_entity'), `tag_id ULID FK core_tags`)) applies to these blobs.
The **`core_blobs` table** (ULID PK, `content_annex_key TEXT UNIQUE`, `content_blake3_hash TEXT UNIQUE`, `mime_type`, `size_bytes`, `original_filenames ARRAY<TEXT>`, `user_description`, `extracted_media_metadata JSONB`, `schema_id ULID FK sinex_schemas.event_payload_schemas`) registers all annex blobs.
**Ingestion workflow for PKM blobs** (e.g., image drag-drop in Neovim):

1. Plugin/helper adds file to git-annex, gets `annex_key`.
2. `core_blobs` entry created/updated. `sinex.blob.ingested_for_pkm` event logged.
3. `annex_key:` reference inserted into Markdown.
4. Note save triggers `sinex.pkm.note_updated`; downstream agents create `core_artifact_links` (note `artifact_id` to blob's `blob_id` from `core_blobs`).

This ensures all personal knowledge assets form a cohesive, queryable, integrity-checked, and richly interconnected part of the Exocortex.

---

**Part III: The Architecture of Awareness – Building the Universal Substrate**
---

*(This Part details the foundational "how" – the technical architecture that underpins and enables the rich cognitive habitats and user experiences described in Part II. It embodies the core principles of Universal Capture, Emergent Structure, and Continuous Context.)*

The power and flexibility of the Sinex Exocortex stem from a meticulously designed foundational architecture. This architecture is not a monolithic block but a series of interconnected layers, each adhering to the system's core principles. It begins with an immutable, universal event substrate that captures every whisper of digital activity, progresses through a sophisticated ingestion network that senses the user's diverse environments, and culminates in a structuring engine that iteratively transforms raw data into actionable knowledge. This underlying machinery is what makes the Exocortex a truly "sentient" archive.

**3.1. The Canonical Event Substrate: The Immutable Heart of the Exocortex**

* **3.1.1. Philosophy Revisited: The Universal Log, Append-Only Truth, Auditability, Replayability**

At the absolute core of the Exocortex lies the **Canonical Event Substrate**, realized primarily as the `raw.events` table. This is conceived as a **universal log**, an append-only, immutable record of every piece of information the system captures. Its design philosophy is rooted in the understanding that raw data, in its original fidelity, is an invaluable asset. By deferring complex transformations and strict schema enforcement, this substrate ensures:

* **Auditability:** Every piece of structured knowledge or agentic decision can be traced back to its originating raw events.
* **Replayability:** Ingestion or promotion pipelines can be re-run over historical raw data if logic improves or bugs are fixed, without loss of the original signal.
* **Future-Proofing:** As new analytical techniques, AI models, or understanding emerge, the raw data remains available for novel forms of processing that might not be conceivable today.
* **Error Recovery:** If a downstream structuring process introduces errors, the pristine raw data provides a basis for correction.

* **3.1.2. Technology Stack & Rationale:**

The chosen technology stack for the event substrate prioritizes robustness, scalability, query power, and extensibility:

* **PostgreSQL:** Serves as the primary database management system. Its maturity, transactional integrity (ACID compliance), powerful SQL dialect, rich support for JSONB (for schemaless payloads), and extensive ecosystem of extensions make it an ideal foundation.
* **TimescaleDB:** This PostgreSQL extension transforms standard tables (like `raw.events`) into **hypertables**, which are automatically partitioned by time. This is crucial for managing the potentially vast volume of time-series event data generated by the Exocortex, providing significant performance benefits for time-bound queries, data ingestion, and data lifecycle management (e.g., compression or tiering of older data chunks).
* **ULIDs (Universally Unique Lexicographically Sortable Identifiers):** All primary keys for events (`raw.events.id`) and other core entities (artifacts, contents, blobs, agents, etc.) are ULIDs. Generated client-side by ingestors (for offline robustness, using a standard ULID library) or database-side (via `DEFAULT generate_ulid()` for always-online ingestors), they ensure global uniqueness and time-sortability.

* **3.1.3. Core Schema `raw.events` - Unified and Refined:**

The `raw.events` table is the entry point for all data. Its columns are carefully chosen to capture essential metadata while leaving the core event-specific data unstructured until downstream processing.

* `id ULID PRIMARY KEY`: Generated by the ingestor or `DEFAULT generate_ulid()`.
* `source TEXT NOT NULL`: Canonical identifier for the event origin (e.g., `"hyprland_ingestor"`, `"sinex.pkm.note_sync_agent"`). References `sinex_schemas.agent_manifests.agent_name` for agent-generated events, or a conceptual registry of sources.
* `event_type TEXT NOT NULL`: Type string, often namespaced by source (e.g., `"window_focused"`, `"note_updated"`, `"agent.heartbeat"`). References `sinex_schemas.event_payload_schemas.event_type`.
* `ts_ingest TIMESTAMPTZ DEFAULT now()`: Database insertion timestamp. Primary TimescaleDB partitioning key.
* `ts_orig TIMESTAMPTZ`: Original timestamp from the source system; best effort for accuracy.
* `host TEXT NOT NULL`: Originating machine/device identifier.
* `ingestor_version TEXT`: Version of the ingestor code.
* `payload_schema_id ULID REFERENCES sinex_schemas.event_payload_schemas(id) NULLABLE`: FK to the JSON Schema definition for this event's `payload`. Null if ad-hoc/unknown.
* `payload JSONB NOT NULL`: The core event data. Conventionally includes a `_provenance` sub-object for:
  * `_provenance.correlation_id UUID/TEXT`: Propagated across events from a single logical user interaction.
  * Other specific provenance details (script hash, input file, generating agent ULID, retry count, original event ULID if correcting).
* *(No direct top-level `parent_id`, `related_ids`, `tags`, `embedding_vector`, `blob_refs`. These are handled via `event_relations`, `artifact_tags`, `artifact_embeddings`, and references within `payload` or links to `core_artifacts`/`core_blobs`.)*

* **3.1.4. The Schema Registry (`sinex_schemas.event_payload_schemas` & `sinex_schemas.agent_manifests`):**

These tables (defined in Phase 2) are critical:

* `sinex_schemas.event_payload_schemas`: Stores versioned JSON Schema definitions for the `payload` of each `(event_source, event_type)` combination. Provides documentation, enables optional validation by promotion agents, supports code generation. Schema changes are eventified (`sinex.schema.definition_updated`).
* `sinex_schemas.agent_manifests`: Registers all ingestors and agents, their versions, capabilities (event types produced/consumed, schema IDs), configuration schema pointers, and operational status (including `last_seen_heartbeat`). Agents self-register/update.

* **3.1.5. Indexing Strategy for `raw.events`:**
* Primary Key: `id ULID`.
* TimescaleDB Hypertable Partitioning: By `ts_ingest`.
* Composite B-tree: `(source, event_type, ts_ingest DESC)`.
* B-tree: `(ts_orig DESC)`.
* B-tree: `(host, ts_ingest DESC)`.
* B-tree: `(payload_schema_id)`.
* GIN on `payload jsonb_path_ops` and `payload jsonb_ops`.

This refined event substrate provides the robust, flexible, and queryable foundation.

---

**3.2. The Sensory Network: The Universal Ingestion Layer**
---

* **3.2.0. Philosophy of Ingestion: Layered Fidelity, Redundancy, Ambient Capture, Minimal Source-Side Processing, Direct Ingestion Patterns**

The Exocortex's capacity to build a comprehensive understanding of the user's digital and cognitive world hinges entirely on the quality, breadth, and reliability of its **Sensory Network**—the diverse array_of ingestors that capture data from myriad sources. The philosophy guiding this layer is one of *layered fidelity*: data is sought from the most direct and semantically rich point available for any given phenomenon, often resulting in *strategic redundancy*. For example, a user interaction might be captured as raw hardware input, as a compositor-level window event, and as an application-specific semantic action. This overlap is not seen as inefficient but as a crucial mechanism for error-checking, context fusion, and providing different levels of abstraction for later analysis.

*Ambient capture* is another core tenet: many ingestors are designed to operate continuously and unobtrusively in the background, gathering data from the user's natural interactions without requiring explicit "logging" actions for every detail. Finally, processing at the source (within the ingestor itself) is kept to a *bare minimum*. Ingestors are primarily responsible for reliable data acquisition, basic normalization (e.g., timestamping, structuring into a JSON envelope conforming to a registered schema if possible), and secure insertion directly into the `raw.events` PostgreSQL table. Complex transformations, enrichments, and interpretations are deliberately deferred to downstream agents and promotion pipelines, ensuring the raw signal remains pristine and the ingestors themselves remain lightweight and robust. The rejection of intermediary message buses like Vector for this personal-scale system underscores the preference for **direct ingestion patterns**:

1. *Direct Database Library Usage:* For ingestors in Rust, Python, Go, etc., using native PostgreSQL client libraries for batched, asynchronous inserts is the most common and performant pattern.
2. *Local HTTP Endpoint:* A minimal local web service (e.g., Flask/FastAPI/Actix) can receive JSON POSTs from browser extensions or simple scripts, then insert into the DB.
3. *Named Pipes/UNIX Sockets:* For high-frequency local IPC where HTTP is too heavy.
4. *Journald as Structured Log Transport:* Services logging JSON to stdout/stderr can have this captured by systemd and ingested by the `JournaldBridgeIngestor`.

* **3.2.1. Ingestor Management & Common Patterns (Systemd, Idempotency, DLQs, Standardized Output, Health Monitoring):**

To manage this diverse array of data collectors effectively and ensure system stability, a set of common patterns and management practices are employed, building upon the Phase 2.5 specification:

A cornerstone of ingestor management is their integration as **Systemd User Services**, declaratively configured via NixOS modules. Each significant ingestor runs as a systemd user service or timer unit, providing standardized lifecycle control, robust logging to journald (itself an ingestible source for `meta.observability`), and resource quotas (CPU, memory) defined in its NixOS module and reflected in its `sinex_schemas.agent_manifests` entry.

**Idempotency** is critical. Ingestors use watermarks (`last_processed_ts_orig` or source-specific IDs, stored in local SQLite or retrieved from a dedicated DB table like `ingestor_watermarks`) to avoid duplicate processing. The `raw.events` table itself might have unique constraints on `(source, event_type, host, ts_orig, hash_of_key_payload_fields)` for certain event types as a final backstop, though ingestor-side prevention is preferred.

Robust **error handling and Dead Letter Queues (DLQs)** are implemented per-ingestor. After exhausting retries for DB writes (with exponential backoff), failed *original data events* are serialized to a local file in a per-agent DLQ directory (e.g., `/var/lib/sinex/dlq/hyprland/`). The ingestor then attempts to emit a `sinex.agent.dlq_event_written` meta-event to `raw.events`. If this meta-event *also* fails to write to the DB (e.g., DB completely down), this critical "meta-failure" is logged to stdout/stderr and to a specific local append-only text file for that ingestor (e.g., `/var/log/sinex/hyprland/critical_meta_failures.log`). A systemd timer service periodically attempts to resend these critical meta-failures. A separate, dedicated agent handles reprocessing of the main file-based DLQs.

All ingestors adhere to a **standardized output format**, emitting JSON payloads that conform to their registered schemas in `sinex_schemas.event_payload_schemas`, and populating all required top-level fields in `raw.events` (`id`, `source`, `event_type`, `ts_orig`, `host`, `ingestor_version`, `payload_schema_id`).

**Health monitoring and meta-event generation** are standard: periodic `sinex.agent.heartbeat` events, `sinex.agent.error` for operational issues, and specific events for significant actions (e.g., `sinex.ingestor.batch_completed`, `sinex.ingestor.config_reloaded`). Each ingestor updates its entry in `sinex_schemas.agent_manifests` on startup with its current version and capabilities.

* **3.2.2. Detailed Ingestor Domain Breakdowns (Incorporating Phase 2.5 Completeness & PCCD Depth):**

  * **A. Compositor & Direct Input (Hyprland, evdev, Keyboard, Mouse):** This layer provides the highest fidelity capture of direct user interaction with the graphical environment and input hardware.

        The **Hyprland Ingestor** (Rust, direct IPC) is responsible for capturing the full spectrum of events from `socket2.sock` and enriched data from `hyprctl`:
    * *Window Events:* Comprehensive lifecycle (create, map, close, destroy), focus changes (with reason, if available), geometry (move, resize, fullscreen, floating), title/class/app_id changes, workspace switches, monitor additions/removals, urgent hints, config reloads, submap changes. Payloads are rich with all available properties from `hyprctl clients -j` for the affected window(s).
    * *Input Events (Primary via Hyprland IPC):* Hyprland's IPC is the preferred source for low-latency, context-rich keyboard (keysym, scancode, all modifiers) and mouse events (position, button, scroll, deltas), captured as `hyprland.input.key` and `hyprland.input.mouse`. Input latency metrics (compositor-acknowledged vs. application-processed) are a development goal.
    * *Clipboard/PRIMARY:* Full capture of content changes, source application, and MIME types for both CLIPBOARD and PRIMARY selections.
    * *State Snapshots:* Periodic (e.g., every 30 mins) and on-startup full dumps of `hyprctl clients -j, workspaces -j, monitors -j, activewindow -j` as a single `hyprland.state_snapshot` event.
    * *(Advanced features like damage-region video/OCR and VLM hooks for retention decisions are deferred but architecturally anticipated).*

  * B. The ultimate vision for Hyprland integration involves a **native C++ Hyprland Plugin**. This plugin, operating directly within the compositor's process space, unlocks a deeper layer of telemetry and interaction context otherwise inaccessible:

    * **True Render & Frame Timing Data:** Per-monitor render times, exact frame presentation timestamps, achieved FPS, VRR status, and GPU load attributable to composition. This is crucial for diagnosing system performance and correlating user experience with compositor behavior.
    * **Low-Level Input Event Details with Full Compositor Context:**
      * **Keyboard/Mouse Events:** Capture includes not just keysyms/scancodes/coordinates but also *computed modifier states* (as Hyprland sees them), the precise window that *consumed* an input (e.g., a keybinding), and importantly, **input latency metrics** (e.g., time from hardware event detection, through libinput, to compositor processing, to client notification).
      * This offers a richer and more reliable input stream than `evdev` alone, as it includes the compositor's semantic interpretation.
    * **Window Geometry, Animation, and Layer States:** Precise real-time tracking of window positions, sizes, stacking order, animation states (progress, type), and interactions with layer-shell surfaces (panels, notifications).
    * **Compositor Internal Performance Metrics:** Access to data like damage regions (for highly efficient screen capture triggering), texture memory usage per window, and compositor thread CPU/GPU time.
    * **Focus Path and Reason Tracking:** Beyond just knowing *which* window gained focus, the plugin can capture *why* focus changed (e.g., mouse click, Alt-Tab, window closure, new window creation), providing critical causal context for user behavior analysis.
    * **Advanced Visual Capture & Interaction:**
      * **Intelligent Screen Recording:** Direct access to window textures allows for highly efficient, damage-aware video recording (e.g., using VAAPI/NVENC for hardware encoding), capturing only changed regions or specific windows, optionally excluding the cursor.
      * **VLM/OCR Hooks:** The plugin can expose hooks for Vision-Language Models or OCR engines to analyze captured frames or damage regions in near real-time, enabling content-aware storage decisions ("is this video frame worth keeping?") or live text extraction from non-accessible applications.

    The development path envisages the Rust/IPC ingestor as a robust foundational component, with the C++ plugin being a subsequent development phase to unlock these deeper levels of system introspection and visual context capture. Data from both sources would be correlated within the Exocortex."

        The **Keyboard Ingestor** (`ingestor/keyboard`):
    * *Redundant/Fallback Method (`interception-tools` + `journald_bridge`):* As outlined in Phase 2.5, a minimal `interception-tools` plugin writes raw evdev keyboard JSON (device, scancode, value, type, hw_timestamp) to `stdout`. The `ingestor/journald_bridge` agent consumes these specific journald entries, parses the JSON, and emits `input.evdev.keyboard` events to `raw.events`. This ensures capture even if Hyprland is unavailable and provides ground-truth scancodes. Latency added by the plugin must be negligible.

        The **Mouse Ingestor** (`ingestor/mouse`):
    * *Redundant/Fallback Method (evdev + `journald_bridge`):* Similar to keyboard, evdev plugin for mouse devices, with the `journald_bridge` agent emitting `input.evdev.mouse` events (move, button, scroll).

* **B. Application Semantics (Browser, Neovim, Terminal, AT-SPI2):** This layer captures the *semantic meaning* of interactions within key applications.

        A **Browser Ingestor** (extension for Firefox/Chromium + local helper service):
  * *Comprehensive Capture:* Tab lifecycle, navigation history (including SPAs), form submissions (payloads captured, sensitive fields redacted per config), downloads.
  * *Content Archiving (Core for PKM):* On user demand, bookmark sync (Raindrop.io), or heuristic triggers, sends URL and current DOM to a server-side **Web Archiving Agent**. This agent fetches HTML (via headless browser for fidelity, using user's session), extracts text (Trafilatura/Jina Reader), converts to Markdown, stores HTML and Markdown as git-annex blobs (via `core_blobs`), and emits `sinex.web.page_archived` event linking to these blobs and a new `core_artifacts` entry (type `webpage_archive`).
  * Events: `browser.tab.created/activated/closed/updated`, `browser.navigation.completed`, `browser.form.submitted`.

        The **Neovim Plugin** (Lua):
  * *Comprehensive Capture:* Buffer/file operations, text changes (diffs/patches or periodic content snapshots to git-annex), cursor/mode, Ex commands, yanks/pastes, LSP interactions (diagnostics, hover, goto def).
  * *Integration with PKM/LivingDoc:* (Covered in Part II).

        The **Kitty Terminal Ingestor** (`ingestor/kitty`, Rust, using Kitty remote control protocol):
  * *Full Protocol Exploitation:* `command_executed` (with CWD, exit code, selective env vars), window/tab/split lifecycle, title changes, scrollback changes (periodic hash/diff or full capture to git-annex blob on session end), internal clipboard, key binding triggers.

        **PTY Session Recording (e.g., Asciinema-like):**
  * *Philosophy:* For complete, replayable fidelity of terminal sessions, including all visual output and timing, a PTY logger is crucial, complementing the semantic data from the Kitty protocol.
  * *Mechanism:* A wrapper script (e.g., configured as the user's default shell or via a shell alias/function) initiates a PTY recording tool (like `script`, `asciinema rec`, or a custom logger) for each new terminal session.
  * *Output:* The recording tool saves the session data (timing information + byte stream) to a file. This file is then treated as a blob.
  * *Eventification:*
    * `terminal.session.started`: `payload: { session_id_local: "...", recording_tool: "asciinema", terminal_emulator: "kitty" }`
    * `terminal.session.ended`: `payload: { session_id_local: "...", duration_seconds: N, recording_blob_hash_blake3: "...", recording_annex_key: "..." (if applicable) }`
  * *Integration:* The `recording_blob_hash_blake3` links to the `raw_blobs` entry for the session recording. Commands captured by the Kitty protocol can be temporally correlated with the PTY recording for full context. This ensures both semantic understanding (commands) and perfect recall (visual replay).

        The **AT-SPI2 Ingestor** (Python/Rust):
  * *Deep UI Semantics:* Focused app/widget (path, role, name), text changes in input fields (full text), value changes, state changes (checked, expanded), action invocations.
  * *Widget Tree Snapshots:* On app focus or significant state change, dumps full accessibility widget tree as JSONB payload. LLM agents later parse these for semantic UI models.

* **C. System & Environment (Journald Bridge, Filesystem, System Sensors):**

        The **Journald Bridge Ingestor** (`ingestor/journald_bridge`):
  * Also ingests *all other relevant systemd journal entries* (filtered by unit, priority, identifier) beyond keyboard/mouse evdev. Includes Exocortex agent logs, critical system service logs. Emits `journald.log_entry` events.

        The **Filesystem Ingestor** (`ingestor/filesystem`, Rust, `inotify`):
  * *Comprehensive Ops:* Create, delete, modify, attribute change, rename/move for configured watch directories.
  * *Content Hashing & Git-Annex Blobbing:* **Mandatorily computes BLAKE3 hash for all created/modified files.** For files matching configured MIME/size criteria (e.g., text, small images, configs), adds content to **git-annex** and includes `annex_key` and `content_hash_blake3` in the `filesystem.file_updated/created` event.
  * *Context:* PID/command of acting process where possible.
  * *Efficiency:* Robust debouncing and DB write batching.

        **System Sensors & Hardware Events Ingestor (`ingestor/system_sensors`):**
  * Consolidates capture of CPU/memory/disk/network usage, temperatures, battery status/cycles, power events, general process lifecycle events.

* **D. Audio/Visual Streams (PipeWire, Speech-to-Text, OCR):** Capturing and processing ephemeral audio-visual information.

        **System Audio (PipeWire/PulseAudio Integration - `ingestor/audio_system`):**
        1. *Stream Monitoring & Metadata:* Captures events for audio stream lifecycle (creation, destruction, property changes like app name, PID, media role, title, volume, mute). `audio.stream.state_changed` events correlate audio with Exocortex context.
        2. *Selective Audio Recording & Blob Management:* Based on config or real-time triggers, records segments of specified streams to git-annex (FLAC/Opus). `audio.recording.completed` event logs `annex_key`, duration, metadata.
        3. *Speech/Music Detection (Local Analysis):* VAD and music fingerprinting classify recorded segments, adding tags to `audio.recording.completed` or as separate `audio.segment.classified` events.
        4. *Speech-to-Text (S2T) Integration:* A dedicated S2T agent consumes audio blobs (flagged as speech), uses local Whisper.cpp (or configured API) for transcription. `audio.transcript.completed` event stores transcript text (or `content_id` if text is large) and links to the audio `annex_key`. Transcript text becomes embeddable via `core_artifact_contents`.

        **Targeted OCR (Optical Character Recognition - `agent_ocr_processor`):** Fallback for text extraction from visual-only sources.
        1. *Triggering:* Manually by user hotkey (region/window selection), or agentically by Hyprland ingestor (damage in OCR-monitored window) or other agents (e.g., image linked in note suggests text).
        2. *Process:* Screenshot of target region (temp blob) sent to OCR agent (Tesseract/PaddleOCR/API).
        3. *Output & Eventification:* `ocr.text_recognized.completed` event stores extracted text, confidence, bounding boxes, reference to source screenshot `annex_key` (now permanent), and window/app context.

* **E. Mobile, Wearable & IoT Context: Extending Beyond the Desktop**

        Integrating signals from the user's broader personal ecosystem.
        **Phone & Watch Integration (`ingestor/mobile_bridge` on host, companion app/scripts on device):**
        1. *Mechanism:* Android (Termux/Tasker/CompanionApp), iOS (Shortcuts/HealthKit exports). Secure HTTPS POST/MQTT to host ingest endpoint. Local buffering and replay for connectivity issues. HMAC for integrity.
        2. *Data Captured:* Notifications (app, title, text, actions). Call/SMS metadata (hashed IDs, timestamp, duration). App usage (foreground app, duration). Device State (screen, unlock, battery, charge, network). Location (opt-in, coarse/geofence or GPS for specific activities). Sensors (steps, light, proximity). Wearable data (heart rate, sleep phases, activity types) via synced phone app.
        3. *Event Structure:* `raw.events` entries with `source` like `"mobile_android_sinex"` and correct device `host` ID.

        **BLE/IoT Sensors & Presence (`ingestor/presence_iot`):**
        1. *BLE-Based Presence:* Host script scans for known BLE devices (phone, watch, beacons). Emits `presence.ble_device.detected/lost` events.
        2. *Environmental IoT Sensor Streams:* Agent subscribes to MQTT/HTTP from ESPHome, Home Assistant, etc. Ingests temperature, humidity, CO2, light, motion as `sensor.environment.<type>.<location>` events.

* **F. Meta-Cognitive & Subjective Ingestion: Capturing the Inner Landscape**

    This domain, foundational to the Exocortex, eventifies the user's internal states, reflections, and plans. Events are primarily user-initiated but can be agent-prompted. Payloads are rich and structured, with `source` prefixes like `meta.`, `planning.`, `subjective.`.
    **Manual Logging Interfaces:** `exo log <meta_type>`, Neovim commands (`:ExoLogFriction`), future TUI/GUI forms, Living Document commands (`/insight ...`).
    **Key Event Types & Payloads:**
* `meta.friction_logged`: `{ description, perceived_cause, intensity, linked_task_ids, associated_event_ids, resolution_status, resolution_notes }`
* `meta.insight_captured`: `{ description, confidence_level, related_project_id, trigger_event_ids, novelty_score, actionable_steps_proposed, tags }`
* `meta.activation_energy_shift`: `{ direction ('up'/'down'), new_level_estimate_raw_or_percent, perceived_reason, linked_context_events }`
* `planning.milestone_defined`: `{ blueprint_ref_document_id, milestone_name, description, status ('defined', 'in_progress', 'completed', 'blocked', 'deferred'), target_date, dependencies_jsonb, owning_project_id }`
* `planning.goal.defined`: `{ description, timeframe_description, success_metrics, priority, parent_goal_id }`
* `meta.narrative_generated` (User or Agent): `{ title, narrative_text, timespan_start_orig, timespan_end_orig, key_object_ids_referenced_jsonb, mood_tags_inferred, themes_identified, generation_source, user_rating_of_narrative }`
* `subjective.mood_reported`: `{ mood_scale_name, mood_values_jsonb, mood_descriptors_freeform, context_notes, trigger_event_id }`
* `physio.sleep_logged`: `{ start_ts_orig, end_ts_orig, duration_total_minutes, quality_score_subjective, sleep_data_source, notes, interruptions_count, deep_sleep_minutes, rem_sleep_minutes }`
* `substance.dose_logged`: `{ substance_name, dosage_amount, dosage_unit, route_of_administration, reason_for_use, subjective_effects_short_term, ts_effects_onset_orig }`
    **Agent-Prompted Logging:** Agents detect patterns (high error rates, task stagnation) and suggest logging relevant meta-events via `sinex.system.suggestion_created`, which the user can act upon.

* **3.3. The Structuring Engine: From Raw Signals to Actionable Knowledge**

* **3.3.1. Philosophy: Emergent Order, Lossless Transformation, Traceable Lineage, Retroactive Processing**

The vast and diverse streams of raw data captured by the Exocortex's sensory network, while foundational, are often not immediately actionable or insightful in their unprocessed state. The **Structuring Engine** is the layer responsible for transforming this raw data into meaningful, queryable, and interconnected knowledge. Its operation is guided by core philosophies that ensure flexibility, integrity, and long-term utility. **Emergent order** is paramount; rather than imposing rigid, predefined schemas onto all incoming data, structure is introduced iteratively and often retrospectively, as patterns of use and analytical needs become clear. **Lossless transformation** dictates that all structuring processes—be it promotion to typed tables, semantic enrichment, or linking—must preserve the integrity of the original raw events; derived data is always additive and traceable, never overwriting the source truth. This **traceable lineage** ensures that every piece of structured information can be audited back to its raw origins and the specific agents or pipelines that produced it. Finally, **retroactive processing** is a key capability: as new structuring techniques, AI models, or understanding of the data evolve, the system must be ables to reprocess historical raw data to apply these new lenses, continuously refining and deepening the knowledge base.

* **3.3.2. Promotion Pipelines: Mechanism & Orchestration**

The primary mechanism for transforming data from the schemaless `raw.events` table into more structured **Domain Tables** (though direct querying of `raw.events` with JSONB operators remains always possible) is through **Promotion Pipelines**. These pipelines are typically implemented as **asynchronous agents** that subscribe to specific `(source, event_type)` combinations in `raw.events` or run periodically. While PostgreSQL triggers can be used for very simple, synchronous transformations, agents offer greater flexibility, resilience, and the ability to incorporate complex logic, including LLM calls or external API interactions.

The typical **agent logic** for a promotion pipeline involves several steps:

1. Fetch a batch of new raw events (e.g., using a watermark like `last_processed_raw_event_id` or `last_processed_ts_ingest` for a given `(source, event_type)`).
2. For each event, parse its JSONB `payload`. Using the `payload_schema_id` from `raw.events`, retrieve the corresponding JSON Schema definition from `sinex_schemas.event_payload_schemas`. The agent *should* attempt to validate the payload against this schema. Validation failures result in a `sinex.schema.validation_failure` meta-event being logged, and the raw event might be routed to a specific "quarantine" state for review or processed with relaxed parsing, but it's never lost. The promotion agent must be robust to schema evolution in the `payload`.
3. Extract and transform relevant fields from the payload into the strongly-typed columns of one or more target Domain Tables (e.g., `domain_hyprland.focus_changes`, `domain_kitty.commands_executed`).
4. Insert the transformed data into these Domain Tables.

Crucially, for **lineage**, every row inserted into a Domain Table *must* include a `raw_event_id_fk ULID REFERENCES raw.events(id)` column. Additionally, the provenance of the promotion itself (e.g., `promotion_agent_name TEXT` (from `agent_manifests`), `promotion_agent_version TEXT`, `promotion_timestamp TIMESTAMPTZ`) should be stored.

**Idempotency** of promotion agents is critical, typically achieved using the `raw_event_id_fk` in an `INSERT ... ON CONFLICT (raw_event_id_fk) DO UPDATE SET ...` (if promotions can be refined) or `DO NOTHING` statement.

* **3.3.3. Domain Table Design Principles:**

Domain Tables provide structured views for optimized querying:

* **Granularity:** Tables focus on specific event types or entities (e.g., `core_artifacts` for PKM/web, `domain_web.page_visits`, `meta_cognitive.friction_reports`).
* **Pragmatic Normalization:** Balance redundancy reduction with query performance. Key contextual fields (`host`, `ts_orig`) are often copied from `raw.events`.
* **Common Columns:** `id ULID PK`, `raw_event_id_fk ULID`, `ts_orig TIMESTAMPTZ`, `host TEXT`, specific structured fields, `enrichment_metadata JSONB`.

* **3.3.4. Enrichment Processes (Layered and Agent-Driven):**

Asynchronous **Enrichment Agents** apply further semantic layers:

* **Tagging:** Universal Tagging System (`core_tags`, `core_tag_aliases`, `artifact_tags` join table linking tags to `raw.events.id`, `core_artifacts.artifact_id`, or `core_blobs.blob_id`). Agents suggest tags (LLM-driven), users manually tag.
* **Embedding Generation (Phase 3 focus):** The `EmbeddingAgent` consumes textual content from `core_artifact_contents.content_text`, selected `raw.events.payload` fields, or Living Document segments. Handles chunking (fixed-size with overlap initially, semantic chunking future), calls embedding models, stores vectors in `artifact_embeddings` (linked to source content ULID and `embedding_name` for context/chunk). Input text hashing for `embedding_cache` deduplication is planned.
* **Named Entity Recognition (NER) & Linking:** LLM-based NER agents identify/resolve entities to `core_entities`. Links created in `core_entity_relations`.
* **Semantic Hashing & Content Deduplication:** Beyond git-annex, semantic hashes (MinHash, pHash) identify near-duplicates, triggering `core.entity_relations` links or flags.
* **Summarization:** LLM agents generate summaries stored as linked `core_artifacts` (type: `summary`) or in `enrichment_metadata`.

All enrichment is additive, traceable, and re-computable.

* **3.3.5. The Knowledge Graph as an Emergent Semantic Network:**

    The Exocortex's Knowledge Graph (KG) is not a predefined ontology imposed top-down, but rather an **emergent semantic network** that grows organically from the captured data and the structuring/enrichment processes. It aims to represent key "things" (entities) and the relationships between them, providing a powerful layer for contextual understanding, discovery, and inference.

  * **Core Components:**
    * **`core_entities` Table:** This is the central registry for all canonical "things" or "concepts" the Exocortex recognizes. Each entity has a stable ULID and a defined type.

            ```sql
            CREATE TABLE IF NOT EXISTS core_entities (
                entity_id               ULID PRIMARY KEY DEFAULT generate_ulid(),
                entity_type             TEXT NOT NULL, -- e.g., 'pkm_note', 'webpage_archive', 'person', 'project', 'task', 'intent', 'activity_segment', 'software_application', 'file_path', 'topic_tag', 'geographic_location'
                canonical_label         TEXT NOT NULL, -- Primary human-readable name/identifier (e.g., note title, URL, person's name, project name)
                aliases                 TEXT[],        -- Alternative names or identifiers
                properties              JSONB,         -- Type-specific attributes (e.g., for 'task': {status, priority, due_date}; for 'person': {email, role})
                description             TEXT,
                created_at_ts_orig      TIMESTAMPTZ,   -- When this entity was first recognized/created
                last_event_ts_orig      TIMESTAMPTZ,   -- Timestamp of the last raw event linked to this entity
                embedding_vector        VECTOR         -- Optional: embedding of the canonical_label + key properties for entity similarity
            );
            CREATE INDEX IF NOT EXISTS idx_core_entities_type_label ON core_entities (entity_type, canonical_label);
            CREATE INDEX IF NOT EXISTS idx_core_entities_aliases_gin ON core_entities USING GIN (aliases);
            CREATE INDEX IF NOT EXISTS idx_core_entities_embedding ON core_entities USING ivfflat (embedding_vector vector_cosine_ops) WHERE embedding_vector IS NOT NULL;
            ```

    * **`core_entity_relations` Table:** Defines typed, directed relationships between entities.

            ```sql
            CREATE TABLE IF NOT EXISTS core_entity_relations (
                relation_id             ULID PRIMARY KEY DEFAULT generate_ulid(),
                source_entity_id        ULID NOT NULL REFERENCES core_entities(entity_id) ON DELETE CASCADE,
                target_entity_id        ULID NOT NULL REFERENCES core_entities(entity_id) ON DELETE CASCADE,
                relation_type           TEXT NOT NULL, -- e.g., 'mentions', 'links_to', 'works_on_project', 'uses_tool', 'depends_on_task', 'located_at', 'authored_by', 'related_to_topic'
                properties              JSONB,         -- Attributes of the relationship itself (e.g., confidence score, role in relation)
                ts_start_orig           TIMESTAMPTZ,   -- Optional: start time of the relationship's validity
                ts_end_orig             TIMESTAMPTZ,   -- Optional: end time of the relationship's validity
                created_at              TIMESTAMPTZ DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS idx_core_entity_relations_source_type ON core_entity_relations (source_entity_id, relation_type);
            CREATE INDEX IF NOT EXISTS idx_core_entity_relations_target_type ON core_entity_relations (target_entity_id, relation_type);
            ```

    * **Linking `raw.events` to Entities:** A join table like `event_entity_links` (`event_id ULID`, `entity_id ULID`, `role_in_event TEXT`, PRIMARY KEY (`event_id`, `entity_id`, `role_in_event`)) can explicitly connect raw events to entities mentioned or involved in them.

  * **Population and Evolution:**
    * **PKM & Web Artifacts:** Entries in `core_artifacts` (for PKM notes, web archives) are directly represented as `core_entities` nodes (e.g., `entity_type='pkm_note'`, `canonical_label`=note title, `entity_id`= `artifact_id`). Links parsed from these artifacts populate `core_entity_relations`.
    * **NER Agents:** Scan text from `core_artifact_contents` and event payloads to identify mentions of people, organizations, projects, topics. These are resolved to existing `core_entities` or used to create new ones, with `mentions` relations created.
    * **Derived Semantic Layers:** Identified `activity_segment.identified` events, `intent.declared` events, or `composite_action.identified` events also become nodes in `core_entities`.
    * **User Curation:** Users can manually create entities, define relationships, and merge duplicate entities via UI/CLI commands.
    * **Tagging Integration:** `core_tags` can themselves be represented as `core_entities` of `entity_type='topic_tag'`, and `artifact_tags` links translate into `related_to_topic` relations in `core_entity_relations`.

  * **Utility:**
    * Provides a unified semantic view over all captured data.
    * Enables complex contextual queries: "Show me all PKM notes and web pages (`entity_type='pkm_note' OR entity_type='webpage_archive'`) that `mention` the `person` 'John Doe' and are also `related_to_topic` 'NixOS', created during an `activity_segment` where I was `working_on_project` 'Sinex Documentation'."
    * Forms the substrate for advanced agentic reasoning and insight generation.
    * Supports graph-based visualizations and exploration of the user's knowledge landscape.

* **3.3.6. Embedding Generation and Semantic Indexing (Future Phase Foundation):**

* **3.3.6. Embedding Generation and Semantic Indexing (Future Phase Foundation):**

  While the active implementation of a full embedding pipeline is deferred beyond Phase 2.5, the foundational architectural thinking and schema preparations are crucial for enabling future semantic search and AI-driven analysis. The Exocortex's approach to embeddings will be characterized by contextual richness, efficient storage, and support for diverse content types.

  * **Philosophy of Embedding:** Embeddings transform textual (and potentially other modal) data into dense vector representations, capturing semantic meaning. The goal is to enable similarity searches ("find content like this") and provide rich input for LLM agents. The strategy will favor embedding contextually complete "semantic documents" rather than isolated text snippets, and will aim for efficiency through caching and deduplication.

  * **Target Content for Embedding:** Primarily, textual content from `core_artifact_contents` (PKM notes, markdownified web pages, extracted PDF text), significant textual payloads from selected `raw.events` (e.g., clipboard copies, detailed error messages, user-logged insights), and segments of the Living Document. The policy for what gets embedded will be configurable and evolve.

  * **Semantic Document Construction:** Rather than embedding isolated fields, the system will construct a "semantic document" for embedding by combining the primary text with relevant contextual metadata (e.g., for a clipboard copy, include the source application and window title alongside the copied text). This will be defined via configurable templates or rules per event/artifact type.

  * **Chunking Strategies:** For texts exceeding model token limits, sophisticated chunking will be employed:
    * **Initial:** Fixed-size token chunks with significant overlap to maintain semantic continuity.
    * **Future:** Semantic chunking (e.g., based on paragraphs, sections, or LLM-identified logical units) will be preferred for higher-quality chunk vectors. Specialized chunkers might be developed for different content types (prose, code, logs).

  * **`artifact_embeddings` Table (Primary Embedding Store):**
      This table will store embeddings generated from the textual content within `core_artifact_contents`.

      ```sql
      CREATE TABLE IF NOT EXISTS artifact_embeddings (
          content_id              ULID NOT NULL REFERENCES core_artifact_contents(content_id) ON DELETE CASCADE,
          embedding_name          TEXT NOT NULL, -- e.g., "full_text_chunk_001", "title_summary_v1"
          model_name              TEXT NOT NULL, -- e.g., "openai/text-embedding-3-small"
          model_dimension         INT NOT NULL,
          embedding_vector        VECTOR,        -- Using pgvector's vector type
          input_text_hash_blake3  TEXT,          -- BLAKE3 hash of the exact text chunk sent for embedding
          created_at              TIMESTAMPTZ DEFAULT now(),
          PRIMARY KEY (content_id, embedding_name, model_name)
      );
      CREATE INDEX IF NOT EXISTS idx_artifact_embeddings_vector ON artifact_embeddings USING ivfflat (embedding_vector vector_cosine_ops); -- Or HNSW
      CREATE INDEX IF NOT EXISTS idx_artifact_embeddings_model_name ON artifact_embeddings (model_name);
      CREATE INDEX IF NOT EXISTS idx_artifact_embeddings_input_text_hash ON artifact_embeddings (input_text_hash_blake3);
      ```

      The `embedding_name` allows multiple embeddings per `content_id` (e.g., for different chunks, different models, or different summarization levels).

  * **`event_embeddings` Table (For Direct Raw Event Payloads):**
      For specific `raw.events` whose textual payload is directly valuable for semantic search without promotion to `core_artifact_contents`.

      ```sql
      CREATE TABLE IF NOT EXISTS event_embeddings (
          event_id                ULID NOT NULL REFERENCES raw.events(id) ON DELETE CASCADE,
          embedding_name          TEXT NOT NULL, -- e.g., "payload_text_field_X_chunk_001"
          model_name              TEXT NOT NULL,
          model_dimension         INT NOT NULL,
          embedding_vector        VECTOR,
          input_text_hash_blake3  TEXT,
          created_at              TIMESTAMPTZ DEFAULT now(),
          PRIMARY KEY (event_id, embedding_name, model_name)
      );
      CREATE INDEX IF NOT EXISTS idx_event_embeddings_vector ON event_embeddings USING ivfflat (embedding_vector vector_cosine_ops);
      CREATE INDEX IF NOT EXISTS idx_event_embeddings_model_name ON event_embeddings (model_name);
      CREATE INDEX IF NOT EXISTS idx_event_embeddings_input_text_hash ON event_embeddings (input_text_hash_blake3);
      ```

  * **`embedding_cache` Table (Deduplication):**
      To avoid re-embedding identical text chunks with the same model, a cache based on the hash of the input text will be used.

      ```sql
      CREATE TABLE IF NOT EXISTS embedding_cache (
          input_text_hash_blake3  TEXT NOT NULL,     -- Hash of the exact input text string
          model_name              TEXT NOT NULL,     -- Model used for this cached embedding
          model_dimension         INT NOT NULL,
          embedding_vector        VECTOR NOT NULL,   -- The cached vector
          first_generated_at      TIMESTAMPTZ DEFAULT now(),
          PRIMARY KEY (input_text_hash_blake3, model_name)
      );
      ```

      The `EmbeddingAgent` will first check this cache. If a hash/model match is found, the existing vector is reused (i.e., a new row is added to `artifact_embeddings` or `event_embeddings` referencing this `input_text_hash_blake3` and `model_name`). If not found, the text is embedded, and the new vector is added to the cache.

  * **The Embedding Agent:** A dedicated agent will be responsible for:
    * Identifying embeddable content based on configurable rules (source/type patterns, JSONPaths to text fields, content heuristics).
    * Constructing the "semantic document" for embedding.
    * Performing chunking.
    * Managing the `embedding_cache`.
    * Calling embedding model APIs (local or remote).
    * Storing results in `artifact_embeddings` or `event_embeddings`.
    * Handling errors and logging its operations (`sinex.agent.embedding_generated`, `sinex.agent.error`).

* **3.4. Blob Management with Git-Annex: The Physical Archive for Non-Textual and Original Source Content**

* **3.4.1. Philosophy: Content-Addressing for Integrity, Deduplication, Location Independence. Metadata in DB, Content in Annex.**

PostgreSQL manages metadata, event logs, and primary textual content (e.g., Markdown in `core_artifact_contents`). Physical storage of large binary objects (images, audio, video, original HTML, datasets) is delegated to **git-annex**. Core principle: **content-addressing** via cryptographic hash (annex key, typically SHA256E) for integrity, deduplication, and location independence.

* **3.4.2. `core_blobs` Table: Central Metadata Registry for Annexed Content**
    `blob_id ULID PK`, `content_annex_key TEXT NOT NULL UNIQUE`, `content_blake3_hash TEXT UNIQUE NULLABLE`, `mime_type TEXT`, `size_bytes BIGINT`, `original_filenames ARRAY<TEXT>`, `user_description TEXT`, `extracted_media_metadata JSONB`, `schema_id ULID FK sinex_schemas.event_payload_schemas NULLABLE`, `created_at_ts_orig TIMESTAMPTZ`, `ingested_at_ts TIMESTAMPTZ DEFAULT now()`.

* **3.4.3. Integration Workflow with Exocortex Events & Artifacts:**

1. Ingestor identifies/receives blob.
2. Adds to git-annex (`git annex add`), obtains `annex_key`.
3. Emits event to `raw.events` (e.g., `pkm.note.attachment_added`). Payload includes `blob_refs` (array of `annex_key`s or `blob_id`s).
4. `core_blobs` entry created/updated. `core_artifacts` (like a PKM note) links to this `blob_id` if it's a primary representation (e.g., `current_content_blob_id` for an image note) or via `core_artifact_links` if an attachment.

* **3.4.4. Accessing Blobs:** UI/Agents query DB for `annex_key`, then use `git annex get` to make content locally available.
* **3.4.5. Benefits & Management:** Native deduplication, integrity (`git annex fsck`), distributed storage (remotes), versioning (of symlink structure via underlying Git repo). Agents manage integrity checks, orphan detection, backup coordination.

3.5. The Semantic Desktop Stream: Synthesizing Context for Advanced Agency**
---

While individual ingestors provide granular data streams, the true power of the Exocortex for enabling advanced AI agency (both for user assistance and potential system automation) lies in its ability to **synthesize these streams into a coherent, real-time model of the user's current desktop context and available actions.** This is conceptualized as the **Semantic Desktop Stream**.

* **III.5.1. Philosophy: Beyond Raw Events – Towards Actionable Understanding**
    Raw event logs, while complete, are often too low-level for an LLM agent to directly "understand" the user's situation or to effectively plan and execute actions. The Semantic Desktop Stream aims to provide a continuously updated, structured representation of:
  * The currently focused application and its specific context (e.g., active file in an editor, URL in a browser, command being typed in a terminal).
  * The content visible or directly accessible to the user (e.g., text in focused widgets, DOM elements, editor buffer content).
  * The set of permissible actions or affordances within the current application state (e.g., available menu items, buttons, keybindings, API calls).
  * The broader desktop context (other open windows, active projects, recent notifications, ongoing background tasks).
  * The user's recent interaction history and inferred short-term intent.

* **III.5.2. Architectural Realization (`SemanticDesktopContextManager` Agent):**
    A dedicated, high-priority agent (e.g., `sinex.agent.semantic_desktop_manager`) is responsible for constructing and maintaining this stream. It subscribes to key, high-fidelity event sources:
  * Hyprland Ingestor (especially the C++ plugin for focus, window state, input).
  * AT-SPI2 Ingestor (for widget trees, roles, names, values, and text content).
  * Application-specific ingestors (Neovim for buffer/mode, Browser for DOM/URL, Kitty for CWD/command).
  * Living Document for current user thoughts/plans.

    This agent continuously fuses these inputs, maintaining an in-memory model (or a rapidly updated cache in a dedicated DB table/view) of the current semantic state.

* **III.5.3. Dynamic Widget Tree Analysis with LLM Assistance:**
    A critical component is the interpretation of dynamic application UIs via AT-SPI2. The `SemanticDesktopContextManager` (or a specialized sub-agent) employs a strategy for this:
    1. **Pattern Learning:** It observes and hashes the structure of widget trees from different applications.
    2. **LLM for Novel Layouts:** When a significantly new or unrecognised widget tree structure is encountered for an application, it queries an LLM (with a prompt like: "Analyze this widget tree for app X. Identify main content areas, key input fields, primary action buttons, and navigation elements. Provide JSONPaths or selectors to extract these.")
    3. **Cached Parsers/Rules:** The LLM-generated (or manually refined) extraction rules for known app layouts are cached.
    4. **Real-time Application:** For known layouts, cached rules extract semantic information. For new layouts, the LLM is consulted. This balances efficiency with adaptability to UI changes.

* **III.5.4. Output: The Structured Semantic Contextual Stream**
    The `SemanticDesktopContextManager` exposes its synthesized context via:
  * **A well-defined event type** (e.g., `sinex.desktop.semantic_context_updated`) pushed into `raw.events` whenever significant state changes occur. The payload of this event would be a rich JSON object representing the current understanding (see example in ONCB summary: focused window app/file/cursor/visible_text/actions, other windows, recent interaction).
  * **A queryable API or database view** that LLM agents or UI components can access on demand to get the latest synthesized context.

* **III.5.5. Enabling Advanced AI Agency (Read and Write):**
    This Semantic Desktop Stream is the crucial input for sophisticated LLM-driven agents intended for:
  * **Deep Contextual Understanding:** Allowing an LLM to "see" what the user sees and understand the available interactions.
  * **Task Automation:** Providing the necessary information for an agent to plan and execute multi-step tasks across applications (e.g., "Find the email from John about Project X, summarize it, and draft a reply incorporating information from my PKM note titled 'Project X Scope'").
  * **Proactive Assistance:** Enabling agents to offer relevant suggestions, context, or actions based on a deep understanding of the current user state.
  * **"Write" Capabilities:** The same hooks used for capture (Hyprland for synthetic input, AT-SPI2 for direct widget manipulation, browser extension for JS execution) become the output channels for LLM agents acting upon the semantic stream, allowing them to type, click, navigate, and interact with applications on the user's behalf (with appropriate user consent and control).

The development of the Semantic Desktop Stream, particularly its LLM-driven dynamic UI analysis and its "write" capabilities for agentic control, represents an advanced stage of the Exocortex. However, its conceptual place as the synthesizer of all primary capture streams into an actionable model for AI partnership is core to the ultimate vision."

---

**Part IV: The Agentic Ecosystem – Automation, Intelligence, and Partnership**

*(This Part details the "active intelligence" layer of the Sinnix Exocortex. It builds upon the foundational principles of Agentic Partnership, leveraging LLMs as Co-Pilots, and using Feedback as Fuel. Here, we explore how modular software agents, including sophisticated Large Language Models, process the captured data, automate tasks, generate insights, and collaborate with the user to transform the Exocortex from a passive archive into a dynamic cognitive partner.)*

The raw data streams and structured knowledge within the Exocortex, while valuable in themselves for query and recall, achieve their full potential through the actions of an **Agentic Ecosystem**. This ecosystem comprises a diverse array of specialized software components—agents—that operate on the data substrate to perform tasks ranging from routine data maintenance and enrichment to complex semantic analysis and proactive user assistance.

**4.1. The Agent Framework: Orchestrating Distributed Intelligence**

* **4.1.1. Philosophy of Agentic Design:**

The design of agents within the Sinnix Exocortex is guided by several core tenets:

* **Modularity and Specialization:** Agents are not envisioned as monolithic, general-purpose AIs. Instead, each agent has a clearly defined, relatively narrow responsibility (e.g., "embed PKM notes," "extract TODOs from Living Document segments," "monitor system disk space"). This makes them easier to develop, test, manage, and reason about.
* **Event-Driven and Asynchronous Operation:** The primary mode of operation for most agents is to react to new data appearing in the Exocortex—new entries in `raw.events`, new rows in domain tables, or changes to specific artifacts. They typically operate asynchronously in the background, polling for new work or being triggered by database notifications (e.g., PostgreSQL `LISTEN/NOTIFY`).
* **Transparency and Auditability:** All significant actions taken by an agent (e.g., creating a new tag, linking two notes, generating a summary, failing to process an event) are themselves logged as `sinex.agent.action_taken` events in `raw.events`. These logs include the agent's ID/name, the input event(s) or data that triggered the action, the parameters used, and the outcome (including references to any new artifacts created). This ensures full provenance and allows the user (or other agents) to understand and debug agent behavior.
* **User Control and Override:** While agents can operate autonomously based on their configuration, the user always retains ultimate control. This includes the ability to enable/disable agents via their manifest, configure their behavior (e.g., thresholds, frequency of operation, aggressiveness of suggestions via NixOS options that generate agent config files), review their proposed actions before execution (for critical operations, often via an "inbox" or `sinex.system.suggestion_created` event that requires user approval), and easily override or correct their outputs (which itself generates feedback events). The interaction model for agents involves not just confirmation but also a potential for dialogue, where users can *teach* agents by correcting their suggestions or providing explicit feedback, which is logged and used by meta-agents for future refinement.

* **4.1.2. Agent Registry (`sinex_schemas.agent_manifests`): Centralized Definition and Discovery**

The **Agent Registry**, implemented as the `sinex_schemas.agent_manifests` PostgreSQL table (defined in Phase 2), serves as the canonical source of information about all available agents within the user's Exocortex instance.
The schema includes:

* `agent_name TEXT PRIMARY KEY`: Canonical, unique name (e.g., `"PkmNoteEmbedderAgent"`, `"KittyIngestor"`).
* `description TEXT`: Human-readable purpose.
* `version TEXT`: Code version.
* `status TEXT`: e.g., `'development'`, `'stable'`, `'disabled_by_user'`, `'error_state'`.
* `config_schema_id ULID FK sinex_schemas.event_payload_schemas NULLABLE`: Points to a JSON Schema defining the structure of this agent's configuration file. The actual configuration values for an agent are typically loaded from a file (e.g., TOML, JSON) whose path is known to the agent (often via command-line argument or environment variable). This file is often generated declaratively by the NixOS system configuration, adhering to the structure defined by this `config_schema_id`.
* `produces_event_types JSONB`: Declares the `(source, event_type)` combinations this agent is known to generate, including the `payload_schema_id` for each.
* `subscribes_to_event_types JSONB NULLABLE`: Declares `(source, event_type)` combinations this agent consumes as triggers, and potentially the expected `payload_schema_id`.
* `repo_url TEXT NULLABLE`: Link to agent's source code.
* `last_seen_heartbeat TIMESTAMPTZ NULLABLE`: Updated by a monitoring process from `sinex.agent.heartbeat` events.
* `registered_at TIMESTAMPTZ DEFAULT now()`.

Agents self-register or update their manifest entry upon startup, advertising their capabilities and current version. The `exo agent list/status` CLI commands provide manual introspection.

* **4.1.3. Systemd Integration & Lifecycle Management (via NixOS Modules):**

Each agent is typically managed as a dedicated systemd user service (for continuously running daemons or event subscribers) or a systemd timer unit (for periodic tasks). This integration, configured declaratively within its own NixOS module, provides:

* **Lifecycle Control & Restart Policies.**
* **Resource Quotas** (CPU, Memory), enforced via systemd.
* **Logging:** All agent `stdout/stderr` captured by journald, then ingested by the `JournaldBridgeIngestor` into `raw.events`, making operational logs queryable.

* **4.1.4. Communication & Data Flow Patterns:**

Primary agent communication is **event-driven via the PostgreSQL database**:

* **Consumption:** Agents poll `raw.events` or domain tables for new/relevant entries (using watermarks) or use PostgreSQL `LISTEN/NOTIFY` for near real-time triggers.
* **Production:** Agents insert new `raw.events` or rows into domain/core tables.
* **Inter-Agent Communication (Event Chains):** Agent A emits event X; Agent B consumes event X and emits event Y. For complex DAGs, a dedicated Workflow Orchestrator Agent (future) could manage state by emitting command-like events or tasks into a `core_tasks` table (a type of `core_entity`).
* **Agent DLQs:** If an agent fails to process an input event after retries, it writes details to a PostgreSQL table like `agent_processing_dlq` (`id ULID PK`, `failed_raw_event_id ULID FK raw.events`, `processing_agent_name TEXT FK sinex_schemas.agent_manifests`, `error_details JSONB`, `retry_count INT`, `status TEXT (pending_review, resolved, ignored)`). This ensures processing failures are tracked centrally.

* **4.2. LLMs in the Exocortex: Roles, Integration, and Meta-Programming**

Large Language Models are a pervasive enabling technology, acting as co-pilots for the user and engines for various autonomous processes.

* **4.2.1. Diverse Roles of LLMs:**
  * Content Generation (summaries, narratives, PKM drafts).
  * Structuring & Parsing (Living Doc deltas, artifact extraction from text, UI widget tree interpretation from AT-SPI2).
  * Classification & Tagging (semantic tags, sentiment, topic modeling).
  * Query Understanding (natural language to `exo query` syntax or SQL).
  * Code Generation (assisting in drafting new ingestors, agent logic, data transformations).
  * Prompt Engineering & Refinement (meta-agents improving other agents' prompts).
  * Anomaly Detection & Suggestion (identifying unusual patterns, suggesting actions).
  * Semantic Linking (proposing connections based on conceptual similarity).

* **4.2.2. Model Management & Access:**
  * Support for **Local Models** (Ollama, Llama.cpp for privacy/cost) and **Remote APIs** (OpenAI, Anthropic, Google for SOTA capabilities). API keys via `agenix`.
  * **`core_llm_models` Registry (DB Table):** `id ULID PK`, `model_name_unique TEXT`, `provider TEXT`, `api_endpoint TEXT`, `capabilities JSONB` (context window, modalities), `cost_per_token JSONB`, `access_tier TEXT`.
  * **LLM Router/Proxy Service (Conceptual):** Agents request models by capability (e.g., "32k_context_code_gen") or named role ("default_summarizer"). Router selects best available/configured model from `core_llm_models`, considering cost, privacy preferences (user-configurable), and load.

* **4.2.3. Prompt Engineering & Management (`core_prompts`):**
  * **`core_prompts` Table:** `id ULID PK`, `prompt_name TEXT UNIQUE`, `prompt_template TEXT`, `version TEXT`, `variables_schema_id ULID FK sinex_schemas.event_payload_schemas NULLABLE` (JSON Schema for expected template inputs), `description TEXT`, `target_llm_family TEXT`. Unique on `(prompt_name, version)`.
  * Agents retrieve prompts by name/version, interpolate with runtime data.
  * **Meta-Agents for Prompt Optimization:** Consume `sinex.agent.llm_output_feedback` (user ratings/edits of LLM outputs), run A/B tests, suggest/apply refinements to `core_prompts`.

* **4.2.4. Cost Tracking and Budgeting:**
  * All LLM API calls logged as `sinex.agent.llm_api_call` events in `raw.events` (payload: `agent_name_calling`, `prompt_name_used`, `model_name_invoked`, `input_tokens`, `output_tokens`, `cost_usd`, `latency_ms`).
  * Agents have configurable `daily_llm_cost_budget_usd` in their manifest/NixOS-generated config. They monitor spend, throttle, switch to cheaper models, or alert user if nearing budget.
  * Grafana dashboards visualize LLM costs per agent/model/prompt.

* **4.3. Archetypal Agents and Their Capabilities (Illustrative Examples)**

The agent ecosystem is designed to be diverse and extensible. Below are archetypes illustrating the range of functionalities, categorized for clarity. Each would be an instance in `sinex_schemas.agent_manifests` and run as a systemd service managed by its NixOS module.

* **4.3.1. Task-Oriented & Proactive Agents:** These agents typically perform specific, often user-facing tasks or provide proactive assistance.
  * `DailyJournalPrompter`: Runs daily via systemd timer. Queries recent significant events (e.g., major PKM edits, completed milestones, high-friction periods) and Living Document changes. Generates a personalized prompt for daily reflection using an LLM. Creates a new `core_artifacts` entry (type: `pkm_note`, subtype: `daily_journal`, title: `Journal - YYYY-MM-DD`) containing this prompt and links (via `core_artifact_links`) to the contextual events/artifacts. Emits `sinex.pkm.note_created` and `sinex.system.suggestion_created` (for user notification).
  * `WebPageArchiverAndMarkdownifier` (detailed in Part II.2.3): Consumes `sinex.web.capture_request` events. Fetches HTML (stores in git-annex via `core_blobs`). Extracts main text (Trafilatura). Converts to Markdown (stores in git-annex via `core_blobs`). Creates/updates `core_artifacts` entry (type: `webpage_archive`) and associated `core_artifact_contents` entry with Markdown. Generates embedding for Markdown. Parses for links. Emits `sinex.web.page_archived`.
  * `StaleProjectDetector`: Periodically (e.g., weekly) scans `core_artifacts` (where `artifact_type='project_entity'` or notes tagged with project identifiers), `artifact.todo.updated`, and Living Document nodes linked to projects. If a project shows no significant new activity (edits, new tasks, new linked events) for a configurable period (e.g., N weeks), it creates a `sinex.system.suggestion_created` event: "Project X appears stale. Would you like to review its status or archive it?" Links to the main project entity/note.
  * `TodoExtractorFromText`: Consumes `core_artifact_contents.updated` (for PKM, web archives), `livingdoc.delta`, `audio.transcript.completed`, and potentially `domain_comm.chat_message.ingested` events. Uses an LLM (with a prompt optimized for task identification) to find phrases indicative of actionable tasks ("I need to...", "TODO:", "Follow up on..."). For each identified potential task, it proposes a new `core_artifacts` entry (type: `task_item`, status: `proposed`, payload includes suggested title, source text snippet, link to source artifact ULID). These proposed TODOs are surfaced in a UI (e.g., Neovim panel, "Inbox" view) for user confirmation, editing, or dismissal. Confirmed TODOs have their status changed to `open` and a `sinex.task.created` event is emitted.
  * `NotificationDispatcher`: Consumes `sinex.system.suggestion_created`, `sinex.agent.budget_warning`, `sinex.agent.error` (critical severity), or other alert-worthy events. Based on event priority and user preferences (stored in `core.configuration` or user profile), it dispatches notifications via `notify-send` (desktop), email, or a mobile push service bridge.

* **4.3.2. Analytical & Retrospective Agents:** These agents focus on analyzing historical data to uncover patterns, generate summaries, or reconstruct narratives.
  * `ActivityPatternMiner`: Runs periodically (e.g., nightly). Queries various domain tables (`domain_desktop.focus_spans`, `domain_input.key_presses`, `domain_web.visits`, `core_artifact_contents` (for PKM edit timestamps)). Identifies common work routines (e.g., "coding sessions" typically involve Neovim + Terminal + Browser on API docs), application usage sequences, context switching frequency, time distribution across projects/tags. Generates `sinex.analytics.pattern_report` events or updates a dedicated analytics dashboard (e.g., Grafana, or custom views).
  * `ProjectTimelineConstructor`: Triggered on demand by user (`exo agent trigger ProjectTimelineConstructor --project_id X`) or when a `planning.milestone_defined` (status: `completed`) event occurs for a project. Traverses all events, notes, commits (if git ingestor exists for project repos), and tasks linked to the given project ULID (`core_entities` where `entity_type='project'`). Uses an LLM to construct a chronological narrative of the project's evolution, highlighting key phases, decisions, contributions, major deliverables, and encountered `meta.friction_logged` events. Output is a `meta.narrative_generated` event, linked to the project entity.
  * `WeeklyNarrator`: Runs weekly. Consumes all significant events from the past week (major PKM edits, new insights, friction logs, completed tasks, Living Document themes, key web archives). Uses an LLM to generate a `meta.narrative_generated` event summarizing key accomplishments, challenges, emergent themes, and suggestions for the upcoming week.

* **4.3.3. Integration & Synchronization Agents:** These agents bridge the Exocortex with external systems or manage internal data consistency.
  * `ExternalFeedImporter` (e.g., for RSS, Twitter/Mastodon personal archives, specific Subreddits): Periodically fetches new items from configured external feeds. Creates `external.feed_item.ingested` events in `raw.events` (payload includes source feed URL, item title, link, content snippet, author, publication date). These can then be tagged, embedded, and linked by other agents (e.g., triggering `WebPageArchiverAndMarkdownifier` for linked articles).
  * `CalendarSyncAgent`: If the user uses an external calendar (Google Calendar, iCalendar feed), this agent periodically syncs events (one-way import initially). Creates/updates entries in a `domain_calendar.events` table (`id ULID`, `summary`, `start_ts`, `end_ts`, `description`, `location`, `attendees JSONB`, `external_calendar_id TEXT`). Links calendar events to related Exocortex projects, notes, or tasks where possible.
  * `PKMSyncAgent` (detailed in Part II.2.2): Responsible for bi-directional synchronization between filesystem Markdown notes and their canonical representation in `core_artifacts` / `core_artifact_contents` / `core_blobs` (via git-annex).
  * `GitAnnexDBReconciler`: Periodically compares `core_blobs` entries with the state of the git-annex repository. Detects orphaned annex objects, missing content, or metadata discrepancies. Generates `sinex.data_integrity.annex_issue_detected` events.

* **4.3.4. Meta-Reflective & System Maintenance Agents:** These agents focus on the health, efficiency, and improvement of the Exocortex itself.
  * `SystemHealthMonitor`: Consumes `sinex.agent.heartbeat`, `sinex.agent.error`, `sinex.ingestor.dlq_item_added`, `sinex.db_stats.size_report` events. Aggregates these into a system health overview. Generates alerts (via `NotificationDispatcher`) for critical issues.
  * `LLMCostAnalyzer`: Aggregates `sinex.agent.llm_api_call` events. Provides reports on LLM API costs. Flags agents nearing budgets.
  * `PromptOptimizerAgent`: Consumes `meta.llm_output_feedback` events. Suggests modifications to prompts in `core_prompts`.
  * `OrphanedArtifactDetector`: Scans `core_artifacts`, `core_blobs`, `core_tags`, `core_entities` for items no longer meaningfully referenced. Generates `sinex.data_cleanup.suggestion_created` events proposing archival or review.

This archetypal list is illustrative. The agent framework is designed for easy addition of new specialized agents as needs arise.

---

**Part V: The Bridge to Self – Interaction, Query, Feedback, and Self-Modeling**

*(This Part translates the architectural and agentic capabilities of the Sinnix Exocortex into the lived experience of the user. It focuses on how individuals directly interact with their sentient archive, how they query it to retrieve memories and insights, how understanding is woven from disparate events, and ultimately, how the system serves as a powerful tool for self-modeling and cognitive feedback. It embodies the core principles of User Agency, Continuous Context, and Feedback as Fuel.)*

The true measure of the Exocortex lies not in the sheer volume of data it can capture or the sophistication of its internal machinery, but in its ability to serve as a seamless, intuitive, and empowering extension of the user's own mind. This requires carefully designed user interfaces (UIs) and user experiences (UX) that bridge the gap between the vast data substrate and the user's immediate cognitive needs.

**5.1. UI/UX Philosophy & Primary Interaction Channels**

The design of all user-facing aspects of the Exocortex is guided by a set of core principles, previously outlined (Part I.3) and reiterated here for emphasis: **Frictionless Capture, Always; Context is King; Discoverability & Learnability; User in Control; Hackability & Extensibility; Performance as a Feature; Aesthetics of Clarity and Calm; and deep Support for Neurodiversity and Varied Cognitive Styles.** These principles inform the choice and implementation of the primary interaction channels.

* **5.1.2. Neovim Plugin: The Power-User Cockpit**

For users who live within a terminal-centric, keyboard-driven workflow, Neovim is envisioned as the **primary power-user cockpit** for the Exocortex. The `sinnix-nvim` plugin (or its equivalent) provides deep, contextual integration:

* **Unified Search & Navigation (Telescope.nvim):** Telescope pickers are the main interface for finding and opening:
  * PKM Notes/Web Archives (from `core_artifacts` + `core_artifact_contents`): By title, content (full-text search via `to_tsvector` on `core_artifact_contents.content_text`), tags (from `artifact_tags` joined with `core_tags`), semantic similarity (vector search on `artifact_embeddings`), or links (backlinks/outlinks from `core_artifact_links` or `core_entity_relations`).
  * Raw Events: Filterable by `source`, `host`, `event_type`, time range, and searchable by `payload` content (JSONB operators, GIN indexes), potentially using `payload._provenance.correlation_id` to group.
  * Living Document Nodes/Artifacts: Navigating the Living Document's structure (if represented hierarchically), searching its content.
  * Blobs/Files (`core_blobs`): Searching metadata and tags, then opening the file (after `git annex get` if needed) or viewing its content/preview.
  * Tags (`core_tags`), Entities (`core_entities`), Links (`core_entity_relations`, `core_artifact_links`): Browsing the knowledge graph components.
* **Contextual Panels & Floating Windows:**
  * When editing a PKM note (`core_artifacts` type `pkm_note`) or viewing an event, sidebars or floating windows can display:
    * Backlinks and outlinks for the current `core_artifacts` note/entity.
    * Related raw events (e.g., browser visits or terminal commands that occurred temporally close to the creation/editing of the current note, or events sharing a `payload._provenance.correlation_id`).
    * Semantically similar notes or artifacts (from embedding search).
    * Agent suggestions or clarification prompts related to the current buffer or task (from `sinex.system.suggestion_created` events linked to current context).
    * Detailed metadata for the current artifact (tags, provenance, linked items, git-annex status for linked blobs).
* **Living Document Interaction:**
  * A dedicated buffer type or filetype for interacting with the Living Document, allowing for live editing, appending of thoughts (via typed input or by invoking a voice-to-text agent).
  * Hotkeys for quick capture of selected text, current buffer context, or fleeting thoughts directly into designated sections of the Living Document.
  * Commands for invoking LLM actions on selected text or current Living Document sections (e.g., `:ExoLivingDocSummarize`, `:ExoLivingDocExtractTasks`, `:ExoLivingDocLinkToEvent <ULID>`).
* **Command Palette (`:Exo...`):** A comprehensive suite of Ex commands for:
  * Manually logging events (especially `meta.*`, `subjective.*`, and `planning.*` types).
  * Triggering specific agents or promotion pipelines.
  * Managing PKM synchronization status, creating new notes.
  * Querying system health and meta-observability data.
  * Adding/removing tags, creating links between artifacts.
* **Visual Cues & Status Line Integration:**
  * The Neovim status line can display indicators of Exocortex connection status, number of pending agent suggestions, or context about the current file's linkage (e.g., number of backlinks, associated project tag).
  * Virtual text or highlighting can be used to display inline annotations (from `event_annotations`), unresolved Wikilinks, or agent-suggested modifications directly in buffers.

* **5.1.3. `exo` CLI: The Scriptable Backbone**

The `exo` command-line interface is the **scriptable and universally accessible backbone** for all Exocortex interactions. It provides comprehensive functionality for power users, automation scripts, and integration with other tools. (Detailed commands outlined in Phase 2.5/Phase 3 specs, including `log`, `query`, `find`, `pkm`, `livingdoc`, `agent`, `blob`, `schema`, `tag` subcommands).

* **Output Formatting & Scriptability:** Designed for easy parsing by other scripts. JSON output is default for most query commands; options for YAML, CSV, and human-readable tables.
* **Shell Completions & `fzf` Integration:** Rich shell completions (Bash, Zsh, Fish) for commands and arguments. Potential for `fzf`-powered interactive selection for commands like `exo pkm find` or `exo agent trigger`.

* **5.1.4. Dashboards (Grafana; Web UI - Future): Visualizing Trends and States**

While Neovim and CLI cater to focused interaction, dashboards provide a broader, visual overview.

* **Grafana:**
  * *Personal Analytics:* Visualizing trends in focus time (derived from `domain_hyprland.focus_changes` domain table, aggregated by application/project tag), task completion rates (from `core_artifacts` of type `task_item` where `properties.status = 'completed'`), correlations between logged mood/energy (`subjective.*` events) and activity levels, sleep patterns vs. productivity indicators, frequency and types of `meta.friction_logged` events.
  * *Knowledge Graph Exploration (Limited in Grafana, better in dedicated Web UI):* Displaying metrics like notes created per day, average links per note, evolution of tag clouds, co-occurrence of entities in notes/events.
  * *System & Agent Health:* Visualizing data from the meta-observability stream: ingestion rates, event lag, agent error rates, LLM token usage/cost, database performance metrics, disk space. (Connects to Part VI.1).
* **Future Web UI/Canvas:**
  * An interactive, read-write web interface is a longer-term goal. Key features would include:
    * Full graph visualization and navigation of `core_entity_relations`, `core_artifact_links`, and `event_relations` (e.g., using libraries like Vis.js or Cytoscape.js).
    * Timeline views that interleave different event streams (e.g., browser history alongside PKM edits and terminal commands for a specific project).
    * A rich interface for editing and interacting with the Living Document, potentially with drag-and-drop organization of nodes and embedded media previews.
    * Mobile-friendly interface for quick capture (voice, text, photo) and basic querying on the go.

* **5.1.5. Inbox Workflow as a Core Interaction Pattern:**
The "Inbox as Workflow Bedrock" principle is operationalized as a dynamic, query-driven view (or set of views) surfacing new, unprocessed, or attention-requiring items from across the Exocortex.
* **Concept:** The Inbox aggregates items like:
  * Newly captured PKM notes (`core_artifacts` of `artifact_type='pkm_note'`) or web archives (`artifact_type='webpage_archive'`) that are not yet extensively tagged or linked.
  * Agent-extracted `core_artifacts` of `artifact_type='task_item'` with `properties.status = 'proposed'` awaiting user confirmation.
  * `sinex.agent.error` or `agent_processing_dlq` entries needing review.
  * Agent-generated `sinex.system.suggestion_created` events (e.g., "link these two notes?", "archive this stale project?").
  * Living Document segments flagged by agents as needing clarification or structuring.
  * Unresolved links from `core_artifact_links` where `resolved_target_artifact_id IS NULL`.
* **Interface (Neovim/CLI/Web):** A dedicated "Inbox" view/command. Users can quickly:
  * **Triage:** Assign tags (populating `artifact_tags`), link to projects/entities (creating `core_entity_relations`), set due dates for tasks (updating `core_artifacts.properties`).
  * **Process:** Confirm a proposed TODO (updating its status), convert an insight into a full PKM note, merge related items.
  * **Delegate:** Assign to an agent for further processing (e.g., "summarize this web archive," "find related events for this friction log" – by creating a `sinex.agent.command_request` event).
  * **Defer:** Snooze or move to a "someday/maybe" list (which itself is just another tag or status).
  * **Dismiss:** Mark as irrelevant or already handled (e.g., updating a status field on the suggestion event).
* **Workflow Integration:** Processing Inbox items generates new events (e.g., `core_artifacts.updated` with new tags, `sinex.task.status_changed`, `sinex.system.suggestion.actioned`), ensuring all triage actions are captured and feed back into the Exocortex.

**5.2. The Art of the Query: Unlocking the Sentient Archive**

The true power of the Exocortex is realized through its ability to answer complex questions about the user's past and present digital life.

* **5.2.1. Query Capabilities & Language:**

The system offers a layered approach to querying:

* **Direct SQL on PostgreSQL:** For maximum power and flexibility, querying `raw.events`, domain tables (e.g., `domain_hyprland.focus_changes`), and knowledge graph tables (`core_entities`, `core_entity_relations`, `core_artifact_links`, `artifact_tags`, etc.), using JSONB operators, FTS (`to_tsvector`), GIS (if location data is rich), window functions, recursive CTEs for graph traversal, and `pgvector` operators for ANN search (`<=>`).
* **Simplified Query Syntax (`exo` CLI & Neovim):** The `exo find` and `exo query` commands translate user-friendly syntax to underlying SQL. Supports:
  * *Temporal Filtering:* `since:yesterday`, `between:"YYYY-MM-DD" "YYYY-MM-DD"`, `last:3h`.
  * *Source/Type/Host Filtering:* `source:neovim_plugin`, `event_type:pkm.note.updated`, `host:laptop_nixos`.
  * *Payload Content Filtering:* Keyword search (FTS/`ILIKE`), field-specific search (e.g., `payload.url_contains:nixos.org`, `payload_jq:'.title | test("Exocortex")'`).
  * *Semantic Search:* `similar_to_text:"concept of exocortex"` or `similar_to_id:<ULID_of_artifact_content>`.
  * *Tag-Based Filtering:* `tags:#ProjectX`, `tags_all:"#important, #review"`, `tags_none:"#archive"`.
  * *Graph Traversal (Simplified):* `linked_to_id:<ULID_entity_or_artifact> --hops 2 --relation_type "mentions_entity"`.
* **Hybrid Queries:** Combining multiple filter types.

* **5.2.2. Query Cookbook: Practical Examples for Daily Use & Reflection**

A collection of example queries (stored as Exocortex PKM notes, accessible via `exo help query-examples`) illustrates common patterns:

* **Contextual Recall:**
  * "What Firefox tabs (`core_artifacts` where `properties.browser_app_class='firefox'` and `artifact_type='browser_tab_state'`) were active when I last edited PKM note with `artifact_id:XYZ`?" (Correlate based on `ts_orig` proximity and potentially `payload._provenance.correlation_id`).
  * "Show terminal commands (`domain_kitty.commands_executed`) executed while project entity `core_entity_id:ProjectZ` was my active focus (derived from `activity_segment.identified` events linked to ProjectZ, or window title heuristics from `domain_hyprland.focus_changes`) in the last 3 days."
* **Knowledge Discovery:**
  * "Find PKM notes or web archives (`core_artifacts` + `core_artifact_contents`) semantically similar to the currently selected text in Neovim (querying `artifact_embeddings`), limited to those tagged `#AI` but not `#obsolete` (querying `artifact_tags`)."
* **Self-Reflection & Pattern Analysis:**
  * "Chart the daily count of `meta.friction_logged` events (from `raw.events` where `source='meta.friction_logged'`) where `payload.perceived_cause` contained 'distraction', grouped by `host`."
  * "List all `meta.insight_captured` events that have an `event_relations` link of type `resolves_friction_from` pointing to a `meta.friction_logged` event that occurred within the preceding 7 days."
* **Workflow Support:**
  * "List all `core_artifacts` of type `task_item` with `properties.status = 'open'` and tagged `#ProjectX` or linked (via `core_entity_relations`) to `core_entity_id:ProjectX`."

**5.3. Weaving Understanding: Event Relations & Narrative Construction**

Beyond simple retrieval, the Exocortex aims to help the user (and its own agents) understand the *connections and stories* within the data.

* **5.3.1. Explicit Event & Artifact Relations (`event_relations`, `core_entity_relations`, `core_artifact_links`):**

The system uses multiple tables to model different types of relationships:

* `event_relations`: For rich, typed links *specifically between raw events* or between raw events and other core artifacts. Schema: `id ULID PK`, `from_object_id ULID NOT NULL` (can be `raw.events.id` or `core_artifacts.artifact_id`, etc.), `from_object_type TEXT NOT NULL`, `to_object_id ULID NOT NULL`, `to_object_type TEXT NOT NULL`, `relation_type TEXT NOT NULL` (e.g., `"derives_from"`, `"explains_context_of"`), `description TEXT NULLABLE`, `confidence FLOAT NULLABLE`, `created_by_actor TEXT NOT NULL`, `ts TIMESTAMPTZ DEFAULT now()`.
* `core_entity_relations`: For typed links *between canonical entities* in `core_entities`. Schema: `id ULID PK`, `source_entity_id ULID FK core_entities`, `target_entity_id ULID FK core_entities`, `relation_type TEXT`, `properties JSONB`, `ts_start_orig`, `ts_end_orig`.
* `core_artifact_links`: Primarily for links *parsed from PKM notes or web archives*, connecting `core_artifacts` entries. Schema: `link_id ULID PK`, `source_artifact_id ULID FK core_artifacts`, `target_identifier_text TEXT` (the raw link target), `resolved_target_artifact_id ULID FK core_artifacts NULLABLE`, `link_type TEXT`.
These links are created manually by the user or by agents inferring relationships.

* **5.3.2. Agent-Driven Narrativization (`meta.narrative_generated` events):**

LLM agents play a key role in synthesizing human-readable narratives from complex sets of Exocortex data.

* **Process:** A narrativization agent is triggered. It receives a scope (e.g., all events related to `core_entity_id:ProjectX` in the last month, or all events linked to a specific `planning.milestone_defined` event, or even "tell me the story of this `payload._provenance.correlation_id`").
* **Output (`meta.narrative_generated` event payload):** `title TEXT`, `summary_text TEXT` (the LLM-generated narrative), `key_object_ids_referenced JSONB` (e.g., `{"raw_events": ["ULID1", ...], "core_artifacts": ["ULID_A", ...]}`), `themes_identified ARRAY<TEXT>`, `sentiment_analysis JSONB`, `suggestions_for_reflection ARRAY<TEXT>`, `generation_prompt_id ULID FK core_prompts`, `generation_llm_model_id ULID FK core_llm_models`.
* **Integration:** Narratives are stored as events (or `core_artifacts` of type `narrative`), tagged, embedded, linked. They can be reviewed in the Living Document, PKM, or dedicated UI views, becoming valuable artifacts for retrospectives, planning, and providing context to other LLMs.

* **5.3.3. Generic Event Annotations (`event_annotations`): Layering User and Agent Insights**

  Beyond formal typed relations, a crucial mechanism for adding flexible, evolving meaning to the raw event stream is through generic annotations. The `event_annotations` table allows users and agents to attach comments, flags, preliminary tags, summaries, or any other form of metadata directly to individual `raw.events` entries without altering the immutable event itself.

  * **Significance:**
    * **User-Driven Sensemaking:** Users can directly comment on events, mark them as important, link them to fleeting thoughts, or correct initial interpretations. This is vital for personalizing the archive and capturing subjective context.
    * **Agent Iteration & Feedback:** Agents can use annotations to flag events for review, store intermediate processing results (e.g., a "confidence score" for an extracted entity before it's fully promoted), or propose links that the user can then confirm or reject by annotating further.
    * **Bridging Raw Data and Higher-Level Structures:** Annotations can serve as the initial step for promoting raw event data into more structured artifacts or knowledge graph entities. For example, a user annotation "This error log seems related to Project X" can trigger an agent to create a formal link.
    * **Progressive Disclosure of Complexity:** Instead of overloading `raw.events.payload` with all possible interpretations immediately, annotations provide a way to layer insights as they emerge.

  * **Schema for `event_annotations`:**

      ```sql
      CREATE TABLE IF NOT EXISTS event_annotations (
          annotation_id           ULID PRIMARY KEY DEFAULT generate_ulid(),
          target_event_id         ULID NOT NULL REFERENCES raw.events(id) ON DELETE CASCADE,
          annotator_actor         TEXT NOT NULL, -- e.g., 'user_sinex', 'agent_summarizer', 'llm_gpt4_feedback'
          annotation_type         TEXT NOT NULL, -- e.g., 'user_comment', 'agent_flag_review', 'llm_summary_chunk', 'correction_suggestion'
          content_text            TEXT,          -- For textual annotations
          content_jsonb           JSONB,         -- For structured annotations (e.g., tags, key-value pairs, proposed links)
          ts_created              TIMESTAMPTZ DEFAULT now(),
          embedding_vector        VECTOR         -- Optional: embedding of the annotation's content_text for similarity searches on annotations
      );
      CREATE INDEX IF NOT EXISTS idx_event_annotations_target_event_id ON event_annotations (target_event_id);
      CREATE INDEX IF NOT EXISTS idx_event_annotations_actor_type ON event_annotations (annotator_actor, annotation_type);
      CREATE INDEX IF NOT EXISTS idx_event_annotations_embedding ON event_annotations USING ivfflat (embedding_vector vector_cosine_ops) WHERE embedding_vector IS NOT NULL;
      ```

  * **Interaction:** UIs (Neovim, CLI, Web) will provide means to view, add, and filter annotations for any given event. Agents can both read and write annotations as part of their processing pipelines. This table becomes a dynamic, collaborative layer enriching the static raw event log.

**5.4. Cognitive Feedback Loops & Instrumented Self-Modeling**

The ultimate goal of the interaction layer is to close the loop between data capture, analysis, and user action, enabling instrumented self-modeling and intentional personal development.

* **5.4.1. Surfacing Patterns & Anomalies:**
  * UI elements proactively display trends: focus time, task velocity, friction event clusters, mood/energy correlations.
  * Agents monitor streams for significant deviations, generating `sinex.system.suggestion_created` or `sinex.analytics.pattern_alert` events.

* **5.4.2. Intentional Tracking & Goal Alignment:**
  * Users log goals/intentions as `planning.goal.defined` or `meta.intention.created` (which become `core_entities` of that type).
  * Agents correlate subsequent activity with these goals, visualizing progress or flagging deviations.

* **5.4.3. The Exocortex as a Mirror for Self-Understanding and Experimentation:**
  * By making internal states (mood, friction, insights) and external actions (digital traces) equally queryable and linkable, the system facilitates data-driven introspection.
  * Users can formulate and test hypotheses about their own cognition and behavior, transforming the Exocortex into an active laboratory for personal development.

This comprehensive interaction and feedback layer aims to make the Exocortex not just a passive extension of memory, but an active, intelligent partner in the user's ongoing quest for understanding, effectiveness, and self-authorship.

**5.5. Modeling User Context: Derived Semantic Layers for Sessions, Intents, and Composite Actions**

While `raw.events` captures the granular "what happened," significant user value comes from understanding the "why" and "how" these events cluster into meaningful, higher-level contexts. Instead of overloading individual `raw.events` with fields like `session_id` or `task_id`, the Exocortex models these broader contexts as **derived semantic layers**, often realized as new, distinct event types or through rich relationships in the knowledge graph. This approach keeps `raw.events` lean while allowing for flexible and powerful contextual analysis.

* **5.5.1. Activity Segments (Sessions):**
    User activity naturally falls into segments or "sessions" (e.g., "working on Project Sinex Phase 2," "researching NixOS flakes," "unfocused web browsing").
  * **Modeling:** These are not simple tags on raw events. Instead, an agent (e.g., `sinex.agent.activity_segmenter`) or direct user input (`exo session start/end "description"`) generates `activity_segment.identified` events.
  * **Event Structure (`activity_segment.identified`):**
    * `source`: "sinex.agent.activity_segmenter" or "user.cli"
    * `payload`: `{ "segment_id": "ULID", "segment_type_user_defined": "project_work_sinex", "ts_start_orig": "...", "ts_end_orig": "...", "description": "Focused work on...", "inferred_entities_involved": ["ULID_project_sinex", "ULID_tool_neovim"], "confidence_score": 0.9 (if agent-derived) }`
  * **Utility:** Users can query for all raw events `WHERE ts_orig BETWEEN segment.ts_start_orig AND segment.ts_end_orig`. These segments themselves become nodes in the knowledge graph, linkable to projects, tasks, and outcomes. Different agents can identify overlapping or hierarchical segments based on various heuristics (application focus, project context, idle time).

* **5.5.2. User Intents and Tasks:**
    Explicitly stated user goals or tasks provide strong context for interpreting activity.
  * **Modeling:** Intents (`intent.declared`, `intent.concluded`) and tasks (`task.created`, `task.status_changed`) are first-class event types or entities in `core_entities` (e.g., `entity_type='task'`).
  * **Linking, Not Embedding in Raw Events:** Raw activity events (keystrokes, window focus) do *not* carry `task_id` directly. Instead, correlation is achieved by:
        1. Agents analyzing activity within the `ts_start_orig`/`ts_end_orig` of an active intent/task.
        2. Creating `event_entity_links` or `event_relations` between raw events and the canonical task/intent entity.
        3. LLM-driven analysis to infer "work on task X" from a sequence of raw events.
  * **Utility:** Provides a clear link between high-level objectives and the granular actions taken, enabling progress tracking and focused retrospectives.

* **5.5.3. Composite Actions and Event Correlation:**
    A single logical user action (e.g., "save file and commit") can trigger a burst of low-level events across different systems.
  * **Modeling:** Instead of a single `event_correlation_key` on raw events, this is better modeled by:
        1. **Derived Composite Action Events:** An agent (`sinex.agent.action_correlator`) identifies these clusters (e.g., Neovim save + Filesystem modify + Git plugin stage, all within a few seconds for the same file) and emits a new event like `composite_action.file_save_committed`.
            * `payload`: `{ "description": "File main.rs saved and staged", "constituent_raw_event_ids": ["ulid_save", "ulid_modify", "ulid_stage"], "primary_target_artifact_id": "ulid_of_main_rs_artifact" }`
        2. **Event Relations:** The correlator agent can also create explicit `event_relations` entries linking the constituent raw events with a type like `"part_of_composite_save_action"`.
  * **Utility:** Allows users and agents to reason about higher-level user actions rather than getting lost in the raw event stream. Simplifies narrative construction and workflow analysis.

This layered approach ensures that `raw.events` remains a clean log of direct observations, while powerful semantic understanding of user context, sessions, and multi-step actions is built on top through dedicated derived events and knowledge graph relationships, often facilitated by intelligent agents.

---

**Part VI: Sustaining the Covenant – System Integrity, Evolution, and the Path Forward**

*(This Part addresses the operational and long-term aspects crucial for ensuring the Sinex Exocortex remains a robust, trustworthy, and adaptable cognitive partner. It focuses on how the system maintains its integrity, evolves gracefully with changing needs and technologies, and provides a practical path for its ongoing development, all while upholding the core principles of System Resilience, Iterative Growth, and User Agency, especially concerning the system's own maintenance and future.)*

The Exocortex, as a lifelong cognitive habitat, must be more than just a collection of features; it must be a sustainable and resilient ecosystem. This requires a deep commitment to system integrity, thoughtful strategies for evolution, and a clear vision for its practical implementation and long-term viability.

**6.1. Meta-Observability: The System Observing Itself – A First-Class Data Stream**

* **6.1.1. Philosophy: All Exocortex Operational Data *is* Exocortex Data – No Distinction.**

A core tenet of the Exocortex is that its own operational health, performance, and behavior are not external concerns to be monitored by separate tooling alone. Instead, this **meta-observability data is treated as a first-class event stream**, ingested into the same `raw.events` substrate as user activity and external information, using the `source="sinex"` and specific `event_type`s like `agent.heartbeat`, `agent.error`, `db.query_slow`, `ingestor.dlq_item_added`, etc. This allows the Exocortex to apply its full analytical and agentic capabilities to its own functioning, enabling self-diagnosis, adaptive optimization, and transparent reporting to the user. This holistic approach is fundamental because system health directly impacts user experience and data integrity, understanding how the system processes data is key to optimizing workflows, and the evolution of the Exocortex itself can be data-driven. Meta-observability is an intrinsic property from day one.

* **6.1.2. Key Metrics & Events Captured:**

Comprehensive suite including: Ingestion pipeline health (throughput, latency, DLQ sizes per agent), Agent ecosystem performance (uptime, errors, resource use, LLM costs via `sinex.agent.llm_api_call`), Database performance (slow queries, bloat, disk I/O, connection stats), Host resources (CPU, memory, disk, network), Backup & Integrity status (outcomes of `pg_dump`, `git annex fsck`, DR tests).

* **6.1.3. Ingestion of Meta-Observability Data:**

Via Journald ingestor (for all systemd unit logs), agent self-reporting (emitting `sinex.agent.*` events directly), optional Prometheus bridge (scraping Prometheus metrics and converting to `sinex.observability.metric_point` events), and DB triggers/scheduled queries (for DB internal stats).

* **6.1.4. Utilization for Self-Management and User Awareness:**

Dashboards (Grafana), Alerting Agents (subscribing to critical `sinex.*` event patterns and notifying user via `NotificationDispatcher`), future Automated Remediation (e.g., restarting failing non-critical agent), and Capacity Planning (longitudinal analysis of resource usage).

**6.2. Security, Privacy, and Data Sovereignty: Protecting the Cognitive Core**

The Exocortex, by its very nature, will contain an unprecedented concentration of its user's most personal and sensitive information. Therefore, security and privacy are not afterthoughts but foundational design requirements. The principle of data sovereignty dictates that the user must have ultimate control and ownership over their data.

* **6.2.1. Access Control & Authentication:**
  * **PostgreSQL Roles:** A granular system of PostgreSQL roles is implemented. Common roles include:
    * `exocortex_ingest_default`: Can only `INSERT` into `raw.events` and specific log/DLQ tables. Cannot `SELECT` from most tables or `UPDATE`/`DELETE`.
    * `exocortex_agent_default_template`: A template role with `SELECT` access to `raw.events` and relevant domain/core tables, and `INSERT/UPDATE` access only to the specific tables or event types that agent is designed to produce. Each specific agent (e.g., `agent_pkm_embedder`) runs as its own PostgreSQL user inheriting from this template with further restrictions.
    * `exocortex_query_user`: The role used by UI frontends (Neovim plugin, CLI, Web UI) for user-initiated queries. Has `SELECT` access to most views and domain/core tables, limited `INSERT` for manual event logging (e.g., `meta.friction_logged`).
    * `exocortex_admin_user`: Full administrative privileges, used only for schema migrations, backups, and system maintenance.
  * **Systemd User Services:** All ingestors and agents run as a dedicated, unprivileged system user (e.g., `sinnix-exo`), with filesystem permissions restricted to their own configuration, state directories, and necessary data paths (like the PKM vault for the `PKMSyncAgent`). `ReadWritePaths` and `ProtectHome=read-only` (with specific exceptions) are used in systemd unit files.
  * **API Endpoints:** Any network-facing API endpoints (e.g., for mobile data ingestion or a future web UI) must use strong authentication (e.g., HTTPS with client certificate authentication, robust API keys/bearer tokens managed by the user). OAuth2/OIDC flows would be considered for any user-facing web applications.

* **6.2.2. Encryption:**
  * **At Rest:**
    * Full-disk encryption (LUKS) is strongly recommended for the host machine(s) running the Exocortex.
    * PostgreSQL data directory can be encrypted at the filesystem level or using PostgreSQL's native tablespace encryption features if available and appropriate.
    * Git-annex repositories containing sensitive blobs should be configured with git-annex's native encryption capabilities (symmetric or GPG-based).
  * **In Transit:** All remote communication (e.g., mobile ingest to host, calls to external LLM APIs, future federated sync between Exocortex instances) must use TLS (HTTPS, WSS, MQTTS).
  * **Secrets Management:** API keys, database passwords, encryption passphrases, and other secrets are managed declaratively and securely using a tool like `agenix`, integrated with the NixOS configuration. Secrets are never hardcoded in scripts or version-controlled files.

* **6.2.3. Consent & Control for Sensitive Data:**
  * **Explicit Opt-In:** Ingestors designed to capture highly sensitive data (e.g., raw keystrokes from `evdev`, continuous audio recording, full screen recording, content of password fields via AT-SPI2) must be *explicitly enabled by the user*. They should default to *off*.
  * **Clear UI Indicators:** When sensitive capture is active, the system should provide clear, persistent, and unambiguous visual indicators in the user interface (e.g., a status bar icon, a desktop notification).
  * **Global Pause/Resume:** The user must have an easily accessible global "panic button" or hotkey to temporarily pause *all* (or specific categories of) sensitive data ingestion.
  * **Configurable Redaction Policies:** For certain event sources (like AT-SPI2 capturing text fields), provide user-configurable redaction rules (e.g., regex-based filters for credit card numbers, social security numbers, or keywords indicating passwords) that are applied by the ingestor or an early-stage promotion agent *before* data is written to `raw.events` or with sensitive portions replaced by placeholders.
  * **Privacy Zones/Tags:** The user can designate certain applications, projects, or time periods as "highly private." Events originating from these contexts can be automatically tagged (e.g., `#privacy_sensitive`), potentially triggering stricter access policies for agents or exclusion from certain types of LLM processing that involve external APIs.

* **6.2.4. Data Export and Deletion (Right to Be Forgotten Considerations):**
  * **Export:** The `exo` CLI and other UIs must provide robust mechanisms for exporting user data in common, open formats (e.g., all notes as a Markdown zip, all events for a given source as JSONL or CSV, a full database dump).
  * **Deletion:** While the `raw.events` table is conceptually append-only for auditability, the system must provide mechanisms to address the "right to be forgotten" or to remove data the user no longer wishes to keep. This can be implemented by:
        1. *Logical Deletion/Redaction:* Marking specific events, notes, or blobs as "archived" or "redacted." Views and queries would then filter these out by default. The raw data might remain in backups or deep storage for a configurable period for disaster recovery but is not accessible in normal operation.
        2. *Cryptographic Erasure (for blobs):* If blobs are encrypted with per-blob keys, deleting the key effectively renders the blob unrecoverable.
        3. *Selective Physical Deletion (Complex):* True physical deletion from PostgreSQL (especially historical WAL segments and backups) and git-annex (including all remotes) is complex but may be necessary for certain compliance scenarios. This would involve specialized procedures and would inherently break perfect replayability for the deleted segments.

**6.3. Backup, Disaster Recovery, and Data Integrity: Ensuring Permanence**

The Exocortex, as a lifelong archive, demands a robust strategy for data permanence and recovery.

* **6.3.1. PostgreSQL Backup Strategy:**
  * **Continuous WAL Archiving:** PostgreSQL's Write-Ahead Logs (WALs) are continuously archived to a secure, separate storage location (e.g., a different local disk, a NAS, or encrypted cloud storage). This is essential for Point-in-Time Recovery (PITR).
  * **Regular Full Base Backups:** Scheduled (e.g., daily or weekly) full physical backups of the PostgreSQL data directory (e.g., using `pg_basebackup`) or logical backups (`pg_dumpall`).
  * **Encryption:** All backups (base backups and WAL archives) must be encrypted before being stored, especially if off-site.
  * **Retention Policy:** A clear retention policy for backups (e.g., keep 7 daily backups, 4 weekly backups, 12 monthly backups, and 1 yearly backup) to balance storage costs with recovery needs.
  * **PITR Capability:** The combination of base backups and WAL archives allows restoration to any specific point in time, minimizing data loss in case of catastrophic failure.

* **6.3.2. Git-Annex Backup Strategy:**
  * **Multiple Remotes:** Leverage git-annex's core strength by configuring multiple remotes for the Exocortex annex repository. These can include:
    * External USB hard drives (for local, offline backups).
    * A personal NAS or home server.
    * Encrypted cloud storage (via git-annex special remotes like `rclone`).
  * **Regular Synchronization:** Systemd timers schedule regular `git annex sync --content` operations to push new blob content to backup remotes and pull any missing content.
  * **Git Repository Backup:** The git repository metadata itself (which contains the pointers and history for git-annex) must also be regularly backed up (e.g., `git bundle` or pushing to a private git remote).

* **6.3.3. NixOS Configuration Backup:**
  * The entire system configuration, including all NixOS flakes, modules defining the Exocortex services, agent scripts, and secrets managed by `agenix`, is version-controlled in a private Git repository. This repository is backed up regularly, ensuring the entire Exocortex environment can be reproduced from scratch on new hardware.

* **6.3.4. Disaster Recovery Plan (Documented and Tested Periodically):**
  * A clear, step-by-step disaster recovery (DR) plan must be documented. This plan covers scenarios like complete host failure, database corruption, or major data loss.
  * The DR plan includes procedures to:
        1. Re-provision a new host using the NixOS configuration.
        2. Restore the PostgreSQL database from the latest suitable base backup and replay WALs to the desired recovery point.
        3. Restore the git-annex repository metadata and re-establish connections to content remotes (or restore content from a backup remote).
        4. Re-initialize ingestor and agent states (e.g., reprocessing DLQs, resetting watermarks if necessary, though ideally, most state is in the DB).
  * The DR plan should be tested periodically (e.g., annually, or after major system changes) to ensure its validity and to identify any gaps. The outcome of DR tests is logged as a `sinex.system.dr_test_completed` event.

* **6.3.5. Data Integrity Checks:**
  * **Git-Annex:** Regular execution of `git annex fsck` (full or `--fast`) to verify the integrity of stored blobs against their checksums. Results are logged as `sinex.data_integrity.annex_fsck_result` events.
  * **PostgreSQL:** ULID uniqueness constraints enforced by the database. JSON Schema validation (as a non-blocking check performed by promotion agents or a dedicated integrity agent) for `raw.events.payload` against definitions in `sinex_schemas.event_payload_schemas`. Discrepancies log `sinex.schema.validation_failure`.
  * **Link Integrity:** A periodic agent scans `core_entity_relations`, `core_artifact_links`, `event_relations`, and other tables with foreign keys or ULID references to identify broken links (pointing to non-existent entities/events/notes/blobs). These are logged as `sinex.data_integrity.broken_link_detected` events for review.
  * **Orphaned Data:** Agents detect orphaned blobs (in annex but unreferenced in `core_blobs`), orphaned `core_artifact_contents` (not linked from `core_artifacts`), or orphaned entities/notes (in DB but with no meaningful connections or recent activity). These trigger `sinex.data_cleanup.suggestion_created` events.

This comprehensive approach to integrity, backup, and recovery ensures that the valuable data accumulated within the Exocortex remains safe, consistent, and available over the long term.

* **6.4. Performance, Scalability, and Schema Evolution: Growing Gracefully**

An Exocortex is a system designed for lifelong use, meaning it must be able to grow gracefully in terms of data volume, query complexity, and feature set without becoming sluggish or unmanageable.

* **6.4.1. Database Performance Tuning & Management:**
  * **Regular Maintenance:** Standard PostgreSQL maintenance tasks such as `VACUUM` (especially `VACUUM ANALYZE`) are scheduled regularly. TimescaleDB handles some aspects of this automatically for hypertables.
  * **Index Monitoring:** Periodically monitor index usage (`pg_stat_user_indexes`) to identify unused or inefficient indexes, and to spot queries performing frequent sequential scans. Index bloat is also monitored.
  * **TimescaleDB Chunk Management:** For hypertables like `raw.events`, chunk time intervals are initially set based on expected volume and can be adjusted. Compression policies for older chunks (e.g., segment-by columnar) are defined to reduce storage. Data tiering/retention to cheaper storage or offline archival is a future option for very old data.
  * **Query Optimization:** Slow or frequent queries (identified via `log_min_duration_statement` or `pg_stat_statements`) are analyzed with `EXPLAIN ANALYZE`. Optimization may involve query rewriting, adding indexes, or creating materialized views.

* **6.4.2. Agent & Ingestion Scalability:**
  * **Asynchronous Processing & Batching:** Most agents operate asynchronously, processing data in batches to reduce overhead.
  * **Connection Pooling:** All components use PostgreSQL connection pooling.
  * **Parallelization (If Needed):** For high-volume streams, multiple instances of the same agent can run in parallel, processing shards of input (e.g., via work queues or `SKIP LOCKED` patterns).
  * **Resource Limits (Systemd):** Enforced via NixOS-configured systemd units.

* **6.4.3. Schema Evolution Strategy:**
  * **Flexibility of `raw.events.payload` (JSONB):** New fields can be added by ingestors without DDL changes.
  * **Domain Table & Core Schema Evolution:** Managed via versioned SQL migration scripts. **A formal migration tool (e.g., Sqitch, Diesel Migrations) is adopted post-Phase 2.5, once the core schema stabilizes and persistent data integrity across changes becomes paramount.** Prior to that, a single `master_schema.sql` (idempotently designed) is used for simplicity during rapid early development on an effectively ephemeral database.
  * **Promotion Agent Versioning:** Promotion agents are version-aware. When a domain table schema changes, the agent is updated. It can process new raw events to the new schema and optionally reprocess historical data.
  * **`sinex_schemas.event_payload_schemas` Registry:** Versioned JSON Schema definitions here help manage payload variations over time.
  * **Impact Logging:** Schema changes are logged as `sinex.schema.definition_updated` or `sinex.schema.migration_applied` events, triggering reviews of dependent components.

* **6.5. Federation and Multi-Device Coherence: The Distributed Exocortex (Future Vision)**
  * Core Principles: Local-first operation, eventual consistency. User controls sync policies.
  * Technical Enablers: ULIDs for global IDs, consistent timestamping (NTP), git-annex for blobs.
  * Synchronization Mechanisms (Speculative): Dedicated sync agents. CRDTs for specific data types (Living Doc, shared PKM notes). Event stream replication (hub-based or P2P). Robust conflict resolution strategies (last-write-wins, user-prompted merge, per-datatype logic), with conflicts logged as `sinex.sync.conflict_detected` events.

* **6.6. The Journey: MVP, Phased Implementation, and Open Horizons**

* **6.6.1. Recap of the Minimum Viable Exocortex (MVP): The Seed Crystal**
    The Exocortex journey began with an MVP establishing the core capture-store-query loop: `raw.events` table, a single Hyprland ingestor for basic window events, and a rudimentary CLI for event retrieval. This validated the foundational concept and initiated the accumulation of personal historical data.

* **6.6.2. Current Phase (Post-Phase 2.5): Deepened Core Capture & Foundational Tooling**
    The system has now evolved through Phase 2 and 2.5, achieving:
  * A refined `raw.events` schema (ULID PKs, `event_type`, `ts_orig`, structured top-level fields like `host` and `ingestor_version`, `payload_schema_id` FK).
  * Robust ingestors for comprehensive Hyprland IPC, Kitty terminal protocol, and filesystem activity (with content hashing and basic git-annex integration for small eligible files).
  * Initial keyboard and mouse event capture (via Hyprland IPC or `interception-tools` + `journald_bridge`).
  * Foundational database tables for blobs (`core_blobs`) and initial Nayuki-inspired tagging (`core_tags`, `artifact_tags`).
  * A schema registry (`sinex_schemas.event_payload_schemas`) and agent manifest system (`sinex_schemas.agent_manifests`).
  * Systematic operational eventing using the `sinex.agent.*` namespace.
  * An enhanced `exo` CLI for querying and basic system introspection.
  * A development script (`script/dev_watch.sh`) for real-time testing and observation.
    This phase solidified the "bedrock" of data capture and system manageability.

* **6.6.3. Next Phase (Phase 3): Deepening Semantic Capture & User Interaction: PKM, Web, and Basic Embeddings**
    As outlined in the "Phase 3 - Standalone Instructions":
  * Integrate existing Markdown PKM into `core_artifacts` and `core_artifact_contents`, with bi-directional sync and full eventification.
  * Implement a robust Web Archiver agent (Trafilatura, git-annex for HTML/Markdown blobs).
  * Introduce the `artifact_embeddings` table and an Embedding Agent for PKM notes and web archives (OpenAI/local models, chunking, `pgvector`).
  * Enhance Neovim plugin for PKM navigation and basic semantic search.
  * Expand `exo` CLI for PKM, web archiving, and embedding interaction.

* **6.6.4. Subsequent Phases (Illustrative): Semantic Saturation, Agentic Partnership, Full CognitiveOS**
  * *Semantic Saturation:* Comprehensive embedding of all textual content, advanced knowledge graph construction (`core_entities`, `core_entity_relations`), richer relation types, ontology management, inference.
  * *Living Document v1-vN:* Full implementation of the LLM node-graph, delta engine, artifact extraction, and interactive UI (Neovim, Web/Canvas).
  * *Advanced Agent Ecosystem:* Proactive agents, causal reasoners, pattern miners, self-improving prompt agents, sophisticated self-modeling tools.
  * *Full Multi-Modal/Multi-Device Integration:* Seamless mobile, wearable, IoT data flows. Deep fusion of text, audio, visual understanding. Robust federation.
  * *Mature UI/UX:* Highly interactive graph visualizations, advanced personal analytics dashboards, refined cognitive feedback loops.

* **6.6.5. Friction-Driven Prioritization: The Guiding Light for Development**
    Throughout all phases, the primary driver for selecting the *next* feature, ingestor, or agent to build is **personally felt pain, inefficiency, or missing cognitive leverage**. The system evolves organically to solve the user's most pressing problems, ensuring development effort maximizes immediate personal utility.

* **6.7. Open Horizons & The Spirit of Continuous Evolution**

The Sinex Exocortex, as envisioned, is not a destination but a journey—a lifelong project of building and co-evolving with a personalized cognitive infrastructure. While the roadmap outlines a path towards a highly capable system, many horizons remain open, inviting ongoing exploration and innovation.

* **The Future of Human-AI Cognitive Symbiosis:** What does a truly deep, seamless, and empowering partnership between a human mind and its personalized AI (running on its Exocortex data) look like? How can agents move beyond simple automation to become genuine collaborators in creative thought, problem-solving, and self-discovery? The Exocortex aims to be the ideal laboratory for exploring these questions.
* **Ethical Frameworks for Pervasive Self-Tracking:** As the Exocortex captures ever more granular and personal data, including subjective and physiological states, ongoing ethical reflection is paramount. This includes developing personal frameworks for data governance, defining boundaries for agent autonomy, considering the psychological impact of total recall and pervasive self-monitoring, and ensuring robust safeguards against misuse of data (even by oneself in moments of compromised judgment).
* **Models for Sharing or Collaborating (If Ever Desired):** While conceived as a profoundly personal system, questions may arise about selectively and safely sharing portions of Exocortex data or insights with trusted collaborators, or using its infrastructure for collaborative knowledge work. Designing secure, consent-driven, and privacy-preserving mechanisms for such interactions is a complex future challenge.
* **Long-Term Sustainability and Maintainability:** A system intended for lifelong use must be sustainable. This involves not just technical robustness (backups, data integrity) but also ensuring that the system remains manageable, adaptable, and comprehensible to the user as technologies and personal needs evolve over decades. The choice of NixOS for declarative configuration, the emphasis on modularity, and the principle of hackability are all contributions to this long-term goal. The "cost" of running the Exocortex (time, mental energy, financial for LLM APIs/storage) must also remain manageable.
* **Beyond Individual Augmentation:** Could the principles and architectural patterns of the Exocortex—universal eventification, emergent structure, agentic partnership, user sovereignty—inform the design of broader digital environments that are more humane, context-aware, and agency-preserving for everyone?
* **Advanced Multi-Modal Capture and Fusion:** Exploring deeper integration of visual context through compositor-level visual change analysis (damage-region OCR/VLM) and comprehensive session replay technologies (e.g., Asciinema for terminals, and potentially similar for graphical sessions) to create truly rich, replayable records of digital experiences.

The Exocortex is ultimately an embodiment of a **practice**: the practice of paying attention to one's own cognitive life, of valuing one's own experiences and insights, and of actively shaping one's tools to better serve one's deepest intentions. It is a commitment to continuous learning, both about the world and about oneself.

Concluding Call to Action: Building Your Sentient Archive – An Ongoing Commitment to Self-Authorship

The vision laid out in this document is ambitious, but its pursuit is not predicated on achieving some distant, perfect end-state. Rather, the value lies in the iterative act of building, capturing, reflecting, and refining. It is a call to reject digital amnesia and embrace the power of a self-authored, sentient archive.

The journey of the Sinex Exocortex is an ongoing commitment to transforming one's digital life from a source of fragmentation and forgotten potential into a wellspring of integrated knowledge, focused agency, and profound self-understanding. It is an invitation to become the meticulous archivist, the insightful analyst, and the intentional architect of your own cognitive landscape. Build your Exocortex not because it is easy, but because the endeavor itself is a vital act of self-creation in the digital age.

---

**Appendices (Conceptual List - To Be Generated if Requested)**
    *A. SQL Data Definition Language (DDL) for All Core Tables (including `raw.events`, `sinex_schemas.*`, `core_artifacts`, `core_artifact_contents`, `core_blobs`, `core_tags`, `artifact_tags`, `core_entities`, `core_entity_relations`, `event_relations`, `artifact_embeddings`, etc.)
    *   B. Canonical Event `source` Identifiers & Core Payload JSON Schema Examples (for key `(source, event_type)` pairs)
    *C. Example Agent Manifests & Key LLM Prompt Templates (from `core.prompts`)
    *   D. `exo` CLI Command Reference (Generated/Comprehensive)
    *E. Glossary of Key Exocortex Terms
    *   F. Security Threat Model & Detailed Mitigation Strategies
    *G. Backup and Recovery Detailed Operational Procedures
    *   H. NixOS Module Design Patterns for Sinex Services & Agents
    *I. Ingestor Development Guide & Common Library Usage
    *   J. Data Flow Diagrams for Key Workflows (e.g., PKM Sync, Web Archiving, Living Doc Update)

---

Okay, I will generate the content for each of the listed Appendices, one by one, aiming for a good level of detail suitable for a definitive system document. This will be quite extensive.

---

Appendix A: SQL Data Definition Language (DDL) for Core Tables
---

This appendix provides the PostgreSQL DDL for the core tables of the Sinex Exocortex as envisioned post-Phase 2.5, and looking towards Phase 3 and beyond. It assumes the availability of a ULID generation function (`generate_ulid()`) and the `vector` type from `pgvector`.

```sql
-- Enable necessary extensions if not already enabled database-wide
CREATE EXTENSION IF NOT EXISTS "uuid-ossp"; -- For uuid_generate_v4 as fallback or for other uses
-- CREATE EXTENSION IF NOT EXISTS "pg_ulid"; -- If using a dedicated ULID extension
CREATE EXTENSION IF NOT EXISTS "vector";  -- For pgvector

-- Custom Domain for ULID if not using a native type from an extension
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ulid') THEN
        CREATE DOMAIN ULID AS BYTEA CHECK(octet_length(VALUE) = 16);
    END IF;
END $$;

-- ULID Generation Function (Example if not using pg_ulid extension)
CREATE OR REPLACE FUNCTION generate_ulid() RETURNS ULID AS $$
DECLARE
   timestamp_ms BIGINT;
   random_bytes BYTEA;
   ulid_bytes BYTEA;
BEGIN
   timestamp_ms := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
   random_bytes := gen_random_bytes(10); -- PostgreSQL built-in for random bytes
   ulid_bytes := substring((E'\\x' || lpad(to_hex(timestamp_ms), 12, '0'))::bytea FROM 1 FOR 6) || random_bytes;
   RETURN ulid_bytes::ULID;
END $$ LANGUAGE plpgsql VOLATILE;

-- Core Schemas for Organization
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS sinex_schemas;
CREATE SCHEMA IF NOT EXISTS core;
-- Domain schemas would be created as needed, e.g., domain_hyprland, domain_kitty

---
--- Table: raw.events (The Canonical Event Substrate)
---
CREATE TABLE IF NOT EXISTS raw.events (
    id                      ULID PRIMARY KEY DEFAULT generate_ulid(),
    source                  TEXT NOT NULL,
    event_type              TEXT NOT NULL,
    ts_ingest               TIMESTAMPTZ NOT NULL DEFAULT now(),
    ts_orig                 TIMESTAMPTZ,
    host                    TEXT NOT NULL,
    ingestor_version        TEXT,
    payload_schema_id       ULID, -- FK added later
    payload                 JSONB NOT NULL
);

COMMENT ON TABLE raw.events IS 'Universal log for all captured raw events before promotion or detailed structuring. Immutable.';
COMMENT ON COLUMN raw.events.id IS 'Globally unique, time-sortable ULID for the event.';
COMMENT ON COLUMN raw.events.source IS 'Canonical identifier for the event origin/producer (e.g., "hyprland_ingestor", "sinex.pkm.sync_agent").';
COMMENT ON COLUMN raw.events.event_type IS 'Type string for the event, often namespaced by source (e.g., "window_focused", "note_updated", "agent.heartbeat").';
COMMENT ON COLUMN raw.events.ts_ingest IS 'Timestamp of ingestion into this table (database server time). Primary TimescaleDB partitioning key.';
COMMENT ON COLUMN raw.events.ts_orig IS 'Original timestamp from the source system/sensor when the event occurred.';
COMMENT ON COLUMN raw.events.host IS 'Identifier of the machine or device where the event originated.';
COMMENT ON COLUMN raw.events.ingestor_version IS 'Version of the ingestor code/binary that produced this event.';
COMMENT ON COLUMN raw.events.payload_schema_id IS 'Foreign key to sinex_schemas.event_payload_schemas, identifying the schema for the payload.';
COMMENT ON COLUMN raw.events.payload IS 'Complete raw event data as JSONB. May contain a "_provenance" sub-object for correlation_id, etc.';

-- Indexes for raw.events
CREATE INDEX IF NOT EXISTS idx_raw_events_source_type_ts_ingest ON raw.events (source, event_type, ts_ingest DESC);
CREATE INDEX IF NOT EXISTS idx_raw_events_ts_orig ON raw.events (ts_orig DESC);
CREATE INDEX IF NOT EXISTS idx_raw_events_host_ts_ingest ON raw.events (host, ts_ingest DESC);
CREATE INDEX IF NOT EXISTS idx_raw_events_payload_schema_id ON raw.events (payload_schema_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_payload_gin ON raw.events USING GIN (payload jsonb_path_ops); -- Use jsonb_path_ops for better specific path indexing

-- TimescaleDB Hypertable (if TimescaleDB is used)
-- SELECT create_hypertable('raw.events', 'ts_ingest', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 day');

---
--- Table: sinex_schemas.event_payload_schemas (Schema Registry for event payloads)
---
CREATE TABLE IF NOT EXISTS sinex_schemas.event_payload_schemas (
    id                      ULID PRIMARY KEY DEFAULT generate_ulid(),
    event_source            TEXT NOT NULL,
    event_type              TEXT NOT NULL,
    schema_version          TEXT NOT NULL,
    json_schema_definition  JSONB NOT NULL,
    description             TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (event_source, event_type, schema_version)
);
COMMENT ON TABLE sinex_schemas.event_payload_schemas IS 'Registry for JSON Schema definitions of raw.events payloads.';
-- Add FK from raw.events to here
ALTER TABLE raw.events
ADD CONSTRAINT IF NOT EXISTS fk_raw_events_payload_schema
FOREIGN KEY (payload_schema_id) REFERENCES sinex_schemas.event_payload_schemas(id);

---
--- Table: sinex_schemas.agent_manifests (Registry for all agents/ingestors)
---
CREATE TABLE IF NOT EXISTS sinex_schemas.agent_manifests (
    agent_name              TEXT PRIMARY KEY,
    description             TEXT,
    version                 TEXT NOT NULL,
    status                  TEXT NOT NULL DEFAULT 'development', -- e.g., development, stable, deprecated, disabled_by_user, error_state
    config_schema_id        ULID REFERENCES sinex_schemas.event_payload_schemas(id) NULLABLE,
    produces_event_types    JSONB, -- {"source_A": [{"type": "type1", "schema_id": "ULID_schema1"}, ...], ...}
    subscribes_to_event_types JSONB NULLABLE, -- Similar structure for consumed events
    repo_url                TEXT NULLABLE,
    last_seen_heartbeat     TIMESTAMPTZ NULLABLE,
    registered_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE sinex_schemas.agent_manifests IS 'Central registry for all Sinex agents and ingestors, their capabilities, and status.';

---
--- Table: core.artifacts (Canonical representation of conceptual documents/items like PKM notes, webpages)
---
CREATE TABLE IF NOT EXISTS core.artifacts (
    artifact_id             ULID PRIMARY KEY DEFAULT generate_ulid(),
    artifact_type           TEXT NOT NULL, -- 'pkm_note', 'webpage_archive', 'pdf_document', 'task_item', 'project_entity', 'summary', 'narrative'
    canonical_identifier    TEXT UNIQUE NOT NULL, -- Normalized URL, unique PKM note filename/id, project name
    current_title           TEXT,
    tags_denormalized       TEXT[], -- Denormalized array of current tags for quick filtering
    properties              JSONB,  -- Type-specific properties, e.g. {"status": "open"} for task_item
    created_at_ts_orig      TIMESTAMPTZ,
    last_event_ts_orig      TIMESTAMPTZ, -- Timestamp of the last raw.event related to this artifact
    current_content_id      ULID NULLABLE -- FK to core_artifact_contents.content_id (points to current version's content)
);
COMMENT ON TABLE core.artifacts IS 'Canonical entities for documents, notes, webpages, tasks, projects etc.';
CREATE INDEX IF NOT EXISTS idx_core_artifacts_type ON core.artifacts (artifact_type);
CREATE INDEX IF NOT EXISTS idx_core_artifacts_tags_gin ON core.artifacts USING GIN (tags_denormalized);

---
--- Table: core.artifact_contents (Stores actual textual content versions for artifacts)
---
CREATE TABLE IF NOT EXISTS core.artifact_contents (
    content_id              ULID PRIMARY KEY DEFAULT generate_ulid(),
    artifact_id             ULID NOT NULL, -- FK added later
    content_text            TEXT,
    content_hash_blake3     TEXT UNIQUE NOT NULL, -- BLAKE3 hash of content_text
    content_format          TEXT NOT NULL DEFAULT 'text/markdown', -- 'text/markdown', 'text/plain', 'application/json'
    captured_at_ts_orig     TIMESTAMPTZ NOT NULL,
    capture_method          TEXT,
    source_blob_hash_blake3 TEXT NULLABLE, -- Optional: Hash of original raw blob if content_text is derived
    word_count              INT,
    char_count              INT,
    metadata                JSONB -- e.g., for web archive, original title, author etc.
);
COMMENT ON TABLE core.artifact_contents IS 'Versioned textual content for artifacts (PKM notes, webpage markdown, etc.).';
CREATE INDEX IF NOT EXISTS idx_core_artifact_contents_artifact_id_ts ON core.artifact_contents (artifact_id, captured_at_ts_orig DESC);
CREATE INDEX IF NOT EXISTS idx_core_artifact_contents_text_fts ON core.artifact_contents USING GIN (to_tsvector('english', content_text));
-- Add FKs
ALTER TABLE core.artifacts
ADD CONSTRAINT IF NOT EXISTS fk_core_artifacts_current_content
FOREIGN KEY (current_content_id) REFERENCES core.artifact_contents(content_id);

ALTER TABLE core.artifact_contents
ADD CONSTRAINT IF NOT EXISTS fk_core_artifact_contents_artifact
FOREIGN KEY (artifact_id) REFERENCES core.artifacts(artifact_id) ON DELETE CASCADE;

---
--- Table: core.blobs (Metadata for git-annexed or other content-addressed blobs)
---
CREATE TABLE IF NOT EXISTS core.blobs (
    blob_id                 ULID PRIMARY KEY DEFAULT generate_ulid(),
    content_annex_key       TEXT UNIQUE NOT NULL, -- For git-annex managed blobs
    content_blake3_hash     TEXT UNIQUE NULLABLE, -- Alternative primary hash if not using annex key directly
    mime_type               TEXT,
    size_bytes              BIGINT NOT NULL,
    original_filenames      TEXT[],
    user_description        TEXT,
    extracted_media_metadata JSONB,
    schema_id               ULID REFERENCES sinex_schemas.event_payload_schemas(id) NULLABLE,
    created_at_ts_orig      TIMESTAMPTZ,
    ingested_at_ts          TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE core.blobs IS 'Metadata registry for content-addressed blobs, typically managed by git-annex.';

---
--- Table: core.tags (Canonical tag definitions)
---
CREATE TABLE IF NOT EXISTS core.tags (
    tag_id                  ULID PRIMARY KEY DEFAULT generate_ulid(),
    tag_name                TEXT UNIQUE NOT NULL, -- e.g., "project.sinex", "media.anime.evangelion", "status.todo"
    description             TEXT,
    parent_tag_id           ULID REFERENCES core.tags(tag_id) NULLABLE, -- For hierarchical tags
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE core.tags IS 'Canonical definitions for all tags used in the system.';
CREATE INDEX IF NOT EXISTS idx_core_tags_name_fts ON core.tags USING GIN (to_tsvector('english', tag_name));
CREATE INDEX IF NOT EXISTS idx_core_tags_parent ON core.tags (parent_tag_id);

---
--- Table: artifact_tags (Linking tags to artifacts, events, blobs)
---
CREATE TABLE IF NOT EXISTS artifact_tags (
    target_object_id        ULID NOT NULL, -- ULID of the tagged item (raw.events.id, core.artifacts.artifact_id, core.blobs.blob_id, etc.)
    target_object_type      TEXT NOT NULL, -- 'raw_event', 'core_artifact', 'core_blob', 'core_entity'
    tag_id                  ULID NOT NULL REFERENCES core.tags(tag_id) ON DELETE CASCADE,
    assigned_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    assigner_actor          TEXT NOT NULL, -- 'user_sinex', 'agent_AutoTagger_v1.2'
    confidence_score        FLOAT NULLABLE, -- If assigned by an agent
    PRIMARY KEY (target_object_id, target_object_type, tag_id)
);
COMMENT ON TABLE artifact_tags IS 'Many-to-many join table linking tags to various Exocortex objects.';
CREATE INDEX IF NOT EXISTS idx_artifact_tags_tag_id ON artifact_tags (tag_id);
CREATE INDEX IF NOT EXISTS idx_artifact_tags_target_object ON artifact_tags (target_object_id, target_object_type);


---
--- Table: core.entities (Nodes in the Knowledge Graph)
---
CREATE TABLE IF NOT EXISTS core.entities (
    entity_id               ULID PRIMARY KEY DEFAULT generate_ulid(),
    entity_type             TEXT NOT NULL, -- 'person', 'project', 'location', 'application', 'topic', 'task', 'intent', 'conceptual_document_via_artifact_id'
    canonical_label         TEXT NOT NULL,
    aliases                 TEXT[],
    properties              JSONB, -- Type-specific attributes
    description             TEXT,
    created_at_ts_orig      TIMESTAMPTZ,
    last_event_ts_orig      TIMESTAMPTZ,
    embedding_vector        VECTOR NULLABLE -- Embedding of label + key properties
    -- For conceptual documents, this might link to core_artifacts.artifact_id
    -- For tasks, this might BE the core_artifacts entry of type 'task_item'
);
COMMENT ON TABLE core.entities IS 'Canonical nodes for the Exocortex knowledge graph.';
CREATE UNIQUE INDEX IF NOT EXISTS uidx_core_entities_type_label ON core.entities (entity_type, canonical_label);
CREATE INDEX IF NOT EXISTS idx_core_entities_embedding ON core.entities USING ivfflat (embedding_vector vector_cosine_ops) WHERE embedding_vector IS NOT NULL;

---
--- Table: core.entity_relations (Edges in the Knowledge Graph between entities)
---
CREATE TABLE IF NOT EXISTS core.entity_relations (
    relation_id             ULID PRIMARY KEY DEFAULT generate_ulid(),
    source_entity_id        ULID NOT NULL REFERENCES core.entities(entity_id) ON DELETE CASCADE,
    target_entity_id        ULID NOT NULL REFERENCES core.entities(entity_id) ON DELETE CASCADE,
    relation_type           TEXT NOT NULL, -- 'works_on_project', 'uses_application', 'located_at_place', 'mentions_topic', 'depends_on_task'
    properties              JSONB, -- e.g., {"role": "lead_developer"} for a person-project link
    ts_start_orig           TIMESTAMPTZ, -- Start of relation validity
    ts_end_orig             TIMESTAMPTZ, -- End of relation validity (if applicable)
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by_actor        TEXT NOT NULL -- 'user_sinex', 'agent_LinkFinder_v0.9'
);
COMMENT ON TABLE core.entity_relations IS 'Typed, directed relationships between canonical entities.';
CREATE INDEX IF NOT EXISTS idx_core_entity_relations_source ON core.entity_relations (source_entity_id, relation_type);
CREATE INDEX IF NOT EXISTS idx_core_entity_relations_target ON core.entity_relations (target_entity_id, relation_type);

---
--- Table: event_relations (Typed, semantic links between raw events or events and other objects)
---
CREATE TABLE IF NOT EXISTS event_relations (
    relation_id             ULID PRIMARY KEY DEFAULT generate_ulid(),
    from_object_id          ULID NOT NULL,
    from_object_type        TEXT NOT NULL, -- 'raw_event', 'core_artifact', 'core_entity'
    to_object_id            ULID NOT NULL,
    to_object_type          TEXT NOT NULL,
    relation_type           TEXT NOT NULL, -- 'derives_from', 'explains_context_of', 'triggered_by', 'resolves_friction'
    description             TEXT NULLABLE,
    confidence_score        FLOAT NULLABLE, -- If inferred by an agent
    created_by_actor        TEXT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE event_relations IS 'Explicit semantic links between raw events and/or other core objects.';
CREATE INDEX IF NOT EXISTS idx_event_relations_from_object ON event_relations (from_object_id, from_object_type, relation_type);
CREATE INDEX IF NOT EXISTS idx_event_relations_to_object ON event_relations (to_object_id, to_object_type, relation_type);

---
--- Table: artifact_embeddings (Storing embeddings for textual content of artifacts)
---
CREATE TABLE IF NOT EXISTS artifact_embeddings (
   content_id              ULID NOT NULL REFERENCES core.artifact_contents(content_id) ON DELETE CASCADE,
   embedding_name          TEXT NOT NULL, -- e.g., "full_text_chunk_001", "title_summary", "user_selection_for_query"
   model_name              TEXT NOT NULL, -- e.g., "text-embedding-3-small"
   model_dimension         INT NOT NULL,
   embedding_vector        VECTOR,        -- Actual vector from pgvector
   created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
   input_text_hash_blake3  TEXT NULLABLE UNIQUE, -- Optional: Hash of the exact text chunk sent for embedding (for cache/dedup of embedding generation)
   PRIMARY KEY (content_id, embedding_name, model_name)
);
COMMENT ON TABLE artifact_embeddings IS 'Vector embeddings for chunks or summaries of artifact textual content.';
CREATE INDEX IF NOT EXISTS idx_artifact_embeddings_vector ON artifact_embeddings USING ivfflat (embedding_vector vector_cosine_ops) WITH (lists = 100); -- Or HNSW
CREATE INDEX IF NOT EXISTS idx_artifact_embeddings_model ON artifact_embeddings (model_name);
CREATE INDEX IF NOT EXISTS idx_artifact_embeddings_input_hash ON artifact_embeddings (input_text_hash_blake3) WHERE input_text_hash_blake3 IS NOT NULL;

---
--- Table: agent_processing_dlq (Dead Letter Queue for agent processing failures)
---
CREATE TABLE IF NOT EXISTS agent_processing_dlq (
    dlq_id                  ULID PRIMARY KEY DEFAULT generate_ulid(),
    failed_raw_event_id     ULID REFERENCES raw.events(id) ON DELETE SET NULL, -- Keep DLQ entry even if raw event is somehow deleted
    processing_agent_name   TEXT NOT NULL REFERENCES sinex_schemas.agent_manifests(agent_name),
    error_details           JSONB NOT NULL, -- Includes error message, stack trace, context
    retry_count             INT NOT NULL DEFAULT 0,
    status                  TEXT NOT NULL DEFAULT 'pending_review', -- 'pending_review', 'retrying', 'resolved_manual', 'ignored_permanent'
    first_failed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_attempted_at       TIMESTAMPTZ,
    resolved_at             TIMESTAMPTZ
);
COMMENT ON TABLE agent_processing_dlq IS 'Dead Letter Queue for events that agents failed to process after retries.';
CREATE INDEX IF NOT EXISTS idx_agent_dlq_status_agent ON agent_processing_dlq (processing_agent_name, status, first_failed_at);

---
--- Add other domain-specific tables as needed, e.g.,
---
-- CREATE TABLE IF NOT EXISTS domain_hyprland.focus_changes (...);
-- CREATE TABLE IF NOT EXISTS domain_kitty.commands_executed (...);
-- CREATE TABLE IF NOT EXISTS meta_cognitive.friction_reports (...);
-- Each linking back to raw.events.id via raw_event_id_fk.
---
```

This DDL provides a comprehensive starting point for the Exocortex database, incorporating all major entities and relationships discussed. Specific domain tables would be added via migrations as ingestors and promotion pipelines for them are developed.

---

**Appendix B: Canonical Event `source` Identifiers & Core Payload JSON Schema Examples**

This appendix outlines the naming conventions for `raw.events.source` and `raw.events.event_type`, and provides illustrative JSON Schema examples for the `payload` of key event types. These schemas would be registered in `sinex_schemas.event_payload_schemas`.

**B.1. Event `source` Naming Convention:**

The `source` field should be a hierarchical, dot-separated string indicating the logical origin or domain of the event.

* General Pattern: `domain.subdomain.specific_producer`
* Examples:
  * `desktop.hyprland.plugin` (Events directly from a Hyprland plugin/ingestor)
  * `desktop.input.evdev_keyboard` (Raw keyboard events via evdev)
  * `app.neovim.plugin` (Events from the Neovim plugin)
  * `app.browser.firefox_extension` (Events from Firefox extension)
  * `app.terminal.kitty_rc` (Events from Kitty Remote Control)
  * `system.filesystem.inotify_watcher` (Filesystem events)
  * `system.journald.bridge` (Generic journald entries bridged into Exocortex)
  * `sinex.agent.heartbeat_monitor` (Operational events from Sinex agents themselves)
  * `sinex.pkm.sync_agent` (Events related to PKM operations)
  * `sinex.web.archiver_agent` (Events from the web archiving process)
  * `user.manual_log.cli` (User manually logging an event via `exo` CLI)
  * `user.meta.friction_log` (Specific type of manual log)
  * `mobile.android.companion_app` (Events from the Android companion)
  * `iot.env_sensor.office_temp` (Specific IoT sensor)

**B.2. Event `event_type` Naming Convention:**

The `event_type` field describes the specific action or observation. It's often best kept relatively simple and specific to the `source`.

* Examples for `source="desktop.hyprland.plugin"`:
  * `window_focused`
  * `window_title_changed`
  * `workspace_activated`
  * `clipboard_copied`
  * `state_snapshot`
* Examples for `source="app.neovim.plugin"`:
  * `buffer_content_changed`
  * `file_saved`
  * `command_executed`
* Examples for `source="sinex.agent.heartbeat_monitor"`:
  * `agent_heartbeat_received` (payload identifies specific agent)
  * `agent_status_changed`

**B.3. Core Payload JSON Schema Examples (Illustrative):**

These are conceptual examples. Actual schemas will be more detailed and versioned in `sinex_schemas.event_payload_schemas`. All schemas should implicitly allow `"$schema": "http://json-schema.org/draft-07/schema#"`. It's assumed that a `_provenance` object (as detailed in Part III.1.3) may also be part of these payloads if not handled entirely by top-level `raw.events` columns.

* **Schema for: `source="desktop.hyprland.plugin"`, `event_type="window_focused"`, `schema_version="v1.0"`**

    ```json
    {
      "type": "object",
      "properties": {
        "window_id_hex": { "type": "string", "description": "Hyprland's hex ID for the window" },
        "window_class": { "type": "string", "description": "WM_CLASS instance name" },
        "window_initial_class": { "type": "string", "description": "WM_CLASS class name" },
        "window_title": { "type": "string" },
        "pid": { "type": "integer", "description": "Process ID of the window owner" },
        "process_name": { "type": "string", "description": "Name of the process owning the window" },
        "workspace_id": { "type": "integer" },
        "workspace_name": { "type": "string", "description": "User-defined name of the workspace if available" },
        "monitor_id": { "type": "integer" },
        "monitor_description": { "type": "string" },
        "is_floating": { "type": "boolean" },
        "is_fullscreen": { "type": "boolean" },
        "geometry": {
          "type": "object",
          "properties": { "x": {"type": "integer"}, "y": {"type": "integer"}, "width": {"type": "integer"}, "height": {"type": "integer"} },
          "required": ["x", "y", "width", "height"]
        }
      },
      "required": ["window_class", "window_title", "pid", "workspace_id", "monitor_id"]
    }
    ```

* **Schema for: `source="app.terminal.kitty_rc"`, `event_type="command_executed"`, `schema_version="v1.0"`**

    ```json
    {
      "type": "object",
      "properties": {
        "command_string_full": { "type": "string", "description": "The complete command line executed" },
        "command_argv": { "type": "array", "items": { "type": "string" }, "description": "Command and arguments tokenized" },
        "cwd": { "type": "string", "description": "Current working directory when command was run" },
        "exit_code": { "type": "integer", "description": "Exit code of the command" },
        "ts_start_orig_iso": { "type": "string", "format": "date-time", "description": "Timestamp when command started" },
        "ts_end_orig_iso": { "type": "string", "format": "date-time", "description": "Timestamp when command finished" },
        "duration_ms": { "type": "integer", "description": "Execution duration in milliseconds" },
        "kitty_window_id": { "type": "integer" },
        "kitty_tab_title": { "type": "string" },
        "environment_subset": {
            "type": "object",
            "description": "Selected environment variables (e.g., PWD, GIT_BRANCH, VIRTUAL_ENV)",
            "additionalProperties": { "type": "string" }
        },
        "session_id_pty": {"type": "string", "description": "PTY session identifier if available"}
      },
      "required": ["command_string_full", "cwd", "exit_code", "ts_start_orig_iso", "ts_end_orig_iso"]
    }
    ```

* **Schema for: `source="sinex.pkm.sync_agent"`, `event_type="note_updated"`, `schema_version="v1.0"`**

    ```json
    {
      "type": "object",
      "properties": {
        "artifact_id_note": { "type": "string", "format": "ulid", "description": "ULID of the core_artifacts entry for the note" },
        "content_id_new": { "type": "string", "format": "ulid", "description": "ULID of the new core_artifact_contents entry" },
        "content_hash_blake3_new": { "type": "string", "description": "BLAKE3 hash of the new content" },
        "content_id_old": { "type": "string", "format": "ulid", "description": "ULID of the previous core_artifact_contents entry", "nullable": true },
        "content_hash_blake3_old": { "type": "string", "description": "BLAKE3 hash of the old content", "nullable": true },
        "diff_summary_text": { "type": "string", "description": "A short textual summary of changes, or reference to a diff blob", "nullable": true },
        "parsed_title": { "type": "string" },
        "parsed_tags": { "type": "array", "items": { "type": "string" } },
        "sync_trigger": { "type": "string", "enum": ["filesystem_watch", "neovim_save_hook", "manual_exo_sync"] }
      },
      "required": ["artifact_id_note", "content_id_new", "content_hash_blake3_new", "sync_trigger"]
    }
    ```

* **Schema for: `source="user.meta.friction_log"`, `event_type="entry_created"`, `schema_version="v1.0"`**

    ```json
    {
      "type": "object",
      "properties": {
        "description_text": { "type": "string", "description": "User's description of the friction" },
        "perceived_cause_text": { "type": "string", "nullable": true },
        "intensity_score_1_to_5": { "type": "integer", "minimum": 1, "maximum": 5, "nullable": true },
        "linked_task_ids_ulid": { "type": "array", "items": {"type": "string", "format": "ulid"} , "nullable": true },
        "associated_raw_event_ids_ulid": { "type": "array", "items": {"type": "string", "format": "ulid"} , "nullable": true },
        "resolution_status_text": { "type": "string", "enum": ["open", "investigating", "workaround_found", "resolved", "wont_fix"], "default": "open" },
        "resolution_notes_text": { "type": "string", "nullable": true },
        "tags": { "type": "array", "items": { "type": "string" } }
      },
      "required": ["description_text"]
    }
    ```

These examples illustrate the expected level of detail. Each new `(source, event_type)` pair introduced into the system should ideally have a corresponding schema registered. For ad-hoc or experimental events, `payload_schema_id` in `raw.events` can be NULL, and the payload is treated as purely schemaless JSONB by downstream consumers until a schema is defined.

---

Appendix C: Example Agent Manifests & Key LLM Prompt Templates
---

This appendix provides illustrative examples for entries in `sinex_schemas.agent_manifests` and `core.prompts`.

**C.1. Example Agent Manifests (`sinex_schemas.agent_manifests` entries):**

* **Hyprland Ingestor:**

    ```json
    {
      "agent_name": "HyprlandIngestor_Rust_v0.3.1",
      "description": "Captures Hyprland window manager events, focus, state snapshots, and clipboard via IPC.",
      "version": "0.3.1",
      "status": "stable",
      "config_schema_id": "ULID_of_HyprlandIngestorConfigSchema_v1.0",
      "produces_event_types": {
        "desktop.hyprland.plugin": [
          {"type": "window_focused", "schema_id": "ULID_Schema_HyprFocus_v1.0"},
          {"type": "window_title_changed", "schema_id": "ULID_Schema_HyprTitle_v1.0"},
          {"type": "clipboard_copied", "schema_id": "ULID_Schema_HyprClipboard_v1.0"},
          {"type": "state_snapshot", "schema_id": "ULID_Schema_HyprSnapshot_v1.0"}
        ],
        "sinex.agent": [
          {"type": "heartbeat", "schema_id": "ULID_Schema_SinexAgentHeartbeat_v1.0"},
          {"type": "error", "schema_id": "ULID_Schema_SinexAgentError_v1.0"}
        ]
      },
      "subscribes_to_event_types": null,
      "repo_url": "https://github.com/user/sinex/tree/main/ingestor/hyprland",
      "last_seen_heartbeat": "2025-06-01T12:00:00Z",
      "registered_at": "2025-05-01T00:00:00Z",
      "updated_at": "2025-06-01T12:00:05Z"
    }
    ```

    *(Note: `agent_name` should be the primary key. Above is JSON representation of a row).*

* **PKM Note Embedding Agent:**

    ```json
    {
      "agent_name": "PkmNoteEmbedderAgent_Python_v0.1.0",
      "description": "Generates vector embeddings for new or updated PKM note content from core_artifact_contents.",
      "version": "0.1.0",
      "status": "development",
      "config_schema_id": "ULID_of_EmbeddingAgentConfigSchema_v1.0",
      "produces_event_types": {
        "sinex.agent": [
          {"type": "heartbeat", "schema_id": "ULID_Schema_SinexAgentHeartbeat_v1.0"},
          {"type": "error", "schema_id": "ULID_Schema_SinexAgentError_v1.0"},
          {"type": "embedding_generated", "schema_id": "ULID_Schema_EmbeddingResult_v1.0"}
        ],
        "sinex.agent.llm_api_call": [
            {"type": "embedding_request_completed", "schema_id": "ULID_Schema_LLMCall_v1.0"}
        ]
      },
      "subscribes_to_event_types": {
        "sinex.pkm": [
            {"type": "note_imported", "schema_id": "ULID_Schema_PKMNoteImported_v1.0"},
            {"type": "note_updated", "schema_id": "ULID_Schema_PKMNoteUpdated_v1.0"}
        ]
        // Or subscribes to core_artifact_contents table changes via LISTEN/NOTIFY or polling
      },
      "llm_dependencies": ["openai/text-embedding-3-small", "ollama/all-minilm-l6-v2"],
      "repo_url": "https://github.com/user/sinex/tree/main/agent/pkm_embedder",
      ...
    }
    ```

**C.2. Example Key LLM Prompt Templates (`core.prompts` entries):**

* **Prompt for Summarizing a Web Page (Markdown):**
  * `prompt_name`: "SummarizeWebPageMarkdown_Concise_v1.0"
  * `version`: "1.0"
  * `target_llm_family`: "general_instruct"
  * `variables_schema_id`: ULID for a schema like `{"type":"object", "properties": {"markdown_content":{"type":"string"}, "original_url":{"type":"string"}, "desired_length_words":{"type":"integer", "default": 150}}}`
  * `prompt_template`:

        ```
        You are a helpful assistant tasked with summarizing web page content.
        The following text is the extracted Markdown content from a web page originally found at: {original_url}

        --- BEGIN MARKDOWN CONTENT ---
        {markdown_content}
        --- END MARKDOWN CONTENT ---

        Please provide a concise summary of this web page's main points and key information.
        The summary should be approximately {desired_length_words} words.
        Focus on factual information and core arguments. Avoid personal opinions or speculation.
        Output format should be a single block of text.
        ```

* **Prompt for Extracting TODOs from Living Document Segment:**
  * `prompt_name`: "ExtractTasksFromLivingDoc_v1.2"
  * `version`: "1.2"
  * `target_llm_family`: "function_calling_capable" (or needs specific output format)
  * `variables_schema_id`: ULID for `{"type":"object", "properties": {"text_segment":{"type":"string"}, "current_date":{"type":"string", "format":"date"}}}`
  * `prompt_template`:

        ```
        Analyze the following text segment from a user's Living Document. Identify any actionable tasks, to-do items, or explicit commitments.
        For each identified task, extract:
        1. A concise task description.
        2. Any implied or stated deadline (interpret relative terms like "tomorrow" based on current_date: {current_date}).
        3. Any mentioned project or context.
        4. A suggested priority (low, medium, high).

        Format your output as a JSON array of objects, where each object represents a task and has keys: "description", "deadline_iso_date_or_null", "project_context_tags", "priority".
        If no tasks are found, output an empty JSON array [].

        --- BEGIN TEXT SEGMENT ---
        {text_segment}
        --- END TEXT SEGMENT ---
        ```

* **Prompt for Daily Journal Reflection (used by `DailyJournalPrompter` agent):**
  * `prompt_name`: "GenerateDailyReflectionPrompt_v0.9"
  * `version`: "0.9"
  * `target_llm_family`: "creative_writing_support"
  * `variables_schema_id`: ULID for `{"type":"object", "properties": {"user_name":{"type":"string"}, "current_date_formatted":{"type":"string"}, "key_events_today_summary":{"type":"string"}, "recent_friction_points_summary":{"type":"string"}, "recent_insights_summary":{"type":"string"}}}`
  * `prompt_template`:

        ```
        Hello {user_name}, it's {current_date_formatted}. Time for your daily reflection.

        Today, your Exocortex captured some notable activity:
        Key Events/Accomplishments:
        {key_events_today_summary}

        Potential Friction or Challenges Logged:
        {recent_friction_points_summary}

        Insights or Learnings Captured:
        {recent_insights_summary}

        Consider these prompts for your reflection:
        - What was the most significant thing that happened today, and why?
        - What did you learn, or what surprised you?
        - What went well? What could have gone better?
        - What are your top 1-2 priorities or intentions for tomorrow based on today's experience?

        Write freely.
        ```

These examples illustrate how agents are defined and how LLM interactions are templated and managed within the Exocortex, allowing for both structured automation and flexible intelligence.

---

**Appendix D: `exo` CLI Command Reference (Generated/Comprehensive)**

*(This would be a detailed, man-page style reference for all `exo` subcommands and their options, ideally auto-generated from the CLI's argument parsing library (e.g., `clap` for Rust, `argparse` or `click` for Python) and supplemented with usage examples. For brevity here, I will outline the main command structure and key subcommands envisioned by the end of Phase 3, based on our discussions.)*

**`exo --help` (Main Help)**

**Usage:** `exo [OPTIONS] <COMMAND>`

**Global Options:**

* `--config <PATH>`: Path to Sinex CLI config file (overrides default).
* `--db-url <URL>`: Override database URL from config/env.
* `--output-format <json|yaml|table|csv>`: Default 'table' for human, 'json' for scripts.
* `--verbose, -v`: Increase verbosity.
* `--quiet, -q`: Suppress informational output.
* `--version`: Show version.

**Commands:**

* **`log`**: Manually log a raw event.
  * `exo log <source> <event_type> --payload-json '{"key":"val"}' [--ts_orig <ISO_TS>] [--host <H>] [--tags "t1,t2"] [--correlation-id <ID>] ...`
  * `exo log meta.friction --description "..." [--intensity N] [--cause "..."] ...` (Specialized helpers for common meta-events)
* **`query`**: Execute a simplified or raw SQL query against `raw.events` or domain tables.
  * `exo query --source X --event-type Y --since "1d" --limit 10 --payload-jq '.field == "val"'`
  * `exo query --sql "SELECT * FROM raw.events WHERE source='foo' LIMIT 5;"`
* **`find`**: Unified search across artifacts, events, entities.
  * `exo find "search terms" [--type <pkm_note|web_archive|raw_event|core_entity|...>] [--tags <tag_expr>] [--semantic-similar-to-text "text"] [--semantic-similar-to-id <ULID>] ...`
* **`pkm`**: Manage Personal Knowledge Management notes.
  * `pkm import <VAULT_PATH>`
  * `pkm sync [<NOTE_PATH_OR_ID>] [--force]`
  * `pkm new --title "My New Note" [--tags "t1,t2"] [--edit]`
  * `pkm get <NOTE_ID_OR_TITLE_OR_PATH>`
  * `pkm edit <NOTE_ID_OR_TITLE_OR_PATH>` (opens in $EDITOR)
  * `pkm tag <NOTE_ID_OR_TITLE_OR_PATH> add|rm <tag1> [<tag2>...]`
  * `pkm link <SOURCE_NOTE_ID> <TARGET_NOTE_ID_OR_TITLE> [--type <relation>]`
* **`web`**: Manage web archives.
  * `web archive <URL> [--method <trafilatura|jina|singlefile>] [--tags "t1"]`
  * `web get <URL_OR_ARTIFACT_ID>` (shows archived content metadata)
* **`blob`**: Interact with git-annex managed blobs.
  * `blob add <FILE_PATH> [--tags "t1"] [--description "desc"]`
  * `blob get <ANNEX_KEY_OR_BLOB_ID>` (ensures file is locally present)
  * `blob info <ANNEX_KEY_OR_BLOB_ID_OR_HASH>`
  * `blob tag <ANNEX_KEY_OR_BLOB_ID> add|rm <tag1> ...`
* **`tag`**: Manage global tags.
  * `tag create <TAG_NAME> [--description "desc"] [--parent <PARENT_TAG_ID>]`
  * `tag list [--hierarchy]`
  * `tag alias <CANONICAL_TAG_NAME> <ALIAS_NAME>`
* **`entity`**: Manage core entities.
  * `entity create --type <TYPE> --label "Label" [--aliases "a1,a2"] [--properties '{"k":"v"}']`
  * `entity get <ENTITY_ID_OR_TYPE_LABEL>`
  * `entity link <SOURCE_ID> <TARGET_ID> --type <RELATION_TYPE>`
* **`livingdoc`**: Interact with the Living Document.
  * `livingdoc append --text "My thought..." [--voice-transcript-path <FILE>]`
  * `livingdoc query "Find nodes related to X"`
  * `livingdoc patch <NODE_ID> --json-patch '[{"op":"replace", ...}]'`
  * `livingdoc extract <tasks|claims|...> [--node <NODE_ID>]`
* **`agent`**: Manage and inspect agents.
  * `agent list [--status <enabled|disabled|error>]`
  * `agent status <AGENT_NAME>` (shows manifest, last heartbeat, recent errors from `raw.events`)
  * `agent enable|disable|restart <AGENT_NAME>` (interacts with systemd via user, or sends command event)
  * `agent logs <AGENT_NAME> [--since "1h"]` (queries journald events for that agent)
  * `agent trigger <AGENT_NAME> [--payload-json '{"input":"val"}']` (for agents that support manual trigger)
* **`schema`**: Inspect event payload schemas and agent manifests.
  * `schema list-payloads [--source S] [--event-type T] [--active-only]`
  * `schema get-payload <SCHEMA_ULID_OR_SOURCE_TYPE_VERSION>`
  * `schema list-agents`
  * `schema get-agent <AGENT_NAME>`
* **`embed`**: Manage and query embeddings (Phase 3+).
  * `embed status [--artifact-type T]` (shows count of embedded vs. unembedded items)
  * `embed queue-artifact <ARTIFACT_ID_OR_CONTENT_ID>`
  * `embed find-similar-to-text "query text" [--limit N] [--type <pkm_note|web_archive>]`
  * `embed find-similar-to-id <ARTIFACT_ID_OR_CONTENT_ID> [--limit N]`
* **`system`**: System-level operations.
  * `system health` (checks DB connection, agent heartbeats, disk space via meta-events)
  * `system backup-db [--target <PATH_OR_REMOTE>]`
  * `system backup-annex [--remote <REMOTE_NAME>]`
  * `system dr-test --scenario <SCENARIO_NAME>` (triggers a documented DR test procedure)

This reference would be richly populated with examples for each command and option.

---

**Appendix E: Glossary of Key Exocortex Terms**

* **Agent:** A modular software component (often a systemd service) responsible for a specific task like data ingestion, enrichment, analysis, or automation.
* **Agent Manifest (`sinex_schemas.agent_manifests`):** A database record describing an agent's capabilities, version, configuration schema, and operational status.
* **Artifact (`core_artifacts` & `core_artifact_contents`):** A canonical representation of a significant piece of knowledge or data, such as a PKM note, an archived web page, or a PDF document. Artifacts have content that can be versioned.
* **Blob (`core_blobs` & git-annex):** A large binary object (image, video, audio, raw HTML, dataset) managed by git-annex for content-addressed storage and deduplication. Metadata about blobs is stored in `core_blobs`.
* **Capture-First Principle:** The foundational Exocortex principle that prioritizes comprehensive, lossless data ingestion in its rawest form. Structure and meaning are applied downstream.
* **Cognitive Habitat:** The Exocortex conceptualized as an active, adaptive digital environment that supports and extends the user's cognitive processes.
* **Correlation ID (`payload._provenance.correlation_id`):** A unique identifier propagated across multiple `raw.events` generated by a single logical user interaction or workflow, enabling tracing of complex operations.
* **Domain Table:** A PostgreSQL table with a strong schema, holding data promoted from `raw.events` for a specific domain (e.g., `domain_hyprland.focus_changes`). Rows link back to their source `raw.events.id`.
* **DLQ (Dead Letter Queue):** A persistent store (file-based per agent, or a central DB table like `agent_processing_dlq`) for events or tasks that an agent failed to process after retries.
* **Embedding (`artifact_embeddings`):** A dense vector representation of textual content, generated by an LLM, enabling semantic search and similarity comparisons. Stored using `pgvector`.
* **Entity (`core_entities`):** A node in the Knowledge Graph representing a canonical concept, person, project, topic, application, etc.
* **Event (`raw.events`):** The atomic unit of data in the Exocortex. An immutable record (ULID PK) with `source`, `event_type`, timestamps, `host`, `ingestor_version`, `payload_schema_id`, and a JSONB `payload`.
* **Event Relation (`event_relations`):** A typed, directed link between two events or between an event and another Exocortex object, capturing semantic relationships like causality or derivation.
* **Exocortex Covenant:** The set of philosophical commitments the system makes to its user regarding data ownership, agency, transparency, and evolution.
* **Friction-Driven Development:** The principle of prioritizing system development based on alleviating personally felt pain points or inefficiencies in the user's workflow.
* **Git-Annex:** The content-addressed filesystem used for storing and managing large blobs.
* **Ingestor:** An agent specifically responsible for capturing data from an external source and writing it as events into `raw.events`.
* **Knowledge Graph:** The emergent network of `core_entities` and their `core_entity_relations`, supplemented by links from `core_artifact_links` and `event_relations`.
* **Living Document:** A dynamic, event-sourced, AI-augmented cognitive workspace for stream-of-consciousness capture, planning, and active thought.
* **LLM Node Graph:** The conceptual architecture for Living Document processing, where different LLM agents (nodes) perform sequential or parallel operations (segmentation, delta generation, artifact extraction).
* **Meta-Event:** An event describing the state or operation of the Exocortex itself, or a subjective/meta-cognitive report from the user (e.g., `sinex.agent.heartbeat`, `meta.friction_logged`).
* **Meta-Observability:** The principle and practice of capturing all Exocortex operational data as first-class events within the system.
* **NixOS:** The declarative Linux distribution used as the operating system foundation, enabling reproducible and robust system configuration.
* **PKM (Personal Knowledge Management):** The user's existing or evolving system of notes, documents, and curated knowledge, which the Exocortex aims to integrate and enhance.
* **Promotion Pipeline:** An agent or process that transforms data from `raw.events` into more structured domain tables or enriches existing data.
* **Provenance (`raw.events.payload._provenance` and other mechanisms):** Metadata tracking the origin, history, and transformations of data within the Exocortex.
* **Schema Registry (`sinex_schemas.event_payload_schemas`):** A database table storing versioned JSON Schema definitions for the `payload` of different event types.
* **Sentient Archive:** A metaphorical description of the Exocortex, emphasizing its comprehensive awareness of user context and its capacity for intelligent, proactive assistance.
* **Sinex:** The official name of the Exocortex project. (Ensuring "Sinnix" is not used).
* **Tag (`core_tags`, `artifact_tags`):** A descriptive label applied to events, artifacts, blobs, or entities, supporting organization and faceted search. Tags can be hierarchical.
* **TimescaleDB:** A PostgreSQL extension for managing time-series data efficiently, used for partitioning and optimizing `raw.events`.
* **ULID (Universally Unique Lexicographically Sortable Identifier):** The standard for primary keys in the Exocortex, ensuring global uniqueness and time-based sortability.

---

**Appendix F: Security Threat Model & Detailed Mitigation Strategies**

*(This appendix would detail potential security threats to the Exocortex and the specific technical and operational mitigations in place. It builds upon Part VI.2.)*

**F.1. Threat Categories:**
    *Unauthorized Data Access (Confidentiality)
    *   Data Corruption or Unauthorized Modification (Integrity)
    *Denial of Service / Data Unavailability (Availability)
    *   Privacy Violations (Misuse of Sensitive Personal Data)
    *   Agent Misbehavior or Hijacking

**F.2. Mitigations (Examples):**

* **Unauthorized Data Access:**
  * *Threat:* Attacker gains filesystem access to database files or git-annex blobs.
  * *Mitigation:* Full-disk encryption (LUKS). Git-annex native encryption. Strong PostgreSQL user passwords (managed via `agenix`). Restricted PostgreSQL `pg_hba.conf` (localhost access only for most roles).
  * *Threat:* Network snooping on DB connections or agent communication.
  * *Mitigation:* PostgreSQL connections via UNIX domain sockets where possible; TLS for TCP connections if ever needed. Secure local HTTP/MQTT endpoints for ingestors with token/cert authentication.
  * *Threat:* Malicious browser extension or local malware reading Exocortex data.
  * *Mitigation:* Standard OS security practices. Limited privileges for Exocortex service users. User vigilance. (Exocortex cannot fully defend against a compromised host OS).

* **Data Corruption/Modification:**
  * *Threat:* Accidental `DELETE` or `UPDATE` by admin/user in DB.
  * *Mitigation:* `raw.events` is append-only by principle; updates handled by creating new correcting events. Regular backups with PITR. Restricted DB roles.
  * *Threat:* Git-annex blob corruption.
  * *Mitigation:* `git annex fsck`. Multiple annex remotes for redundancy. Content-addressing detects corruption.

* **Denial of Service:**
  * *Threat:* Ingestor flood overwhelming database or disk.
  * *Mitigation:* Systemd resource quotas for agents/ingestors. Rate limiting on ingest endpoints (if any). Database connection pooling. TimescaleDB for ingest performance. Disk space monitoring and alerts.
  * *Threat:* LLM API rate limiting or cost runaway.
  * *Mitigation:* Agent-level budgeting, throttling, exponential backoff for API calls. `sinex.agent.llm_api_call` logging for monitoring.

* **Privacy Violations:**
  * *Threat:* Accidental capture of highly sensitive data (passwords, PII).
  * *Mitigation:* Opt-in for sensitive ingestors. Configurable redaction policies. User education. "Privacy Zones" tagging. Secure deletion mechanisms (logical + cryptographic for blobs).
  * *Threat:* LLM agents processing sensitive data and leaking it to external APIs.
  * *Mitigation:* User configuration to restrict certain data types/tags from being sent to external LLMs. Preference for local LLMs for sensitive tasks. Audit logs of LLM API calls.

* **Agent Misbehavior:**
  * *Threat:* Buggy or compromised agent corrupts data or performs unauthorized actions.
  * *Mitigation:* Least-privilege DB roles for each agent. Agent manifest defines allowed inputs/outputs. All agent actions logged as events for audit. Code reviews. Sandboxing (via systemd).

**F.3. Ongoing Security Practices:**
    *Regular software updates (NixOS).
    *   Review of agent permissions.
    *Monitoring of meta-observability logs for anomalies.
    *   Periodic security self-assessment.

---

Appendix G: Backup and Recovery Detailed Operational Procedures
---

**G.1. PostgreSQL Backup Procedure (Example using `pg_basebackup` and WAL archiving):**
    1.  Configure `postgresql.conf`: `wal_level = replica`, `archive_mode = on`, `archive_command = '...'` (e.g., to copy WALs to a backup location).
    2.  Initial Full Backup: `pg_basebackup -D /path/to/backup/base -Ft -z -P -U postgres` (executed by `exocortex_admin_user` or `postgres` user).
    3.  Daily/Weekly Base Backups: Repeat step 2, store in versioned directories.
    4.  WAL Archiving: Ensure `archive_command` is robustly copying WAL files.
    5.  Encryption: Encrypt backup files and WAL archives using GPG or similar before off-site storage.
    6.  Testing: Periodically test restoration to a separate PostgreSQL instance.

**G.2. Git-Annex Backup Procedure:**
    1.  Configure at least one (preferably two) git-annex backup remotes (e.g., an external USB drive, a remote server via SSH, encrypted cloud storage via `rclone`).
    2.  Daily Script (Systemd Timer):
        *`cd /path/to/exocortex_annex_repo`
        *   `git annex sync --content <backup_remote_1_name>`
        *`git annex copy --all --to <backup_remote_2_name>` (if using copy semantics for full redundancy)
        *   `git gc --prune=now` (to pack the git repo itself)
        *   `git bundle create /path/to/backup/git_repo_meta/annex_meta_$(date +%Y%m%d).bundle --all` (backup git metadata)
    3.  Verification: Periodically run `git annex fsck --from <backup_remote_name>` on remotes.

**G.3. NixOS Configuration Backup:**
    1.  Ensure `/etc/nixos` (or wherever flakes are stored) is a Git repository.
    2.  Push regularly to a private, backed-up Git remote (e.g., personal Gitea, GitHub private).
    3.  Include `agenix` encrypted secrets in this backup.

**G.4. Disaster Recovery Steps (Example for Full Host Loss):**
    1.  Provision new hardware. Install NixOS.
    2.  Restore NixOS configuration from Git backup (including `agenix` secrets). Run `nixos-rebuild switch`. This sets up users, PostgreSQL, git-annex, systemd services.
    3.  Restore PostgreSQL:
        *Initialize new PostgreSQL cluster.
        *   Restore latest full base backup.
        *Configure `recovery.conf` (or `postgresql.auto.conf` settings) to replay WALs from archive up to desired PITR point.
        *   Start PostgreSQL; it will enter recovery mode and apply WALs. Once complete, promote to primary.
    4.  Restore Git-Annex:
        *Initialize new git-annex repository (`git init && git annex init`).
        *   Restore git repository metadata from bundle (`git clone /path/to/annex_meta.bundle .`).
        *Add backup remotes (`git remote add ...`, `git annex initremote ...`).
        *   `git annex sync <backup_remote_name>` (to pull symlink structure).
        *   `git annex get --all --from <backup_remote_name>` (to retrieve content, can be done gradually).
    5.  Start Sinex Services: `systemctl --user start sinex-*.service`.
    6.  Verify data integrity and agent status. Reprocess DLQs if necessary.

**G.5. Backup Monitoring:**
    *Agents log `sinex.system.backup.completed` events (with status, size, duration) for DB and annex backups.
    *   Alerts trigger if backups fail or haven't run for X days.

---

Essay 1: The Exocortex as a Laboratory for Self: A Guide to Personal Experimentation
---

The modern pursuit of self-improvement is often hampered by a lack of reliable data and systematic methodology. We adopt new habits, try different productivity techniques, or alter our routines based on anecdotal evidence, fleeting intuitions, or the latest self-help trends, yet we rarely possess the tools to rigorously measure their impact on our actual cognition, behavior, and well-being. The Sinex Exocortex, by its nature as a comprehensive, queryable archive of personal digital life and subjective experience, transforms this landscape. It offers the potential to turn the user's own life into a living **laboratory for self-experimentation**, providing both the raw data and the analytical power to move beyond guesswork towards genuine, data-driven personal insight and development.

The core principle is simple: what can be measured can be understood, and what can be understood can be intentionally changed. The Exocortex provides the means to measure. Consider the common desire to enhance focus and reduce procrastination. Instead of vaguely "trying harder," an Exocortex user can formulate specific hypotheses and test them systematically.

**Formulating Hypotheses:**
One might hypothesize: "My ability to engage in deep work on coding tasks (measured by long, uninterrupted focus spans in Neovim/terminal on project-related files) is positively correlated with at least 7 hours of logged sleep the previous night (`physio.sleep_logged.duration_total_minutes`) and negatively correlated with high morning social media usage (derived from `browser.history.visit` events to specific domains before noon)." Or, "Taking a 15-minute walk (logged as `activity.physical.walk_started/ended`) after every 90 minutes of focused work reduces the number of `meta.friction_logged` events related to 'mental fatigue' in the subsequent work block."

**Designing the Experiment & Logging Variables:**
The Exocortex user doesn't need specialized new tools to run such an experiment. The existing ingestors capture most of the "dependent variables" (focus spans, friction events, task completion rates). The "independent variables" (sleep, walks, dietary changes, new work routines, meditation practice) are logged explicitly by the user as `physio.*`, `activity.*`, or `meta.experiment.intervention_applied` events, with precise timestamps and relevant parameters (e.g., `payload: { intervention_type: "mindfulness_meditation", duration_minutes: 10 }`). The crucial step is consistency in logging these interventions and any relevant subjective state changes (`subjective.mood_reported`, `meta.activation_energy_shift`).

**Data Collection & Analysis:**
Over a period of weeks or months, the Exocortex accumulates a rich, longitudinal dataset. The user can then leverage the `exo` CLI or SQL queries (perhaps assisted by an LLM agent to formulate them) to analyze correlations:

* `SELECT AVG(focus_duration_minutes) FROM domain_desktop.focus_spans WHERE associated_project = 'X' GROUP BY DATE(previous_night_sleep_duration_hours > 7)`
* `SELECT COUNT(*) FROM meta_cognitive.friction_logs WHERE DATE(ts_orig) = current_date AND payload->>'perceived_cause' = 'fatigue' AND EXISTS (SELECT 1 FROM activity.physical_walk_events WHERE ts_orig BETWEEN current_date - INTERVAL '2 hours' AND current_date)`
Dashboards in Grafana can visualize these trends over time, making patterns more apparent. An "Analytical Agent" could even be tasked with periodically running pre-defined statistical tests on these correlations and reporting significant findings as `sinex.analytics.experiment_result` events.

**Iterating and Refining Personal Systems:**
The true power lies in the iterative loop. The results of one experiment inform the next. If increased sleep doesn't significantly impact coding focus, but reduced morning social media does, the user has actionable data. If short walks *do* reduce fatigue-related friction, that habit can be reinforced. The Exocortex becomes a tool for **A/B testing life strategies** on oneself. The "Living Document" can serve as the lab notebook, where hypotheses, experimental designs, raw observations (linked from `raw.events`), analyses, and conclusions are recorded and evolved.

This approach extends beyond simple productivity. One could experiment with:

* The impact of different nutritional choices on mood and energy (`physio.meal_logged` vs. `subjective.mood_reported`).
* The effectiveness of various learning techniques on knowledge retention (correlating study methods logged in `meta.learning_session.started` with later recall success when quizzed by an agent).
* The triggers for creative insights (analyzing `meta.insight_captured` events against preceding activities, browser history, or even ambient environmental data from IoT sensors).

The Exocortex, therefore, is not just a memory aid; it is a **personal research platform**. It provides the infrastructure for a more empirical, evidence-based approach to self-understanding and intentional change. By transforming subjective goals into testable hypotheses and daily actions into analyzable data, it empowers the user to become the scientist of their own life, systematically exploring what truly contributes to their well-being, effectiveness, and fulfillment. The "laboratory for self" is always open, always recording, and always ready for the next experiment.

---

Essay 2: The Accidental Philosopher: Emergent Insights from a Universal Personal Archive
---

The explicit goal of the Sinex Exocortex is to augment agency and memory, to provide a robust substrate for personal knowledge and workflow. Yet, embedded within its architecture of universal capture and relentless interconnection lies the potential for something more profound: the emergence of unexpected philosophical insights and a subtle reordering of one's relationship with time, self, and the nature of thought itself. The Exocortex user, in meticulously curating their digital and cognitive life, may find themselves an **accidental philosopher**, confronted with patterns and questions that transcend mere utility.

Consider the nature of **causality and influence**. Traditional memory is notoriously unreliable in reconstructing the precise antecedents of an idea or a decision. The Exocortex, with its timestamped events and linkable artifacts, offers a far more granular and objective (though still interpreted) record. By querying "What was I reading/doing/thinking just before insight X (`meta.insight_captured`) occurred?" or "Trace the chain of events leading from initial project idea A (`planning.milestone_defined`) to critical failure B (`meta.friction_logged` with `resolution_status='failed'`)", the user can begin to map the subtle, often invisible, currents of influence that shape their trajectory. This can lead to a deeper appreciation for the non-linear, contingent nature of creative thought, or a more sobering understanding of how seemingly minor earlier choices cascade into significant later outcomes. The illusion of a purely rational, linear self may begin to fray, replaced by an understanding of a self more akin to a complex system, constantly responding to a multitude of interacting inputs.

The concept of **identity and continuity of self** also comes under new scrutiny. A long-term Exocortex archive allows one to "visit" past versions of oneself—to read notes written, websites browsed, code committed, and even subjective states logged years prior. What patterns persist? What core beliefs or cognitive styles endure, and which have transformed utterly? The archive provides an empirical basis for exploring questions of personal evolution. Does a "core self" emerge from the data, or is the self revealed as a more fluid, context-dependent series_of states and responses? The ability to query for "all notes tagged with #core_value_X across all years" versus "all instances of `meta.friction_logged` where `perceived_cause` was 'imposter_syndrome'" might offer a data-driven path to understanding both the stable and the malleable aspects of one's identity.

The experience of **time itself** may be altered. While the Exocortex meticulously records linear, chronological time (`ts_orig`, `ts_ingest`), its query capabilities allow for the reconstruction of *subjective* or *kairotic* time—moments of intense focus, creative flow, or significant decision. By analyzing clusters of activity, the density of insights, or the duration of "deep work" sessions (derived from `activity_segment.identified` events), the user can map their personal temporal landscape, identifying rhythms of productivity, fallow periods, and the "gravitational pull" of certain projects or ideas that warp their perception of time. The archive might reveal that some of the most "valuable" work, in retrospect, occurred in short, intense bursts, challenging conventional notions of sustained effort.

Furthermore, the practice of **explicitly logging meta-cognitive events**—friction, insights, plans, intentions—forces a degree of self-reflection that can, in itself, be transformative. The act of articulating an "internal state" as a structured event externalizes it, makes it an object of potential analysis, and subtly changes one's relationship to it. A recurring pattern of `meta.friction_logged` events with similar `perceived_cause` descriptions is no longer just a vague feeling of being stuck; it is a data point, a signal prompting investigation. This disciplined introspection, mediated by the Exocortex, can foster a more mindful and less reactive engagement with one's own thought processes.

Even the **limits of the system** can provoke philosophical inquiry. What remains uncaptured? What aspects of experience resist eventification? The very act of designing and using the Exocortex highlights the inherentselectivity of any recording process, prompting questions about the nature of "completeness" in memory and the unavoidable biases in what any system—human or artificial—chooses to deem significant. The "gaps" in the Exocortex may become as informative as the data it holds.

The Sinex Exocortex, therefore, is more than an advanced tool. It is a mirror reflecting the user's cognitive and digital life with unprecedented fidelity. In gazing into this mirror, querying its depths, and observing the emergent patterns, the user may find themselves grappling with fundamental questions about who they are, how they think, and what it means to live an examined life in the digital age. The accidental philosopher within the Exocortex user discovers that the pursuit of perfect memory is also, unexpectedly, a path to deeper self-inquiry.

---

Essay 3: The Poetics of Data: Finding Narrative and Meaning in Personal Event Streams
---

In an age awash with data, we often equate information with understanding, and comprehensive archives with complete memory. Yet, raw data, however voluminous, rarely speaks for itself. It is through narrative—the artful weaving of disparate events into coherent, meaningful stories—that we transform mere information into wisdom, and personal history into a source of identity and guidance. The Sinex Exocortex, with its commitment to universal capture, provides an unparalleled substrate of raw personal data. Its ultimate humanistic value, however, may lie not just in its capacity for perfect recall, but in its potential as a **foundry for personal narrative and a canvas for the poetics of a lived digital life.**

The human mind is a natural storyteller. We make sense of our experiences by arranging them into sequences with causes, effects, turning points, and thematic resonances. A simple chronological log of `raw.events`, while factually accurate, can feel like an undifferentiated stream, lacking the emotional weight or explanatory power of a well-told story. The Exocortex, particularly through its agentic capabilities and its support for meta-cognitive logging, offers tools to bridge this gap.

Consider the **`meta.narrative_generated` event type**. This is not merely a summarization feature; it is an invitation to engage with one's own data through the lens of narrative construction. An LLM agent, prompted to "tell the story of Project Exocortex's development in May 2025," would not just list git commits and PKM edits. It would be tasked with identifying key `planning.milestone_defined` events, periods of intense activity inferred from high `app.neovim.plugin` event density, significant `meta.friction_logged` blockages, and culminating `meta.insight_captured` breakthroughs. It might weave in `subjective.mood_reported` events to color the emotional arc of that period. The resulting narrative, while AI-assisted, is grounded in the user's actual recorded experience, offering a perspective that is both data-driven and humanly resonant. The user can then interact with this narrative—edit it, annotate it, dispute its interpretations, or use it as a springboard for deeper personal reflection.

The **Living Document** itself can become a primary site for this narrative co-creation. As the user streams their thoughts, plans, and reflections, they are, in essence, authoring the first draft of their ongoing personal story. LLM agents can then act as "editors" or "dramaturgs," suggesting thematic connections, highlighting recurring motifs (e.g., "I notice you've logged friction related to 'API documentation clarity' across three different projects this month"), or even structuring disparate entries into more coherent narrative arcs. A query like `exo find --type meta.narrative_generated --tags "#ProjectX, #retrospective"` might retrieve several AI-generated (and user-refined) stories about different phases of a single project, allowing for a multi-faceted understanding of its journey.

The **tagging system (`core_tags`) and the knowledge graph (`core_entities`, `core_entity_relations`)** also contribute to a "poetics of data." Tags are not just organizational labels; they can be evocative keywords, personal symbols, or thematic markers. A user might tag events, notes, or even periods of time with `#serendipity`, `#struggle`, `#flow_state`, or `#epiphany`. Querying by these tags allows for the retrieval of affectively or thematically related moments across vast stretches of time, enabling the user to discern the unique "poetic structures" of their own cognitive and emotional life. Similarly, the relationships in the knowledge graph—`entity_A --inspired_by--> entity_B`, `friction_event_X --resolved_by--> insight_event_Y`—are the building blocks of causal and thematic narratives.

Even the **visualization of data** can take on a poetic dimension. A timeline view that intersperses coding activity with browser history, notes taken, music listened to (from `audio.stream.state_changed` if it logs song titles), and mood logs can paint a rich, multi-sensory picture of a specific day or project phase. A graph visualization showing the dense interconnections around a core "special interest" entity, with links radiating out to hundreds of notes, web archives, and related concepts, can be a powerful visual testament to the depth and passion of that engagement.

The Exocortex, in this light, moves beyond being a tool for mere information retrieval. It becomes a medium for **self-storytelling**. It acknowledges that meaning is not inherent in raw data but is constructed through acts of interpretation, connection-making, and narrative framing. By providing both the comprehensive raw material and increasingly sophisticated tools for weaving that material into personal stories, the system supports a deeper, more humanistic form_of self-understanding. It allows the user to see their life not just as a sequence of events, but as an ongoing, evolving narrative rich with themes, turning points, and the quiet poetry of daily thought and action. The "Sentient Archive" is sentient not because it thinks, but because it enables its user to think—and to feel, and to narrate—more deeply about the arc of their own becoming.
