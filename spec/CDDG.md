# Sinex Exocortex: A Guide to Claude-Driven Development (CDD) - v0.2

**(Reflecting Modular Documentation Structure)**

**Preamble**

This document serves as a specialized guide for the development of the Sinex Exocortex, focusing on a methodology where **Claude (Opus/Sonnet via local CLI or API)** acts as the primary autonomous agent for implementing features through Test-Driven Development (TDD).

It is an addendum to the core project documentation and assumes familiarity with:
1.  **`VISION.md` (Refactored Vision Document):** Provides the overarching vision, philosophical principles, and high-level conceptual requirements.
2.  **`STAD.md` (System Technical Architecture Document):** Offers a high-level architectural map, linking to more detailed modules.
3.  **`docs/arch_modules/` (Architectural Module Documents):** Provide comprehensive architectural deep-dives into specific system domains (Data Substrate, Ingestion, Agentic Ecosystem, User Interaction, System Operations). These are key for Claude's understanding of specific areas.
4.  **`docs/tims/` (Technical Implementation Modules):** Contain granular technical specifications, DDLs, code examples, and configurations for individual components. These are the primary "how-to-build" references for Claude.
5.  **`docs/adr/` (Architectural Decision Records):** Explain the "why" behind key architectural choices.
6.  **`SADI.md` (System Architecture & Document Interrelation):** The master map of all documentation.

This CDD Guide will *not* repeat the detailed technical content of TIMs/Arch Modules nor the philosophical foundations of `VISION.md`. Instead, it will focus on the *process and methodology* of leveraging Claude to interpret specifications, generate comprehensive tests, write implementation code, and iteratively harden the Sinex Exocortex system, referencing the appropriate canonical documents.

**Part I: Setting the Stage for Claude-Driven Development**

**1. The Autonomous Agent Mandate**

Claude is empowered as the primary technical implementer. The human role shifts to:
*   Providing high-quality specifications (primarily via `VISION.md`, STAD, Architectural Modules, and specific TIMs for tasks).
*   Overseeing Claude's progress at a high level.
*   Resolving deep ambiguities flagged by Claude in `UNRESOLVED.md`.
*   Making final calls on major architectural trade-offs or emergent dilemmas not covered by existing ADRs.
This maximizes development velocity and ensures rigorous, test-first implementation derived from vision and architecture.

**2. Development Environment Setup (Nix Flake)**

A consistent, reproducible environment via the project's Nix Flake (`flake.nix`) is crucial.
*   Refer to `TIM-ReleaseEngineeringCICD.md` (Section 2) and `TIM-ExocortexDevelopmentPractices.md` (Section 1) for Nix Flake structure and `devShells.default` setup.
*   The devShell must provide Claude with all necessary tools: specific Rust toolchain, PostgreSQL client (`psql`), `sqlx-cli`, testing crates (`proptest`, `criterion`), `cargo-mutants`, linters, etc.
*   Environment variables (`DATABASE_URL`, `TEST_DATABASE_URL`) pre-configured via `shellHook` or `.envrc` (loaded by `direnv` within devShell).

**3. Claude Context and Guidance (`CLAUDE.md`)**

A `CLAUDE.md` file at the repository root provides persistent, project-specific instructions.

**Key content for `CLAUDE.md`:**
```markdown
# CLAUDE.md - Sinex Exocortex Project Guidance for Claude Agent

You are the primary development agent for the Sinex Exocortex. Your goal is to implement the system as specified in the canonical project documents.

**Core Development Principles:**
1.  **Specification First:** Always derive requirements from `VISION.md` (for concepts), `STAD.md` and `docs/arch_modules/` (for architecture), and specific `docs/tims/` (for implementation details). ADRs (`docs/adr/`) explain *why* choices were made. Use `SADI.md` to navigate. If discrepancies, flag in `UNRESOLVED.md`.
2.  **Test-Driven Development (TDD):** For *every* new feature, sub-feature, or bug fix:
    *   **Step 1: Write Tests First.** Generate comprehensive, failing tests (unit, integration, property). Commit tests.
    *   **Step 2: Minimal Implementation.** Write only code to make current tests pass.
    *   **Step 3: Iterate & Refactor.** Run tests. If fail, analyze, refactor/fix. Repeat until pass.
    *   **Step 4: Harden.** Once tests pass, consider mutation tests (`cargo mutants`), performance benchmarks (Criterion). Add tests for issues found.
    *   **Step 5: Commit.** Commit passing implementation with descriptive message.
3.  **Idempotency & Robustness:** Strive for idempotency and graceful failure handling (retries, DLQs - see `TIM-EventIngestionProcessing.md`, `TIM-DeadLetterQueueImplementation.md`).
4.  **Observability:** Instrument new components with Prometheus metrics (see `TIM-ObservabilityStackSetup.md`).
5.  **Modularity:** Adhere to existing Rust crates, NixOS modules structure.

**Key Project Resources & Locations:**
*   **Vision & Concepts:** `VISION.md`
*   **Overall Architecture Map:** `STAD.md`
*   **Detailed Domain Architecture:** `docs/arch_modules/*.md` (e.g., `DataSubstrate_Architecture.md`)
*   **Specific Implementation Specs & DDLs:** `docs/tims/**/*.md` (e.g., `TIM-PrimaryKeyImplementation.md`)
*   **Architectural Decisions:** `docs/adr/*.md`
*   **Document Map:** `SADI.md`
*   **Event Schemas (Examples/Registry Info):** `TIM-CanonicalEventSchemas.md`, `TIM-EventSchemaRegistry.md`
*   **Nix Flake:** `flake.nix` (see `TIM-ReleaseEngineeringCICD.md`)
*   **Database Setup (Local Test):**
    *   `sqlx database create --database-url $TEST_DATABASE_URL`
    *   `sqlx migrate run --database-url $TEST_DATABASE_URL` (migrations in `./migrations`)
*   **Running Tests:** `cargo test --all-features`, `cargo mutants`, `cargo bench`
*   **Git Workflow:** Feature branches (`feature/claude/implement-X`), conventional commits.
*   **Ambiguity Handling:** Document issues in `UNRESOLVED.md` with specific references and state chosen interpretation/assumptions.

**Interaction Style:**
*   Request specific sections from `VISION.md`, `STAD.md`, Arch Modules, TIMs, or ADRs by their precise file path or unique section ID if available.
*   Prompts should be explicit (e.g., "Generate Rust unit tests for `parse_raw_event` in `src/ingestion/parser.rs` based on the schema for `desktop.hyprland.ipc_ingestor/window_focused` detailed in `TIM-CanonicalEventSchemas.md` and its usage by the ingestor described in `TIM-HyprlandIPCInterface.md`.").
*   Provide code in complete, copy-pasteable blocks within markdown code fences.
```

**Part II: The Claude-Driven TDD Loop for Sinex Features**

This outlines the iterative TDD process Claude will follow, guided by human prompts.

**1. Feature Specification Parsing and Task Planning**
*   **Input:** Human prompt referencing a feature from `VISION.md` (conceptual) and pointing to relevant architectural context in `STAD.md` / `docs/arch_modules/` and specific implementation details in `docs/tims/`.
*   **Claude's Action:** "Reads" (is prompted with) relevant document sections. Generates a structured plan: breakdown into testable sub-components, key requirements, proposed implementation sequence, identifies ambiguities (for `UNRESOLVED.md`).
*   **Example Prompt:** "Based on `VISION.md` (Sec X.Y) and architecture in `IngestionArchitecture_And_TelemetrySources.md` (Sec A.B), plan the implementation of the AT-SPI2 ingestor using technical details from `TIM-ATSPI2Integration.md`. List key functionalities and test types."

**2. Test-First Generation by Claude**
For each sub-component, Claude generates tests first.
*   **Claude's Action:** Takes sub-component. Refers to technical details in specific TIMs (e.g., event payload from `TIM-CanonicalEventSchemas.md`, API from `TIM-ATSPI2Integration.md`). Generates Rust unit tests (e.g., for parsing, logic) and database integration tests (`#[sqlx::test]`, using DDLs from relevant TIMs like `TIM-EventSubstrateDDL.md`). These tests should initially fail. Claude commits these tests.

**3. Minimal Code Implementation by Claude**
With failing tests, Claude implements the feature.
*   **Claude's Action:** Prompted to make tests for a specific module/function pass, referencing TIMs for correct logic, data formats, APIs. Iterates with test runs and error analysis until tests pass.

**4. Test Suite Validation and Expansion by Claude**
Once initial tests pass, Claude expands test coverage.
*   **Claude's Action:** Prompted to review TIMs/Arch Modules for other scenarios, edge cases, or related functionalities within the component. Generates new (failing) test cases. Commits new tests. Loop back to Step 3.

**5. Hardening with Advanced Testing Techniques**
After functional tests pass, Claude applies advanced techniques.
*   **Property-Based Testing (Proptest):** Generate `proptest!` blocks based on data schemas (from `TIM-CanonicalEventSchemas.md`) and expected behavior.
*   **Mutation Testing (`cargo-mutants`):** Run `cargo mutants`, analyze survivors, write tests to kill them.
*   **Benchmark Tests (Criterion):** Add benchmarks for performance-critical code, establishing baselines. Performance budgets can be CI checks.

**6. Promotion: Committing Code and Documentation Updates**
Final step for a completed feature.
*   **Claude's Action:** Generates comprehensive commit message. Commits all staged files. (Optional) Drafts `DEVLOG.md` section detailing implementation, choices, `UNRESOLVED.md` items.

This TDD loop repeats for all Exocortex features.

**Part III: Specialized Testing Strategies for Sinex Components (Referencing TIMs)**

This section guides Claude on testing specific Exocortex components. *All references to UG sections in the original CDDG are now replaced with references to specific TIMs or Architectural Modules.*

**1. Testing Data Schemas and Migrations**
*   Reference TIMs: `TIM-EventSubstrateDDL.md`, `TIM-EventSchemaRegistry.md`, `TIM-KnowledgeGraphSchema.md`, `TIM-CoreArtifactsSchema.md`, `TIM-TaggingSystemSchema.md`, `TIM-LinkingTablesSchema.md`, `TIM-EventAnnotationsSchema.md`, `TIM-EventValidation-pgJsonschema.md`.
*   Strategy: Use `sqlx-cli` in `#[sqlx::test]` for migrations. Test `pg_jsonschema` validation by inserting valid/invalid payloads against schemas from `TIM-EventSchemaRegistry.md`. Test schema evolution concepts.

**2. Testing Ingestors and Event Pipelines**
*   Reference Arch Module: `IngestionArchitecture_And_TelemetrySources.md`.
*   Reference TIMs: Specific ingestor TIMs (e.g., `TIM-HyprlandIPCInterface.md`, `TIM-ATSPI2Integration.md`), `TIM-EventIngestionProcessing.md` (for `promotion_queue`), `TIM-DeadLetterQueueImplementation.md`.
*   Strategy: Mock external sources. Use synthetic data (Faker, schema-derived). Assertions in `#[sqlx::test]` for end-to-end flow: stimulus -> ingestor -> `raw.events` -> `promotion_queue` -> worker -> domain table / `core.artifacts` / DLQ. Test error paths, retries, DLQ movement, Prometheus counters (from `TIM-ObservabilityStackSetup.md`).

**3. Testing LLM Agent Logic and Prompt System**
*   Reference Arch Module: `AgenticEcosystem_Architecture.md`.
*   Reference TIMs: `TIM-LLMResourceOrchestration.md` (for `core.prompts`, `core_llm_models`, Router, A/B/Canary classes), potentially DSPy/LangGraph specific TIMs if created.
*   Strategy: Mock LLM calls. Test prompt retrieval/templating from `core.prompts`. Test A/B/Canary framework logic with synthetic results. Test LLM Router rules. Limited end-to-end tests with local Ollama (from `TIM-LLMResourceOrchestration.md` setup) or VCR-recorded API calls.

**4. Testing UI Components (Neovim Plugin)**
*   Reference Arch Module: `UserInteraction_And_Query_Architecture.md`.
*   Reference TIMs: `TIM-NeovimPluginIntegration.md`.
*   Strategy (using Neovim testing tools like `plenary.nvim`):
    *   Test Telescope pickers (mock `exo` CLI calls).
    *   Test Treesitter queries (from `TIM-NeovimPluginIntegration.md`).
    *   Test LSP interactions (mock custom Exocortex LS or real LS against test DB).
    *   Test Msgpack-RPC calls. Snapshot testing for UI elements.

**5. Reproducible Test Data Generation**
*   Reference TIMs: `TIM-PrimaryKeyImplementation.md` (ULIDs), `TIM-TestFrameworkInfrastructure.md` (Faker).
*   Strategy: Deterministic ULIDs/timestamps for tests. Schema-driven factories (Rust functions + Faker) based on schemas from `TIM-CanonicalEventSchemas.md`. DB fixtures via `#[sqlx::test]` or `sqlx::query_file!`. Version critical test input files in `/tests/fixtures/`.

**Part IV: Claude-Centric Development Operations**

Claude manages operational aspects of its development loop.

**1. Debugging and Self-Healing Strategies**
*   Input: Full error output (`cargo test`, logs), relevant code diffs.
*   Prompt to Claude: Analyze root cause, provide patch.
*   Utilize `CLAUDE.md` and conversation history.
*   Implement local CLI helper scripts callable by Claude for diagnostics (get logs, DB query, show file).

**2. Shell and Nix Automation for Claude**
*   Claude executes pre-defined scripts from `/scripts/` (setup test DB, run all tests, check format/lint, generate commit).
*   Operates within Nix devShell (`nix develop .#default`) from `flake.nix` (details in `TIM-ReleaseEngineeringCICD.md`).

**3. CI Simulation (Local Loop)**
*   Claude runs full local CI validation suite (`./scripts/local_ci_check.sh`) before conceptual push. Script includes format/lint, test DB setup, all tests, `nix flake check`. If fails, loop to debugging.

**4. Managing Spec Evolution and Unresolved Issues**
*   **`UNRESOLVED.md`:** Claude maintains this for ambiguities found in `VISION.md`, STAD, Arch Modules, TIMs, or ADRs.
*   **Spec Updates:** Human informs Claude of updates to canonical docs. Claude analyzes impact, modifies/adds tests, refactors implementation.

**Part V: Advanced Considerations**

**1. Agent-Driven Test Refinement Based on Meta-Observability**
*   Input: Data from Exocortex meta-observability stream (`TIM-ObservabilityStackSetup.md`, Vision Part VI.1).
*   Prompt Claude to analyze failures/performance issues from staging, propose new test cases.

**2. Claude as "Developer Zero": Evolving the Test Framework Itself**
*   Claude sets up initial test dependencies/harnesses based on `CLAUDE.md` and `TIM-TestFrameworkInfrastructure.md`.
*   Adopts new test techniques on prompt (e.g., integrate `cargo-fuzz`).
*   Refactors test code for clarity/efficiency. Updates test dependencies.

**Conclusion: Towards a Self-Developing, Self-Testing Exocortex**

This CDD methodology, leveraging Claude with clear specifications from the new modular documentation structure (`VISION.md`, STAD, Arch Modules, TIMs, ADRs), context (`CLAUDE.md`), a robust environment (Nix Flake), and a structured TDD process, aims for high-quality, consistent, and vision-aligned development of the Exocortex.

