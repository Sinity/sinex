# TIM-APIStabilityVersioning: API Stability and Versioning Requirements

*   **Relevant ADR:** (N/A directly, operational concern)
*   **Original UG Context:** Section 31

This TIM outlines key version requirements for Exocortex system dependencies and strategies for managing API instability and evolution.

## 1. Rationale Summary

The Exocortex relies on numerous external tools, libraries, and system APIs. Proactive management of their versions and stability is crucial for long-term maintainability and to prevent unexpected breakages.

## 2. Key Version Requirements for System Dependencies [UG Sec 31.1, CR2]

This is a non-exhaustive list of minimum or recommended versions for key external components. Always consult specific Exocortex component documentation (other TIMs) for their precise dependencies.

*   **Hyprland (IPC/Plugin API):** v0.33.1+ (IPC features/stability). Plugin API is unstable.
*   **Browser Extensions (Manifest V3):**
    *   Chrome/Chromium: Chrome 88+ (initial MV3). MV3 mandatory.
    *   Firefox: Firefox 101+ (MV3 support).
*   **Linux Kernel (eBPF, inotify, fanotify):**
    *   eBPF (advanced): Kernel 5.8+ (ring buffers, CO-RE).
    *   `fanotify` (admin privs): Pre-v5.1/v5.12 behavior differs.
    *   `inotify` (watch limits): Kernels 5.15+ may have higher defaults/better support.
*   **PostgreSQL:** Version 15+ recommended baseline for Exocortex. PG14+ for some replication features.
    *   Extensions (`pgvector`, `pgsodium`, `pg_jsonschema`, TimescaleDB, `pgx_ulid`, AGE) have their own PG version compatibility matrices. These must be respected.
*   **Git:** v2.25+ (modern sparse checkout). `git-annex` has its own Git version compat.
*   **macOS FSEvents:** File-level events reliable since macOS 10.7+.

## 3. Strategies for Managing API Instability and Evolution [UG Sec 31.2, SR1, SA1]

*   **Pinning Dependencies (Nix Flakes, Language Lock Files):**
    *   **Nix Flakes:** `flake.lock` pins Nixpkgs and other flake inputs. This is the primary mechanism for ensuring a reproducible Exocortex system environment and toolchain.
    *   **Language Package Managers:** `Cargo.lock` (Rust), `poetry.lock` / pinned `requirements.txt` (Python), `package-lock.json` (Node.js/TypeScript for extensions or UI). Commit these lock files.
    *   **Hyprland Plugins:** Build C++ plugin against a specific Hyprland source commit/tag.
*   **Abstraction Layers:** Wrap direct interactions with volatile external APIs (Hyprland IPC/plugin, AT-SPI2, browser extension APIs) in internal Exocortex abstraction layers/facades within agent code. Changes to external API are then localized to the adapter.
*   **Versioned Schemas & Internal APIs:**
    *   Event Payloads: Versioned schemas in `sinex_schemas.event_payload_schemas`.
    *   Internal Exocortex APIs (if any between agents beyond events): Use semantic versioning.
    *   Database Schema: Managed by versioned SQL migration scripts (see `TIM-PostgreSQLBackupDR_pgBackRest.md` for migration tool mention, though full DDL evolution strategy is more complex).
*   **Comprehensive Testing (CI/CD):**
    *   CI pipeline (see `TIM-ReleaseEngineeringCICD.md`) tests against specific, pinned versions of key dependencies (e.g., Hyprland version in NixOS VM test).
    *   Regularly update pinned dependencies in a controlled manner (e.g., dedicated PRs) and run full regression test suites.
*   **Fallback Mechanisms [SR1]:** For critical ingestors relying on potentially unstable external systems:
    *   Design fallback strategies (e.g., AT-SPI2 fails -> offer OCR option).
    *   Log errors from external APIs clearly (events `sinex.agent.error` with detailed context).
    *   Allow users to disable specific ingestor features if they prove unstable in their environment.
*   **Ongoing Maintenance & Monitoring [SR1, SA1]:**
    *   Monitor changelogs, mailing lists, and issue trackers for key external dependencies (Hyprland, browsers, kernel, PostgreSQL extensions).
    *   Allocate development time for proactive adaptation to announced breaking changes.
    *   Use meta-observability (Prometheus metrics, logs) to detect increased error rates or performance degradation in components interacting with external systems, which might indicate an API compatibility issue after an update.

This structured approach to dependency and API version management aims to minimize surprises and ensure the Exocortex can be maintained and evolved sustainably over time.

