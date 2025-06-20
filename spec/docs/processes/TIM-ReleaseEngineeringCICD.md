# TIM-ReleaseEngineeringCICD: Release Engineering and CI/CD

*   **Relevant ADR:** (N/A directly, core operational process)
*   **Original UG Context:** Section 25
*   **Vision Document Reference:** Part VI.6 (Phased Implementation)

This TIM details the Continuous Integration/Continuous Deployment (CI/CD) pipeline and release engineering practices for the Exocortex, emphasizing Nix Flakes for reproducible builds.

## 1. Rationale Summary

A robust CI/CD pipeline automates building, testing, and releasing Exocortex components, ensuring code quality, reproducibility, and faster iteration cycles. Nix Flakes are central to achieving reproducible builds and development environments.

## 2. Nix Flakes (`flake.nix`) for Reproducible Builds [UG Sec 25.1, OR3, SA4]

The Exocortex project is structured as a Nix Flake.

*   **`flake.nix` Structure:**
    *   **`inputs`:** Defines dependencies (pinned `nixpkgs`, `flake-utils`, `rust-overlay`, `crane` for Rust, etc.).
    *   **`outputs`:** Defines:
        *   **`packages`:** Nix derivations for each Exocortex binary (agents, ingestors, CLI).
            *   Rust packages built with `craneLib.buildPackage` or `pkgs.rustPlatform.buildRustPackage`.
        *   **`nixosModules`:** Modules for deploying Exocortex services.
        *   **`devShells.default`:** Development environment providing all tools (Rust toolchain, `sqlx-cli`, `psql`, linters, `act` for local GitHub Actions testing).
        *   **`checks`:** Derivations for CI validation:
            *   Build checks for all packages.
            *   Unit/integration test runners (e.g., `cargo test` invoked via a Nix derivation).
            *   NixOS VM integration tests (`pkgs.nixosTest`) for testing service deployments and interactions.
*   **`flake.lock`:** Pins exact versions of all inputs, ensuring reproducibility. Committed to Git.
*   **Cargo Hashes (`cargoVendorSha256` for `crane`, `cargoHash` for `buildRustPackage`):** Ensure Cargo dependencies are reproducible. Update when `Cargo.lock` or vendored dependencies change.

## 3. CI Platforms [UG Sec 25.2]

### 3.1. GitHub Actions (Primary CI) [SA4, `openai_sinex_6.md` Sec 13]

*   **Workflow YAML (`.github/workflows/ci.yml` - from UG Sec 25.2.1):**
    ```yaml
    name: Exocortex CI

    on:
      push:
        branches: [ "main", "develop" ]
      pull_request:
        branches: [ "main", "develop" ]

    jobs:
      build_and_test_linux: # Example job name
        runs-on: ubuntu-latest
        steps:
          - name: Checkout repository
            uses: actions/checkout@v4
            with:
              fetch-depth: 0 # For git describe, nix-flake-versioner

          - name: Install Nix
            uses: DeterminateSystems/nix-installer-action@main # Or cachix/install-nix-action

          - name: Configure Magic Nix Cache / Cachix
            # uses: DeterminateSystems/magic-nix-cache-action@main
            uses: cachix/cachix-action@v14
            with:
              name: your-exocortex-cachix-name # Your Cachix cache name
              authToken: '${{ secrets.CACHIX_AUTH_TOKEN }}' # For pushing to private cache
              # signingKey: '${{ secrets.CACHIX_SIGNING_KEY }}' # If pushing private artifacts

          - name: Check Flake, Build Packages, Run NixOS Tests
            run: |
              # This runs all checks defined in flake.nix outputs.checks.x86_64-linux
              # which should include package builds and NixOS VM tests.
              nix flake check .#checks.x86_64-linux.all --show-trace --verbose 
              # Or nix flake check --all-systems if you support more

          - name: Run Application-Level Unit/Integration Tests (e.g., cargo test)
            run: |
              # Assumes devShell provides necessary test tools
              nix develop .#default --command bash -c "source .envrc.ci-example && cargo test --all-features -- --nocapture"
              # .envrc.ci-example would set TEST_DATABASE_URL etc.
              # Or use GitHub Actions services for databases:
              # services:
              #   postgres:
              #     image: postgres:16
              #     env:
              #       POSTGRES_USER: test_user
              #       POSTGRES_PASSWORD: test_password
              #       POSTGRES_DB: test_db
              #     ports: ['5432:5432']
              #     options: >-
              #       --health-cmd pg_isready
              #       --health-interval 10s
              #       --health-timeout 5s
              #       --health-retries 5
              # env:
              #  TEST_DATABASE_URL: postgres://test_user:test_password@localhost:5432/test_db
    ```
*   **Secrets:** `CACHIX_AUTH_TOKEN`, `CACHIX_SIGNING_KEY`, API keys for tests, stored as GitHub secrets.

### 3.2. Hydra (Optional, Nix-Native CI) [OR3]

*   Nix-native CI server. Polls Git repo, evaluates flake, builds derivations on build farm.
*   Good for complex Nix-based projects and managing a binary cache.
*   Requires deploying and maintaining a Hydra instance.

## 4. CI Pipeline Steps [UG Sec 25.3, OR3, SA4]

1.  **Check:**
    *   `nix flake check` (validates flake, runs `checks` derivations).
    *   Linters (`rustfmt --check`, `clippy`, `shellcheck`, `ruff`, `nixpkgs-fmt --check`).
    *   Static Analysis (Semgrep, SonarQube if available).
2.  **Build:**
    *   `nix build .#packages.<system>.all` (or specific packages).
3.  **Test:**
    *   Unit tests (`cargo test`, `pytest`).
    *   Integration tests (DB tests with Testcontainers or GitHub Actions services, NixOS VM tests via `pkgs.nixosTest`).
    *   (Optional) End-to-end tests.
4.  **Publish (on main/tags):**
    *   Push Nix artifacts to Cachix.
    *   Build/push Docker images (if any) to registry (GHCR, Docker Hub).
    *   Create GitHub Releases (attach binaries, changelogs).
    *   (Optional) Deploy to staging/production NixOS hosts (`nixos-rebuild switch --flake ...`).

## 5. Artifact Management [UG Sec 25.4, OR3]

*   **Cachix:** Hosted Nix binary cache. Speeds up CI/dev builds.
*   **Docker Registries:** GHCR, Docker Hub, private.
*   **GitHub Releases:** For distributing tagged release binaries and source tarballs.

## 6. Security Scanning in CI [UG Sec 25.5, OR3]

Integrate into "Check" or a dedicated "Security" stage.

*   **Linters:** Already part of "Check".
*   **Static Analysis (SAST):** Semgrep, SonarQube.
*   **Dependency Vulnerability Scanning (CVE Checks):**
    *   Rust: `cargo audit`.
    *   Python: `pip-audit`, `safety`.
    *   Nix: `vulnix` (Determinate Systems) or custom scripts against `nixpkgs-security-tracker`.
    *   Docker Images: Trivy, Clair, Grype.
*   Fail CI build if critical/high vulnerabilities are found.

