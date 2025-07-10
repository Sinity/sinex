# TIM-ExocortexDevelopmentPractices: NixOS Modules & Agent/Ingestor Development

*   **Purpose:** Provides guidelines and common patterns for developing NixOS modules for Exocortex services and for building new Exocortex agents/ingestors.
*   **Source:** Derived from original Vision Document Appendices H & I, and best practices from existing TIMs.
*   **Dependencies:** Familiarity with Nix language, Rust (primary agent language), Exocortex agent framework (`TIM-AgentManifestManagement.md`).

## 1. NixOS Module Design Patterns for Exocortex Services

Exocortex agents, ingestors, and backend services are deployed as NixOS modules.

### 1.1. Standard Module Structure

```nix
# Example: nixos/modules/services/exocortex/my-agent.nix
# { config, lib, pkgs, ... }:

# with lib;

# let
#   cfg = config.services.exocortex.myAgent; # Path to this module's options
#   agentPackage = pkgs.callPackage ../../../pkgs/exocortex/my-agent-pkg.nix {}; # Assuming package defined elsewhere
# in
# {
#   options.services.exocortex.myAgent = {
//     enable = mkEnableOption "Exocortex My Agent service";

//     package = mkOption {
//       type = types.package;
//       default = agentPackage;
//       description = "Package providing the My Agent binary.";
//     };

//     user = mkOption {
//       type = types.str;
//       default = "sinex-my-agent";
//       description = "User to run My Agent as.";
//     };
//     group = mkOption {
//       type = types.str;
//       default = "sinex-agents";
//       description = "Group for My Agent.";
//     };

//     configFile = mkOption {
//       type = types.path; # Or types.lines for inline content
//       default = null; # Or generate from other options
//       description = "Path to the agent's configuration file (e.g., TOML, YAML).";
//       example = "/etc/exocortex/my-agent-config.toml";
//     };
    
//     # Add other agent-specific options here (port, DB URL, etc.)
//     # These options will be used to generate the configFile or pass as CLI args/env vars.
//     settings = mkOption {
//         type = types.attrsOf types.anything; // Freeform attrs for config file generation
//         default = {};
//         example = { listen_port = 8080; log_level = "info"; };
//     };
//   };

//   config = mkIf cfg.enable {
//     users.users.${cfg.user} = {
//       isSystemUser = true;
//       group = cfg.group;
//       # home = "/var/lib/exocortex/${cfg.user}"; # If agent needs a home dir for state
//     };
//     users.groups.${cfg.group}.members = [ cfg.user ];

//     # Example: Generate config file from options
//     environment.etc."exocortex/my-agent-config.toml".source = pkgs.formats.toml.generate "my-agent-config.toml" cfg.settings;

//     systemd.services."sinex-my-agent" = {
//       description = "Sinnix Exocortex - My Agent Service";
//       wantedBy = [ "multi-user.target" ];
//       after = [ "network.target" "postgresql.service" ]; # Dependencies
//       requires = [ "postgresql.service" ];

//       serviceConfig = {
//         User = cfg.user;
//         Group = cfg.group;
//         ExecStart = "${cfg.package}/bin/my-agent-binary --config ${ # Or generated path:
//            (pkgs.formats.toml.generate "my-agent-config.toml" cfg.settings)
//         }";
//         # Or if configFile option is used:
//         # ExecStart = "${cfg.package}/bin/my-agent-binary --config ${cfg.configFile}";
        
//         Restart = "on-failure";
//         RestartSec = "10s";

//         # Resource Limits
//         MemoryMax = "512M";
//         CPUQuota = "50%";

//         # Sandboxing (see TIM-ProcessSandboxing.md)
//         NoNewPrivileges = true;
//         # SystemCallFilter = [ "@system-service" "~@privileged" ];
//         # AppArmorProfile = "sinex-my-agent-profile"; # If AppArmor profile defined

//         # Environment variables
//         # Environment = [ "RUST_LOG=info,my_agent=debug" ];
//         # EnvironmentFile = config.age.secrets.my_agent_env_vars.path; # For secrets via agenix
//       };
//     };
//   };
// }
```

### 1.2. Options Definition (`mkOption`, `types`)

*   Use `lib.mkEnableOption` for `enable` flags.
*   Use appropriate `lib.types` (str, int, path, package, attrsOf, listOf, submodule).
*   Provide `default`, `description`, `example` for options.

### 1.3. Package Definition (`pkgs/exocortex/my-agent-pkg.nix`)

*   Define how to build the agent binary (e.g., using `rustPlatform.buildRustPackage` or `craneLib.buildPackage` for Rust).
    ```nix
    # pkgs/exocortex/my-agent-pkg.nix
    # { lib, rustPlatform, fetchFromGitHub, ... }: # Or crane
    # rustPlatform.buildRustPackage rec {
    //   pname = "sinex-my-agent";
    //   version = "0.1.0"; # Get from Cargo.toml or flake input

    //   src = fetchFromGitHub { # Or local path: ../../src/my_agent
    //     owner = "your_github_user";
    //     repo = "sinex-exocortex-my-agent-repo";
    //     rev = "v${version}";
    //     hash = "sha256-...";
    //   };

    //   cargoSha256 = "sha256-..."; # Or cargoLock.lockFile for new buildRustPackage

    //   # nativeBuildInputs = [ pkgs.some_build_tool ];
    //   # buildInputs = [ pkgs.openssl pkgs.libpq ]; # Runtime deps if dynamically linked

    //   meta = with lib; {
    //     description = "My Exocortex Agent";
    //     homepage = "https://github.com/your_github_user/sinex-exocortex-my-agent-repo";
    //     license = licenses.mit; # Or your chosen license
    //     maintainers = [ maintainers.your_github_handle ];
    //     platforms = platforms.linux;
    //   };
    // }
    ```

### 1.4. Configuration File Generation

*   Use NixOS `pkgs.formats.<format>.generate` (e.g., `pkgs.formats.toml.generate`) to create config files from module options (e.g., `cfg.settings` attribute set).
*   Place generated configs in `/etc/exocortex/` or `/var/lib/exocortex/<agent_name>/`.
*   Secrets passed via environment variables (using `EnvironmentFile` pointing to `agenix` decrypted file) or directly in config files if `agenix` decrypts the whole config file.

## 2. Agent/Ingestor Development Guidelines

General guidelines for building robust and maintainable Exocortex agents/ingestors (primarily Rust).

### 2.1. Initialization and Configuration

*   Read configuration from file specified by CLI arg or env var. Parse with `serde` (for TOML, YAML, JSON).
*   Connect to PostgreSQL using `sqlx` with connection pooling (`PgPoolOptions`).
*   Perform self-registration/update in `sinex_schemas.agent_manifests` on startup.
*   Set up logging (e.g., `tracing` crate, configured for JSON output to `stdout` for journald).
*   Set up Prometheus metrics endpoint if applicable (`TIM-ObservabilityStackSetup.md`).

### 2.2. Main Loop and Event Processing

*   **Ingestors:** Monitor source (filesystem, socket, API). On new data, format into `raw.events` structure, assign `payload_schema_id` (looked up from `sinex_schemas.event_payload_schemas` or cached), generate `id` ULID, set `ts_orig`, and insert into `raw.events` (batch inserts preferred). Handle local file-based DLQ on DB write failure.
*   **Processing Agents (e.g., for `work_queue`):** Implement polling loop as in `TIM-EventIngestionProcessing.md`.
    *   Fetch batch of items (`SELECT ... FOR UPDATE SKIP LOCKED`).
    *   For each item:
        *   Fetch corresponding `raw.events.payload`.
        *   Validate payload against schema (optional, can assume valid if DB constraint used).
        *   Perform processing logic (transform, enrich, link, call LLM).
        *   Write results to domain tables or create new `raw.events`.
        *   On success, delete item from queue.
        *   On failure, update item for retry (exponential backoff) or move to `core.dead_letter_queue`.
*   Implement graceful shutdown (Ctrl+C / SIGTERM handler) to finish processing in-flight items, update agent status, close DB connections.

### 2.3. Error Handling and Retries

*   Use Rust `Result` type extensively. `anyhow` crate for easy error context.
*   Implement retry logic with exponential backoff and jitter for transient errors (network, DB deadlocks).
*   Log errors to `stderr` (for journald) and as `sinex.agent.error` events for critical/persistent issues.

### 2.4. Logging and Observability

*   Use `tracing` crate with `tracing-subscriber` for structured JSON logging to `stdout`. Journald ingests this. Include `agent_name`, `version`, `correlation_id` (if applicable to a specific workflow batch) in log fields.
*   Expose Prometheus metrics for key operations (items processed, errors, latencies).

### 2.5. Idempotency

Ensure operations are idempotent where possible, especially for ingestors re-processing data or promotion agents that might re-run on same raw event due to retries. Use `ON CONFLICT DO NOTHING` or `DO UPDATE` for DB writes if feasible.

### 2.6. Testing

*   Unit tests for individual functions/modules.
*   Integration tests using Testcontainers (`testcontainers-rs`) for PostgreSQL.
*   Write NixOS VM tests (`pkgs.nixosTest`) to test agent deployment, configuration, and interaction with other services in a realistic environment.

