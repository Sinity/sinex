# source-bindings-generated.nix
#
# Generated-from-catalog NixOS module for sinex source-worker services.
#
# This module imports docs/source-units.json and:
#   1. Defines `services.sinex.generatedBindings.<id>` options per source unit
#      (enable, adapterType, adapterConfig, instances, gated, serviceConfigOverrides)
#   2. Validates that every required field from the adapter's JSON Schema is
#      present in adapterConfig when the binding is enabled — evaluated at
#      config time so `nix flake check` catches missing fields before deploy.
#   3. Emits `sinex-source-worker-<id>-<idx>.service` systemd units for each
#      enabled, non-gated binding.
#
# Hand-maintained support glue (ACL scripts, env bridges) stays in
# source-workers.nix, which passes domain-specific serviceConfigOverrides
# into each binding via `services.sinex.generatedBindings.<id>.serviceConfigOverrides`.
#
# Zero-Nix-change guarantee: once a source unit appears in the catalog with an
# adapterType set, adding a Nix binding is just:
#   services.sinex.generatedBindings."new.unit" = {
#     enable = true;
#     adapterType = "AppendOnlyFileAdapter";
#     adapterConfig = { path = "/some/path"; };
#   };
# No new per-domain mk*Units function needed.

{ config, lib, pkgs, ... }:

with lib;

let
  # ── Catalog import ──────────────────────────────────────────────────────
  catalogPath = ../../docs/source-units.json;
  catalog = builtins.fromJSON (builtins.readFile catalogPath);

  # Index adapters by name for O(1) required-field lookup.
  adapterSchemas = catalog.adapters or { };

  # Source units eligible for generated binding: runner_pack == "source-worker".
  catalogSourceWorkerUnits =
    builtins.filter
      (u: (u.runner_pack or "") == "source-worker")
      (catalog.source_units or [ ]);

  # ── Shared infra from source-workers.nix ───────────────────────────────
  cfg = config.services.sinex;
  coreCfg = cfg.core;
  nodesCfg = cfg.nodes;

  sinexEnabled = cfg.enable;
  nodesEnabled = sinexEnabled && nodesCfg.enable;
  coreEnabled = sinexEnabled && coreCfg.enable;

  natsEnabled = cfg.nats.enable || cfg.nats.autoSetup;
  natsBootstrapEnabled = natsEnabled && cfg.nats.bootstrapStreams.enable;
  schemaApplyEnabled = cfg.database.enable && cfg.database.autoSetup;
  localPostgresEnabled = cfg.database.enable && (cfg.database.autoSetup || config.services.postgresql.enable);

  schemaApplyUnits = optionals schemaApplyEnabled [ "sinex-schema-apply.service" ];
  postgresServiceUnits = optionals localPostgresEnabled [ "postgresql.service" "postgresql-setup.service" ];

  stateRoot = cfg.stateRoot;
  runtimeDir = "${stateRoot}/run";
  logDir = cfg.observability.logDir;
  ingestSpool = coreCfg.ingestd.spoolDir;
  blobDir = cfg.storage.blob.repositoryPath;

  sinexPackage = cfg.package;
  serviceUser = cfg.users.nodes;

  databaseRuntime = import ./lib/database-runtime.nix { inherit lib pkgs; };
  secretResolution = import ./lib/secret-resolution.nix { inherit lib; };
  systemdHardening = import ./lib/systemd-hardening.nix { inherit lib; };
  inherit (databaseRuntime) mkDatabasePasswordExec renderDatabaseUrl;
  inherit (secretResolution) resolveNamedSecretPath;

  databaseUrl = renderDatabaseUrl cfg.database;
  natsUrl = concatStringsSep "," nodesCfg.nats.servers;

  secretPaths = config.sinex.secrets.paths or { };
  resolveSecretPath = resolveNamedSecretPath secretPaths;
  effectiveDatabasePasswordFile = resolveSecretPath cfg.database.passwordFile [
    "sinex-local-db"
    "sinex-remote-db"
  ];
  natsTlsCfg = nodesCfg.nats.tls;
  natsAuthCfg = nodesCfg.nats.auth;
  effectiveNatsCaCertFile = resolveSecretPath natsTlsCfg.caCertFile [
    "sinex-nats-ca" "nats-ca" "sinex-remote-nats-ca"
  ];
  effectiveNatsClientCertFile = resolveSecretPath natsTlsCfg.clientCertFile [
    "sinex-nats-client-cert" "nats-client-cert" "sinex-remote-nats-cert"
  ];
  effectiveNatsClientKeyFile = resolveSecretPath natsTlsCfg.clientKeyFile [
    "sinex-nats-client-key" "nats-client-key" "sinex-remote-nats-key"
  ];
  effectiveNatsTokenFile = resolveSecretPath natsAuthCfg.tokenFile [
    "sinex-nats-token" "nats-token"
  ];
  effectiveNatsCredsFile = resolveSecretPath natsAuthCfg.credsFile [
    "sinex-nats-client-creds" "nats-client-creds"
  ];
  effectiveNatsNkeySeedFile = resolveSecretPath natsAuthCfg.nkeySeedFile [
    "sinex-nats-client-nkey" "nats-client-nkey"
  ];
  inferredNatsTls =
    natsTlsCfg.requireTls
    || any (server: hasPrefix "tls://" server || hasPrefix "wss://" server) nodesCfg.nats.servers;

  toEnvList = envAttrs: mapAttrsToList (name: value: "${name}=${value}") envAttrs;
  collectReadablePaths = paths: filter (path: path != null) paths;

  databaseSecretAssertPaths = collectReadablePaths [
    (if cfg.database.enable then effectiveDatabasePasswordFile else null)
  ];
  natsSecretAssertPaths = collectReadablePaths [
    effectiveNatsCaCertFile effectiveNatsClientCertFile effectiveNatsClientKeyFile
    effectiveNatsTokenFile effectiveNatsCredsFile effectiveNatsNkeySeedFile
  ];

  existingPathAssertions = paths:
    let existingPaths = collectReadablePaths paths;
    in optionalAttrs (existingPaths != [ ]) { AssertPathExists = existingPaths; };

  readWritePaths = [ stateRoot runtimeDir ingestSpool logDir blobDir ];

  restartRateLimits = {
    StartLimitIntervalSec = cfg.runtime.restartPolicy.intervalSec;
    StartLimitBurst = cfg.runtime.restartPolicy.burst;
  };

  baseEnv = optional cfg.database.enable "DATABASE_URL=${databaseUrl}" ++ [
    "SINEX_ENVIRONMENT=${cfg.nats.environment}"
    "SINEX_STATE_DIR=${stateRoot}"
    "SINEX_RUNTIME_DIR=${runtimeDir}"
    "SINEX_LOG_DIR=${logDir}"
    "SINEX_NATS_URL=${natsUrl}"
    "SINEX_NATS_MONITORING_PORT=${toString nodesCfg.nats.monitoringPort}"
    "SINEX_CONTENT_STORE_PATH=${blobDir}"
  ]
    ++ optional inferredNatsTls "SINEX_NATS_REQUIRE_TLS=1"
    ++ optional (effectiveNatsCaCertFile != null) "SINEX_NATS_CA_CERT=${toString effectiveNatsCaCertFile}"
    ++ optional (effectiveNatsClientCertFile != null) "SINEX_NATS_CLIENT_CERT=${toString effectiveNatsClientCertFile}"
    ++ optional (effectiveNatsClientKeyFile != null) "SINEX_NATS_CLIENT_KEY=${toString effectiveNatsClientKeyFile}"
    ++ optional (effectiveNatsTokenFile != null) "SINEX_NATS_TOKEN_FILE=${toString effectiveNatsTokenFile}"
    ++ optional (effectiveNatsCredsFile != null) "SINEX_NATS_CREDS_FILE=${toString effectiveNatsCredsFile}"
    ++ optional (effectiveNatsNkeySeedFile != null) "SINEX_NATS_NKEY_SEED_FILE=${toString effectiveNatsNkeySeedFile}"
    ++ toEnvList nodesCfg.defaults.env;

  coordinationEnv =
    if nodesCfg.coordination.enable then [
      "SINEX_COORDINATION_ENABLED=1"
      "SINEX_COORDINATION_HEARTBEAT=${toString nodesCfg.coordination.heartbeatSec}"
      "SINEX_COORDINATION_TIMEOUT=${toString nodesCfg.coordination.leadershipTimeoutSec}"
      "SINEX_COORDINATION_HANDOFF=${toString nodesCfg.coordination.handoffTimeoutSec}"
    ] else [ ];

  mkServiceEnv = additionalEnv: baseEnv ++ coordinationEnv ++ additionalEnv;

  resolveResources = nodeResources:
    if nodeResources == null then nodesCfg.defaults.resources else nodeResources;

  renderResources = resources: {
    MemoryHigh = resources.memoryHigh;
    CPUWeight = resources.cpuWeight;
    IOWeight = resources.ioWeight;
    IOSchedulingClass = resources.ioSchedulingClass;
    Nice = resources.nice;
    TimeoutStopSec = resources.shutdownTimeoutSec;
  } // optionalAttrs (resources.memoryMax != null) {
    MemoryMax = resources.memoryMax;
  } // optionalAttrs (resources.cpuQuota != null) {
    CPUQuota = resources.cpuQuota;
  } // optionalAttrs (resources.openFilesLimit != null) {
    LimitNOFILE = "${toString resources.openFilesLimit}:${toString resources.openFilesLimit}";
  };

  mkBaseServiceConfig = resources: env: extra:
    {
      Type = "notify";
      User = serviceUser;
      Group = serviceUser;
      Restart = cfg.runtime.restartPolicy.mode;
      RestartSec = cfg.runtime.restartPolicy.backoffSec;
      WatchdogSec = "60s";
      Environment = env;
      ProtectSystem = "strict";
      ProtectHome = true;
      PrivateTmp = true;
      PrivateIPC = true;
      NoNewPrivileges = true;
      RestrictSUIDSGID = true;
      ProtectKernelTunables = true;
      ProtectKernelModules = true;
      ProtectKernelLogs = true;
      ProtectClock = true;
      ProtectControlGroups = true;
      RestrictRealtime = true;
      LockPersonality = true;
      MemoryDenyWriteExecute = true;
      RestrictNamespaces = true;
      SystemCallArchitectures = "native";
      RestrictAddressFamilies = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
      SystemCallFilter = "@system-service";
      SystemCallErrorNumber = "EPERM";
      ReadWritePaths = readWritePaths;
    }
    // renderResources resources
    // extra;

  # ── Required-field assertion helper ────────────────────────────────────
  # For a given adapter type and binding id, assert that all required fields
  # from the JSON Schema are present in the user-supplied adapterConfig.
  # Nix evaluates assertions at config time → `nix flake check` fails if
  # any required field is absent.
  checkRequiredFields = unitId: adapterType: adapterConfig:
    let
      adapterSchema = adapterSchemas.${adapterType} or null;
      required =
        if adapterSchema == null then [ ]
        else adapterSchema.required or [ ];
      missing = filter (field: !(adapterConfig ? ${field})) required;
    in
    assert (
      if missing != [ ] then
        builtins.trace
          "sinex source binding '${unitId}' (adapterType=${adapterType}): missing required fields: ${builtins.toJSON missing}"
          false
      else true
    );
    true;

  # ── Per-unit service generation (post-collapse: removed) ───────────────
  # Pre-collapse this module emitted `sinex-source-worker-<id>-<idx>.service`
  # systemd units. After PR-XXX the supervisor inside sinexd hosts every
  # binding in-process, so unit emission moved into source-workers.nix as
  # the SINEX_SOURCE_BINDINGS_PATH manifest builder. The helper below is
  # retained as a no-op placeholder so future bindings work with the same
  # option-set even though no unit is generated; it is intentionally never
  # invoked from this module's `config` block.
  mkGeneratedUnit = unitId: binding: catalogUnit:
    let
      instances = binding.instances;
      resources = resolveResources binding.resources;
      nodeConfig =
        if binding.adapterConfig != { } then builtins.toJSON (
          {
            # AdapterBackedIngestor reads this before opening the source adapter,
            # so restarted source workers observe persisted private mode before
            # resuming capture.
            private_mode_state_dir = stateRoot;
          }
          // binding.adapterConfig
        )
        else "";
      env = mkServiceEnv (
        [ "RUST_LOG=${nodesCfg.defaults.logLevel}" ]
        ++ toEnvList binding.extraEnv
      );
      execArgParts =
        [ "--source-unit ${escapeShellArg unitId}" ]
        ++ optionals (nodeConfig != "") [ "--node-config ${escapeShellArg nodeConfig}" ]
        ++ binding.extraArgs
        ++ [ "service" ];
      execArgs = concatStringsSep " " execArgParts;
      afterUnits =
        schemaApplyUnits
        ++ optionals coreEnabled [ "sinexd.service" ]
        ++ binding.afterUnits;
      requireUnits = schemaApplyUnits ++ binding.requiresUnits;
      wantsUnits =
        optionals coreEnabled [ "sinexd.service" ]
        ++ binding.wantsUnits;
      isMonitor = (catalogUnit.service_policy or "") == "invoked_on_demand";
      # Base service config; domain glue (ACL scripts etc.) is merged in via
      # binding.serviceConfigOverrides.
      baseServiceConfig = mkBaseServiceConfig resources env (
        {
          ExecStart = mkDatabasePasswordExec {
            name = "source-worker-${unitId}";
            command = "${sinexPackage}/bin/sinexd scan-source-unit ${execArgs}";
            passwordFile = if cfg.database.enable then effectiveDatabasePasswordFile else null;
          };
          WorkingDirectory = stateRoot;
        }
        // optionalAttrs isMonitor {
          Type = mkForce "oneshot";
          RemainAfterExit = mkForce true;
          Restart = mkForce "no";
          WatchdogSec = mkForce "0";
          NotifyAccess = mkForce "none";
        }
        // binding.serviceConfigOverrides
      );
      mkUnit = idx: {
        description = "${binding.description} (instance ${toString idx})";
        wantedBy = lib.optional cfg.runtime.target.attachToMultiUser "multi-user.target";
        restartIfChanged = cfg.runtime.restartOnSwitch;
        after = afterUnits;
        requires = requireUnits;
        wants = wantsUnits;
        unitConfig =
          restartRateLimits
          // { PartOf = [ "sinex-runtime.target" ]; }
          // existingPathAssertions (databaseSecretAssertPaths ++ natsSecretAssertPaths);
        path = binding.unitPath;
        serviceConfig = baseServiceConfig;
      };
    in
    listToAttrs (
      map
        (idx: nameValuePair "sinex-source-worker-${unitId}-${toString idx}" (mkUnit idx))
        (range 1 instances)
    );

  # Build the catalog unit lookup index (id → unit attrs).
  catalogUnitIndex =
    listToAttrs (
      map (u: nameValuePair u.id u) catalogSourceWorkerUnits
    );

  # Bindings configuration from the module option.
  generatedBindingsCfg = config.services.sinex.generatedBindings;

  # Enabled, non-gated bindings that also appear in the catalog.
  activeBindings =
    filterAttrs
      (id: binding:
        binding.enable
        && !binding.gated
        && catalogUnitIndex ? ${id}
      )
      generatedBindingsCfg;

  # Assert required fields for every active binding that has an adapterType.
  # Returns true (for use in assertions) only; side-effect is trace on failure.
  _assertionsCheck =
    mapAttrsToList
      (id: binding:
        if binding.adapterType != null
        then checkRequiredFields id binding.adapterType binding.adapterConfig
        else true
      )
      activeBindings;

  # Generate all systemd units for active bindings.
  generatedServiceUnits =
    foldlAttrs
      (acc: id: binding:
        let
          catalogUnit = catalogUnitIndex.${id};
        in
        acc // mkGeneratedUnit id binding catalogUnit
      )
      { }
      activeBindings;

  # ── Per-unit binding submodule ──────────────────────────────────────────
  bindingSubmodule = types.submodule ({ name, ... }: {
    options = {
      enable = mkEnableOption "this generated source binding" // { default = false; };

      gated = mkOption {
        type = types.bool;
        default = false;
        description = ''
          When true, this source unit is gated (infrastructure not yet wired).
          The service will not be emitted even if enable = true.
          Use this for source units like desktop.window-manager and system.dbus
          that need additional wiring before deployment.
        '';
      };

      adapterType = mkOption {
        type = types.nullOr types.str;
        default = null;
        example = "AppendOnlyFileAdapter";
        description = ''
          Adapter type name as it appears in docs/source-units.json `adapters` block.
          When set, required fields from the adapter's JSON Schema are validated
          at nix flake check time.  Null disables schema validation (for units
          with no adapter or embedded adapters).
        '';
      };

      adapterConfig = mkOption {
        type = types.attrsOf types.anything;
        default = { };
        description = ''
          Adapter config fields passed as --node-config JSON to sinex-source-worker.
          Must contain all fields listed as `required` in the adapter JSON Schema
          (from docs/source-units.json adapters block).  Missing required fields
          cause nix flake check to fail.
        '';
        example = { path = "/home/user/.local/share/atuin/history.db"; query = "history"; };
      };

      description = mkOption {
        type = types.str;
        default = "Sinex source worker ${name}";
        description = "Human-readable description used in the systemd unit Description= field.";
      };

      instances = mkOption {
        type = types.ints.positive;
        default = 1;
        description = "Number of parallel source-worker instances to emit.";
      };

      resources = mkOption {
        type = types.nullOr types.attrs;
        default = null;
        description = "Resource limits. Null inherits from services.sinex.nodes.defaults.resources.";
      };

      extraArgs = mkOption {
        type = types.listOf types.str;
        default = [ ];
        description = "Extra CLI arguments appended before `service` in the ExecStart command.";
      };

      extraEnv = mkOption {
        type = types.attrsOf types.str;
        default = { };
        description = "Additional environment variables set in the service unit.";
      };

      afterUnits = mkOption {
        type = types.listOf types.str;
        default = [ ];
        description = "Extra After= units appended to the standard dependency chain.";
      };

      requiresUnits = mkOption {
        type = types.listOf types.str;
        default = [ ];
        description = "Extra Requires= units.";
      };

      wantsUnits = mkOption {
        type = types.listOf types.str;
        default = [ ];
        description = "Extra Wants= units.";
      };

      unitPath = mkOption {
        type = types.listOf types.package;
        default = [ ];
        description = "Packages added to PATH in the service unit (e.g. pkgs.hyprland).";
      };

      serviceConfigOverrides = mkOption {
        type = types.attrs;
        default = { };
        description = ''
          Systemd service config overrides merged on top of the generated base config.
          Used by source-workers.nix support glue to inject domain-specific settings:
          ProtectHome, ReadWritePaths, ExecStartPre (ACL scripts), EnvironmentFile, etc.
        '';
      };
    };
  });

in
{
  options.services.sinex.generatedBindings = mkOption {
    type = types.attrsOf bindingSubmodule;
    default = { };
    description = ''
      Declarative source-unit bindings driven by the docs/source-units.json catalog.

      Each attribute key is a source unit id (e.g. "terminal.atuin-history",
      "browser.history").  The generated module emits one
      `sinex-source-worker-<id>-<idx>.service` per instance for each enabled,
      non-gated binding.

      Required adapter config fields (from JSON Schema in the catalog) are
      asserted at nix flake check time.  Missing fields cause evaluation to fail
      before any deployment attempt.

      Adding a new source unit that appears in the catalog requires no changes to
      this module — set enable = true and supply adapterConfig.
    '';
    example = {
      "terminal.atuin-history" = {
        enable = true;
        adapterType = "SqliteRowAdapter";
        adapterConfig = {
          path = "/home/user/.local/share/atuin/history.db";
          query = "history";
          table = "history";
        };
      };
      "system.journald" = {
        enable = true;
        adapterType = "JournalctlStreamAdapter";
        adapterConfig = { };
      };
    };
  };

  config = mkIf (sinexEnabled && nodesEnabled) {
    # Fail at evaluation time if any required adapter field is missing.
    # _assertionsCheck is evaluated for its side-effect (builtins.trace + false assertion).
    #
    # Post-collapse: this module no longer emits per-binding systemd units.
    # sinexd hosts every binding in-process and reads the assembled manifest
    # from `SINEX_SOURCE_BINDINGS_PATH` (written by source-workers.nix). We
    # keep the schema validation here so a malformed binding still fails at
    # `nix flake check` time rather than only when sinexd starts.
    assertions = [
      {
        assertion = builtins.all (x: x) _assertionsCheck;
        message = "One or more sinex generatedBindings are missing required adapter config fields. See trace output above for details.";
      }
    ];
  };
}
