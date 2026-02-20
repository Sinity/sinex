{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  coreCfg = cfg.core;
  nodesCfg = cfg.satellites;

  sinexEnabled = cfg.enable;
  coreEnabled = sinexEnabled && coreCfg.enable;
  nodesEnabled = sinexEnabled && nodesCfg.enable;
  natsEnabled = cfg.nats.enable || cfg.nats.autoSetup;

  stateRoot = cfg.stateRoot;
  runtimeDir = "${stateRoot}/run";
  ingestSpool = coreCfg.ingestd.spoolDir;
  logDir = cfg.observability.logDir;
  dlqPath = cfg.storage.dlq.path;
  blobDir = cfg.storage.blob.repositoryPath;
  tlsDir = "${stateRoot}/tls";
  tlsAutoGenEnabled = coreEnabled && coreCfg.gateway.autoGenerateTls;

  genTlsScript = pkgs.writeShellScript "sinex-tls-init" ''
    set -euo pipefail
    cert="${tlsDir}/gateway.crt"
    key="${tlsDir}/gateway.key"
    if [[ -f "$cert" && -f "$key" ]]; then
      echo "Sinex gateway TLS credentials already present, skipping."
      exit 0
    fi
    mkdir -p "${tlsDir}"
    chmod 750 "${tlsDir}"
    ${pkgs.openssl}/bin/openssl req -x509 -newkey ed25519 \
      -keyout "$key" -out "$cert" \
      -days 3650 -nodes \
      -subj "/CN=sinex-gateway/O=sinex" \
      -addext "subjectAltName=IP:127.0.0.1,DNS:localhost"
    chmod 640 "$key" "$cert"
    echo "Sinex gateway TLS credentials generated at ${tlsDir}"
  '';

  sinexPackage = cfg.package;
  serviceUser = cfg.users.satellites;

  databaseUrl = "postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}";

  natsUrl = concatStringsSep "," nodesCfg.nats.servers;
  secretPaths = config.sinex.secrets.paths or {};
  gatewayAdminTokenFile =
    if cfg.secrets.gatewayAdminTokenFile != null then cfg.secrets.gatewayAdminTokenFile
    else if secretPaths ? sinex-gateway-admin-token then secretPaths.sinex-gateway-admin-token
    else null;

  toEnvList = envAttrs: mapAttrsToList (name: value: "${name}=${value}") envAttrs;

  baseEnv = [
    "DATABASE_URL=${databaseUrl}"
    # Propagate environment name so service subjects match bootstrapped stream prefixes.
    # Must stay in sync with services.sinex.nats.environment.
    "SINEX_ENVIRONMENT=${cfg.nats.environment}"
    "SINEX_STATE_DIR=${stateRoot}"
    "SINEX_RUNTIME_DIR=${runtimeDir}"
    "SINEX_LOG_DIR=${logDir}"
    "SINEX_SPOOL_INGESTD=${ingestSpool}"
    "SINEX_DLQ_PATH=${dlqPath}"
    "SINEX_NATS_URL=${natsUrl}"
    "SINEX_NATS_MONITORING_PORT=${toString nodesCfg.nats.monitoringPort}"
    # Both ingestd and gateway access the same git-annex blob repository; set here
    # so all core services share a consistent path without per-service repetition.
    "SINEX_ANNEX_PATH=${blobDir}"
  ] ++ toEnvList nodesCfg.defaults.env;

  coordinationEnv =
    if nodesCfg.coordination.enable then [
      "SINEX_COORDINATION_ENABLED=1"
      "SINEX_COORDINATION_HEARTBEAT=${toString nodesCfg.coordination.heartbeatSec}"
      "SINEX_COORDINATION_TIMEOUT=${toString nodesCfg.coordination.leadershipTimeoutSec}"
      "SINEX_COORDINATION_HANDOFF=${toString nodesCfg.coordination.handoffTimeoutSec}"
    ] else [];

  resolveBatch = nodeBatch:
    if nodeBatch == null then nodesCfg.defaults.batch else nodeBatch;

  resolveResources = nodeResources:
    if nodeResources == null then nodesCfg.defaults.resources else nodeResources;

  resolveInstances = nodeInstances:
    if nodeInstances == null then nodesCfg.defaults.instances else nodeInstances;

  renderResources = resources: {
    MemoryMax = resources.memoryMax;
    CPUQuota = resources.cpuQuota;
    TimeoutStopSec = resources.shutdownTimeoutSec;
  };

  readWritePaths = [
    stateRoot
    runtimeDir
    ingestSpool
    logDir
    dlqPath
    blobDir
  ];

  mkServiceEnv = additionalEnv: baseEnv ++ coordinationEnv ++ additionalEnv;

  mkBaseServiceConfig = resources: env: extra:
    {
      Type = "notify";
      User = serviceUser;
      Group = serviceUser;
      Restart = "on-failure";
      RestartSec = 10;
      Environment = env;
      ProtectSystem = "strict";
      ProtectHome = true;
      PrivateTmp = true;
      NoNewPrivileges = true;
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

  mkCoreServices =
    let
      batch = coreCfg.ingestd.batch;
      ingestArgs = concatStringsSep " " ([
        "--nats-url ${natsUrl}"
        "--consumer-fetch-max-messages ${toString batch.size}"
        # CLI arg expects milliseconds; NixOS option stores seconds for human readability.
        "--consumer-fetch-timeout-ms ${toString (batch.timeoutSec * 1000)}"
        "--log-level ${coreCfg.ingestd.logLevel}"
      ] ++ coreCfg.ingestd.extraArgs);
      gatewayArgs = concatStringsSep " " ([
        "rpc-server"
        "--database-url ${databaseUrl}"
        "--tcp-listen ${coreCfg.gateway.listenAddress}"
        # TLS is mandatory for gateway RPC; cert/key must be provided via env vars.
      ] ++ coreCfg.gateway.extraArgs);
      gatewayLimits = coreCfg.gateway.limits;
      gatewayEnv = mkServiceEnv (
        [
          "RUST_LOG=${coreCfg.gateway.logLevel}"
          "SINEX_GATEWAY_MAX_CONCURRENCY=${toString gatewayLimits.maxConcurrency}"
          "SINEX_GATEWAY_REQUEST_TIMEOUT_SECS=${toString gatewayLimits.requestTimeoutSec}"
          "SINEX_GATEWAY_MAX_BODY_BYTES=${toString gatewayLimits.maxBodyBytes}"
          "SINEX_GATEWAY_MAX_BLOB_BYTES=${toString gatewayLimits.maxBlobBytes}"
          # Pool sized per-service: total max divided by 4 service pools. Set the
          # base so each pool gets a meaningful slice without exhausting Postgres.
          "SINEX_GATEWAY_POOL_MAX_CONNECTIONS=${toString cfg.database.connectionPool.maxConnections}"
          "SINEX_GATEWAY_POOL_MIN_CONNECTIONS=${toString cfg.database.connectionPool.minConnections}"
          "SINEX_GATEWAY_POOL_ACQUIRE_TIMEOUT_SECS=${toString cfg.database.connectionPool.connectionTimeout}"
        ]
        ++ optional (gatewayAdminTokenFile != null) "SINEX_GATEWAY_ADMIN_TOKEN_FILE=${gatewayAdminTokenFile}"
        ++ optional (cfg.core.gateway.tlsCertFile != null) "SINEX_GATEWAY_TLS_CERT=${cfg.core.gateway.tlsCertFile}"
        ++ optional (cfg.core.gateway.tlsKeyFile != null) "SINEX_GATEWAY_TLS_KEY=${cfg.core.gateway.tlsKeyFile}"
        ++ optional (cfg.core.gateway.tlsClientCAFile != null) "SINEX_GATEWAY_TLS_CLIENT_CA=${cfg.core.gateway.tlsClientCAFile}"
        ++ optional (coreCfg.gateway.requireClientTLS) "SINEX_GATEWAY_REQUIRE_CLIENT_TLS=1"
        # Rate limiting
        ++ [
          "SINEX_RPC_RATE_LIMIT_ENABLED=${if coreCfg.gateway.limits.rateLimit.enable then "true" else "false"}"
          "SINEX_RPC_RATE_LIMIT_REQUESTS_PER_SEC=${toString coreCfg.gateway.limits.rateLimit.requestsPerSec}"
          "SINEX_RPC_RATE_LIMIT_BURST=${toString coreCfg.gateway.limits.rateLimit.burst}"
          "SINEX_RPC_RATE_LIMIT_IDLE_TIMEOUT_SECS=${toString coreCfg.gateway.limits.rateLimit.idleTimeoutSec}"
          "SINEX_RPC_RATE_LIMIT_PER_MINUTE=${toString coreCfg.gateway.limits.rateLimit.distributedPerMinute}"
          "SINEX_RPC_RATE_LIMIT_WINDOW_SECS=${toString coreCfg.gateway.limits.rateLimit.distributedWindowSec}"
          "SINEX_NATIVE_MESSAGING_MAX_SIZE_BYTES=${toString coreCfg.gateway.nativeMessagingMaxSizeBytes}"
        ]
        ++ optional (coreCfg.gateway.corsOrigins != null) "SINEX_GATEWAY_CORS_ORIGINS=${coreCfg.gateway.corsOrigins}"
      );
      commonAfter = [ "postgresql.service" ] ++ optionals natsEnabled [ "nats.service" ];
      gatewayAfter = commonAfter ++ optionals tlsAutoGenEnabled [ "sinex-tls-init.service" ];
    in
    if !coreEnabled then {} else
    {
      "sinex-ingestd" = {
        description = "Sinex ingestion daemon";
        wantedBy = [ "multi-user.target" ];
        after = commonAfter;
        requires = commonAfter;
        serviceConfig = mkBaseServiceConfig coreCfg.ingestd.resources (
          mkServiceEnv [
            "RUST_LOG=${coreCfg.ingestd.logLevel}"
            "SINEX_INGESTD_CONSUMER_MAX_ACK_PENDING=${toString coreCfg.ingestd.consumerMaxAckPending}"
            "SINEX_INGESTD_MATERIAL_SLICES_MAX_ACK_PENDING=${toString coreCfg.ingestd.materialSlicesMaxAckPending}"
            # Explicit path prevents ingestd from falling back to dirs::cache_dir() (~/.cache),
            # which is blocked by ProtectHome = true, causing silent /tmp fallback.
            "SINEX_ASSEMBLER_STATE_DIR=${ingestSpool}"
          ]
        ) {
          ExecStart = "${sinexPackage}/bin/sinex-ingestd ${ingestArgs}";
        };
      };
      "sinex-gateway" = {
        description = "Sinex gateway";
        wantedBy = [ "multi-user.target" ];
        after = gatewayAfter;
        requires = [ "postgresql.service" ] ++ optionals tlsAutoGenEnabled [ "sinex-tls-init.service" ];
        wants = optionals natsEnabled [ "nats.service" ];
        serviceConfig = mkBaseServiceConfig coreCfg.gateway.resources gatewayEnv (
          {
            # sinex-gateway does not emit sd_notify, so run it as a simple
            # service to avoid start timeouts in VM tests and CI.
            Type = lib.mkForce "simple";
            ExecStart = "${sinexPackage}/bin/sinex-gateway ${gatewayArgs}";
          }
          // optionalAttrs (gatewayAdminTokenFile != null) {
            ConditionPathReadable = gatewayAdminTokenFile;
          }
        );
      };
    }
    // optionalAttrs tlsAutoGenEnabled {
      "sinex-tls-init" = {
        description = "Generate Sinex gateway TLS credentials";
        wantedBy = [ "multi-user.target" ];
        before = [ "sinex-gateway.service" ];
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
          ExecStart = genTlsScript;
          # Runs as root; the script sets 640 on key/cert after generation.
        };
      };
    };

  mkFilesystemUnits =
    let
      sat = nodesCfg.filesystem;
      instances = resolveInstances sat.instances;
      batch = resolveBatch sat.batch;
      resources = resolveResources sat.resources;
      processorConfig = builtins.toJSON {
        watch_paths = sat.watchPaths;
        max_depth = 10;
        follow_symlinks = false;
        max_capture_bytes = 10485760;
      };
      derivedArgs = [ "--processor-config ${escapeShellArg processorConfig}" ];
      extraArgs = derivedArgs ++ sat.extraArgs;
    in
    mkNodeUnits {
      name = "filesystem";
      binary = "fs-ingestor";
      description = "Filesystem node";
      inherit instances batch resources extraArgs;
      env = [ "RUST_LOG=${nodesCfg.defaults.logLevel}" ] ++ toEnvList sat.env;
    };

  mkTerminalUnits =
    let
      sat = nodesCfg.terminal;
      instances = resolveInstances sat.instances;
      batch = resolveBatch sat.batch;
      resources = resolveResources sat.resources;
    in
    mkNodeUnits {
      name = "terminal";
      binary = "terminal-ingestor";
      description = "Terminal node";
      inherit instances batch resources;
      extraArgs = sat.extraArgs;
      env = [ "RUST_LOG=${nodesCfg.defaults.logLevel}" ] ++ toEnvList sat.env;
    };

  mkDesktopUnits =
    let
      sat = nodesCfg.desktop;
      instances = resolveInstances sat.instances;
      batch = resolveBatch sat.batch;
      resources = resolveResources sat.resources;
      clipboardEnv = if sat.clipboard.enable then [ "SINEX_CLIPBOARD=1" ] else [ "SINEX_CLIPBOARD=0" ];
    in
    mkNodeUnits {
      name = "desktop";
      binary = "desktop-ingestor";
      description = "Desktop node";
      inherit instances batch resources;
      extraArgs = sat.extraArgs;
      env = clipboardEnv ++ [ "RUST_LOG=${nodesCfg.defaults.logLevel}" ] ++ toEnvList sat.env;
    };

  mkSystemUnits =
    let
      sat = nodesCfg.system;
      instances = resolveInstances sat.instances;
      batch = resolveBatch sat.batch;
      resources = resolveResources sat.resources;
    in
    mkNodeUnits {
      name = "system";
      binary = "system-ingestor";
      description = "System node";
      inherit instances batch resources;
      extraArgs = sat.extraArgs;
      env = [ "RUST_LOG=${nodesCfg.defaults.logLevel}" ] ++ toEnvList sat.env;
    };

  mkNodeUnits = params:
    let
      instances = params.instances;
      resources = params.resources;
      extraArgs = params.extraArgs or [];
      envExtras = params.env or [];
      afterUnits = optionals coreEnabled [ "sinex-ingestd.service" ];
      # Satellites publish to NATS and don't strictly require ingestd to be up.
      # Use `wants` so that ingestd going down doesn't cascade-stop all satellites;
      # NATS will buffer events until ingestd recovers.
      wantsUnits = optionals coreEnabled [ "sinex-ingestd.service" ];
      execArgs = concatStringsSep " " ([
        "--service-name sinex-${params.name}"
        "--nats-url ${natsUrl}"
        "--database-url ${databaseUrl}"
      ] ++ extraArgs ++ [ "service" ]);
      env = mkServiceEnv envExtras;
      mkUnit = instance: {
        description = "${params.description} (instance ${toString instance})";
        wantedBy = [ "multi-user.target" ];
        after = afterUnits;
        wants = wantsUnits;
        serviceConfig = mkBaseServiceConfig resources env {
          ExecStart = "${sinexPackage}/bin/sinex-${params.binary} ${execArgs}";
          WorkingDirectory = stateRoot;
        };
      };
    in
    if instances <= 0 then {} else
      listToAttrs (map (idx: nameValuePair "sinex-${params.name}-${toString idx}" (mkUnit idx)) (range 1 instances));

  mkAutomataProfile = profileName:
    let
      profiles = nodesCfg.automata.profiles;
      defaultProfile = profiles.standard;
    in
    lib.attrByPath [ profileName ] defaultProfile profiles;

  mkAutomataUnit = params:
    let
      profile = mkAutomataProfile params.profile;
      resources = profile.resources;
      subjectArgs = map (s: "--subject ${escapeShellArg s}") params.subjects;
      extraArgs = params.extraArgs or [];
      execArgs = concatStringsSep " " ([
        "--service-name sinex-${params.binary}"
        "--nats-url ${natsUrl}"
        "--database-url ${databaseUrl}"
      ] ++ extraArgs ++ subjectArgs ++ [ "service" ]);
      env = mkServiceEnv ([ "RUST_LOG=${nodesCfg.defaults.logLevel}" ] ++ toEnvList params.env);
    in
    {
      description = params.description;
      wantedBy = [ "multi-user.target" ];
      after = [ "postgresql.service" ] ++ optionals coreEnabled [ "sinex-ingestd.service" ];
      requires = [ "postgresql.service" ];
      serviceConfig = mkBaseServiceConfig resources env {
        ExecStart = "${sinexPackage}/bin/sinex-${params.binary} ${execArgs}";
        WorkingDirectory = stateRoot;
      };
    };

  automataServices =
    if !(nodesEnabled && nodesCfg.automata.enable) then {} else
      let
        canon = nodesCfg.automata.canonicalizer;
        health = nodesCfg.automata.healthAggregator;
        canonicalizerUnit =
          if !canon.enable then {} else {
            "sinex-canonicalizer" = mkAutomataUnit {
              binary = "terminal-command-canonicalizer";
              description = "Sinex canonical command synthesizer";
              profile = canon.profile;
              subjects = canon.subjects;
              env = canon.env;
              extraArgs = [];
            };
          };
        healthUnit =
          if !health.enable then {} else {
            "sinex-health-automaton" = mkAutomataUnit {
              binary = "health-automaton";
              description = "Sinex health automaton";
              profile = health.profile;
              subjects = health.subjects;
              env = health.env;
              extraArgs = [];
            };
          };
      in
      canonicalizerUnit // healthUnit;

  nodeservices =
    if !nodesEnabled then {} else
      let
        filesystemUnits = if nodesCfg.filesystem.enable then mkFilesystemUnits else {};
        terminalUnits = if nodesCfg.terminal.enable then mkTerminalUnits else {};
        desktopUnits = if nodesCfg.desktop.enable then mkDesktopUnits else {};
        systemUnits = if nodesCfg.system.enable then mkSystemUnits else {};
      in
      filesystemUnits // terminalUnits // desktopUnits // systemUnits;

  coreServices = mkCoreServices;

  generatedUnits = attrNames nodeservices ++ attrNames automataServices;

in
{
  config = mkMerge [
    (mkIf sinexEnabled {
      systemd.services = mkMerge [ coreServices nodeservices automataServices ];
      services.sinex.satellites.generatedUnits = generatedUnits;
    })
  ];
}
