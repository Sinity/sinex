{ config, lib, pkgs, ... }:

with lib;

let
  systemdHardening = import ./lib/systemd-hardening.nix { inherit lib; };
  databaseRuntime = import ./lib/database-runtime.nix { inherit lib pkgs; };
  secretResolution = import ./lib/secret-resolution.nix { inherit lib; };
  inherit (systemdHardening) mkHelperServiceConfig;
  inherit (databaseRuntime)
    mkDatabasePasswordExec
    renderDatabaseUrl
    ;
  inherit (secretResolution) resolveNamedSecretPath;
  cfg = config.services.sinex;
  coreCfg = cfg.core;
  nodesCfg = cfg.nodes;

  sinexEnabled = cfg.enable;
  coreEnabled = sinexEnabled && coreCfg.enable;
  nodesEnabled = sinexEnabled && nodesCfg.enable;
  natsEnabled = cfg.nats.enable || cfg.nats.autoSetup;
  schemaApplyEnabled = cfg.database.enable && cfg.database.autoSetup;
  localPostgresEnabled = cfg.database.enable && (cfg.database.autoSetup || config.services.postgresql.enable);

  stateRoot = cfg.stateRoot;
  runtimeDir = "${stateRoot}/run";
  ingestSpool = coreCfg.ingestd.spoolDir;
  logDir = cfg.observability.logDir;
  dlqPath = cfg.storage.dlq.path;
  blobDir = cfg.storage.blob.repositoryPath;
  tlsDir = "${stateRoot}/tls";
  tlsAutoGenEnabled = coreEnabled && coreCfg.gateway.autoGenerateTls;
  # Ancillary service flags.
  # JetStream bootstrap is a hard requirement when enabled because ingestd and
  # gateway assume the streams already exist at startup. Blob init remains soft.
  natsBootstrapEnabled = natsEnabled && cfg.nats.bootstrapStreams.enable;
  blobInitEnabled = cfg.storage.blob.enable && cfg.storage.blob.autoInit;
  schemaApplyUnits = optionals schemaApplyEnabled [ "sinex-schema-apply.service" ];
  postgresServiceUnits = optionals localPostgresEnabled [ "postgresql.service" "postgresql-setup.service" ];

  genTlsScript = pkgs.writeShellScript "sinex-tls-init" ''
    set -euo pipefail
    cert="${tlsDir}/server.pem"
    key="${tlsDir}/server-key.pem"
    ca="${tlsDir}/ca.pem"
    if [[ -f "$cert" && -f "$key" && -f "$ca" ]]; then
      echo "Sinex gateway TLS credentials already present, skipping."
      exit 0
    fi
    mkdir -p "${tlsDir}"
    chmod 750 "${tlsDir}"
    "${sinexPackage}/bin/xtask" --format human infra tls-init-gateway \
      --output-dir "${tlsDir}" \
      --san localhost \
      --san 127.0.0.1 \
      --ca-name "Sinex Gateway CA"
    # Gateway needs the server cert, server key, and trust anchor at runtime.
    chown root:${serviceUser} "$key" "$cert" "$ca"
    chmod 640 "$key" "$cert" "$ca"
    # Keep client and CA private keys root-only; they are operator artifacts, not service inputs.
    if [[ -f "${tlsDir}/client.pem" ]]; then
      chmod 644 "${tlsDir}/client.pem"
    fi
    if [[ -f "${tlsDir}/client-key.pem" ]]; then
      chmod 600 "${tlsDir}/client-key.pem"
    fi
    if [[ -f "${tlsDir}/ca-key.pem" ]]; then
      chmod 600 "${tlsDir}/ca-key.pem"
    fi
    echo "Sinex gateway TLS credentials generated at ${tlsDir}"
  '';

  sinexPackage = cfg.package;
  serviceUser = cfg.users.nodes;

  databaseUrl = renderDatabaseUrl cfg.database;

  natsUrl = concatStringsSep "," nodesCfg.nats.servers;
  secretPaths = config.sinex.secrets.paths or { };
  resolveSecretPath = resolveNamedSecretPath secretPaths;
  gatewayAdminTokenFile =
    resolveSecretPath cfg.secrets.gatewayAdminTokenFile [
      "sinex-gateway-admin-token"
    ];
  effectiveDatabasePasswordFile = resolveSecretPath cfg.database.passwordFile [
    "sinex-local-db"
    "sinex-remote-db"
  ];
  natsTlsCfg = nodesCfg.nats.tls;
  natsAuthCfg = nodesCfg.nats.auth;
  effectiveNatsCaCertFile = resolveSecretPath natsTlsCfg.caCertFile [
    "sinex-nats-ca"
    "nats-ca"
    "sinex-remote-nats-ca"
  ];
  effectiveNatsClientCertFile = resolveSecretPath natsTlsCfg.clientCertFile [
    "sinex-nats-client-cert"
    "nats-client-cert"
    "sinex-remote-nats-cert"
  ];
  effectiveNatsClientKeyFile = resolveSecretPath natsTlsCfg.clientKeyFile [
    "sinex-nats-client-key"
    "nats-client-key"
    "sinex-remote-nats-key"
  ];
  effectiveNatsTokenFile = resolveSecretPath natsAuthCfg.tokenFile [
    "sinex-nats-token"
    "nats-token"
  ];
  effectiveNatsCredsFile = resolveSecretPath natsAuthCfg.credsFile [
    "sinex-nats-client-creds"
    "nats-client-creds"
  ];
  effectiveNatsNkeySeedFile = resolveSecretPath natsAuthCfg.nkeySeedFile [
    "sinex-nats-client-nkey"
    "nats-client-nkey"
  ];
  inferredNatsTls =
    natsTlsCfg.requireTls
    || any (server: hasPrefix "tls://" server || hasPrefix "wss://" server) nodesCfg.nats.servers;
  collectReadablePaths = paths: filter (path: path != null) paths;
  databaseSecretAssertPaths = collectReadablePaths [
    (if cfg.database.enable then effectiveDatabasePasswordFile else null)
  ];
  natsSecretAssertPaths = collectReadablePaths [
    effectiveNatsCaCertFile
    effectiveNatsClientCertFile
    effectiveNatsClientKeyFile
    effectiveNatsTokenFile
    effectiveNatsCredsFile
    effectiveNatsNkeySeedFile
  ];
  gatewaySecretAssertPaths = collectReadablePaths (
    [
      gatewayAdminTokenFile
      cfg.core.gateway.tlsCertFile
      cfg.core.gateway.tlsKeyFile
    ]
    ++ optionals coreCfg.gateway.requireClientTLS [ cfg.core.gateway.tlsClientCAFile ]
  );
  existingPathAssertions = paths:
    let
      existingPaths = collectReadablePaths paths;
    in
    optionalAttrs (existingPaths != [ ]) { AssertPathExists = existingPaths; };

  toEnvList = envAttrs: mapAttrsToList (name: value: "${name}=${value}") envAttrs;
  renderBindReadOnlyPaths = mounts:
    map (mount: "${mount.source}:${mount.destination}") mounts;

  baseEnv = optional cfg.database.enable "DATABASE_URL=${databaseUrl}" ++ [
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
  } // optionalAttrs (resources.openFilesLimit != null) {
    LimitNOFILE = "${toString resources.openFilesLimit}:${toString resources.openFilesLimit}";
  };

  readWritePaths = [
    stateRoot
    runtimeDir
    ingestSpool
    logDir
    dlqPath
    blobDir
  ];
  restartRateLimits = {
    # Long-running capture services must recover after transient infra outages
    # without requiring a manual `systemctl reset-failed`.
    StartLimitIntervalSec = 0;
  };

  mkServiceEnv = additionalEnv: baseEnv ++ coordinationEnv ++ additionalEnv;
  targetUser = cfg.users.target;
  targetHome =
    if targetUser == null then null
    else lib.attrByPath [ "users" "users" targetUser "home" ] "/home/${targetUser}" config;
  targetUid =
    if targetUser == null then null
    else lib.attrByPath [ "users" "users" targetUser "uid" ] null config;
  effectiveDocumentRoots =
    if nodesCfg.document.allowedRoots != [ ] then nodesCfg.document.allowedRoots
    else if targetHome == null then [ ]
    else [ "${targetHome}/Documents" ];

  mkBaseServiceConfig = resources: env: extra:
    {
      Type = "notify";
      User = serviceUser;
      Group = serviceUser;
      Restart = "on-failure";
      RestartSec = 10;
      WatchdogSec = "60s";
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

  mkAccessSetupUnit =
    {
      name,
      description,
      script,
      writePaths ? [ ],
      beforeUnits ? [ ],
    }:
    if script == null then { } else {
      "${name}" = {
        inherit description;
        before = beforeUnits;
        serviceConfig =
          {
            ExecStart = script;
          }
          // mkHelperServiceConfig {
            user = "root";
            group = "root";
            remainAfterExit = true;
            protectHome = false;
            privateTmp = true;
            restrictAddressFamilies = [ ];
            readWritePaths = unique (readWritePaths ++ writePaths);
          };
      };
    };

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
        # database_url is read from DATABASE_URL env var (set in baseEnv),
        # so no --database-url CLI arg is needed here.
        "--tcp-listen ${coreCfg.gateway.listenAddress}"
        # TLS is mandatory for gateway RPC; cert/key must be provided via env vars.
      ] ++ coreCfg.gateway.extraArgs);
      gatewayLimits = coreCfg.gateway.limits;
      gatewayAdminTokenRuntimeFile = "${runtimeDir}/gateway-admin-token";
      gatewayTlsCertRuntimeFile = "${runtimeDir}/gateway-server.pem";
      gatewayTlsKeyRuntimeFile = "${runtimeDir}/gateway-server-key.pem";
      gatewayTlsClientCaRuntimeFile = "${runtimeDir}/gateway-client-ca.pem";
      gatewayTlsTrustAnchorRuntimeFile = "${runtimeDir}/gateway-ca.pem";
      gatewayTlsTrustAnchorSourceFile =
        if cfg.core.gateway.autoGenerateTls then
          "${tlsDir}/ca.pem"
        else
          null;
      gatewayRuntimeInputStageScript =
        if gatewayAdminTokenFile == null
          && cfg.core.gateway.tlsCertFile == null
          && cfg.core.gateway.tlsKeyFile == null
          && gatewayTlsTrustAnchorSourceFile == null
          && cfg.core.gateway.tlsClientCAFile == null
        then null else
          pkgs.writeShellScript "sinex-gateway-stage-runtime-inputs" ''
            set -euo pipefail

            INSTALL=${pkgs.coreutils}/bin/install
            runtime_dir=${escapeShellArg runtimeDir}

            stage_file() {
              local source_path="$1"
              local dest_path="$2"
              local mode="$3"

              if [ -z "$source_path" ]; then
                return 0
              fi

              if [ ! -r "$source_path" ]; then
                echo "[sinex] runtime input $source_path is not readable" >&2
                exit 1
              fi

              "$INSTALL" -m "$mode" -o ${serviceUser} -g ${serviceUser} "$source_path" "$dest_path"
            }

            stage_gateway_admin_token() {
              local source_path="$1"
              local dest_path="$2"
              local tmp_path
              local raw_token
              local staged_token

              if [ -z "$source_path" ]; then
                return 0
              fi

              if [ ! -r "$source_path" ]; then
                echo "[sinex] gateway admin token $source_path is not readable" >&2
                exit 1
              fi

              raw_token="$(${pkgs.coreutils}/bin/cat "$source_path")"
              raw_token="$(${pkgs.coreutils}/bin/printf '%s' "$raw_token" | ${pkgs.gnused}/bin/sed -e 's/[[:space:]]*$//')"
              if [ -z "$raw_token" ]; then
                echo "[sinex] gateway admin token $source_path is empty" >&2
                exit 1
              fi

              case "$raw_token" in
                *:admin)
                  staged_token="$raw_token"
                  ;;
                *:readonly|*:write)
                  echo "[sinex] gateway admin token $source_path must be raw or already end with :admin" >&2
                  exit 1
                  ;;
                *)
                  staged_token="$raw_token:admin"
                  ;;
              esac

              tmp_path="$(mktemp "$runtime_dir/gateway-admin-token.XXXXXX")"
              ${pkgs.coreutils}/bin/chmod 0600 "$tmp_path"
              ${pkgs.coreutils}/bin/printf '%s\n' "$staged_token" > "$tmp_path"
              ${pkgs.coreutils}/bin/chown ${serviceUser}:${serviceUser} "$tmp_path"
              ${pkgs.coreutils}/bin/chmod 0400 "$tmp_path"
              ${pkgs.coreutils}/bin/mv "$tmp_path" "$dest_path"
            }

            "$INSTALL" -d -m 0750 -o ${serviceUser} -g ${serviceUser} "$runtime_dir"

            stage_gateway_admin_token ${escapeShellArg (if gatewayAdminTokenFile == null then "" else toString gatewayAdminTokenFile)} ${escapeShellArg gatewayAdminTokenRuntimeFile}
            stage_file ${escapeShellArg (if cfg.core.gateway.tlsCertFile == null then "" else toString cfg.core.gateway.tlsCertFile)} ${escapeShellArg gatewayTlsCertRuntimeFile} 0440
            stage_file ${escapeShellArg (if cfg.core.gateway.tlsKeyFile == null then "" else toString cfg.core.gateway.tlsKeyFile)} ${escapeShellArg gatewayTlsKeyRuntimeFile} 0400
            stage_file ${escapeShellArg (if gatewayTlsTrustAnchorSourceFile == null then "" else toString gatewayTlsTrustAnchorSourceFile)} ${escapeShellArg gatewayTlsTrustAnchorRuntimeFile} 0444
            stage_file ${escapeShellArg (if cfg.core.gateway.tlsClientCAFile == null then "" else toString cfg.core.gateway.tlsClientCAFile)} ${escapeShellArg gatewayTlsClientCaRuntimeFile} 0440
          '';
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
        ++ optional (gatewayAdminTokenFile != null) "SINEX_GATEWAY_ADMIN_TOKEN_FILE=${gatewayAdminTokenRuntimeFile}"
        ++ optional (cfg.core.gateway.tlsCertFile != null) "SINEX_GATEWAY_TLS_CERT=${gatewayTlsCertRuntimeFile}"
        ++ optional (cfg.core.gateway.tlsKeyFile != null) "SINEX_GATEWAY_TLS_KEY=${gatewayTlsKeyRuntimeFile}"
        ++ optional (cfg.core.gateway.tlsClientCAFile != null) "SINEX_GATEWAY_TLS_CLIENT_CA=${gatewayTlsClientCaRuntimeFile}"
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
      # Ordering for core services.
      # Base: hard infrastructure that both services depend on.
      coreRequires =
        postgresServiceUnits
        ++ schemaApplyUnits
        ++ optionals natsEnabled [ "nats.service" ]
        ++ optionals natsBootstrapEnabled [ "sinex-nats-bootstrap.service" ];
      # Core services should not start before stream bootstrap has succeeded when
      # the managed bootstrap path is enabled.
      coreAfter = coreRequires ++ optionals blobInitEnabled [ "sinex-blob-init.service" ];
      coreWants = optionals blobInitEnabled [ "sinex-blob-init.service" ];
      gatewayAfter = coreAfter ++ optionals tlsAutoGenEnabled [ "sinex-tls-init.service" ];
    in
    if !coreEnabled then { } else
    {
      "sinex-ingestd" = {
        description = "Sinex ingestion daemon";
        wantedBy = [ "multi-user.target" ];
        after = coreAfter;
        requires = coreRequires;
        wants = coreWants;
        unitConfig = restartRateLimits // existingPathAssertions (databaseSecretAssertPaths ++ natsSecretAssertPaths);
        path = optionals cfg.storage.blob.enable [ pkgs.git pkgs.git-annex ];
        serviceConfig = mkBaseServiceConfig coreCfg.ingestd.resources
          (
            mkServiceEnv [
              "RUST_LOG=${coreCfg.ingestd.logLevel}"
              # Pool size and timeouts: read by sinex-ingestd via env vars.
              "SINEX_INGESTD_POOL_SIZE=${toString cfg.database.connectionPool.maxConnections}"
              "SINEX_INGESTD_POOL_ACQUIRE_TIMEOUT_SECS=${toString cfg.database.connectionPool.connectionTimeout}"
              "SINEX_INGESTD_POOL_IDLE_TIMEOUT_SECS=${toString cfg.database.connectionPool.idleTimeout}"
              # Ack-pending limits: read by sinex-ingestd via SINEX_INGESTD_CONSUMER_MAX_ACK_PENDING
              # and SINEX_INGESTD_MATERIAL_SLICES_MAX_ACK_PENDING (clap env attribute).
              "SINEX_INGESTD_CONSUMER_MAX_ACK_PENDING=${toString coreCfg.ingestd.consumerMaxAckPending}"
              "SINEX_INGESTD_MATERIAL_SLICES_MAX_ACK_PENDING=${toString coreCfg.ingestd.materialSlicesMaxAckPending}"
              # Explicit work and spool dirs prevent fallback to dirs::cache_dir() (~/.cache)
              # which is blocked by ProtectHome = true in the systemd unit.
              "SINEX_INGESTD_WORK_DIR=${stateRoot}/ingestd/work"
              "SINEX_ASSEMBLER_STATE_DIR=${ingestSpool}"
              # Schema and validation behaviour
              "SINEX_INGESTD_GITOPS_ENABLED=${if coreCfg.ingestd.gitopsEnabled then "true" else "false"}"
              "SINEX_SKIP_SCHEMA_SYNC=${if coreCfg.ingestd.skipSchemaSync then "true" else "false"}"
              "SINEX_INGESTD_STRICT_VALIDATION=${if coreCfg.ingestd.strictValidation then "true" else "false"}"
              "SINEX_VALIDATE_SCHEMAS=${if coreCfg.ingestd.validateSchemas then "true" else "false"}"
              # Operational intervals
              "SINEX_INGESTD_SCHEMA_RELOAD_INTERVAL_SECS=${toString coreCfg.ingestd.schemaReloadIntervalSecs}"
              "SINEX_INGESTD_STATS_LOG_INTERVAL_SECS=${toString coreCfg.ingestd.statsLogIntervalSecs}"
            ]
          )
          {
            ExecStart = mkDatabasePasswordExec {
              name = "ingestd";
              command = "${sinexPackage}/bin/sinex-ingestd ${ingestArgs}";
              passwordFile = if cfg.database.enable then effectiveDatabasePasswordFile else null;
            };
          };
      };
      "sinex-gateway" = {
        description = "Sinex gateway";
        wantedBy = [ "multi-user.target" ];
        after = gatewayAfter;
        requires = coreRequires ++ optionals tlsAutoGenEnabled [ "sinex-tls-init.service" ];
        wants = coreWants;
        unitConfig =
          restartRateLimits
          // existingPathAssertions (
            databaseSecretAssertPaths ++ natsSecretAssertPaths ++ gatewaySecretAssertPaths
          );
        path = optionals cfg.storage.blob.enable [ pkgs.git pkgs.git-annex ];
        serviceConfig = mkBaseServiceConfig coreCfg.gateway.resources gatewayEnv (
          {
            Type = lib.mkForce "notify";
            NotifyAccess = "main";
            ExecStart = mkDatabasePasswordExec {
              name = "gateway";
              command = "${sinexPackage}/bin/sinex-gateway ${gatewayArgs}";
              passwordFile = if cfg.database.enable then effectiveDatabasePasswordFile else null;
            };
          }
          // optionalAttrs (gatewayRuntimeInputStageScript != null) {
            ExecStartPre = lib.mkBefore [ "+${gatewayRuntimeInputStageScript}" ];
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
          ExecStart = genTlsScript;
          # Runs as root; the script sets 640 on key/cert after generation.
        } // mkHelperServiceConfig {
          user = "root";
          group = "root";
          remainAfterExit = true;
          readWritePaths = [ tlsDir ];
          extra = {
            # TLS bootstrap must hand the generated certs off to the sinex service
            # user, so it needs the privileged chown syscall available.
            NoNewPrivileges = mkForce false;
            SystemCallFilter = mkForce [ "@system-service" "@privileged" ];
          };
        };
      };
    };

  mkFilesystemUnits =
    let
      sat = nodesCfg.filesystem;
      instances = resolveInstances sat.instances;
      batch = resolveBatch sat.batch;
      resources = resolveResources sat.resources;
      nodeConfig = builtins.toJSON {
        watch_paths = sat.watchPaths;
        max_depth = 10;
        follow_symlinks = false;
        max_capture_bytes = 10485760;
        max_watches = sat.maxWatches;
        ignored_directory_names = sat.ignoredDirectoryNames;
        poll_interval_secs = sat.pollIntervalSec;
      };
      derivedArgs = [ "--node-config ${escapeShellArg nodeConfig}" ];
      extraArgs = derivedArgs ++ sat.extraArgs;
    in
    mkNodeUnits {
      name = "filesystem";
      binary = "fs-ingestor";
      description = "Filesystem node";
      inherit instances batch resources extraArgs;
      env = [ "RUST_LOG=${nodesCfg.defaults.logLevel}" ] ++ toEnvList sat.env;
      serviceConfig = {
        # The default watch path is /home/<target>; keep home read-only rather
        # than hiding it entirely so the configured watch paths are actually
        # observable on real hosts.
        ProtectHome = lib.mkForce "read-only";
      };
    };

  mkTerminalUnits =
    let
      sat = nodesCfg.terminal;
      instances = resolveInstances sat.instances;
      batch = resolveBatch sat.batch;
      resources = resolveResources sat.resources;
      effectiveHistorySources =
        if sat.historySources != [ ] then sat.historySources
        else if targetHome == null then [ ]
        else [
          {
            path = "${targetHome}/.bash_history";
            shell = "bash";
          }
          {
            path = "${targetHome}/.zsh_history";
            shell = "zsh";
          }
          {
            path = "${targetHome}/.local/share/atuin/history.db";
            shell = "atuin";
          }
          {
            path = "${targetHome}/.local/share/fish/fish_history";
            shell = "fish";
          }
        ];
      nodeConfig = builtins.toJSON {
        history_sources = map
          (source: {
            path = source.path;
            shell = source.shell;
          })
          effectiveHistorySources;
        polling_interval_secs = 5;
        max_capture_bytes = 32768;
      };
      derivedArgs =
        optional (effectiveHistorySources != [ ]) "--node-config ${escapeShellArg nodeConfig}";
      sqliteHistoryPaths =
        unique (
          map (source: source.path) (filter (source: source.shell == "atuin") effectiveHistorySources)
        );
      sqliteHistoryDirs = unique (map builtins.dirOf sqliteHistoryPaths);
      accessAclPaths =
        unique (
          (map (source: source.path) effectiveHistorySources)
          ++ optionals (targetHome != null) [ "${targetHome}/.local/share/atuin/history.db" ]
        );
      accessWritePaths =
        unique (
          optionals (targetHome != null) [
            targetHome
            "${targetHome}/.local"
            "${targetHome}/.local/share"
            "${targetHome}/.local/share/atuin"
          ]
        );
      accessSetupScript =
        if accessAclPaths == [ ] then null else
        pkgs.writeShellScript "sinex-terminal-target-access" ''
          set -euo pipefail

          SERVICE_USER=${escapeShellArg serviceUser}
          SETFACL=${pkgs.acl}/bin/setfacl
          DIRNAME=${pkgs.coreutils}/bin/dirname
          acl_failures=0

          record_acl_failure() {
            local path="$1"
            echo "sinex-terminal-target-access: failed to grant ACLs for $path" >&2
            acl_failures=$((acl_failures + 1))
          }

          set_access_acl() {
            local path="$1"
            local acl_spec="$2"
            local mask_spec=""
            if [ "$#" -ge 3 ]; then
              mask_spec="$3"
            fi
            if [ -n "$mask_spec" ]; then
              "$SETFACL" -m "$acl_spec,m::$mask_spec" "$path" || record_acl_failure "$path"
            else
              "$SETFACL" --mask -m "$acl_spec" "$path" || record_acl_failure "$path"
            fi
          }

          set_default_acl() {
            local path="$1"
            local acl_spec="$2"
            local mask_spec=""
            if [ "$#" -ge 3 ]; then
              mask_spec="$3"
            fi
            if [ -n "$mask_spec" ]; then
              "$SETFACL" -d -m "$acl_spec,m::$mask_spec" "$path" || record_acl_failure "$path"
            else
              "$SETFACL" -d --mask -m "$acl_spec" "$path" || record_acl_failure "$path"
            fi
          }

          grant_parent_dirs() {
            local path="$1"
            local dir
            dir="$path"
            while [ "$dir" != "/" ] && [ "$dir" != "." ]; do
              if [ -d "$dir" ]; then
                set_access_acl "$dir" "u:$SERVICE_USER:--x" "--x"
              fi
              dir="$("$DIRNAME" "$dir")"
            done
          }

          grant_dir_read() {
            local path="$1"
            if [ -d "$path" ]; then
              set_access_acl "$path" "u:$SERVICE_USER:r-x" "r-x"
            fi
          }

          grant_dir_read_defaults() {
            local path="$1"
            if [ -d "$path" ]; then
              set_default_acl "$path" "u:$SERVICE_USER:r-X" "r-X"
            fi
          }

          grant_file_read() {
            local path="$1"
            if [ -f "$path" ]; then
              set_access_acl "$path" "u:$SERVICE_USER:r--" "r--"
            fi
          }

          grant_sqlite_sidecars() {
            local path="$1"
            grant_file_read "$path-wal"
            grant_file_read "$path-shm"
          }

          ${concatStringsSep "\n" (map (path: ''
            grant_parent_dirs ${escapeShellArg path}
            grant_file_read ${escapeShellArg path}
          '') accessAclPaths)}

          ${concatStringsSep "\n" (map (path: ''
            grant_parent_dirs ${escapeShellArg path}
            grant_dir_read ${escapeShellArg path}
            grant_dir_read_defaults ${escapeShellArg path}
          '') sqliteHistoryDirs)}

          ${concatStringsSep "\n" (map (path: ''
            grant_parent_dirs ${escapeShellArg path}
            grant_sqlite_sidecars ${escapeShellArg path}
          '') sqliteHistoryPaths)}

          if [ "$acl_failures" -ne 0 ]; then
            exit 1
          fi
        '';
      units = mkNodeUnits {
        name = "terminal";
        binary = "terminal-ingestor";
        description = "Terminal node";
        inherit instances batch resources;
        extraArgs = derivedArgs ++ sat.extraArgs;
        env = [ "RUST_LOG=${nodesCfg.defaults.logLevel}" ] ++ toEnvList sat.env;
        serviceConfig = {
          # The terminal ingestor needs read access to the target user's shell history
          # (Atuin DB, bash_history, zsh_history). ProtectHome blocks /home entirely,
          # so we use read-only mode to allow reading history files without write access.
          ProtectHome = lib.mkForce "read-only";
          ReadWritePaths = readWritePaths ++ accessWritePaths;
        } // optionalAttrs (sat.access.bindReadOnlyPaths != [ ] && accessSetupScript == null) {
          BindReadOnlyPaths = renderBindReadOnlyPaths sat.access.bindReadOnlyPaths;
        } // optionalAttrs (accessSetupScript != null) {
          ExecStartPre = lib.mkBefore [ "+${accessSetupScript}" ];
        };
      };
      supportUnits = mkAccessSetupUnit {
        name = "sinex-terminal-target-access";
        description = "Prepare target-user access for the Sinex terminal node";
        script = accessSetupScript;
        writePaths = accessWritePaths;
        beforeUnits = [ "sinex-preflight.service" ] ++ map (unit: "${unit}.service") (attrNames units);
      };
    in
    {
      inherit units supportUnits;
    };

  mkDesktopUnits =
    let
      sat = nodesCfg.desktop;
      instances = resolveInstances sat.instances;
      batch = resolveBatch sat.batch;
      resources = resolveResources sat.resources;
      clipboardEnv = if sat.clipboard.enable then [ "SINEX_CLIPBOARD=1" ] else [ "SINEX_CLIPBOARD=0" ];
      bridgeEnvFile = "${runtimeDir}/desktop-target.env";
      runtimeRoot =
        if sat.session.runtimeDir != null then sat.session.runtimeDir
        else if targetUid != null then "/run/user/${toString targetUid}"
        else null;
      sessionEnv =
        optional (sat.session.runtimeDir != null) "SINEX_HYPRLAND_RUNTIME_DIR=${sat.session.runtimeDir}"
        ++ optional (sat.session.runtimeDir != null) "XDG_RUNTIME_DIR=${sat.session.runtimeDir}"
        ++ optional (sat.session.waylandDisplay != null) "WAYLAND_DISPLAY=${sat.session.waylandDisplay}"
        ++ optional (sat.session.hyprlandInstanceSignature != null) "SINEX_HYPRLAND_INSTANCE_SIGNATURE=${sat.session.hyprlandInstanceSignature}"
        ++ optional (sat.session.hyprlandEventSocket != null) "SINEX_HYPRLAND_EVENT_SOCKET=${sat.session.hyprlandEventSocket}"
        ++ optional (sat.session.hyprlandCommandSocket != null) "SINEX_HYPRLAND_COMMAND_SOCKET=${sat.session.hyprlandCommandSocket}"
        ++ optional (sat.history.activitywatchDbPath != null) "SINEX_ACTIVITYWATCH_DB_PATH=${sat.history.activitywatchDbPath}";
      accessWritePaths =
        unique (
          optionals (runtimeRoot != null) [
            runtimeRoot
            "${runtimeRoot}/hypr"
          ]
          ++ optionals (targetHome != null) [
            targetHome
            "${targetHome}/.local"
            "${targetHome}/.local/share"
          ]
          ++ optionals (sat.history.activitywatchDbPath != null) [
            (builtins.dirOf sat.history.activitywatchDbPath)
          ]
        );
      accessSetupScript =
        if targetUser == null then null else
        pkgs.writeShellScript "sinex-desktop-target-access" ''
          set -euo pipefail

          SERVICE_USER=${escapeShellArg serviceUser}
          TARGET_USER=${escapeShellArg targetUser}
          CONFIGURED_RUNTIME_DIR=${escapeShellArg (if sat.session.runtimeDir != null then sat.session.runtimeDir else "")}
          CONFIGURED_WAYLAND_DISPLAY=${escapeShellArg (if sat.session.waylandDisplay != null then sat.session.waylandDisplay else "")}
          CONFIGURED_HYPRLAND_SIGNATURE=${escapeShellArg (if sat.session.hyprlandInstanceSignature != null then sat.session.hyprlandInstanceSignature else "")}
          CONFIGURED_ACTIVITYWATCH_DB=${escapeShellArg (if sat.history.activitywatchDbPath != null then sat.history.activitywatchDbPath else "")}
          ENV_FILE=${escapeShellArg bridgeEnvFile}
          SETFACL=${pkgs.acl}/bin/setfacl
          ID=${pkgs.coreutils}/bin/id
          INSTALL=${pkgs.coreutils}/bin/install
          CHOWN=${pkgs.coreutils}/bin/chown
          CHMOD=${pkgs.coreutils}/bin/chmod
          RM=${pkgs.coreutils}/bin/rm
          FIND=${pkgs.findutils}/bin/find
          SORT=${pkgs.coreutils}/bin/sort
          BASENAME=${pkgs.coreutils}/bin/basename
          DIRNAME=${pkgs.coreutils}/bin/dirname
          acl_failures=0

          record_acl_failure() {
            local path="$1"
            echo "sinex-desktop-target-access: failed to grant ACLs for $path" >&2
            acl_failures=$((acl_failures + 1))
          }

          set_access_acl() {
            local path="$1"
            local acl_spec="$2"
            local mask_spec=""
            if [ "$#" -ge 3 ]; then
              mask_spec="$3"
            fi
            if [ -n "$mask_spec" ]; then
              "$SETFACL" -m "$acl_spec,m::$mask_spec" "$path" || record_acl_failure "$path"
            else
              "$SETFACL" --mask -m "$acl_spec" "$path" || record_acl_failure "$path"
            fi
          }

          set_default_acl() {
            local path="$1"
            local acl_spec="$2"
            local mask_spec=""
            if [ "$#" -ge 3 ]; then
              mask_spec="$3"
            fi
            if [ -n "$mask_spec" ]; then
              "$SETFACL" -d -m "$acl_spec,m::$mask_spec" "$path" || record_acl_failure "$path"
            else
              "$SETFACL" -d --mask -m "$acl_spec" "$path" || record_acl_failure "$path"
            fi
          }

          grant_parent_dirs() {
            local path="$1"
            local dir
            dir="$path"
            while [ "$dir" != "/" ] && [ "$dir" != "." ]; do
              if [ -d "$dir" ]; then
                set_access_acl "$dir" "u:$SERVICE_USER:--x" "--x"
              fi
              dir="$("$DIRNAME" "$dir")"
            done
          }

          grant_dir_defaults() {
            local path="$1"
            if [ -d "$path" ]; then
              set_default_acl "$path" "u:$SERVICE_USER:rwX" "rwX"
            fi
          }

          grant_socket_access() {
            local path="$1"
            if [ -S "$path" ]; then
              grant_parent_dirs "$("$DIRNAME" "$path")"
              set_access_acl "$path" "u:$SERVICE_USER:rw-" "rw-"
            fi
          }

          grant_file_read() {
            local path="$1"
            if [ -f "$path" ]; then
              grant_parent_dirs "$path"
              set_access_acl "$path" "u:$SERVICE_USER:r--" "r--"
            fi
          }

          OWNER="$SERVICE_USER"
          "$INSTALL" -d -m0755 -o "$OWNER" -g "$OWNER" ${escapeShellArg runtimeDir}
          "$RM" -f "$ENV_FILE"
          : > "$ENV_FILE"

          if [ -n "$CONFIGURED_RUNTIME_DIR" ]; then
            RUNTIME_ROOT="$CONFIGURED_RUNTIME_DIR"
          else
            if ! TARGET_UID="$("$ID" -u "$TARGET_USER" 2>/dev/null)"; then
              "$CHOWN" "$OWNER:$OWNER" "$ENV_FILE"
              "$CHMOD" 0640 "$ENV_FILE"
              echo "Sinex desktop bridge failed: target user '$TARGET_USER' does not exist" >&2
              exit 1
            fi
            RUNTIME_ROOT="/run/user/$TARGET_UID"
          fi

          if [ ! -d "$RUNTIME_ROOT" ]; then
            "$CHOWN" "$OWNER:$OWNER" "$ENV_FILE"
            "$CHMOD" 0640 "$ENV_FILE"
            echo "Sinex desktop bridge failed: runtime directory '$RUNTIME_ROOT' is missing" >&2
            exit 1
          fi

          grant_parent_dirs "$RUNTIME_ROOT"
          grant_dir_defaults "$RUNTIME_ROOT"

          WAYLAND_DISPLAY_NAME="$CONFIGURED_WAYLAND_DISPLAY"
          if [ -z "$WAYLAND_DISPLAY_NAME" ]; then
            while IFS= read -r socket_path; do
              [ -n "$socket_path" ] || continue
              grant_socket_access "$socket_path"
              if [ -z "$WAYLAND_DISPLAY_NAME" ]; then
                WAYLAND_DISPLAY_NAME="$("$BASENAME" "$socket_path")"
              fi
            done < <("$FIND" "$RUNTIME_ROOT" -maxdepth 1 -type s -name 'wayland-*' | "$SORT")
          fi

          HYPRLAND_SIGNATURE="$CONFIGURED_HYPRLAND_SIGNATURE"
          if [ -d "$RUNTIME_ROOT/hypr" ]; then
            grant_parent_dirs "$RUNTIME_ROOT/hypr"
            grant_dir_defaults "$RUNTIME_ROOT/hypr"

            while IFS= read -r instance_dir; do
              [ -n "$instance_dir" ] || continue
              grant_parent_dirs "$instance_dir"
              grant_dir_defaults "$instance_dir"
            done < <("$FIND" "$RUNTIME_ROOT/hypr" -mindepth 1 -maxdepth 1 -type d | "$SORT")

            while IFS= read -r socket_path; do
              [ -n "$socket_path" ] || continue
              grant_socket_access "$socket_path"
            done < <("$FIND" "$RUNTIME_ROOT/hypr" -mindepth 2 -maxdepth 2 -type s -name '.socket.sock' | "$SORT")

            HYPRLAND_EVENT_SOCKET_COUNT=0
            while IFS= read -r socket_path; do
              [ -n "$socket_path" ] || continue
              grant_socket_access "$socket_path"
              HYPRLAND_EVENT_SOCKET_COUNT=$((HYPRLAND_EVENT_SOCKET_COUNT + 1))
              if [ -z "$HYPRLAND_SIGNATURE" ]; then
                HYPRLAND_SIGNATURE="$("$BASENAME" "$("$DIRNAME" "$socket_path")")"
              fi
            done < <("$FIND" "$RUNTIME_ROOT/hypr" -mindepth 2 -maxdepth 2 -type s -name '.socket2.sock' | "$SORT")

            if [ -n "$CONFIGURED_HYPRLAND_SIGNATURE" ]; then
              HYPRLAND_SIGNATURE="$CONFIGURED_HYPRLAND_SIGNATURE"
            elif [ "$HYPRLAND_EVENT_SOCKET_COUNT" -ne 1 ]; then
              HYPRLAND_SIGNATURE=""
            fi
          fi

          {
            echo "XDG_RUNTIME_DIR=$RUNTIME_ROOT"
            echo "SINEX_HYPRLAND_RUNTIME_DIR=$RUNTIME_ROOT"
            if [ -n "$WAYLAND_DISPLAY_NAME" ]; then
              echo "WAYLAND_DISPLAY=$WAYLAND_DISPLAY_NAME"
            fi
            if [ -n "$HYPRLAND_SIGNATURE" ]; then
              echo "HYPRLAND_INSTANCE_SIGNATURE=$HYPRLAND_SIGNATURE"
              echo "SINEX_HYPRLAND_INSTANCE_SIGNATURE=$HYPRLAND_SIGNATURE"
            fi
            if [ -n "$CONFIGURED_ACTIVITYWATCH_DB" ]; then
              grant_file_read "$CONFIGURED_ACTIVITYWATCH_DB"
              echo "SINEX_ACTIVITYWATCH_DB_PATH=$CONFIGURED_ACTIVITYWATCH_DB"
            fi
          } > "$ENV_FILE"

          "$CHOWN" "$OWNER:$OWNER" "$ENV_FILE"
          "$CHMOD" 0640 "$ENV_FILE"

          if [ "$acl_failures" -ne 0 ]; then
            exit 1
          fi
        '';
      units = mkNodeUnits {
        name = "desktop";
        binary = "desktop-ingestor";
        description = "Desktop node";
        inherit instances batch resources;
        extraArgs = sat.extraArgs;
        env = clipboardEnv ++ sessionEnv ++ [ "RUST_LOG=${nodesCfg.defaults.logLevel}" ] ++ toEnvList sat.env;
        path = [ pkgs.hyprland ];
        serviceConfig = {
          ProtectHome = lib.mkForce "read-only";
          ReadWritePaths = readWritePaths ++ accessWritePaths;
        } // optionalAttrs (sat.access.bindReadOnlyPaths != [ ] && accessSetupScript == null) {
          BindReadOnlyPaths = renderBindReadOnlyPaths sat.access.bindReadOnlyPaths;
        } // optionalAttrs (accessSetupScript != null) {
          EnvironmentFile = [ "-${bridgeEnvFile}" ];
          ExecStartPre = lib.mkBefore [ "+${accessSetupScript}" ];
        };
      };
      supportUnits = mkAccessSetupUnit {
        name = "sinex-desktop-target-access";
        description = "Prepare target-user access for the Sinex desktop node";
        script = accessSetupScript;
        writePaths = accessWritePaths;
        beforeUnits = [ "sinex-preflight.service" ] ++ map (unit: "${unit}.service") (attrNames units);
      };
    in
    {
      inherit units supportUnits;
    };

  mkBrowserUnits =
    let
      sat = nodesCfg.browser;
      instances = resolveInstances sat.instances;
      batch = resolveBatch sat.batch;
      resources = resolveResources sat.resources;
      nodeConfig = builtins.toJSON {
        dump_sources = sat.dumpSources;
        sqlite_sources = map
          (source: {
            path = source.path;
            browser = source.browser;
            format = source.format;
          })
          sat.sqliteSources;
        polling_interval_secs = sat.pollIntervalSec;
      };
      sqlitePaths = unique (map (source: source.path) sat.sqliteSources);
      sqliteDirs = unique (map builtins.dirOf sqlitePaths);
      accessWritePaths =
        unique (
          sqliteDirs
          ++ optionals (targetHome != null) [
            targetHome
            "${targetHome}/.local"
            "${targetHome}/.local/share"
            "${targetHome}/.local/share/qutebrowser"
            "${targetHome}/.local/share/qutebrowser/webengine"
          ]
        );
      accessSetupScript =
        if sqlitePaths == [ ] then null else
        pkgs.writeShellScript "sinex-browser-target-access" ''
          set -euo pipefail

          SERVICE_USER=${escapeShellArg serviceUser}
          SETFACL=${pkgs.acl}/bin/setfacl
          DIRNAME=${pkgs.coreutils}/bin/dirname
          acl_failures=0

          record_acl_failure() {
            local path="$1"
            echo "sinex-browser-target-access: failed to grant ACLs for $path" >&2
            acl_failures=$((acl_failures + 1))
          }

          set_access_acl() {
            local path="$1"
            local acl_spec="$2"
            local mask_spec=""
            if [ "$#" -ge 3 ]; then
              mask_spec="$3"
            fi
            if [ -n "$mask_spec" ]; then
              "$SETFACL" -m "$acl_spec,m::$mask_spec" "$path" || record_acl_failure "$path"
            else
              "$SETFACL" --mask -m "$acl_spec" "$path" || record_acl_failure "$path"
            fi
          }

          set_default_acl() {
            local path="$1"
            local acl_spec="$2"
            local mask_spec=""
            if [ "$#" -ge 3 ]; then
              mask_spec="$3"
            fi
            if [ -n "$mask_spec" ]; then
              "$SETFACL" -d -m "$acl_spec,m::$mask_spec" "$path" || record_acl_failure "$path"
            else
              "$SETFACL" -d --mask -m "$acl_spec" "$path" || record_acl_failure "$path"
            fi
          }

          grant_parent_dirs() {
            local path="$1"
            local dir
            dir="$path"
            while [ "$dir" != "/" ] && [ "$dir" != "." ]; do
              if [ -d "$dir" ]; then
                set_access_acl "$dir" "u:$SERVICE_USER:--x" "--x"
              fi
              dir="$("$DIRNAME" "$dir")"
            done
          }

          grant_dir_read() {
            local path="$1"
            if [ -d "$path" ]; then
              set_access_acl "$path" "u:$SERVICE_USER:r-x" "r-x"
            fi
          }

          grant_dir_read_defaults() {
            local path="$1"
            if [ -d "$path" ]; then
              set_default_acl "$path" "u:$SERVICE_USER:r-X" "r-X"
            fi
          }

          grant_file_read() {
            local path="$1"
            if [ -f "$path" ]; then
              set_access_acl "$path" "u:$SERVICE_USER:r--" "r--"
            fi
          }

          grant_sqlite_sidecars() {
            local path="$1"
            grant_file_read "$path-wal"
            grant_file_read "$path-shm"
          }

          ${concatStringsSep "\n" (map (path: ''
            grant_parent_dirs ${escapeShellArg path}
            grant_file_read ${escapeShellArg path}
            grant_sqlite_sidecars ${escapeShellArg path}
          '') sqlitePaths)}

          ${concatStringsSep "\n" (map (path: ''
            grant_parent_dirs ${escapeShellArg path}
            grant_dir_read ${escapeShellArg path}
            grant_dir_read_defaults ${escapeShellArg path}
          '') sqliteDirs)}

          if [ "$acl_failures" -ne 0 ]; then
            exit 1
          fi
        '';
      units = mkNodeUnits {
        name = "browser";
        binary = "browser-ingestor";
        description = "Browser history node";
        inherit instances batch resources;
        extraArgs = [ "--node-config ${escapeShellArg nodeConfig}" ] ++ sat.extraArgs;
        env = [ "RUST_LOG=${nodesCfg.defaults.logLevel}" ] ++ toEnvList sat.env;
        serviceConfig = {
          ProtectHome = lib.mkForce "read-only";
          ReadWritePaths = readWritePaths ++ accessWritePaths;
        } // optionalAttrs (sat.access.bindReadOnlyPaths != [ ] && accessSetupScript == null) {
          BindReadOnlyPaths = renderBindReadOnlyPaths sat.access.bindReadOnlyPaths;
        } // optionalAttrs (accessSetupScript != null) {
          ExecStartPre = lib.mkBefore [ "+${accessSetupScript}" ];
        };
      };
      supportUnits = mkAccessSetupUnit {
        name = "sinex-browser-target-access";
        description = "Prepare target-user access for the Sinex browser node";
        script = accessSetupScript;
        writePaths = accessWritePaths;
        beforeUnits = [ "sinex-preflight.service" ] ++ map (unit: "${unit}.service") (attrNames units);
      };
    in
    {
      inherit units supportUnits;
    };

  mkSystemUnits =
    let
      sat = nodesCfg.system;
      instances = resolveInstances sat.instances;
      batch = resolveBatch sat.batch;
      resources = resolveResources sat.resources;
      nodeConfig = builtins.toJSON {
        dbus_buses = "system";
      };
    in
    mkNodeUnits {
      name = "system";
      binary = "system-ingestor";
      description = "System node";
      inherit instances batch resources;
      extraArgs = [ "--node-config ${escapeShellArg nodeConfig}" ] ++ sat.extraArgs;
      env = [ "RUST_LOG=${nodesCfg.defaults.logLevel}" ] ++ toEnvList sat.env;
      serviceConfig = {
        SupplementaryGroups = [ "systemd-journal" ];
      };
    };

  mkDocumentUnits =
    let
      sat = nodesCfg.document;
      resources = resolveResources sat.resources;
      documentRoots = unique (map toString effectiveDocumentRoots);
      nodeConfig = builtins.toJSON {
        supported_mime_types = sat.supportedMimeTypes;
        max_document_size = sat.maxDocumentSize;
        allowed_roots = documentRoots;
      };
      scanCommand = concatStringsSep " " (
        [
          "${sinexPackage}/bin/sinex-document-ingestor"
          "--service-name"
          "sinex-document-scan"
          "--node-config"
          (escapeShellArg nodeConfig)
          "scan"
          "--until"
          "snapshot"
        ]
        ++ sat.extraArgs
      );
      env = mkServiceEnv ([ "RUST_LOG=${nodesCfg.defaults.logLevel}" ] ++ toEnvList sat.env);
      requiredUnits =
        schemaApplyUnits
        ++ postgresServiceUnits
        ++ optionals natsEnabled [ "nats.service" ]
        ++ optionals natsBootstrapEnabled [ "sinex-nats-bootstrap.service" ];
      accessWritePaths =
        unique (
          optionals (targetHome != null) [ targetHome ]
          ++ documentRoots
        );
      accessSetupScript =
        if documentRoots == [ ] then null else
        pkgs.writeShellScript "sinex-document-target-access" ''
          set -euo pipefail

          SERVICE_USER=${escapeShellArg serviceUser}
          SETFACL=${pkgs.acl}/bin/setfacl
          FIND=${pkgs.findutils}/bin/find
          DIRNAME=${pkgs.coreutils}/bin/dirname
          INSTALL=${pkgs.coreutils}/bin/install
          ID=${pkgs.coreutils}/bin/id
          ${optionalString (targetUser != null) ''
          TARGET_USER=${escapeShellArg targetUser}
          TARGET_GROUP="$("$ID" -gn "$TARGET_USER")"
          ''}
          acl_failures=0

          record_acl_failure() {
            local path="$1"
            echo "sinex-document-target-access: failed to grant ACLs for $path" >&2
            acl_failures=$((acl_failures + 1))
          }

          set_access_acl() {
            local path="$1"
            local acl_spec="$2"
            local mask_spec=""
            if [ "$#" -ge 3 ]; then
              mask_spec="$3"
            fi
            if [ -n "$mask_spec" ]; then
              "$SETFACL" -m "$acl_spec,m::$mask_spec" "$path" || record_acl_failure "$path"
            else
              "$SETFACL" --mask -m "$acl_spec" "$path" || record_acl_failure "$path"
            fi
          }

          set_default_acl() {
            local path="$1"
            local acl_spec="$2"
            local mask_spec=""
            if [ "$#" -ge 3 ]; then
              mask_spec="$3"
            fi
            if [ -n "$mask_spec" ]; then
              "$SETFACL" -d -m "$acl_spec,m::$mask_spec" "$path" || record_acl_failure "$path"
            else
              "$SETFACL" -d --mask -m "$acl_spec" "$path" || record_acl_failure "$path"
            fi
          }

          grant_parent_dirs() {
            local path="$1"
            local dir
            dir="$path"
            while [ "$dir" != "/" ] && [ "$dir" != "." ]; do
              if [ -d "$dir" ]; then
                set_access_acl "$dir" "u:$SERVICE_USER:--x" "--x"
              fi
              dir="$("$DIRNAME" "$dir")"
            done
          }

          grant_recursive_document_access() {
            local path="$1"

            if [ ! -e "$path" ] && [ -n "''${TARGET_USER:-}" ]; then
              "$INSTALL" -d -m 0750 -o "$TARGET_USER" -g "$TARGET_GROUP" "$path" || record_acl_failure "$path"
            fi

            if [ -f "$path" ]; then
              grant_parent_dirs "$path"
              set_access_acl "$path" "u:$SERVICE_USER:r--" "r--"
              return
            fi

            if [ ! -d "$path" ]; then
              return
            fi

            grant_parent_dirs "$path"
            set_access_acl "$path" "u:$SERVICE_USER:r-X" "r-X"
            "$SETFACL" -R -m "u:$SERVICE_USER:r-X,m::r-X" "$path" || record_acl_failure "$path"
            while IFS= read -r dir; do
              [ -n "$dir" ] || continue
              set_default_acl "$dir" "u:$SERVICE_USER:r-X" "r-X"
            done < <("$FIND" "$path" -type d)
          }

          ${concatStringsSep "\n" (map (path: ''
            grant_recursive_document_access ${escapeShellArg path}
          '') documentRoots)}

          if [ "$acl_failures" -ne 0 ]; then
            exit 1
          fi
        '';
      documentService = {
        description = "Sinex document snapshot scan";
        after = requiredUnits;
        requires = requiredUnits;
        wants = optionals coreEnabled [ "sinex-ingestd.service" ];
        unitConfig = existingPathAssertions (databaseSecretAssertPaths ++ natsSecretAssertPaths);
        path = optionals cfg.storage.blob.enable [ pkgs.git pkgs.git-annex ];
        serviceConfig = (mkBaseServiceConfig resources env {
          Type = lib.mkForce "oneshot";
          Restart = lib.mkForce "no";
          WatchdogSec = lib.mkForce "0";
          ProtectHome = lib.mkForce "read-only";
          ReadWritePaths = readWritePaths ++ accessWritePaths;
          WorkingDirectory = stateRoot;
          ExecStart = mkDatabasePasswordExec {
            name = "document-scan";
            command = scanCommand;
            passwordFile = if cfg.database.enable then effectiveDatabasePasswordFile else null;
          };
        }) // optionalAttrs (accessSetupScript != null) {
          ExecStartPre = lib.mkBefore [ "+${accessSetupScript}" ];
        };
      };
      units = {
        "sinex-document-scan" =
          documentService
          // optionalAttrs sat.runOnBoot {
            wantedBy = [ "multi-user.target" ];
          };
      };
      supportUnits = mkAccessSetupUnit {
        name = "sinex-document-target-access";
        description = "Prepare target-user access for the Sinex document scan";
        script = accessSetupScript;
        writePaths = accessWritePaths;
        beforeUnits = [ "sinex-preflight.service" ] ++ map (unit: "${unit}.service") (attrNames units);
      };
    in
    {
      inherit units supportUnits;
    };

  mkNodeUnits = params:
    let
      instances = params.instances;
      resources = params.resources;
      extraArgs = params.extraArgs or [ ];
      envExtras = params.env or [ ];
      unitPath = params.path or [ ];
      serviceConfigOverrides = params.serviceConfig or { };
      afterUnits = schemaApplyUnits ++ optionals coreEnabled [ "sinex-ingestd.service" ];
      requireUnits = schemaApplyUnits;
      # Nodes publish to NATS and don't strictly require ingestd to be up.
      # Use `wants` so that ingestd going down doesn't cascade-stop all nodes;
      # NATS will buffer events until ingestd recovers.
      wantsUnits = optionals coreEnabled [ "sinex-ingestd.service" ];
      execArgs = concatStringsSep " " (
        [ "--service-name sinex-${params.name}" ] ++ extraArgs ++ [ "service" ]
      );
      env = mkServiceEnv envExtras;
      mkUnit = instance: {
        description = "${params.description} (instance ${toString instance})";
        wantedBy = [ "multi-user.target" ];
        after = afterUnits;
        requires = requireUnits;
        wants = wantsUnits;
        unitConfig = restartRateLimits // existingPathAssertions (databaseSecretAssertPaths ++ natsSecretAssertPaths);
        path = unitPath;
        serviceConfig = mkBaseServiceConfig resources env ({
          ExecStart = mkDatabasePasswordExec {
            name = "${params.name}-${toString instance}";
            command = "${sinexPackage}/bin/sinex-${params.binary} ${execArgs}";
            passwordFile = if cfg.database.enable then effectiveDatabasePasswordFile else null;
          };
          WorkingDirectory = stateRoot;
        } // serviceConfigOverrides);
      };
    in
    if instances <= 0 then { } else
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
      extraArgs = params.extraArgs or [ ];
      execArgs = concatStringsSep " " (
        [ "--service-name sinex-${params.binary}" ] ++ extraArgs ++ [ "service" ]
      );
      env = mkServiceEnv ([ "RUST_LOG=${nodesCfg.defaults.logLevel}" ] ++ toEnvList params.env);
    in
    {
      description = params.description;
      wantedBy = [ "multi-user.target" ];
      after = schemaApplyUnits ++ postgresServiceUnits ++ optionals coreEnabled [ "sinex-ingestd.service" ];
      requires = schemaApplyUnits ++ postgresServiceUnits;
      unitConfig = restartRateLimits // existingPathAssertions (databaseSecretAssertPaths ++ natsSecretAssertPaths);
      serviceConfig = mkBaseServiceConfig resources env {
        ExecStart = mkDatabasePasswordExec {
          name = params.binary;
          command = "${sinexPackage}/bin/sinex-${params.binary} ${execArgs}";
          passwordFile = if cfg.database.enable then effectiveDatabasePasswordFile else null;
        };
        WorkingDirectory = stateRoot;
      };
    };

  automataServices =
    if !(nodesEnabled && nodesCfg.automata.enable) then { } else
    let
      canon = nodesCfg.automata.canonicalizer;
      health = nodesCfg.automata.healthAggregator;
      analytics = nodesCfg.automata.analyticsAutomaton;
      session = nodesCfg.automata.sessionDetector;
      canonicalizerUnit =
        if !canon.enable then { } else {
          "sinex-canonicalizer" = mkAutomataUnit {
            binary = "terminal-command-canonicalizer";
            description = "Sinex canonical command synthesizer";
            profile = canon.profile;
            env = canon.env;
            extraArgs = [ ];
          };
        };
      healthUnit =
        if !health.enable then { } else {
          "sinex-health-automaton" = mkAutomataUnit {
            binary = "health-automaton";
            description = "Sinex health automaton";
            profile = health.profile;
            env = health.env;
            extraArgs = [ ];
          };
        };
      analyticsUnit =
        if !analytics.enable then { } else {
          "sinex-analytics-automaton" = mkAutomataUnit {
            binary = "analytics-automaton";
            description = "Sinex analytics automaton";
            profile = analytics.profile;
            env = analytics.env;
            extraArgs = [ ];
          };
        };
      sessionUnit =
        if !session.enable then { } else {
          "sinex-session-detector" = mkAutomataUnit {
            binary = "session-detector";
            description = "Sinex session detector";
            profile = session.profile;
            env = session.env;
            extraArgs = [ ];
          };
        };
    in
    canonicalizerUnit // healthUnit // analyticsUnit // sessionUnit;

  nodeservices =
    if !nodesEnabled then { units = { }; supportUnits = { }; } else
    let
      filesystemUnits = if nodesCfg.filesystem.enable then mkFilesystemUnits else { };
      terminalUnits =
        if nodesCfg.terminal.enable then mkTerminalUnits else { units = { }; supportUnits = { }; };
      browserUnits =
        if nodesCfg.browser.enable then mkBrowserUnits else { units = { }; supportUnits = { }; };
      desktopUnits =
        if nodesCfg.desktop.enable then mkDesktopUnits else { units = { }; supportUnits = { }; };
      systemUnits = if nodesCfg.system.enable then mkSystemUnits else { };
    in
    {
      units =
        filesystemUnits
        // terminalUnits.units
        // browserUnits.units
        // desktopUnits.units
        // systemUnits;
      supportUnits =
        terminalUnits.supportUnits
        // browserUnits.supportUnits
        // desktopUnits.supportUnits;
    };

  documentScanService =
    if !(nodesEnabled && nodesCfg.document.enable) then { units = { }; supportUnits = { }; } else mkDocumentUnits;

  coreServices = mkCoreServices;

  accessSupportServices = nodeservices.supportUnits // documentScanService.supportUnits;

  generatedUnits = attrNames nodeservices.units ++ attrNames automataServices;
  preflightSupportUnits = map (unit: "${unit}.service") (attrNames accessSupportServices);

in
{
  # Internal option declared here to break the evaluation cycle.
  # node-services.nix reads config.services.sinex (via cfg) and must
  # communicate generated unit names to preflight-verification.nix.
  # Writing back to services.sinex.nodes.* from a module that reads
  # config.services.sinex causes infinite recursion because the module
  # system must merge all definitions of the submodule to evaluate any
  # sub-option.  A separate top-level path avoids the cycle.
  options.sinex._generatedUnits = mkOption {
    type = with types; listOf str;
    default = [ ];
    internal = true;
    description = "Systemd units generated by node-services.nix (internal, breaks cycle).";
  };

  options.sinex._preflightSupportUnits = mkOption {
    type = with types; listOf str;
    default = [ ];
    internal = true;
    description = "Support units that must run before Sinex preflight verification.";
  };

  config = mkMerge [
    (mkIf sinexEnabled {
      systemd.services = mkMerge [
        coreServices
        nodeservices.units
        nodeservices.supportUnits
        documentScanService.units
        documentScanService.supportUnits
        automataServices
      ];
      systemd.timers = mkMerge [
        (optionalAttrs (nodesEnabled && nodesCfg.document.enable && nodesCfg.document.schedule != null) {
          "sinex-document-scan" = {
            description = "Schedule Sinex document snapshot scans";
            wantedBy = [ "timers.target" ];
            timerConfig = {
              OnCalendar = nodesCfg.document.schedule;
              Persistent = nodesCfg.document.persistentTimer;
            };
          };
        })
      ];
    })
    { sinex._generatedUnits = generatedUnits; }
    { sinex._preflightSupportUnits = preflightSupportUnits; }
  ];
}
