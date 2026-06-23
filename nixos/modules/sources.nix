{ config, lib, pkgs, ... }:

with lib;

let
  systemdHardening = import ./lib/systemd-hardening.nix { inherit lib; };
  databaseRuntime = import ./lib/database-runtime.nix { inherit lib pkgs; };
  secretResolution = import ./lib/secret-resolution.nix { inherit lib; };
  automataLib = import ./lib/automata.nix { inherit lib; };
  sourceCatalog = import ./lib/source-catalog.nix { inherit lib; };
  inherit (systemdHardening) mkHelperServiceConfig;
  inherit (databaseRuntime)
    mkDatabasePasswordExec
    renderDatabaseUrl
    ;
  inherit (secretResolution) resolveNamedSecretPath;
  cfg = config.services.sinex;
  coreCfg = cfg.core;
  runtimeCfg = cfg.runtime;
  sourceCfg = cfg.sources;
  automataCfg = cfg.automata;

  sinexEnabled = cfg.enable;
  coreEnabled = sinexEnabled && coreCfg.enable;
  runtimeEnabled = sinexEnabled && runtimeCfg.enable;
  sourceRuntimeEnabled = runtimeEnabled && sourceCfg.enable;
  natsEnabled = cfg.nats.enable || cfg.nats.autoSetup;
  localPostgresEnabled = cfg.database.enable && (cfg.database.autoSetup || config.services.postgresql.enable);

  stateRoot = cfg.stateRoot;
  runtimeDir = "${stateRoot}/run";
  ingestSpool = coreCfg.event_engine.spoolDir;
  logDir = cfg.observability.logDir;
  blobDir = cfg.storage.blob.repositoryPath;
  tlsDir = "${stateRoot}/tls";
  tlsAutoGenEnabled = coreEnabled && coreCfg.api.autoGenerateTls;
  # Ancillary service flags.
  # JetStream bootstrap is a hard requirement when enabled because event_engine and
  # API assume the streams already exist at startup. Blob init remains soft.
  natsBootstrapEnabled = natsEnabled && cfg.nats.bootstrapStreams.enable;
  blobInitEnabled = cfg.storage.blob.enable && cfg.storage.blob.autoInit;
  postgresServiceUnits = optionals localPostgresEnabled [ "postgresql.service" "postgresql-setup.service" ];

  genTlsScript = pkgs.writeShellScript "sinex-tls-init" ''
    set -euo pipefail
    cert="${tlsDir}/server.pem"
    key="${tlsDir}/server-key.pem"
    ca="${tlsDir}/ca.pem"
    if [[ -f "$cert" && -f "$key" && -f "$ca" ]]; then
      echo "Sinex API TLS credentials already present, skipping."
      exit 0
    fi
    mkdir -p "${tlsDir}"
    chmod 750 "${tlsDir}"
    "${adminPackage}/bin/xtask" --format human infra tls-init-gateway \
      --output-dir "${tlsDir}" \
      --san localhost \
      --san 127.0.0.1 \
      --ca-name "Sinex API CA"
    # API needs the server cert, server key, and trust anchor at runtime.
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
    echo "Sinex API TLS credentials generated at ${tlsDir}"
  '';

  sinexPackage = cfg.package;
  adminPackage = cfg.adminPackage;
  serviceUser = cfg.users.runtime;

  databaseUrl = renderDatabaseUrl cfg.database;

  natsUrl = concatStringsSep "," runtimeCfg.nats.servers;
  secretPaths = config.sinex.secrets.paths or { };
  resolveSecretPath = resolveNamedSecretPath secretPaths;
  apiAdminTokenFile =
    resolveSecretPath cfg.secrets.apiAdminTokenFile [
      "sinex-api-admin-token"
    ];
  effectiveDatabasePasswordFile = resolveSecretPath cfg.database.passwordFile [
    "sinex-local-db"
    "sinex-remote-db"
  ];
  natsTlsCfg = runtimeCfg.nats.tls;
  natsAuthCfg = runtimeCfg.nats.auth;
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
    || any (server: hasPrefix "tls://" server || hasPrefix "wss://" server) runtimeCfg.nats.servers;
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
  apiSecretAssertPaths = collectReadablePaths (
    [
      apiAdminTokenFile
      cfg.core.api.tlsCertFile
      cfg.core.api.tlsKeyFile
    ]
    ++ optionals coreCfg.api.requireClientTLS [ cfg.core.api.tlsClientCAFile ]
  );
  existingPathAssertions = paths:
    let
      existingPaths = collectReadablePaths paths;
    in
    optionalAttrs (existingPaths != [ ]) { AssertPathExists = existingPaths; };

  toEnvList = envAttrs: mapAttrsToList (name: value: "${name}=${value}") envAttrs;
  renderBindReadOnlyPaths = mounts:
    map (mount: "${mount.source}:${mount.destination}") mounts;

  # Shared bash ACL helper functions injected into multiple writeShellScript calls.
  # set_access_acl / set_default_acl / grant_parent_dirs are used by all ACL scripts.
  commonBaseAclFunctions = ''
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
  '';

  # Additional read-access helpers used by terminal and browser ACL scripts.
  commonReadAclFunctions = ''
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

    # RW helpers used by sources whose SQLite DBs are in WAL mode with a live
    # writer (atuin, fish, qutebrowser, ActivityWatch). SQLite checkpoints
    # the WAL on open, which requires write access to the DB + sidecars +
    # the containing directory. See #1325.
    grant_file_readwrite() {
      local path="$1"
      if [ -f "$path" ]; then
        set_access_acl "$path" "u:$SERVICE_USER:rw-" "rw-"
      fi
    }

    grant_sqlite_sidecars_rw() {
      local path="$1"
      grant_file_readwrite "$path-wal"
      grant_file_readwrite "$path-shm"
    }

    grant_dir_readwrite() {
      local path="$1"
      if [ -d "$path" ]; then
        set_access_acl "$path" "u:$SERVICE_USER:rwx" "rwx"
      fi
    }
  '';

  baseEnv = optional cfg.database.enable "DATABASE_URL=${databaseUrl}" ++ [
    # Propagate environment name so service subjects match bootstrapped stream prefixes.
    # Must stay in sync with services.sinex.nats.environment.
    "SINEX_ENVIRONMENT=${cfg.nats.environment}"
    "SINEX_STATE_DIR=${stateRoot}"
    "SINEX_RUNTIME_DIR=${runtimeDir}"
    "SINEX_LOG_DIR=${logDir}"
    "SINEX_NATS_URL=${natsUrl}"
    "SINEX_NATS_MONITORING_PORT=${toString runtimeCfg.nats.monitoringPort}"
    # Both event_engine and API access the same content-store root; set here
    # so all core services share a consistent path without per-service repetition.
    "SINEX_CONTENT_STORE_PATH=${blobDir}"
  ]
    ++ optional inferredNatsTls "SINEX_NATS_REQUIRE_TLS=1"
    ++ optional (effectiveNatsCaCertFile != null) "SINEX_NATS_CA_CERT=${toString effectiveNatsCaCertFile}"
    ++ optional (effectiveNatsClientCertFile != null) "SINEX_NATS_CLIENT_CERT=${toString effectiveNatsClientCertFile}"
    ++ optional (effectiveNatsClientKeyFile != null) "SINEX_NATS_CLIENT_KEY=${toString effectiveNatsClientKeyFile}"
    ++ optional (effectiveNatsTokenFile != null) "SINEX_NATS_TOKEN_FILE=${toString effectiveNatsTokenFile}"
    ++ optional (effectiveNatsCredsFile != null) "SINEX_NATS_CREDS_FILE=${toString effectiveNatsCredsFile}"
    ++ optional (effectiveNatsNkeySeedFile != null) "SINEX_NATS_NKEY_SEED_FILE=${toString effectiveNatsNkeySeedFile}"
    ++ toEnvList runtimeCfg.defaults.env;

  coordinationEnv =
    if runtimeCfg.coordination.enable then [
      "SINEX_COORDINATION_ENABLED=1"
      "SINEX_COORDINATION_HEARTBEAT=${toString runtimeCfg.coordination.heartbeatSec}"
      "SINEX_COORDINATION_TIMEOUT=${toString runtimeCfg.coordination.leadershipTimeoutSec}"
      "SINEX_COORDINATION_HANDOFF=${toString runtimeCfg.coordination.handoffTimeoutSec}"
    ] else [ ];

  resolveResources = nodeResources:
    if nodeResources == null then runtimeCfg.defaults.resources else nodeResources;

  resolveSourceResources = sourceId: nodeResources:
    if nodeResources == null then sourceCatalog.resourceDefaultsFor sourceId else nodeResources;

  resolveInstances = nodeInstances:
    if nodeInstances == null then runtimeCfg.defaults.instances else nodeInstances;

  resolveSourceInstances = sourceId: nodeInstances:
    if nodeInstances == null then sourceCatalog.instanceDefaultFor sourceId else nodeInstances;

  catalogMetadataFor = sourceId: sourceCatalog.manifestMetadataFor sourceId;

  renderResources = resources: {
    MemoryHigh = resources.memoryHigh;
    CPUWeight = resources.cpuWeight;
    IOWeight = resources.ioWeight;
    IOSchedulingClass = resources.ioSchedulingClass;
    Nice = resources.nice;
    TimeoutStopSec = resources.shutdownTimeoutSec;
    # Single-daemon sinexd hosts every automaton + source binding;
    # DB pool warm-up + per-binding init can exceed the 30s default before
    # the supervisor calls sd_notify(READY=1).
    TimeoutStartSec = 600;
  } // optionalAttrs (resources.memoryMax != null) {
    MemoryMax = resources.memoryMax;
  } // optionalAttrs (resources.cpuQuota != null) {
    CPUQuota = resources.cpuQuota;
  } // optionalAttrs (resources.openFilesLimit != null) {
    LimitNOFILE = "${toString resources.openFilesLimit}:${toString resources.openFilesLimit}";
  };

  readWritePaths = [
    stateRoot
    runtimeDir
    ingestSpool
    logDir
    blobDir
  ];
  restartRateLimits = {
    # Long-running capture services must recover after transient infra outages
    # without requiring a manual `systemctl reset-failed`. Defaults disable the
    # limit; workstations bound it via services.sinex.runtime.restartPolicy.
    StartLimitIntervalSec = cfg.runtime.restartPolicy.intervalSec;
    StartLimitBurst = cfg.runtime.restartPolicy.burst;
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
    if sourceCfg.document.allowedRoots != [ ] then sourceCfg.document.allowedRoots
    else if targetHome == null then [ ]
    else [ "${targetHome}/Documents" ];
  terminalSourceIdForShell = shell:
    let
      normalized = toLower shell;
    in
    if normalized == "atuin" then "terminal.atuin-history"
    else if normalized == "zsh" then "terminal.zsh-history"
    else if normalized == "fish" then "terminal.fish-history"
    else if normalized == "bash" then "terminal.bash-history"
    else "terminal.text-history";

  mkBaseServiceConfig = resources: env: extra:
    {
      Type = "notify";
      User = serviceUser;
      Group = serviceUser;
      Restart = cfg.runtime.restartPolicy.mode;
      RestartSec = cfg.runtime.restartPolicy.backoffSec;
      # 60s watchdog; the spawn_watchdog impl runs the pinger on a
      # dedicated std::thread (not a tokio task), so heavy COPY batches
      # on the async runtime can't starve the ping.
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

  mkAccessSetupUnit =
    { name
    , description
    , script
    , writePaths ? [ ]
    , afterUnits ? [ ]
    , wantsUnits ? [ ]
    , beforeUnits ? [ ]
    ,
    }:
    if script == null then { } else {
      "${name}" = {
        inherit description;
        after = afterUnits;
        wants = wantsUnits;
        before = beforeUnits;
        # `before = beforeUnits` only ORDERS this unit — it does not pull
        # the unit in. Without `requiredBy`, the workers in `beforeUnits`
        # start without ever triggering the access setup, so `setfacl`
        # never runs. The workers then fail with permission errors on
        # target-user paths — e.g. the ActivityWatch SQLite open returns
        # "unable to open database file" because the sinex service user
        # can't traverse into /home/<target>.
        #
        # `requiredBy = beforeUnits` is the inverse of `Requires=` on the
        # workers: when those workers start, systemd pulls this unit in
        # AND waits for it to succeed. Combined with `before = beforeUnits`
        # this gives both ordering AND activation.
        requiredBy = beforeUnits;
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

  mkCoreServices = { automataEnabledList, sourceBindingsManifestFile, runtimeOverlay }:
    let
      batch = coreCfg.event_engine.batch;
      sinexdArgs = concatStringsSep " " ([
        "--nats-url ${natsUrl}"
        "--log-level ${coreCfg.event_engine.logLevel}"
      ] ++ coreCfg.event_engine.extraArgs);
      apiLimits = coreCfg.api.limits;
      apiAdminTokenRuntimeFile = "${runtimeDir}/api-admin-token";
      apiTlsCertRuntimeFile = "${runtimeDir}/api-server.pem";
      apiTlsKeyRuntimeFile = "${runtimeDir}/api-server-key.pem";
      apiTlsClientCaRuntimeFile = "${runtimeDir}/api-client-ca.pem";
      apiTlsTrustAnchorRuntimeFile = "${runtimeDir}/api-ca.pem";
      apiTlsTrustAnchorSourceFile =
        if cfg.core.api.autoGenerateTls then
          "${tlsDir}/ca.pem"
        else
          null;
      apiRuntimeInputStageScript =
        if apiAdminTokenFile == null
          && cfg.core.api.tlsCertFile == null
          && cfg.core.api.tlsKeyFile == null
          && apiTlsTrustAnchorSourceFile == null
          && cfg.core.api.tlsClientCAFile == null
        then null else
          pkgs.writeShellScript "sinexd-stage-runtime-inputs" ''
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

            stage_api_admin_token() {
              local source_path="$1"
              local dest_path="$2"
              local tmp_path
              local raw_token
              local staged_token

              if [ -z "$source_path" ]; then
                return 0
              fi

              if [ ! -r "$source_path" ]; then
                echo "[sinex] API admin token $source_path is not readable" >&2
                exit 1
              fi

              raw_token="$(${pkgs.coreutils}/bin/cat "$source_path")"
              raw_token="$(${pkgs.coreutils}/bin/printf '%s' "$raw_token" | ${pkgs.gnused}/bin/sed -e 's/[[:space:]]*$//')"
              if [ -z "$raw_token" ]; then
                echo "[sinex] API admin token $source_path is empty" >&2
                exit 1
              fi

              case "$raw_token" in
                *:admin)
                  staged_token="$raw_token"
                  ;;
                *:readonly|*:write)
                  echo "[sinex] API admin token $source_path must be raw or already end with :admin" >&2
                  exit 1
                  ;;
                *)
                  staged_token="$raw_token:admin"
                  ;;
              esac

              tmp_path="$(mktemp "$runtime_dir/api-admin-token.XXXXXX")"
              ${pkgs.coreutils}/bin/chmod 0600 "$tmp_path"
              ${pkgs.coreutils}/bin/printf '%s\n' "$staged_token" > "$tmp_path"
              ${pkgs.coreutils}/bin/chown ${serviceUser}:${serviceUser} "$tmp_path"
              ${pkgs.coreutils}/bin/chmod 0400 "$tmp_path"
              ${pkgs.coreutils}/bin/mv "$tmp_path" "$dest_path"
            }

            "$INSTALL" -d -m 0750 -o ${serviceUser} -g ${serviceUser} "$runtime_dir"

            stage_api_admin_token ${escapeShellArg (if apiAdminTokenFile == null then "" else toString apiAdminTokenFile)} ${escapeShellArg apiAdminTokenRuntimeFile}
            stage_file ${escapeShellArg (if cfg.core.api.tlsCertFile == null then "" else toString cfg.core.api.tlsCertFile)} ${escapeShellArg apiTlsCertRuntimeFile} 0440
            stage_file ${escapeShellArg (if cfg.core.api.tlsKeyFile == null then "" else toString cfg.core.api.tlsKeyFile)} ${escapeShellArg apiTlsKeyRuntimeFile} 0400
            stage_file ${escapeShellArg (if apiTlsTrustAnchorSourceFile == null then "" else toString apiTlsTrustAnchorSourceFile)} ${escapeShellArg apiTlsTrustAnchorRuntimeFile} 0444
            stage_file ${escapeShellArg (if cfg.core.api.tlsClientCAFile == null then "" else toString cfg.core.api.tlsClientCAFile)} ${escapeShellArg apiTlsClientCaRuntimeFile} 0440
          '';
      # Ordering for core services.
      # Base: hard infrastructure that both services depend on.
      coreRequires =
        postgresServiceUnits
        ++ optionals natsEnabled [ "nats.service" ]
        ++ optionals natsBootstrapEnabled [ "sinex-nats-bootstrap.service" ];
      # Core services should not start before stream bootstrap has succeeded when
      # the managed bootstrap path is enabled.
      coreAfter = coreRequires ++ optionals blobInitEnabled [ "sinex-blob-init.service" ];
      coreWants = optionals blobInitEnabled [ "sinex-blob-init.service" ];
      apiAfter = coreAfter ++ optionals tlsAutoGenEnabled [ "sinex-tls-init.service" ];
    in
    if !coreEnabled then { } else
    {
      "sinexd" = {
        description = "Sinex daemon (event engine + API + automata + hosted source bindings)";
        wantedBy = lib.optional cfg.runtime.target.attachToMultiUser "multi-user.target";
        restartIfChanged = cfg.runtime.restartOnSwitch;
        after = apiAfter ++ (runtimeOverlay.afterUnits or [ ]);
        requires = coreRequires ++ optionals tlsAutoGenEnabled [ "sinex-tls-init.service" ];
        wants = coreWants ++ (runtimeOverlay.wantsUnits or [ ]);
        unitConfig =
          restartRateLimits
          // { PartOf = [ "sinex-runtime.target" ]; }
          // existingPathAssertions (
            databaseSecretAssertPaths ++ natsSecretAssertPaths ++ apiSecretAssertPaths
          );
        serviceConfig = mkBaseServiceConfig coreCfg.api.resources
          (
            mkServiceEnv (
              [
                "RUST_LOG=${coreCfg.event_engine.logLevel}"
                # Event engine pool size and timeouts.
                "SINEX_EVENT_ENGINE_POOL_SIZE=${toString cfg.database.connectionPool.maxConnections}"
                "SINEX_EVENT_ENGINE_POOL_ACQUIRE_TIMEOUT_SECS=${toString cfg.database.connectionPool.connectionTimeout}"
                "SINEX_EVENT_ENGINE_POOL_IDLE_TIMEOUT_SECS=${toString cfg.database.connectionPool.idleTimeout}"
                # Ack-pending limits (read via env by EventEngineConfig::from_args).
                "SINEX_EVENT_ENGINE_CONSUMER_MAX_ACK_PENDING=${toString coreCfg.event_engine.consumerMaxAckPending}"
                "SINEX_EVENT_ENGINE_MATERIAL_SLICES_MAX_ACK_PENDING=${toString coreCfg.event_engine.materialSlicesMaxAckPending}"
                # Explicit work and spool dirs prevent fallback to dirs::cache_dir() (~/.cache)
                # which is blocked by ProtectHome = true in the systemd unit.
                "SINEX_EVENT_ENGINE_WORK_DIR=${stateRoot}/event_engine/work"
                "SINEX_MATERIAL_ASSEMBLER_DIR=${ingestSpool}"
                # Schema and validation behaviour.
                "SINEX_SCHEMA_APPLY_ON_STARTUP=${if cfg.database.enable && cfg.database.autoSetup then "1" else "0"}"
                "SINEX_SKIP_SCHEMA_SYNC=${if coreCfg.event_engine.skipSchemaSync then "true" else "false"}"
                "SINEX_EVENT_ENGINE_STRICT_VALIDATION=${if coreCfg.event_engine.strictValidation then "true" else "false"}"
                "SINEX_VALIDATE_SCHEMAS=${if coreCfg.event_engine.validateSchemas then "true" else "false"}"
                # Skip internal stream bootstrap when the module manages streams declaratively.
                "SINEX_NATS_STREAMS_MANAGED_EXTERNALLY=${if natsBootstrapEnabled then "true" else "false"}"
                # Operational intervals.
                "SINEX_EVENT_ENGINE_SCHEMA_RELOAD_INTERVAL_SECS=${toString coreCfg.event_engine.schemaReloadIntervalSecs}"
                "SINEX_EVENT_ENGINE_TELEMETRY_INTERVAL_SECS=${toString coreCfg.event_engine.telemetryIntervalSecs}"
                # API config.
                "SINEX_API_MAX_CONCURRENCY=${toString apiLimits.maxConcurrency}"
                "SINEX_API_REQUEST_TIMEOUT_SECS=${toString apiLimits.requestTimeoutSec}"
                "SINEX_API_MAX_BODY_BYTES=${toString apiLimits.maxBodyBytes}"
                "SINEX_API_MAX_BLOB_BYTES=${toString apiLimits.maxBlobBytes}"
                "SINEX_API_POOL_MAX_CONNECTIONS=${toString cfg.database.connectionPool.maxConnections}"
                "SINEX_API_POOL_MIN_CONNECTIONS=${toString cfg.database.connectionPool.minConnections}"
                "SINEX_API_POOL_ACQUIRE_TIMEOUT_SECS=${toString cfg.database.connectionPool.connectionTimeout}"
                "SINEX_API_RATE_LIMIT_ENABLED=${if coreCfg.api.limits.rateLimit.enable then "true" else "false"}"
                "SINEX_API_RATE_LIMIT_REQUESTS_PER_SEC=${toString coreCfg.api.limits.rateLimit.requestsPerSec}"
                "SINEX_API_RATE_LIMIT_BURST=${toString coreCfg.api.limits.rateLimit.burst}"
                "SINEX_API_RATE_LIMIT_IDLE_TIMEOUT_SECS=${toString coreCfg.api.limits.rateLimit.idleTimeoutSec}"
                "SINEX_API_RATE_LIMIT_WINDOW_SECS=${toString coreCfg.api.limits.rateLimit.distributedWindowSec}"
                "SINEX_NATIVE_MESSAGING_MAX_SIZE_BYTES=${toString coreCfg.api.nativeMessagingMaxSizeBytes}"
                "SINEX_API_TCP_LISTEN=${coreCfg.api.listenAddress}"
                # Collapsed-daemon selectors: tell sinexd which automata to
                # host internally and where to load the source-binding
                # manifest. Empty/unset = host none in that subsystem.
                "SINEX_AUTOMATA_ENABLED=${concatStringsSep "," automataEnabledList}"
              ]
              ++ lib.optional (sourceBindingsManifestFile != null)
                "SINEX_SOURCE_BINDINGS_PATH=${toString sourceBindingsManifestFile}"
              ++ lib.optional
                (coreCfg.event_engine.blobGcIntervalSecs != null)
                "SINEX_EVENT_ENGINE_BLOB_GC_INTERVAL_SECS=${toString coreCfg.event_engine.blobGcIntervalSecs}"
              ++ optional (apiAdminTokenFile != null) "SINEX_API_ADMIN_TOKEN_FILE=${apiAdminTokenRuntimeFile}"
              ++ optional (cfg.core.api.tlsCertFile != null) "SINEX_API_TLS_CERT=${apiTlsCertRuntimeFile}"
              ++ optional (cfg.core.api.tlsKeyFile != null) "SINEX_API_TLS_KEY=${apiTlsKeyRuntimeFile}"
              ++ optional (cfg.core.api.tlsClientCAFile != null) "SINEX_API_TLS_CLIENT_CA=${apiTlsClientCaRuntimeFile}"
              ++ optional (coreCfg.api.requireClientTLS) "SINEX_API_REQUIRE_CLIENT_TLS=1"
              ++ optional (coreCfg.api.corsOrigins != null) "SINEX_API_CORS_ORIGINS=${coreCfg.api.corsOrigins}"
              ++ (runtimeOverlay.environment or [ ])
            )
          )
          (
            {
              Type = lib.mkForce "notify";
              NotifyAccess = "main";
              LogRateLimitIntervalSec = "10s";
              LogRateLimitBurst = 500;
              ExecStart = mkDatabasePasswordExec {
                name = "sinexd";
                command = "${sinexPackage}/bin/sinexd ${sinexdArgs} serve";
                passwordFile = if cfg.database.enable then effectiveDatabasePasswordFile else null;
              };
            }
            // optionalAttrs (apiRuntimeInputStageScript != null || (runtimeOverlay.execStartPre or [ ]) != [ ]) {
              ExecStartPre = lib.mkBefore (
                lib.optional (apiRuntimeInputStageScript != null) "+${apiRuntimeInputStageScript}"
                ++ (runtimeOverlay.execStartPre or [ ])
              );
            }
            // optionalAttrs ((runtimeOverlay.readWritePaths or [ ]) != [ ]) {
              ReadWritePaths = lib.mkForce (unique (readWritePaths ++ runtimeOverlay.readWritePaths));
            }
            // optionalAttrs ((runtimeOverlay.protectHome or null) != null) {
              ProtectHome = lib.mkForce runtimeOverlay.protectHome;
            }
            // optionalAttrs ((runtimeOverlay.environmentFile or [ ]) != [ ]) {
              EnvironmentFile = runtimeOverlay.environmentFile;
            }
            // optionalAttrs ((runtimeOverlay.supplementaryGroups or [ ]) != [ ]) {
              SupplementaryGroups = unique runtimeOverlay.supplementaryGroups;
            }
            // optionalAttrs ((runtimeOverlay.serviceConfig or { }) != { } && coreCfg.api.resources.memoryMax == null) runtimeOverlay.serviceConfig
          );
        path = optionals (cfg.storage.blob.enable && cfg.storage.blob.legacyAnnexData) [ pkgs.git pkgs.git-annex ]
          ++ (runtimeOverlay.path or [ ]);
      };
    }
    // optionalAttrs tlsAutoGenEnabled {
      "sinex-tls-init" = {
        description = "Generate Sinex API TLS credentials";
        wantedBy = [ "multi-user.target" ];
        before = [ "sinexd.service" ];
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

  # ── Filesystem support glue ─────────────────────────────────────────────
  # Filesystem binding contribution consumed by the in-process sinexd source
  # binding manifest below.
  mkFilesystemBindings =
    let
      sat = sourceCfg.filesystem;
      instances = resolveSourceInstances "fs" sat.instances;
      resources = resolveSourceResources "fs" sat.resources;
      runtimeConfig = {
        watch_paths = sat.watchPaths;
        max_depth = 10;
        follow_symlinks = false;
        max_capture_bytes = 10485760;
        max_watches = sat.maxWatches;
        ignored_directory_names = sat.ignoredDirectoryNames;
        ignored_file_suffixes = sat.ignoredFileSuffixes;
      };
    in
    {
      bindings = {
        "fs" = {
          enable = sat.enable;
          description = "Filesystem watcher (hosted source binding)";
          adapterType = null;
          adapterConfig = runtimeConfig;
          inherit instances resources;
          catalogMetadata = catalogMetadataFor "fs";
          extraArgs = sat.extraArgs;
          extraEnv = { RUST_LOG = runtimeCfg.defaults.logLevel; } // sat.env;
          serviceConfigOverrides = { };
        };
      };
      overlay = {
        protectHome = "read-only";
      };
    };

  # ── Terminal support glue ───────────────────────────────────────────────
  # Returns source bindings plus the ACL setup one-shot service.
  mkTerminalGlue =
    let
      sat = sourceCfg.terminal;
      effectiveHistorySources =
        if sat.historySources != [ ] then sat.historySources
        else if targetHome == null then [ ]
        else [
          { path = "${targetHome}/.bash_history"; shell = "bash"; }
          { path = "${targetHome}/.zsh_history"; shell = "zsh"; }
          { path = "${targetHome}/.local/share/atuin/history.db"; shell = "atuin"; }
          { path = "${targetHome}/.local/share/fish/fish_history"; shell = "fish"; }
        ];
      historySourcesWithUnits =
        map
          (source:
            let explicit = source.sourceId or null;
            in source // {
              sourceId =
                if explicit != null then explicit
                else terminalSourceIdForShell source.shell;
            })
          effectiveHistorySources;
      sourceUnitGroups =
        mapAttrsToList
          (sourceId: sources: { inherit sourceId sources; })
          (groupBy (source: source.sourceId) historySourcesWithUnits);
      # Post-Wave-B fold (#1081): per-shell adapter Config shapes.
      #   SqliteRowAdapter (atuin, fish): { path, query }
      #   AppendOnlyFileAdapter (bash, zsh, text): { path, skip_empty }
      # `immutable = false` + `read_only = false` because both atuin and fish
      # have a live daemon writing to their DBs (atuin shell hook, fish_history
      # background sync). `immutable=true` (the SqliteRow default) returns
      # SQLITE_CANTOPEN against an active WAL writer; `read_only=true` blocks
      # SQLite's own WAL recovery on open. Same pattern as ActivityWatch (#1299)
      # and qutebrowser (#1325). sinex only issues SELECTs.
      mkSourceDriverAdapterConfig = group:
        let
          source = builtins.head group.sources;
          shell = toLower source.shell;
        in
        if shell == "atuin" then {
          path = source.path;
          query = "history";
          table = "history";
          immutable = false;
          read_only = false;
        }
        else if shell == "fish" then {
          path = source.path;
          query = "fish_history";
          table = "fish_history";
          immutable = false;
          read_only = false;
        }
        else { path = source.path; skip_empty = true; };
      mkSourceDriverAdapterType = group:
        let shell = toLower (builtins.head group.sources).shell;
        in if elem shell [ "atuin" "fish" ] then "SqliteRowAdapter"
           else "AppendOnlyFileAdapter";
      sqliteHistoryPaths =
        unique (
          map (source: source.path)
            (filter (source: elem (toLower source.shell) [ "atuin" "fish" ]) effectiveHistorySources)
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
            "${targetHome}/.local/share/fish"
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

          ${commonBaseAclFunctions}
          ${commonReadAclFunctions}

          # Atuin/fish SQLite DBs need RW (SQLite WAL recovery writes -wal/-shm
          # on every open, even for SELECT). bash/zsh/text histories are
          # append-only files where read is sufficient. See #1325 for the
          # equivalent qutebrowser fix; matches the pattern used in the
          # browser ACL script.
          grant_file_readwrite() {
            local path="$1"
            if [ -f "$path" ]; then
              set_access_acl "$path" "u:$SERVICE_USER:rw-" "rw-"
            fi
          }
          grant_sqlite_sidecars_rw() {
            local path="$1"
            grant_file_readwrite "$path-wal"
            grant_file_readwrite "$path-shm"
          }
          grant_dir_readwrite() {
            local path="$1"
            if [ -d "$path" ]; then
              set_access_acl "$path" "u:$SERVICE_USER:rwx" "rwx"
            fi
          }

          # Ordering matters (see #1329): all `grant_parent_dirs` first, then
          # the leaf grants. Otherwise a later parent-walk that passes through
          # an earlier-granted dir downgrades its mask back to `--x`.
          ${concatStringsSep "\n" (map (path: ''
            grant_parent_dirs ${escapeShellArg path}
          '') accessAclPaths)}
          ${concatStringsSep "\n" (map (path: ''
            grant_parent_dirs ${escapeShellArg path}
          '') sqliteHistoryPaths)}
          ${concatStringsSep "\n" (map (path: ''
            grant_parent_dirs ${escapeShellArg path}
          '') sqliteHistoryDirs)}

          # accessAclPaths includes atuin DB explicitly; grant RW there too so
          # SQLite WAL recovery succeeds. Non-SQLite paths in accessAclPaths
          # are append-only files where r-- is what we want; we filter in a
          # second pass that overlays RW on SQLite paths.
          ${concatStringsSep "\n" (map (path: ''
            grant_file_read ${escapeShellArg path}
          '') accessAclPaths)}

          # SQLite DBs: RW on file + -wal + -shm, RWX on containing dir.
          ${concatStringsSep "\n" (map (path: ''
            grant_file_readwrite ${escapeShellArg path}
            grant_sqlite_sidecars_rw ${escapeShellArg path}
          '') sqliteHistoryPaths)}
          ${concatStringsSep "\n" (map (path: ''
            grant_dir_readwrite ${escapeShellArg path}
            grant_dir_read_defaults ${escapeShellArg path}
          '') sqliteHistoryDirs)}

          if [ "$acl_failures" -ne 0 ]; then
            exit 1
          fi
        '';
      # Post-collapse: per-binding service overrides become contributions
      # to the sinexd unit's overlay. We do not emit per-source host
      # systemd units anymore.
      serviceConfigOverrides = { };
      terminalBindings =
        listToAttrs (
          map
            (group:
              let
                instances = resolveSourceInstances group.sourceId sat.instances;
                resources = resolveSourceResources group.sourceId sat.resources;
              in
              nameValuePair group.sourceId {
                enable = true;
                description = "Terminal history (${group.sourceId})";
                adapterType = mkSourceDriverAdapterType group;
                adapterConfig = mkSourceDriverAdapterConfig group;
                inherit instances resources serviceConfigOverrides;
                catalogMetadata = catalogMetadataFor group.sourceId;
                extraArgs = sat.extraArgs;
                extraEnv = { RUST_LOG = runtimeCfg.defaults.logLevel; } // sat.env;
              })
            sourceUnitGroups
        );
      monitorBinding =
        let
          resources = resolveSourceResources "terminal.monitor" sat.resources;
        in
        {
          "terminal.monitor" = {
            enable = true;
            description = "Terminal monitoring lifecycle event (hosted source binding)";
            adapterType = null;
            adapterConfig = { };
            instances = resolveSourceInstances "terminal.monitor" sat.instances;
            inherit resources;
            catalogMetadata = catalogMetadataFor "terminal.monitor";
            extraEnv = { RUST_LOG = runtimeCfg.defaults.logLevel; } // sat.env;
            serviceConfigOverrides = { };
            extraArgs = [ ];
          };
        };
      # Post-collapse: all source bindings fold into sinexd.service. The
      # ACL setup must run before sinexd so the in-process terminal source
      # units can traverse target-user paths.
      supportUnits = mkAccessSetupUnit {
        name = "sinex-terminal-target-access";
        description = "Prepare target-user access for the Sinex terminal source runtime";
        script = accessSetupScript;
        writePaths = accessWritePaths;
        beforeUnits = [ "sinex-preflight.service" "sinexd.service" ];
      };
    in
    {
      bindings = terminalBindings // monitorBinding;
      inherit supportUnits;
      overlay = {
        protectHome = "read-only";
        readWritePaths = accessWritePaths;
        execStartPre = lib.optional (accessSetupScript != null) "+${accessSetupScript}";
        bindReadOnlyPaths = lib.optionals (sat.access.bindReadOnlyPaths != [ ] && accessSetupScript == null)
          (renderBindReadOnlyPaths sat.access.bindReadOnlyPaths);
      };
    };

  # ── Desktop support glue ────────────────────────────────────────────────
  # Returns source bindings, support units, and desktop bridge paths.
  mkDesktopGlue =
    let
      sat = sourceCfg.desktop;
      clipboardEnv = if sat.clipboard.enable then { SINEX_CLIPBOARD = "1"; } else { SINEX_CLIPBOARD = "0"; };
      bridgeEnvFile = "${runtimeDir}/desktop-target.env";
      defaultRuntimeRoot =
        if targetUid != null then "/run/user/${toString targetUid}" else null;
      runtimeRoot =
        if sat.session.runtimeDir != null then sat.session.runtimeDir
        else if targetUid != null then "/run/user/${toString targetUid}"
        else null;
      runtimeRootUnits =
        optionals (runtimeRoot != null && defaultRuntimeRoot != null && runtimeRoot == defaultRuntimeRoot) [
          "user-runtime-dir@${toString targetUid}.service"
        ];
      sessionEnv =
        optionalAttrs (sat.session.runtimeDir != null) {
          SINEX_HYPRLAND_RUNTIME_DIR = sat.session.runtimeDir;
          XDG_RUNTIME_DIR = sat.session.runtimeDir;
        }
        // optionalAttrs (sat.session.waylandDisplay != null) {
          WAYLAND_DISPLAY = sat.session.waylandDisplay;
        }
        // optionalAttrs (sat.session.hyprlandInstanceSignature != null) {
          SINEX_HYPRLAND_INSTANCE_SIGNATURE = sat.session.hyprlandInstanceSignature;
        }
        // optionalAttrs (sat.session.hyprlandEventSocket != null) {
          SINEX_HYPRLAND_EVENT_SOCKET = sat.session.hyprlandEventSocket;
        }
        // optionalAttrs (sat.session.hyprlandCommandSocket != null) {
          SINEX_HYPRLAND_COMMAND_SOCKET = sat.session.hyprlandCommandSocket;
        }
        // optionalAttrs (sat.history.activitywatchDbPath != null) {
          SINEX_ACTIVITYWATCH_DB_PATH = sat.history.activitywatchDbPath;
        };
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
          FIND=${pkgs.findutils}/bin/find
          SORT=${pkgs.coreutils}/bin/sort
          BASENAME=${pkgs.coreutils}/bin/basename
          DIRNAME=${pkgs.coreutils}/bin/dirname
          SLEEP=${pkgs.coreutils}/bin/sleep
          SYSTEMCTL=${pkgs.systemd}/bin/systemctl
          acl_failures=0

          record_acl_failure() {
            local path="$1"
            echo "sinex-desktop-target-access: failed to grant ACLs for $path" >&2
            acl_failures=$((acl_failures + 1))
          }

          ${commonBaseAclFunctions}
          ${commonReadAclFunctions}
          # The desktop script previously defined a local `grant_file_read`
          # that also walked parent dirs; the commonReadAclFunctions version
          # only sets ACL on the file itself. Re-introduce the dir-walking
          # variant as a separate name so callers that need both behaviors
          # can pick.
          grant_file_read_with_parents() {
            local path="$1"
            if [ -f "$path" ]; then
              grant_parent_dirs "$path"
              set_access_acl "$path" "u:$SERVICE_USER:r--" "r--"
            fi
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

          "$INSTALL" -d -m0755 ${escapeShellArg runtimeDir}
          "$INSTALL" -m0600 /dev/null "$ENV_FILE"

          if [ -n "$CONFIGURED_RUNTIME_DIR" ]; then
            RUNTIME_ROOT="$CONFIGURED_RUNTIME_DIR"
          else
            if ! TARGET_UID="$("$ID" -u "$TARGET_USER" 2>/dev/null)"; then
              echo "Sinex desktop bridge failed: target user '$TARGET_USER' does not exist" >&2
              exit 1
            fi
            RUNTIME_ROOT="/run/user/$TARGET_UID"
          fi

          if [ ! -d "$RUNTIME_ROOT" ]; then
            if [ -n "''${TARGET_UID:-}" ] && [ -z "$CONFIGURED_RUNTIME_DIR" ]; then
              "$SYSTEMCTL" start "user-runtime-dir@$TARGET_UID.service" >/dev/null 2>&1 || true
            fi
            runtime_wait_attempt=0
            while [ ! -d "$RUNTIME_ROOT" ] && [ "$runtime_wait_attempt" -lt 50 ]; do
              runtime_wait_attempt=$((runtime_wait_attempt + 1))
              "$SLEEP" 0.2
            done
          fi

          if [ ! -d "$RUNTIME_ROOT" ]; then
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
            done < <("$FIND" "$RUNTIME_ROOT" -maxdepth 1 -type s -name 'wayland-*' 2>/dev/null | "$SORT")
          fi

          HYPRLAND_SIGNATURE="$CONFIGURED_HYPRLAND_SIGNATURE"
          if [ -d "$RUNTIME_ROOT/hypr" ]; then
            grant_parent_dirs "$RUNTIME_ROOT/hypr"
            grant_dir_defaults "$RUNTIME_ROOT/hypr"

            while IFS= read -r instance_dir; do
              [ -n "$instance_dir" ] || continue
              grant_parent_dirs "$instance_dir"
              grant_dir_defaults "$instance_dir"
            done < <("$FIND" "$RUNTIME_ROOT/hypr" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | "$SORT")

            while IFS= read -r socket_path; do
              [ -n "$socket_path" ] || continue
              grant_socket_access "$socket_path"
            done < <("$FIND" "$RUNTIME_ROOT/hypr" -mindepth 2 -maxdepth 2 -type s -name '.socket.sock' 2>/dev/null | "$SORT")

            HYPRLAND_EVENT_SOCKET_COUNT=0
            while IFS= read -r socket_path; do
              [ -n "$socket_path" ] || continue
              grant_socket_access "$socket_path"
              HYPRLAND_EVENT_SOCKET_COUNT=$((HYPRLAND_EVENT_SOCKET_COUNT + 1))
              if [ -z "$HYPRLAND_SIGNATURE" ]; then
                HYPRLAND_SIGNATURE="$("$BASENAME" "$("$DIRNAME" "$socket_path")")"
              fi
            done < <("$FIND" "$RUNTIME_ROOT/hypr" -mindepth 2 -maxdepth 2 -type s -name '.socket2.sock' 2>/dev/null | "$SORT" -r)

            if [ -n "$CONFIGURED_HYPRLAND_SIGNATURE" ]; then
              HYPRLAND_SIGNATURE="$CONFIGURED_HYPRLAND_SIGNATURE"
            elif [ "$HYPRLAND_EVENT_SOCKET_COUNT" -eq 0 ]; then
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
              # ActivityWatch sqlite.db is WAL-mode with aw-server-rust as the
              # live writer. SQLite WAL recovery on open requires write access
              # to the DB + sidecars + the dir. See #1325 (qutebrowser fix)
              # and #1330 (atuin fix) for the same pattern.
              ACTIVITYWATCH_DIR="$("$DIRNAME" "$CONFIGURED_ACTIVITYWATCH_DB")"
              grant_parent_dirs "$CONFIGURED_ACTIVITYWATCH_DB"
              grant_dir_readwrite "$ACTIVITYWATCH_DIR"
              grant_file_readwrite "$CONFIGURED_ACTIVITYWATCH_DB"
              grant_sqlite_sidecars_rw "$CONFIGURED_ACTIVITYWATCH_DB"
              echo "SINEX_ACTIVITYWATCH_DB_PATH=$CONFIGURED_ACTIVITYWATCH_DB"
            fi
          } > "$ENV_FILE"

          if [ "$acl_failures" -ne 0 ]; then
            exit 1
          fi
        '';
      # Post-Wave-B fold (#1081): desktop source contracts.
      # desktop.activitywatch is adapter-backed (SqliteRowAdapter);
      # window-manager and clipboard are live-stream adapters that depend on
      # the target-runtime bridge below.
      activitywatchDbPath =
        if targetHome != null
        then "${targetHome}/.local/share/activitywatch/aw-server-rust/sqlite.db"
        else "";
      desktopServiceConfigOverrides = { };
      desktopExtraEnv =
        clipboardEnv
        // sessionEnv
        // { RUST_LOG = runtimeCfg.defaults.logLevel; }
        // sat.env;
      supportUnits = mkAccessSetupUnit {
        name = "sinex-desktop-target-access";
        description = "Prepare target-user access for the Sinex desktop source runtime";
        script = accessSetupScript;
        writePaths = accessWritePaths;
        afterUnits = runtimeRootUnits;
        wantsUnits = runtimeRootUnits;
        beforeUnits = [
          # Post-collapse: every desktop source lives inside sinexd, so
          # the runtime bridge setup must run before sinexd itself.
          "sinex-preflight.service"
          "sinexd.service"
        ];
      };
      # Hyprland rotates instance signature directories under
      # /run/user/UID/hypr/<sig> on every compositor restart.  The oneshot
      # access-setup unit only runs at boot, so newly-created instance
      # directories never receive the u:sinex:rw- ACL grant on their
      # sockets, and reconnection from the desktop source fails with
      # Permission denied (issue #680).  Add a non-RemainAfterExit refresh
      # service plus a path unit that re-runs the ACL setup whenever the
      # Hyprland runtime root changes.
      aclRefreshUnits =
        if accessSetupScript == null || targetUid == null then { }
        else {
          "sinex-desktop-acl-refresh" = {
            description = "Re-apply Sinex desktop target-user ACLs on Hyprland instance rotation";
            after = runtimeRootUnits;
            wants = runtimeRootUnits;
            serviceConfig =
              {
                Type = "oneshot";
                ExecStart = accessSetupScript;
                # No RemainAfterExit — we want the unit to be re-runnable
                # via the path trigger.
              }
              // mkHelperServiceConfig {
                user = "root";
                group = "root";
                remainAfterExit = false;
                protectHome = false;
                privateTmp = true;
                restrictAddressFamilies = [ ];
                readWritePaths = unique (readWritePaths ++ accessWritePaths);
              };
          };
        };
      aclRefreshPaths =
        if accessSetupScript == null || targetUid == null then { }
        else {
          "sinex-desktop-acl-refresh" = {
            description = "Watch Hyprland instance rotation to re-apply Sinex desktop ACLs";
            wantedBy = [ "multi-user.target" ];
            pathConfig = {
              PathChanged = "/run/user/${toString targetUid}/hypr";
              MakeDirectory = true;
            };
          };
        };
      # Shared fields for all desktop source contracts.
      mkDesktopBinding = sourceId: description: adapterConfig: gated:
        let
          resources = resolveSourceResources sourceId sat.resources;
        in
        {
          enable = sat.enable;
          inherit description;
          adapterType = null;
          adapterConfig = adapterConfig;
          instances = resolveSourceInstances sourceId sat.instances;
          inherit resources;
          catalogMetadata = catalogMetadataFor sourceId;
          afterUnits = runtimeRootUnits;
          wantsUnits = runtimeRootUnits;
          extraArgs = sat.extraArgs;
          extraEnv = desktopExtraEnv;
          unitPath = [ pkgs.hyprland ];
          serviceConfigOverrides = desktopServiceConfigOverrides;
        } // (if gated then { gated = true; } else { });
      # desktop.activitywatch only supplies `path`; query/table come from
      # the Rust source's default_config (schema validation skipped).
      # desktop.window-manager resolves its socket from the bridge-written
      # environment file at runtime. desktop.clipboard uses adapter defaults
      # and the same XDG_RUNTIME_DIR/WAYLAND_DISPLAY bridge.
      desktopBindings = {
        # immutable=false: aw-server-rust holds the WAL active while writing,
        # which makes SQLite's immutable=1 path fail SQLITE_CANTOPEN. Without
        # this override the worker spams "unable to open database file" every
        # 30 s. The rest of the SqliteRow defaults stay (read_only=true, etc.).
        "desktop.activitywatch" = mkDesktopBinding "desktop.activitywatch" "ActivityWatch SQLite (hosted source binding)" { path = activitywatchDbPath; immutable = false; } false;
        "desktop.window-manager" = mkDesktopBinding "desktop.window-manager" "Desktop window manager (hosted source binding)" { } false;
      } // optionalAttrs sat.clipboard.enable {
        "desktop.clipboard" = mkDesktopBinding "desktop.clipboard" "Desktop clipboard (hosted source binding)" { } false;
      };
    in
    {
      bindings = desktopBindings;
      supportUnits = supportUnits // aclRefreshUnits;
      paths = aclRefreshPaths;
      overlay = {
        protectHome = "read-only";
        readWritePaths = accessWritePaths;
        execStartPre = lib.optional (accessSetupScript != null) "+${accessSetupScript}";
        environmentFile = lib.optional (accessSetupScript != null) "-${bridgeEnvFile}";
        bindReadOnlyPaths = lib.optionals (sat.access.bindReadOnlyPaths != [ ] && accessSetupScript == null)
          (renderBindReadOnlyPaths sat.access.bindReadOnlyPaths);
        path = [ pkgs.hyprland ];
      };
    };

  # ── Browser support glue ─────────────────────────────────────────────────
  # Browser source binding contribution.
  mkBrowserGlue =
    let
      sat = sourceCfg.browser;
      instances = resolveSourceInstances "browser.history" sat.instances;
      resources = resolveSourceResources "browser.history" sat.resources;
      # Post-Wave-B fold (#1081): browser.history uses
      # ChainedAdapter<SqliteRowAdapter, AppendOnlyFileAdapter>. The
      # ChainedConfig shape is `{primary, secondary, interleaved}` where
      # primary is SqliteRowConfig and secondary is AppendOnlyFileConfig.
      primarySqlitePath =
        if sat.sqliteSources != [ ] then (builtins.head sat.sqliteSources).path else "";
      secondaryDumpPath =
        if sat.dumpSources != [ ] then (builtins.head sat.dumpSources).path or "" else "";
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

          ${commonBaseAclFunctions}
          ${commonReadAclFunctions}

          # qutebrowser's history.sqlite is WAL-mode with an active writer.
          # SQLite WAL recovery needs WRITE access to the main DB + sidecars
          # even for read-only connections (otherwise prepare fails with
          # "attempt to write a readonly database"). See #1325.
          grant_file_readwrite() {
            local path="$1"
            if [ -f "$path" ]; then
              set_access_acl "$path" "u:$SERVICE_USER:rw-" "rw-"
            fi
          }
          grant_sqlite_sidecars_rw() {
            local path="$1"
            grant_file_readwrite "$path-wal"
            grant_file_readwrite "$path-shm"
          }
          grant_dir_readwrite() {
            local path="$1"
            if [ -d "$path" ]; then
              set_access_acl "$path" "u:$SERVICE_USER:rwx" "rwx"
            fi
          }

          # IMPORTANT: do all `grant_parent_dirs` walks BEFORE any
          # `grant_dir_readwrite`. Otherwise a later parent-walk that passes
          # through an earlier-granted dir downgrades its mask back to `--x`,
          # which is exactly what happened on the qutebrowser deploy under
          # #1325. grant_dir_readwrite runs strictly second so its `rwx` is
          # the final mask.
          ${concatStringsSep "\n" (map (path: ''
            grant_parent_dirs ${escapeShellArg path}
          '') sqlitePaths)}
          ${concatStringsSep "\n" (map (path: ''
            grant_parent_dirs ${escapeShellArg path}
          '') sqliteDirs)}

          ${concatStringsSep "\n" (map (path: ''
            grant_file_readwrite ${escapeShellArg path}
            grant_sqlite_sidecars_rw ${escapeShellArg path}
          '') sqlitePaths)}

          ${concatStringsSep "\n" (map (path: ''
            grant_dir_readwrite ${escapeShellArg path}
          '') sqliteDirs)}

          if [ "$acl_failures" -ne 0 ]; then
            exit 1
          fi
        '';
      browserServiceConfigOverrides = { };
      supportUnits = mkAccessSetupUnit {
        name = "sinex-browser-target-access";
        description = "Prepare target-user access for the Sinex browser source runtime";
        script = accessSetupScript;
        writePaths = accessWritePaths;
        beforeUnits = [ "sinex-preflight.service" "sinexd.service" ];
      };
    in
    {
      bindings = {
        "browser.history" = {
          enable = sat.enable;
          description = "Browser history (hosted source binding)";
          adapterType = "ChainedAdapter";
          # ChainedConfig: primary=SqliteRowConfig, secondary=AppendOnlyFileConfig.
          # qutebrowser's history.sqlite is in WAL mode with a live writer:
          #   - immutable=true (default) returns SQLITE_CANTOPEN (live writer
          #     holds exclusive lock)
          #   - immutable=false + read_only=true (default) opens the DB but
          #     query prep fails with "attempt to write a readonly database"
          #     when SQLite needs to journal/recover for the connection
          # Setting read_only=false lets SQLite manage WAL recovery for its
          # own connection without touching qutebrowser's data. We only ever
          # SELECT — no INSERT/UPDATE/DELETE.
          adapterConfig = {
            primary = {
              path = primarySqlitePath;
              immutable = false;
              read_only = false;
            };
            secondary = { path = secondaryDumpPath; };
          };
          inherit instances resources;
          catalogMetadata = catalogMetadataFor "browser.history";
          extraArgs = sat.extraArgs;
          extraEnv = { RUST_LOG = runtimeCfg.defaults.logLevel; } // sat.env;
          serviceConfigOverrides = browserServiceConfigOverrides;
        };
      };
      inherit supportUnits;
      overlay = {
        protectHome = "read-only";
        readWritePaths = accessWritePaths;
        execStartPre = lib.optional (accessSetupScript != null) "+${accessSetupScript}";
        bindReadOnlyPaths = lib.optionals (sat.access.bindReadOnlyPaths != [ ] && accessSetupScript == null)
          (renderBindReadOnlyPaths sat.access.bindReadOnlyPaths);
      };
    };

  # ── System support glue ──────────────────────────────────────────────────
  # System source binding contribution.
  mkSystemGlue =
    let
      sat = sourceCfg.system;
      # Post-Wave-B fold (#1081): system source contracts share this config blob.
      # Each parser only reads what its source-specific code touches.
      runtimeConfig = {
        # Hosted continuous system sources should live-tail on first daemon
        # startup. Explicit historical scans remain available through the
        # runtime historical path and do not consume this continuous-only knob.
        continuous_start_position = "latest";
        dbus_enabled = true;
        journal_enabled = true;
        udev_enabled = true;
        systemd_enabled = true;
        dbus_buses = "system";
        journal_timeout_secs = 5;
        systemd_config = {
          monitor_services = true;
          monitor_timers = true;
          monitor_all_units = false;
          monitor_timeout_secs = 5;
        };
        dbus_config = {
          monitor_session = true;
          monitor_system = true;
          include_interfaces = [ ];
          exclude_interfaces = [
            "org.freedesktop.DBus.Introspectable"
            "org.freedesktop.DBus.Peer"
          ];
          extract_notifications = true;
          extract_media = true;
          extract_power = true;
          extract_hardware = true;
          extract_session = false;
          extract_bluetooth = true;
          extract_network = true;
          extract_mounts = true;
          health_check_interval_secs = 5;
          inactivity_timeout_secs = 30;
        };
        journal_config = {
          follow = true;
          import_hours = 24;
          units = [ ];
          priorities = [ ];
          include_kernel = true;
          include_user = true;
          exclude_units = [ ];
          exclude_fields = [
            "__CURSOR"
            "__REALTIME_TIMESTAMP"
            "__MONOTONIC_TIMESTAMP"
            "_TRANSPORT"
          ];
          cursor_file = "${stateRoot}/journal.cursor";
          batch_size = 1000;
          cursor_flush_event_threshold = 100;
          cursor_flush_interval_secs = 10;
        };
      };
      systemServiceConfig = {
        SupplementaryGroups = [ "systemd-journal" ];
      };
      mkSystemBinding = id: description: {
        enable = sat.enable;
        inherit description;
        adapterType = null;
        adapterConfig = runtimeConfig;
        instances = resolveSourceInstances id sat.instances;
        resources = resolveSourceResources id sat.resources;
        catalogMetadata = catalogMetadataFor id;
        extraArgs = sat.extraArgs;
        extraEnv = { RUST_LOG = runtimeCfg.defaults.logLevel; } // sat.env;
        serviceConfigOverrides = { };
      };
    in
    # system.dbus is hosted in-process since #1235 wired `RealDbusBackend`
    # into `DbusStreamAdapter::open` (zbus 5.x).
    {
      bindings = {
        "system.journald" = mkSystemBinding "system.journald" "systemd journal (hosted source binding)";
        "system.systemd" = mkSystemBinding "system.systemd" "systemd unit state (hosted source binding)";
        "system.udev" = mkSystemBinding "system.udev" "udev events (hosted source binding)";
        "system.dbus" = mkSystemBinding "system.dbus" "D-Bus signal stream (hosted source binding)";
        "system.monitor" = {
          enable = sat.enable;
          description = "System monitoring lifecycle event (hosted source binding)";
          adapterType = null;
          adapterConfig = { };
          instances = resolveSourceInstances "system.monitor" sat.instances;
          resources = resolveSourceResources "system.monitor" sat.resources;
          catalogMetadata = catalogMetadataFor "system.monitor";
          extraEnv = { RUST_LOG = runtimeCfg.defaults.logLevel; } // sat.env;
          serviceConfigOverrides = { };
          extraArgs = [ ];
        };
      };
      overlay = {
        supplementaryGroups = systemServiceConfig.SupplementaryGroups or [ ];
      };
    };

  mkDocumentUnits =
    let
      sat = sourceCfg.document;
      resources = resolveSourceResources "document.staging" sat.resources;
      documentRoots = unique (map toString effectiveDocumentRoots);
      runtimeConfig = builtins.toJSON {
        supported_mime_types = sat.supportedMimeTypes;
        max_document_size = sat.maxDocumentSize;
        allowed_roots = documentRoots;
      };
      scanCommand = concatStringsSep " " (
        [
          "${sinexPackage}/bin/sinexd"
          "scan-source-driver"
          "--source"
          "document.staging"
          "--service-name"
          "sinex-document-scan"
          "--runtime-config"
          (escapeShellArg runtimeConfig)
          "--extra-arg"
          "scan"
          "--extra-arg"
          "--until"
          "--extra-arg"
          "snapshot"
        ]
        ++ concatMap (arg: [ "--extra-arg" arg ]) sat.extraArgs
      );
      env = mkServiceEnv ([ "RUST_LOG=${runtimeCfg.defaults.logLevel}" ] ++ toEnvList sat.env);
      requiredUnits =
        postgresServiceUnits
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

          ${commonBaseAclFunctions}
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
        wants = optionals coreEnabled [ "sinexd.service" ];
        unitConfig = existingPathAssertions (databaseSecretAssertPaths ++ natsSecretAssertPaths);
        path = optionals (cfg.storage.blob.enable && cfg.storage.blob.legacyAnnexData) [ pkgs.git pkgs.git-annex ];
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

  # Post-collapse: automata are hosted in-process by sinexd via
  # SINEX_AUTOMATA_ENABLED. The catalog still drives which names are
  # eligible; selection comes from each automaton's enable flag.
  automataEnabledNames =
    if !(runtimeEnabled && automataCfg.enable) then [ ] else
    map (spec: spec.automaton)
      (filter (spec: automataCfg.${spec.optionName}.enable) automataLib.specs);

  # ── Support-glue assembly ────────────────────────────────────────────────
  # Post-collapse: per-source service emission is gone. Each domain glue
  # contributes: `bindings` (passed to sinexd via the source-binding manifest
  # JSON), `supportUnits` (ACL/env bridge oneshot units that still need to
  # run before sinexd), and `overlay` (per-domain serviceConfig contributions
  # — ProtectHome / ReadWritePaths / ExecStartPre ACL / EnvironmentFile /
  # SupplementaryGroups / unit path packages — merged into sinexd.service).
  emptyGlue = { bindings = { }; supportUnits = { }; overlay = { }; };
  terminalGlue =
    if sourceRuntimeEnabled && sourceCfg.terminal.enable then mkTerminalGlue
    else emptyGlue;
  desktopGlue =
    if sourceRuntimeEnabled && sourceCfg.desktop.enable then mkDesktopGlue
    else emptyGlue // { paths = { }; };
  browserGlue =
    if sourceRuntimeEnabled && sourceCfg.browser.enable then mkBrowserGlue
    else emptyGlue;
  systemGlue =
    if sourceRuntimeEnabled && sourceCfg.system.enable then mkSystemGlue
    else { bindings = { }; overlay = { }; };
  filesystemGlue =
    if sourceRuntimeEnabled && sourceCfg.filesystem.enable then mkFilesystemBindings
    else { bindings = { }; overlay = { }; };

  # All domain-specific source bindings merged together.
  allDomainBindings =
    filesystemGlue.bindings
    // terminalGlue.bindings
    // browserGlue.bindings
    // desktopGlue.bindings
    // systemGlue.bindings;

  # Support units (ACL/env bridges) that generate their own systemd services.
  runtimeSupportUnits =
    terminalGlue.supportUnits
    // browserGlue.supportUnits
    // desktopGlue.supportUnits;

  # Per-domain serviceConfig contributions for sinexd. Each overlay is an
  # attribute set with the optional fields documented on `mkCoreServices`.
  # Union-merge by concatenating list fields and taking the strictest value
  # for ProtectHome (read-only > true; we never need to relax to "tmpfs" or
  # false because every source either tolerates read-only or pays for
  # its own ACL bridge into the readonly namespace).
  glueOverlays = [
    filesystemGlue.overlay
    terminalGlue.overlay
    browserGlue.overlay
    desktopGlue.overlay
    systemGlue.overlay
  ];
  collectList = field: concatMap (o: o.${field} or [ ]) glueOverlays;
  runtimeOverlay = {
    protectHome =
      if any (o: (o.protectHome or null) == "read-only") glueOverlays then "read-only"
      else null;
    readWritePaths = unique (collectList "readWritePaths");
    execStartPre = collectList "execStartPre";
    environmentFile = collectList "environmentFile";
    bindReadOnlyPaths = collectList "bindReadOnlyPaths";
    supplementaryGroups = unique (collectList "supplementaryGroups");
    path = collectList "path";
    afterUnits = [ ];
    wantsUnits = [ ];
    environment = [ ];
  };

  # Source-binding manifest consumed by sinexd via SINEX_SOURCE_BINDINGS_PATH.
  # Schema mirrors `sinexd::sources::bindings::SourceBindingsManifest`.
  activeManifestBindings =
    if !sourceRuntimeEnabled then [ ]
    else
      concatMap
        (id:
          let
            binding = allDomainBindings.${id} or { };
            enable = binding.enable or false;
            gated = binding.gated or false;
            instances = binding.instances or 1;
            runtimeConfig = binding.adapterConfig or { };
            extraArgs = binding.extraArgs or [ ];
            catalogMetadata = binding.catalogMetadata or null;
            instanceList = range 1 instances;
          in
          if enable && !gated then
            map
              (idx: {
                source_id = id;
                instance_idx = idx;
                service_name = "source-driver-${id}-${toString idx}";
                runtime_config = runtimeConfig;
                extra_args = extraArgs;
                catalog_metadata = catalogMetadata;
              })
              instanceList
          else [ ]
        )
        (attrNames allDomainBindings);

  activeCatalogSourceIds =
    if !sourceRuntimeEnabled then [ ]
    else
      concatMap
        (id:
          let
            binding = allDomainBindings.${id} or { };
            enable = binding.enable or false;
            gated = binding.gated or false;
            instances = binding.instances or 1;
          in
          if enable && !gated then map (_: id) (range 1 instances) else [ ]
        )
        (attrNames allDomainBindings);

  activeCatalogUnitLimits = sourceCatalog.unitMemoryLimitFor activeCatalogSourceIds;

  sourceBindingsManifestFile =
    if activeManifestBindings == [ ] then null
    else pkgs.writeText "sinex-source-bindings.json" (
      builtins.toJSON { bindings = activeManifestBindings; }
    );

  documentScanService =
    if !(sourceRuntimeEnabled && sourceCfg.document.enable) then { units = { }; supportUnits = { }; } else mkDocumentUnits;

  coreServices = mkCoreServices {
    automataEnabledList = automataEnabledNames;
    inherit sourceBindingsManifestFile;
    runtimeOverlay = runtimeOverlay // {
      serviceConfig = activeCatalogUnitLimits;
    };
  };

  # Preflight only needs to guard the collapsed sinexd unit. Source
  # and per-automaton unit names no longer exist.
  generatedUnits = [ ];
in
{
  # Internal option declared here to break the evaluation cycle.
  # sources.nix reads config.services.sinex (via cfg) and must
  # communicate generated unit names to preflight-verification.nix.
  # Writing back to services.sinex.runtime.* from a module that reads
  # config.services.sinex causes infinite recursion because the module
  # system must merge all definitions of the submodule to evaluate any
  # sub-option.  A separate top-level path avoids the cycle.
  options.sinex._generatedUnits = mkOption {
    type = with types; listOf str;
    default = [ ];
    internal = true;
    description = "Systemd units generated by sources.nix (internal, breaks cycle).";
  };

  config = mkMerge [
    (mkIf sinexEnabled {
      systemd.services = mkMerge [
        coreServices
        # ACL/env bridge support units (not source host services).
        runtimeSupportUnits
        documentScanService.units
        documentScanService.supportUnits
      ];
      systemd.paths = desktopGlue.paths or { };
      systemd.timers = mkMerge [
        (optionalAttrs (sourceRuntimeEnabled && sourceCfg.document.enable && sourceCfg.document.schedule != null) {
          "sinex-document-scan" = {
            description = "Schedule Sinex document snapshot scans";
            wantedBy = [ "timers.target" ];
            timerConfig = {
              OnCalendar = sourceCfg.document.schedule;
              Persistent = sourceCfg.document.persistentTimer;
            };
          };
        })
      ];
    })
    { sinex._generatedUnits = generatedUnits; }
  ];
}
