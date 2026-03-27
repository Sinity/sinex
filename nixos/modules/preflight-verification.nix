{ config, lib, pkgs, ... }:

with lib;

let
  systemdHardening = import ./lib/systemd-hardening.nix { inherit lib; };
  databaseRuntime = import ./lib/database-runtime.nix { inherit lib pkgs; };
  inherit (systemdHardening) mkHelperServiceConfig;
  inherit (databaseRuntime)
    mkDatabasePasswordExec
    renderDatabaseUrl
    ;
  cfg = config.services.sinex;
  nodesCfg = cfg.nodes;
  lifecycle = cfg.lifecycle;
  preflight = lifecycle.preflight;
  updates = lifecycle.updates;
  natsEnabled = cfg.nats.enable || cfg.nats.autoSetup;

  sinexEnabled = cfg.enable;
  schemaApplyEnabled = sinexEnabled && cfg.database.enable;
  preflightEnabled = sinexEnabled && preflight.enable;
  updatesEnabled = sinexEnabled && updates.enable;

  generatedUnits = config.sinex._generatedUnits;
  localPostgresEnabled = cfg.database.enable && (cfg.database.autoSetup || config.services.postgresql.enable);
  localPostgresUnits = optionals localPostgresEnabled [ "postgresql.service" "postgresql-setup.service" ];
  schemaApplyUnits = optionals schemaApplyEnabled [ "sinex-schema-apply.service" ];
  # Guard core units only when the core subsystem is enabled.
  # Always guard both core and node units: nodes emit to NATS, ingestd must pass preflight
  # before either layer accepts production traffic.
  coreEnabled = sinexEnabled && (cfg.core.enable or false);
  coreUnitsToGuard = lib.optionals coreEnabled [ "sinex-ingestd" "sinex-gateway" ];
  unitsToGuard = coreUnitsToGuard ++ generatedUnits;
  allDatabases = unique ([ cfg.database.name ] ++ cfg.database.extraDatabases);

  stateRoot = cfg.stateRoot;
  logDir = cfg.observability.logDir;
  ingestSpool = cfg.core.ingestd.spoolDir;
  nodeSpool = "${cfg.stateRoot}/spool/nodes";
  dlqCfg = cfg.storage.dlq;
  serviceUser = cfg.users.nodes;

  databaseUrl = renderDatabaseUrl cfg.database;
  natsUrl = concatStringsSep "," nodesCfg.nats.servers;
  secretPaths = config.sinex.secrets.paths or {};
  resolveSecretPath = explicit: names:
    if explicit != null then explicit else
    let
      match = findFirst (name: builtins.hasAttr name secretPaths) null names;
    in
    if match == null then null else builtins.getAttr match secretPaths;
  natsTlsCfg = nodesCfg.nats.tls;
  natsAuthCfg = nodesCfg.nats.auth;
  effectiveNatsCaCertFile = resolveSecretPath natsTlsCfg.caCertFile [
    "sinex-nats-ca"
    "nats-ca"
  ];
  effectiveNatsClientCertFile = resolveSecretPath natsTlsCfg.clientCertFile [
    "sinex-nats-client-cert"
    "nats-client-cert"
  ];
  effectiveNatsClientKeyFile = resolveSecretPath natsTlsCfg.clientKeyFile [
    "sinex-nats-client-key"
    "nats-client-key"
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
  preflightEnvironment =
    optional cfg.database.enable "DATABASE_URL=${databaseUrl}"
    ++ [
      "SINEX_ENVIRONMENT=${cfg.nats.environment}"
      "SINEX_NATS_URL=${natsUrl}"
    ]
    ++ optional inferredNatsTls "SINEX_NATS_REQUIRE_TLS=1"
    ++ optional (effectiveNatsCaCertFile != null) "SINEX_NATS_CA_CERT=${toString effectiveNatsCaCertFile}"
    ++ optional (effectiveNatsClientCertFile != null) "SINEX_NATS_CLIENT_CERT=${toString effectiveNatsClientCertFile}"
    ++ optional (effectiveNatsClientKeyFile != null) "SINEX_NATS_CLIENT_KEY=${toString effectiveNatsClientKeyFile}"
    ++ optional (effectiveNatsTokenFile != null) "SINEX_NATS_TOKEN_FILE=${toString effectiveNatsTokenFile}"
    ++ optional (effectiveNatsCredsFile != null) "SINEX_NATS_CREDS_FILE=${toString effectiveNatsCredsFile}"
    ++ optional (effectiveNatsNkeySeedFile != null) "SINEX_NATS_NKEY_SEED_FILE=${toString effectiveNatsNkeySeedFile}";
  sanitizeName = lib.strings.sanitizeDerivationName;

  skipArgs = concatMapStringsSep " " (phase: "--skip ${phase}") preflight.skip;
  preflightCommand = concatStringsSep " " (filter (arg: arg != "") [
    "${cfg.package}/bin/sinex-preflight"
    "verify"
    "--timeout"
    "${toString preflight.timeoutSec}"
    (if preflight.skip == [] then "" else skipArgs)
  ]);

  runPreflightScript = pkgs.writeShellScript "sinex-preflight-run" ''
    set -euo pipefail
    echo "$(date): starting Sinex pre-flight verification"
    if ${preflightCommand}; then
      echo "Sinex pre-flight verification passed"
      exit 0
    else
      STATUS=$?
    fi

    echo "Sinex pre-flight verification failed (exit code: $STATUS)"

    case "${preflight.failureAction}" in
      abort)
        exit $STATUS
        ;;
      warn)
        echo "WARNING: continuing despite verification failure"
        exit 0
        ;;
      ignore)
        echo "INFO: ignoring verification failure per configuration"
        exit 0
        ;;
    esac
  '';

  schemaApplyScript = pkgs.writeShellScript "sinex-schema-apply" ''
    set -euo pipefail

    for db_name in ${concatStringsSep " " (map escapeShellArg allDatabases)}; do
      echo "$(date): applying Sinex schema to $db_name"
      ${cfg.package}/bin/xtask infra schema-apply \
        --database-url "postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/$db_name"
    done
  '';

  unitsShellList = concatStringsSep " " (map (unit: escapeShellArg "${unit}.service") unitsToGuard);

  updateScript = pkgs.writeShellScript "sinex-coordinated-update" ''
    set -euo pipefail

    UNITS=(${unitsShellList})
    if [ "''${#UNITS[@]}" -eq 0 ]; then
      echo "No units configured for coordinated update; nothing to do."
      exit 0
    fi

    echo "$(date): running Sinex coordinated update"
    if ! systemctl start --wait sinex-preflight.service; then
      echo "ERROR: pre-flight verification failed" >&2
      exit 1
    fi

    ACTIVE_UNITS=()
    for unit in "''${UNITS[@]}"; do
      if systemctl is-active --quiet "$unit"; then
        ACTIVE_UNITS+=("$unit")
      fi
    done

    if [ "''${#ACTIVE_UNITS[@]}" -eq 0 ]; then
      echo "No active units detected; update complete"
      exit 0
    fi

    GRACE_PERIOD=${toString updates.gracePeriodSec}
    HEALTH_TIMEOUT=${toString updates.healthCheckTimeoutSec}
    ROLLBACK=${if updates.rollbackOnFailure then "1" else "0"}
    PRESERVE_DATA=${if updates.preserveData then "1" else "0"}
    DLQ_ENABLED=${if dlqCfg.enable then "1" else "0"}
    DLQ_PATH="${dlqCfg.path}"

    BACKUP_DIR=""
    if [ "$PRESERVE_DATA" = "1" ] && [ "$DLQ_ENABLED" = "1" ] && [ -d "$DLQ_PATH" ]; then
      BACKUP_DIR="${dlqCfg.path}.backup-$(date +%Y%m%d-%H%M%S)"
      cp -a "$DLQ_PATH" "$BACKUP_DIR" || true
    fi

    for unit in "''${ACTIVE_UNITS[@]}"; do
      systemctl stop "$unit" || true
    done

    if [ "''${#ACTIVE_UNITS[@]}" -gt 0 ]; then
      sleep "$GRACE_PERIOD"
    fi

    for unit in "''${ACTIVE_UNITS[@]}"; do
      systemctl start "$unit"
    done

    DEADLINE=$(( $(date +%s) + HEALTH_TIMEOUT ))
    FAILED=()

    for unit in "''${ACTIVE_UNITS[@]}"; do
      while ! systemctl is-active --quiet "$unit"; do
        if [ $(date +%s) -ge $DEADLINE ]; then
          FAILED+=("$unit")
          break
        fi
        sleep 5
      done
    done

    if [ "''${#FAILED[@]}" -gt 0 ]; then
      echo "Update failed for units: ''${FAILED[*]}" >&2
      if [ "$ROLLBACK" = "1" ]; then
        for unit in "''${ACTIVE_UNITS[@]}"; do
          systemctl stop "$unit" || true
        done
        if [ -n "$BACKUP_DIR" ] && [ -d "$BACKUP_DIR" ]; then
          rm -rf "$DLQ_PATH"
          mv "$BACKUP_DIR" "$DLQ_PATH"
        fi
      fi
      exit 1
    fi

    if [ -n "$BACKUP_DIR" ] && [ -d "$BACKUP_DIR" ]; then
      rm -rf "$BACKUP_DIR"
    fi

    echo "$(date): coordinated update completed successfully"
  '';

  guardUnit = unit: {
    after = mkAfter [ "sinex-preflight.service" ];
    requires = mkAfter [ "sinex-preflight.service" ];
    serviceConfig.ExecStartPre = mkAfter [ (pkgs.writeShellScript "sinex-preflight-guard-${sanitizeName unit}" ''
      set -euo pipefail
      UNIT="${unit}.service"
      if ! systemctl start --wait sinex-preflight.service; then
        echo "ERROR: sinex-preflight verification failed; refusing to start $UNIT" >&2
        exit 1
      fi
    '') ];
  };

in
{
  config = mkMerge [
    (mkIf schemaApplyEnabled {
      systemd.services.sinex-schema-apply = {
        description = "Apply Sinex declarative schema";
        wantedBy = [ "multi-user.target" ];
        wants = [ "network-online.target" ];
        after = [ "network-online.target" ] ++ localPostgresUnits;
        requires = localPostgresUnits;
        serviceConfig = {
          ExecStart = mkDatabasePasswordExec {
            name = "schema-apply";
            command = schemaApplyScript;
            passwordFile = cfg.database.passwordFile;
          };
          TimeoutStartSec = preflight.schemaApplyTimeoutSec;
        } // mkHelperServiceConfig {
          user = serviceUser;
          group = serviceUser;
          remainAfterExit = true;
        };
      };
    })

    (mkIf preflightEnabled {
      systemd.services =
        let
          guarded = genAttrs (map (u: removeSuffix ".service" u) unitsToGuard) guardUnit;
        in
        guarded // {
          sinex-preflight = {
            description = "Sinex pre-flight verification";
            wantedBy = [ "multi-user.target" ];
            wants = [ "network-online.target" ];
            after = [ "network-online.target" ]
              ++ schemaApplyUnits
              ++ localPostgresUnits
              ++ optionals natsEnabled [ "nats.service" ];
            # Require the services that preflight actively checks.
            requires = schemaApplyUnits
              ++ localPostgresUnits
              ++ optionals natsEnabled [ "nats.service" ];
            serviceConfig = {
              Type = "oneshot";
              TimeoutStartSec = preflight.timeoutSec;
              User = serviceUser;
              Group = serviceUser;
              ProtectSystem = "strict";
              ProtectHome = lib.mkForce "read-only";
              PrivateTmp = true;
              NoNewPrivileges = true;
              RestrictSUIDSGID = true;
              RemoveIPC = true;
              ProtectKernelTunables = true;
              ProtectControlGroups = true;
              RestrictRealtime = true;
              LockPersonality = true;
              SystemCallFilter = [ "@system-service" "~@privileged" ];
              ReadOnlyPaths = [ stateRoot logDir ingestSpool nodeSpool ];
              Environment = preflightEnvironment;
              ExecStart = mkDatabasePasswordExec {
                name = "preflight";
                command = runPreflightScript;
                passwordFile = cfg.database.passwordFile;
              };
            };
          };
        };

      assertions = [
        {
          assertion = unitsToGuard != [];
          message = "No services found to guard with pre-flight verification";
        }
      ];
    })

    (mkIf updatesEnabled {
      systemd.services.sinex-update = {
        description = "Sinex coordinated update";
        serviceConfig = {
          ExecStart = updateScript;
        } // mkHelperServiceConfig {
          user = "root";
          group = "root";
          remainAfterExit = true;
        };
      };
    })
  ];
}
