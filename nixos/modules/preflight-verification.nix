{ config, lib, pkgs, ... }:

with lib;

let
  systemdHardening = import ./lib/systemd-hardening.nix { inherit lib; };
  inherit (systemdHardening) mkHelperServiceConfig;
  cfg = config.services.sinex;
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

  databaseUrl = "postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}";
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
          ExecStart = schemaApplyScript;
          TimeoutStartSec = preflight.timeoutSec;
        } // mkHelperServiceConfig {
          user = cfg.database.user;
          group = cfg.database.user;
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
              User = cfg.database.user;
              Group = cfg.database.user;
              ProtectSystem = "strict";
              ProtectHome = true;
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
              Environment = [ "DATABASE_URL=${databaseUrl}" ];
              ExecStart = runPreflightScript;
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
