{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  lifecycle = cfg.lifecycle;
  preflight = lifecycle.preflight;
  updates = lifecycle.updates;
  natsEnabled = cfg.nats.enable || cfg.nats.autoSetup;

  sinexEnabled = cfg.enable;
  preflightEnabled = sinexEnabled && preflight.enable;
  updatesEnabled = sinexEnabled && updates.enable;

  generatedUnits = config.services.sinex.nodes.generatedUnits or [];
  coreUnits = [ "sinex-ingestd" "sinex-gateway" ];
  unitsToGuard = if generatedUnits != [] then generatedUnits else coreUnits;

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
    fi

    STATUS=$?
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

  unitsShellList = concatStringsSep " " (map (unit: escapeShellArg "${unit}.service") unitsToGuard);

  updateScript = pkgs.writeShellScript "sinex-coordinated-update" ''
    set -euo pipefail

    UNITS=(${unitsShellList})
    if [ "''${#UNITS[@]}" -eq 0 ]; then
      echo "No units configured for coordinated update; nothing to do."
      exit 0
    fi

    echo "$(date): running Sinex coordinated update"
    if ! systemctl start sinex-preflight.service; then
      echo "ERROR: pre-flight verification failed to start" >&2
      exit 1
    fi

    if ! systemctl is-active --quiet sinex-preflight.service; then
      echo "ERROR: pre-flight verification did not complete" >&2
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
      if ! systemctl is-active --quiet sinex-preflight.service; then
        echo "ERROR: sinex-preflight.service has not run successfully; refusing to start $UNIT" >&2
        exit 1
      fi
    '') ];
  };

in
{
  config = mkMerge [
    (mkIf preflightEnabled {
      systemd.services =
        let
          guarded = genAttrs (map (u: removeSuffix ".service" u) unitsToGuard) guardUnit;
        in
        guarded // {
          sinex-preflight = {
            description = "Sinex pre-flight verification";
            wantedBy = [ "multi-user.target" ];
            after = [ "network-online.target" "postgresql.service" ]
              ++ optionals natsEnabled [ "nats.service" ];
            # Require the services that preflight actively checks.
            requires = [ "postgresql.service" ]
              ++ optionals natsEnabled [ "nats.service" ];
            serviceConfig = {
              Type = "oneshot";
              RemainAfterExit = true;
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
          assertion = cfg.database.autoSetup || config.services.postgresql.enable;
          message = "Pre-flight verification requires PostgreSQL to be enabled";
        }
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
          Type = "oneshot";
          RemainAfterExit = true;
          ExecStart = updateScript;
        };
      };
    })
  ];
}
