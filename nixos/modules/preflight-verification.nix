# Sinex Pre-Flight Verification Module
# Implements the Pre-Flight Verification Model for safe, zero-downtime deployments
{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  
  # Pre-flight verification configuration
  preflightCfg = cfg.preflightVerification;
  
in
{
  options.services.sinex.preflightVerification = {
    enable = mkOption {
      type = types.bool;
      default = true;
      description = ''
        Enable pre-flight verification for all Sinex service deployments.
        This ensures that new versions are thoroughly tested before activation.
      '';
    };
    
    timeout = mkOption {
      type = types.int;
      default = 120;
      description = "Timeout for complete verification process in seconds";
    };
    
    skipPhases = mkOption {
      type = types.listOf (types.enum [ 
        "database" "extensions" "migrations" "resources" 
        "configuration" "services" "integration" 
      ]);
      default = [];
      description = "Verification phases to skip (use with caution)";
    };
    
    failureAction = mkOption {
      type = types.enum [ "abort" "warn" "ignore" ];
      default = "abort";
      description = ''
        Action to take when pre-flight verification fails:
        - abort: Stop deployment (recommended)
        - warn: Continue with warnings
        - ignore: Continue anyway (dangerous)
      '';
    };
    
    recordResults = mkOption {
      type = types.bool;
      default = true;
      description = "Record verification results in database for monitoring";
    };
    
    notifications = {
      enable = mkOption {
        type = types.bool;
        default = false;
        description = "Enable notifications for verification results";
      };
      
      onFailure = mkOption {
        type = types.bool;
        default = true;
        description = "Send notifications on verification failure";
      };
      
      onSuccess = mkOption {
        type = types.bool;
        default = false;
        description = "Send notifications on verification success";
      };
    };

    requiredUnits = mkOption {
      type = types.listOf types.str;
      default = [];
      description = ''
        Systemd service names that must depend on a successful
        sinex-preflight run before starting. When left empty, the module
        derives the list from services.sinex.satellite.generatedUnits or falls
        back to the core ingestion/gateway services.
      '';
    };
  };

  config = mkIf (cfg.enable && preflightCfg.enable) (
    let
      generatedUnits = config.services.sinex.satellite.generatedUnits or [];
      fallbackUnits = [ "sinex-ingestd" "sinex-gateway" ];
      normalizeUnitName = unit:
        let trimmed = if lib.hasSuffix ".service" unit then lib.removeSuffix ".service" unit else unit;
        in trimmed;
      defaultUnits = if generatedUnits != [] then generatedUnits else fallbackUnits;
      rawRequiredUnits =
        let explicit = preflightCfg.requiredUnits;
        in if explicit != [] then explicit else defaultUnits;
      requiredUnits = map normalizeUnitName rawRequiredUnits;
      rawUpdateUnits =
        let explicitUpdate = (cfg.update.units or []);
        in if explicitUpdate != [] then explicitUpdate else rawRequiredUnits;
      updateUnits = map normalizeUnitName rawUpdateUnits;
      skipArgsString =
        if preflightCfg.skipPhases == [] then ""
        else " " + lib.concatStringsSep " " (map (phase: "--skip ${phase}") preflightCfg.skipPhases);
      databaseUrl = "postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}";
      psqlBin = "${cfg.database.package}/bin/psql";
      recordResults = preflightCfg.recordResults;
      preserveDataFlag = cfg.update.preserveData;
      rollbackFlag = cfg.update.rollbackOnFailure;
      dlqEnabled = cfg.dlq.enable or false;
      dlqPath = cfg.dlq.failureStoragePath;
      unitsShellList = lib.concatStringsSep " " (map (unit: lib.escapeShellArg "${unit}.service") updateUnits);
      preflightCheckScript = unit: pkgs.writeShellScript "sinex-preflight-gate-${lib.strings.sanitizeDerivationName unit}" ''
        set -euo pipefail
        UNIT="${unit}.service"

        if ! systemctl is-active --quiet sinex-preflight.service; then
          echo "ERROR: sinex-preflight.service has not completed successfully; refusing to start $UNIT" >&2
          exit 1
        fi

        if ! systemctl show -p Result sinex-preflight.service | grep -q "Result=success"; then
          echo "ERROR: last sinex-preflight run did not succeed; refusing to start $UNIT" >&2
          exit 1
        fi
      '';
      runPreflightScript = pkgs.writeShellScript "sinex-preflight-run" ''
        set -euo pipefail

        RECORD_RESULTS=${if recordResults then "1" else "0"}
        NOTIFY_ENABLE=${if preflightCfg.notifications.enable then "1" else "0"}
        NOTIFY_ON_FAILURE=${if preflightCfg.notifications.onFailure then "1" else "0"}
        NOTIFY_ON_SUCCESS=${if preflightCfg.notifications.onSuccess then "1" else "0"}
        PSQL_BIN="${psqlBin}"
        DATABASE_URL="${databaseUrl}"

        echo "$(date): starting Sinex pre-flight verification"

        VERIFY_CMD="${cfg.package}/bin/sinex-preflight verify --timeout ${toString preflightCfg.timeout} --output json${skipArgsString}"
        echo "Executing: $VERIFY_CMD"

        if VERIFICATION_RESULT=$($VERIFY_CMD); then
          echo "✓ Sinex pre-flight verification passed"
          echo "$VERIFICATION_RESULT" | ${pkgs.jq}/bin/jq .

          if [ "$NOTIFY_ENABLE" = "1" ] && [ "$NOTIFY_ON_SUCCESS" = "1" ]; then
            logger "Sinex pre-flight verification succeeded"
          fi

          if [ "$RECORD_RESULTS" = "1" ]; then
            HOST=$(hostname)
            NOW=$(date +%s)
            "$PSQL_BIN" "$DATABASE_URL" --command "INSERT INTO component_heartbeats (component_name, instance_id, status, metadata, last_seen) VALUES ('sinex-preflight', ''${HOST}-''${NOW}', 'success', '{"event":"preflight_pass"}'::jsonb, NOW())" || true
          fi

          exit 0
        else
          STATUS=$?
          echo "✗ Sinex pre-flight verification failed (exit code: $STATUS)"

          if [ -n "''${VERIFICATION_RESULT:-}" ]; then
            echo "$VERIFICATION_RESULT" | ${pkgs.jq}/bin/jq . || echo "$VERIFICATION_RESULT"
          fi

          if [ "$NOTIFY_ENABLE" = "1" ] && [ "$NOTIFY_ON_FAILURE" = "1" ]; then
            logger "Sinex pre-flight verification failed"
          fi

          if [ "$RECORD_RESULTS" = "1" ]; then
            HOST=$(hostname)
            NOW=$(date +%s)
            "$PSQL_BIN" "$DATABASE_URL" --command "INSERT INTO component_heartbeats (component_name, instance_id, status, metadata, last_seen) VALUES ('sinex-preflight', ''${HOST}-''${NOW}', 'failure', '{"event":"preflight_fail"}'::jsonb, NOW())" || true
          fi

          case "${preflightCfg.failureAction}" in
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
        fi
      '';
      updateScript = pkgs.writeShellScript "sinex-coordinated-update" ''
        set -euo pipefail

        UNITS=(${unitsShellList})
        if [ "''${#UNITS[@]}" -eq 0 ]; then
          echo "No units configured for coordinated update; nothing to do."
          exit 0
        fi

        echo "$(date): running pre-flight verification prior to coordinated update"
        if ! systemctl start sinex-preflight.service; then
          echo "ERROR: sinex-preflight.service failed to start" >&2
          exit 1
        fi

        if ! systemctl is-active --quiet sinex-preflight.service; then
          echo "ERROR: sinex-preflight.service is not active" >&2
          exit 1
        fi

        ACTIVE_UNITS=()
        for unit in "''${UNITS[@]}"; do
          if systemctl is-active --quiet "$unit"; then
            ACTIVE_UNITS+=("$unit")
          fi
        done

        echo "Units participating in coordinated update: ''${#ACTIVE_UNITS[@]}"
        if [ "''${#ACTIVE_UNITS[@]}" -eq 0 ]; then
          exit 0
        fi

        PRESERVE_DATA=${if preserveDataFlag then "1" else "0"}
        ROLLBACK_ENABLED=${if rollbackFlag then "1" else "0"}
        DLQ_ENABLED=${if dlqEnabled then "1" else "0"}
        DLQ_PATH="${dlqPath}"
        PSQL_BIN="${psqlBin}"
        DATABASE_URL="${databaseUrl}"
        RECORD_RESULTS=${if recordResults then "1" else "0"}
        HEALTH_TIMEOUT=${toString cfg.update.healthCheckTimeout}
        GRACE_PERIOD=${toString cfg.update.gracePeriod}

        BACKUP_DIR=""
        if [ "$PRESERVE_DATA" = "1" ] && [ "$DLQ_ENABLED" = "1" ] && [ -d "$DLQ_PATH" ]; then
          BACKUP_DIR="${dlqPath}.backup-$(date +%Y%m%d-%H%M%S)"
          echo "Preserving DLQ data to $BACKUP_DIR"
          cp -a "$DLQ_PATH" "$BACKUP_DIR" || true
        fi

        echo "Stopping active units..."
        for unit in "''${ACTIVE_UNITS[@]}"; do
          systemctl stop "$unit" || true
        done

        if [ "''${#ACTIVE_UNITS[@]}" -gt 0 ]; then
          sleep "$GRACE_PERIOD"
        fi

        echo "Starting units..."
        for unit in "''${ACTIVE_UNITS[@]}"; do
          systemctl start "$unit"
        done

        DEADLINE=$(( $(date +%s) + HEALTH_TIMEOUT ))
        FAILED_UNITS=()

        for unit in "''${ACTIVE_UNITS[@]}"; do
          while ! systemctl is-active --quiet "$unit"; do
            if [ $(date +%s) -ge $DEADLINE ]; then
              echo "ERROR: $unit failed to become active within timeout" >&2
              FAILED_UNITS+=("$unit")
              break
            fi
            sleep 5
          done
        done

        if [ "''${#FAILED_UNITS[@]}" -gt 0 ]; then
          echo "Update failed for units: ''${FAILED_UNITS[*]}" >&2
          if [ "$ROLLBACK_ENABLED" = "1" ]; then
            echo "Attempting rollback..."
            for unit in "''${ACTIVE_UNITS[@]}"; do
              systemctl stop "$unit" || true
            done

            if [ -n "$BACKUP_DIR" ] && [ -d "$BACKUP_DIR" ]; then
              rm -rf "$DLQ_PATH"
              mv "$BACKUP_DIR" "$DLQ_PATH"
            fi

            if [ "$RECORD_RESULTS" = "1" ]; then
              HOST=$(hostname)
              NOW=$(date +%s)
              FAILED_COUNT="''${#FAILED_UNITS[@]}"
              "$PSQL_BIN" "$DATABASE_URL" --command "INSERT INTO component_heartbeats (component_name, instance_id, status, metadata, last_seen) VALUES ('sinex-deployment', ''${HOST}-''${NOW}', 'rollback', ('{\"event\":\"update_rollback\",\"failed_count\":' || ''${FAILED_COUNT}::text || '}')::jsonb, NOW())" || true
            fi
          fi
          exit 1
        fi

        if [ -n "$BACKUP_DIR" ] && [ -d "$BACKUP_DIR" ]; then
          rm -rf "$BACKUP_DIR"
        fi

        if [ "$RECORD_RESULTS" = "1" ]; then
          HOST=$(hostname)
          NOW=$(date +%s)
          UNIT_COUNT="''${#ACTIVE_UNITS[@]}"
          "$PSQL_BIN" "$DATABASE_URL" --command "INSERT INTO component_heartbeats (component_name, instance_id, status, metadata, last_seen) VALUES ('sinex-deployment', ''${HOST}-''${NOW}', 'success', ('{\"event\":\"update_completed\",\"unit_count\":' || ''${UNIT_COUNT}::text || '}')::jsonb, NOW())" || true
        fi

        echo "$(date): coordinated update completed successfully"
      '';
    in
    {
      systemd.services =
        let
          guardUnits = lib.genAttrs requiredUnits (unit: {
            after = lib.mkAfter [ "sinex-preflight.service" ];
            requires = lib.mkAfter [ "sinex-preflight.service" ];
            serviceConfig.ExecStartPre = lib.mkAfter [ "${preflightCheckScript unit}" ];
          });
        in
        {
          sinex-preflight = {
            description = "Sinex Pre-Flight Verification";
            wantedBy = [ "multi-user.target" ];
            after = [ "network-online.target" "postgresql.service" ];
            requires = [ "postgresql.service" ];
            serviceConfig = {
              Type = "oneshot";
              RemainAfterExit = true;
              TimeoutStartSec = preflightCfg.timeout;
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
              ReadOnlyPaths = [
                "/etc/sinex"
                cfg.directories.state
                cfg.directories.logs
                cfg.directories.spool.base
              ];
              Environment = [
                "DATABASE_URL=${databaseUrl}"
                "RUST_LOG=sinex_preflight=info"
              ];
              ExecStart = "${runPreflightScript}";
            };
          };
        }
        // lib.optionalAttrs cfg.update.enable {
          sinex-update = {
            description = "Sinex Coordinated Update";
            serviceConfig = {
              Type = "oneshot";
              RemainAfterExit = true;
              ExecStart = "${updateScript}";
            };
          };
        }
        // guardUnits;

      assertions = [
        {
          assertion = preflightCfg.enable -> (cfg.database.autoSetup || config.services.postgresql.enable);
          message = "Pre-flight verification requires PostgreSQL to be enabled";
        }
        {
          assertion = recordResults -> (cfg.database.autoSetup || config.services.postgresql.enable);
          message = "Recording verification results requires database access";
        }
        {
          assertion = requiredUnits != [];
          message = "No services found to guard with pre-flight verification";
        }
      ];
    }
  );

}
