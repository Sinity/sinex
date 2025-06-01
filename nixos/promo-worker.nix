{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex.promoWorker;
in
{
  options.services.sinex.promoWorker = {
    enable = mkEnableOption "Sinex promotion worker";

    package = mkOption {
      type = types.package;
      default = pkgs.sinex.promoWorker;
      description = "Package providing the sinex-promo-worker binary";
    };

    databaseUrl = mkOption {
      type = types.str;
      description = "PostgreSQL connection string";
      example = "postgres://sinex_app:password@localhost/sinex";
    };

    agentName = mkOption {
      type = types.str;
      description = "Agent name to process events for";
      example = "ExamplePromotionAgent_v1.0.0";
    };

    workerId = mkOption {
      type = types.nullOr types.str;
      default = null;
      description = "Worker ID (defaults to hostname-pid)";
    };

    metricsPort = mkOption {
      type = types.port;
      default = 9090;
      description = "Port for Prometheus metrics endpoint";
    };

    batchSize = mkOption {
      type = types.int;
      default = 10;
      description = "Number of items to process in each batch";
    };

    pollInterval = mkOption {
      type = types.int;
      default = 1;
      description = "Poll interval in seconds when no work available";
    };

    logLevel = mkOption {
      type = types.str;
      default = "info";
      description = "Log level (trace, debug, info, warn, error)";
    };

    extraArgs = mkOption {
      type = types.listOf types.str;
      default = [];
      description = "Extra arguments to pass to the worker";
    };
  };

  config = mkIf cfg.enable {
    systemd.services.sinex-promo-worker = {
      description = "Sinex promotion worker for ${cfg.agentName}";
      after = [ "network.target" "postgresql.service" ];
      wantedBy = [ "multi-user.target" ];

      environment = {
        DATABASE_URL = cfg.databaseUrl;
        AGENT_NAME = cfg.agentName;
        METRICS_PORT = toString cfg.metricsPort;
        BATCH_SIZE = toString cfg.batchSize;
        POLL_INTERVAL = toString cfg.pollInterval;
        RUST_LOG = cfg.logLevel;
      } // optionalAttrs (cfg.workerId != null) {
        WORKER_ID = cfg.workerId;
      };

      serviceConfig = {
        Type = "simple";
        ExecStart = "${cfg.package}/bin/sinex-promo-worker ${escapeShellArgs cfg.extraArgs}";
        Restart = "always";
        RestartSec = 5;

        # Security hardening
        DynamicUser = true;
        PrivateTmp = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        NoNewPrivileges = true;
        RestrictNamespaces = true;
        RestrictRealtime = true;
        RestrictSUIDSGID = true;
        RemoveIPC = true;
        LockPersonality = true;
        ProtectClock = true;
        ProtectHostname = true;
        ProtectKernelLogs = true;
        ProtectKernelModules = true;
        ProtectKernelTunables = true;
        ProtectControlGroups = true;
        RestrictAddressFamilies = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
        SystemCallArchitectures = "native";
        SystemCallFilter = [ "@system-service" "~@privileged" ];
      };
    };

    # Open metrics port in firewall if enabled
    networking.firewall.allowedTCPPorts = mkIf config.networking.firewall.enable [ cfg.metricsPort ];
  };
}