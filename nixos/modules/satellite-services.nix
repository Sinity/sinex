# Sinex Satellite Architecture NixOS Module
# Orchestrates the new constellation of satellite services
{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;

  # Helper function to generate satellite systemd service
  mkSatelliteService = name: serviceConfig: {
    description = "Sinex ${serviceConfig.description or name} Satellite";
    wantedBy = [ "multi-user.target" ];
    after = [ "network-online.target" ] ++ serviceConfig.after or [];
    wants = [ "network-online.target" ];
    requires = serviceConfig.requires or [];

    serviceConfig = {
      Type = "notify";
      User = cfg.satelliteUser;
      Group = cfg.satelliteUser;
      Restart = "on-failure";
      RestartSec = "10s";
      StartLimitIntervalSec = "300s";
      StartLimitBurst = 3;

      # Security hardening
      NoNewPrivileges = true;
      ProtectSystem = "strict";
      ProtectHome = true;
      PrivateTmp = true;
      RemoveIPC = true;
      ProtectKernelTunables = true;
      ProtectControlGroups = true;
      RestrictRealtime = true;
      LockPersonality = true;
      SystemCallFilter = [ "@system-service" "~@privileged" ];

      # Runtime directories
      RuntimeDirectory = "sinex";
      RuntimeDirectoryMode = "0755";
      
      # Working directory and state
      WorkingDirectory = serviceConfig.workingDirectory or "/var/lib/sinex";
      StateDirectory = "sinex";
      StateDirectoryMode = "0755";
      LogsDirectory = "sinex";
      LogsDirectoryMode = "0755";

      # Resource limits
      MemoryMax = serviceConfig.memoryLimit or "512M";
      CPUQuota = serviceConfig.cpuQuota or "50%";
      TasksMax = serviceConfig.tasksMax or 100;

      ExecStart = serviceConfig.execStart;
      Environment = serviceConfig.environment or [];
    } // (serviceConfig.serviceConfigOverrides or {});
  };

  # Generate satellite configurations
  satelliteConfigs = 
    let
      # Core Hub Services
      coreServices = mkIf cfg.satellite.coreServices.enable {
        # Ingestion Hub (sinex-ingestd)
        sinex-ingestd = mkSatelliteService "ingestd" {
          description = "Ingestion Hub";
          after = [ "postgresql.service" "redis.service" ];
          requires = [ "postgresql.service" "redis.service" ];
          execStart = "${cfg.package}/bin/sinex-ingestd --socket-path /run/sinex/ingest.sock --redis-url ${cfg.satellite.redis.url} --batch-size ${toString cfg.satellite.ingestd.batchSize} --log-level ${cfg.satellite.logLevel}";
          environment = [
            "DATABASE_URL=${cfg.satellite.database.url}"
            "RUST_LOG=${cfg.satellite.logLevel}"
          ];
          memoryLimit = "1G";
          cpuQuota = "100%";
          tasksMax = 200;
          serviceConfigOverrides = {
            # Need to create the socket directory
            ExecStartPre = pkgs.writeShellScript "ingestd-pre-start" ''
              mkdir -p /run/sinex
              chown ${cfg.satelliteUser}:${cfg.satelliteUser} /run/sinex
            '';
            # Allow socket creation
            ReadWritePaths = [ "/run/sinex" ];
          };
        };

        # API Gateway (sinex-gateway)
        sinex-gateway = mkSatelliteService "gateway" {
          description = "API Gateway";
          after = [ "postgresql.service" ];
          requires = [ "postgresql.service" ];
          execStart = "${cfg.package}/bin/sinex-gateway rpc-server --database-url ${cfg.satellite.database.url}";
          environment = [
            "DATABASE_URL=${cfg.satellite.database.url}"
            "RUST_LOG=${cfg.satellite.logLevel}"
          ];
          memoryLimit = "512M";
        };
      };

      # Event Source Satellites
      eventSourceServices = 
        let
          mkEventSource = name: sourceConfig: mkIf sourceConfig.enable (mkSatelliteService "sinex-${name}" {
            description = "${sourceConfig.description} Event Source";
            after = [ "sinex-ingestd.service" ];
            requires = [ "sinex-ingestd.service" ];
            execStart = "${cfg.package}/bin/sinex-${name} --ingest-socket /run/sinex/ingest.sock --batch-size ${toString sourceConfig.batchSize} --batch-timeout ${toString sourceConfig.batchTimeout} --log-level ${cfg.satellite.logLevel}" + 
              (if sourceConfig.extraArgs != "" then " ${sourceConfig.extraArgs}" else "");
            environment = [
              "RUST_LOG=${cfg.satellite.logLevel}"
            ] ++ sourceConfig.environment;
            memoryLimit = sourceConfig.memoryLimit;
            serviceConfigOverrides = sourceConfig.serviceConfigOverrides or {};
          });
        in {
          # Filesystem watcher
          sinex-fs-watcher = mkEventSource "fs-watcher" cfg.satellite.eventSources.filesystem;
          
          # Terminal event source
          sinex-terminal-satellite = mkEventSource "terminal-satellite" cfg.satellite.eventSources.terminal;
          
          # Desktop event source (clipboard, window manager)
          sinex-desktop-satellite = mkEventSource "desktop-satellite" cfg.satellite.eventSources.desktop;
          
          # System event source (dbus, journald)
          sinex-system-satellite = mkEventSource "system-satellite" cfg.satellite.eventSources.system;
        };

      # Automaton Satellites
      automatonServices = 
        let
          mkAutomaton = name: automatonConfig: mkIf automatonConfig.enable (mkSatelliteService "sinex-${name}" {
            description = "${automatonConfig.description} Automaton";
            after = [ "postgresql.service" "redis.service" "sinex-ingestd.service" ];
            requires = [ "postgresql.service" "redis.service" ];
            execStart = "${cfg.package}/bin/sinex-${name} --database-url ${cfg.satellite.database.url} --redis-url ${cfg.satellite.redis.url} --consumer-group ${automatonConfig.consumerGroup} --topics ${concatStringsSep "," automatonConfig.topics} --batch-size ${toString automatonConfig.batchSize} --checkpoint-interval ${toString automatonConfig.checkpointInterval} --log-level ${cfg.satellite.logLevel}";
            environment = [
              "DATABASE_URL=${cfg.satellite.database.url}"
              "SINEX_REDIS_URL=${cfg.satellite.redis.url}"
              "RUST_LOG=${cfg.satellite.logLevel}"
            ] ++ automatonConfig.environment;
            memoryLimit = automatonConfig.memoryLimit;
            cpuQuota = automatonConfig.cpuQuota;
          });
        in {
          # Terminal command canonicalizer
          sinex-terminal-command-canonicalizer = mkAutomaton "terminal-command-canonicalizer" cfg.satellite.automata.canonicalCommandSynthesizer;
          
          # Additional automata can be added here
        };

    in coreServices // eventSourceServices // automatonServices;

in {
  options.services.sinex.satellite = {
    enable = mkEnableOption "Sinex satellite architecture services";

    logLevel = mkOption {
      type = types.enum [ "trace" "debug" "info" "warn" "error" ];
      default = "info";
      description = "Log level for all satellite services";
    };

    database = {
      url = mkOption {
        type = types.str;
        default = "postgresql:///sinex_dev?host=/run/postgresql";
        description = "Database URL for satellite services";
      };
    };

    redis = {
      url = mkOption {
        type = types.str;
        default = "redis://localhost:6379";
        description = "Redis URL for message bus";
      };
    };

    # Core hub services configuration
    coreServices = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable core hub services (ingestd, host)";
      };
    };

    ingestd = {
      batchSize = mkOption {
        type = types.int;
        default = 1000;
        description = "Batch size for database writes";
      };

      batchTimeout = mkOption {
        type = types.int;
        default = 5;
        description = "Batch timeout in seconds";
      };
    };

    # Event source satellites configuration
    eventSources = {
      filesystem = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable filesystem watcher satellite";
        };

        description = mkOption {
          type = types.str;
          default = "Filesystem Watcher";
          readOnly = true;
        };

        batchSize = mkOption {
          type = types.int;
          default = 100;
          description = "Event batch size";
        };

        batchTimeout = mkOption {
          type = types.int;
          default = 5;
          description = "Batch timeout in seconds";
        };

        memoryLimit = mkOption {
          type = types.str;
          default = "256M";
          description = "Memory limit for filesystem watcher";
        };

        environment = mkOption {
          type = types.listOf types.str;
          default = [];
          description = "Additional environment variables";
        };

        extraArgs = mkOption {
          type = types.str;
          default = "";
          description = "Additional command line arguments";
        };

        serviceConfigOverrides = mkOption {
          type = types.attrs;
          default = {};
          description = "Additional systemd service configuration";
        };
      };

      terminal = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable terminal satellite";
        };

        description = mkOption {
          type = types.str;
          default = "Terminal Event Source";
          readOnly = true;
        };

        batchSize = mkOption {
          type = types.int;
          default = 100;
          description = "Event batch size";
        };

        batchTimeout = mkOption {
          type = types.int;
          default = 5;
          description = "Batch timeout in seconds";
        };

        memoryLimit = mkOption {
          type = types.str;
          default = "256M";
          description = "Memory limit for terminal satellite";
        };

        environment = mkOption {
          type = types.listOf types.str;
          default = [];
          description = "Additional environment variables";
        };

        extraArgs = mkOption {
          type = types.str;
          default = "";
          description = "Additional command line arguments";
        };

        serviceConfigOverrides = mkOption {
          type = types.attrs;
          default = {};
          description = "Additional systemd service configuration";
        };
      };

      desktop = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable desktop satellite (clipboard, window manager)";
        };

        description = mkOption {
          type = types.str;
          default = "Desktop Event Source";
          readOnly = true;
        };

        batchSize = mkOption {
          type = types.int;
          default = 50;
          description = "Event batch size";
        };

        batchTimeout = mkOption {
          type = types.int;
          default = 5;
          description = "Batch timeout in seconds";
        };

        memoryLimit = mkOption {
          type = types.str;
          default = "256M";
          description = "Memory limit for desktop satellite";
        };

        environment = mkOption {
          type = types.listOf types.str;
          default = [];
          description = "Additional environment variables";
        };

        extraArgs = mkOption {
          type = types.str;
          default = "";
          description = "Additional command line arguments";
        };

        serviceConfigOverrides = mkOption {
          type = types.attrs;
          default = {};
          description = "Additional systemd service configuration";
        };
      };

      system = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable system satellite (dbus, journald)";
        };

        description = mkOption {
          type = types.str;
          default = "System Event Source";
          readOnly = true;
        };

        batchSize = mkOption {
          type = types.int;
          default = 200;
          description = "Event batch size";
        };

        batchTimeout = mkOption {
          type = types.int;
          default = 10;
          description = "Batch timeout in seconds";
        };

        memoryLimit = mkOption {
          type = types.str;
          default = "384M";
          description = "Memory limit for system satellite";
        };

        environment = mkOption {
          type = types.listOf types.str;
          default = [];
          description = "Additional environment variables";
        };

        extraArgs = mkOption {
          type = types.str;
          default = "";
          description = "Additional command line arguments";
        };

        serviceConfigOverrides = mkOption {
          type = types.attrs;
          default = {
            # System satellite needs additional permissions
            CapabilityBoundingSet = [ "CAP_AUDIT_READ" "CAP_DAC_READ_SEARCH" ];
            AmbientCapabilities = [ "CAP_AUDIT_READ" "CAP_DAC_READ_SEARCH" ];
            PrivateUsers = false;
          };
          description = "Additional systemd service configuration";
        };
      };
    };

    # Automaton satellites configuration
    automata = {
      canonicalCommandSynthesizer = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable canonical command synthesizer automaton";
        };

        description = mkOption {
          type = types.str;
          default = "Canonical Command Synthesizer";
          readOnly = true;
        };

        consumerGroup = mkOption {
          type = types.str;
          default = "canonical-synthesizers";
          description = "Redis Streams consumer group";
        };

        topics = mkOption {
          type = types.listOf types.str;
          default = [ "sinex:events:kitty" "sinex:events:atuin" ];
          description = "Redis Streams topics to consume";
        };

        batchSize = mkOption {
          type = types.int;
          default = 50;
          description = "Processing batch size";
        };

        checkpointInterval = mkOption {
          type = types.int;
          default = 30;
          description = "Checkpoint interval in seconds";
        };

        memoryLimit = mkOption {
          type = types.str;
          default = "512M";
          description = "Memory limit";
        };

        cpuQuota = mkOption {
          type = types.str;
          default = "50%";
          description = "CPU quota";
        };

        environment = mkOption {
          type = types.listOf types.str;
          default = [];
          description = "Additional environment variables";
        };
      };

      # Template for additional automata
      # enricher = { ... };
      # serviceResponder = { ... };
    };
  };

  config = mkIf (cfg.enable && cfg.satellite.enable) {
    # Create satellite user
    users.users.${cfg.satelliteUser} = {
      isSystemUser = true;
      group = cfg.satelliteUser;
      description = "Sinex satellite services user";
      home = "/var/lib/sinex";
      createHome = true;
    };

    users.groups.${cfg.satelliteUser} = {};

    # Enable required services
    services.postgresql = mkIf cfg.database.autoSetup {
      enable = true;
      ensureDatabases = [ cfg.database.name ];
      ensureUsers = [
        {
          name = cfg.satelliteUser;
          ensureDBOwnership = true;
        }
      ];
    };

    services.redis.servers."sinex" = {
      enable = true;
      port = 6379;
      bind = "127.0.0.1";
      settings = {
        # Enable Redis Streams
        "maxmemory-policy" = "allkeys-lru";
        "save" = "900 1 300 10 60 10000";
      };
    };

    # Generate systemd services for all satellites
    systemd.services = satelliteConfigs;

    # Directory setup
    systemd.tmpfiles.rules = [
      "d /var/lib/sinex 0755 ${cfg.satelliteUser} ${cfg.satelliteUser} -"
      "d /var/log/sinex 0755 ${cfg.satelliteUser} ${cfg.satelliteUser} -"
      "d /run/sinex 0755 ${cfg.satelliteUser} ${cfg.satelliteUser} -"
    ];

    # Add satellite user option if not already defined
    services.sinex.satelliteUser = mkDefault cfg.database.user;

    # Assertions
    assertions = [
      {
        assertion = cfg.satellite.enable -> cfg.satellite.database.url != "";
        message = "Database URL must be configured for satellite services";
      }
      {
        assertion = cfg.satellite.enable -> cfg.satellite.redis.url != "";
        message = "Redis URL must be configured for satellite services";
      }
    ];
  };
}