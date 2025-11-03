# Sinex Satellite Architecture NixOS Module
# Orchestrates the new constellation of satellite services
{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  coordinationDependencies =
    lib.optionals (cfg.satellite.coordination.enable or false) [
      "sinex-coordination-setup.service"
    ];
  stateDir = cfg.directories.state;
  logsDir = cfg.directories.logs;
  runtimeDir = cfg.directories.runtime;
  spoolBaseDir = cfg.directories.spool.base;
  spoolIngestdDir = cfg.directories.spool.ingestd;
  spoolSatellitesDir = cfg.directories.spool.satellites;
  dlqDir = cfg.directories.dlq;

  # Helper function to generate satellite systemd service with coordination support
  mkSatelliteService = name: serviceConfig: instanceId: {
    description = "Sinex ${serviceConfig.description or name} Satellite (Instance ${instanceId})";
    wantedBy = [ "multi-user.target" ];
    after =
      [ "network-online.target" ]
      ++ coordinationDependencies
      ++ (serviceConfig.after or []);
    wants = [ "network-online.target" ] ++ (serviceConfig.wants or []);
    requires = coordinationDependencies ++ (serviceConfig.requires or []);

    serviceConfig = {
      Type = "notify";
      User = cfg.satelliteUser;
      Group = cfg.satelliteUser;
      Restart = "on-failure";
      RestartSec = "10s";
      StartLimitIntervalSec = "300s";
      StartLimitBurst = 3;
      
      # Graceful shutdown for coordination handoff
      TimeoutStopSec = "120s";
      KillMode = "mixed";
      KillSignal = "SIGTERM";

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

      # Working directory respects configured state path
      WorkingDirectory = serviceConfig.workingDirectory or stateDir;

      # Resource limits
      MemoryMax = serviceConfig.memoryLimit or "512M";
      CPUQuota = serviceConfig.cpuQuota or "50%";
      TasksMax = serviceConfig.tasksMax or 100;

      ExecStart = serviceConfig.execStart;
      EnvironmentFile =
        (cfg.satellite.environmentFiles or [])
        ++ (serviceConfig.environmentFiles or []);
      Environment =
        (cfg.satellite.environment or [])
        ++ (serviceConfig.environment or [])
        ++ [
        "SINEX_STATE_DIR=${stateDir}"
        "SINEX_LOG_DIR=${logsDir}"
        "SINEX_RUNTIME_DIR=${runtimeDir}"
        "SINEX_SPOOL_BASE=${spoolBaseDir}"
        "SINEX_SPOOL_INGESTD=${spoolIngestdDir}"
        "SINEX_SPOOL_SATELLITES=${spoolSatellitesDir}"
        "SINEX_DLQ_DIR=${dlqDir}"
        "COORDINATION_HEARTBEAT_INTERVAL=${toString cfg.satellite.coordination.heartbeatInterval}"
        "COORDINATION_LEADERSHIP_TIMEOUT=${toString cfg.satellite.coordination.leadershipTimeout}"
        "COORDINATION_HANDOFF_TIMEOUT=${toString cfg.satellite.coordination.handoffTimeout}"
        "COORDINATION_INSTANCE_ID=${instanceId}"
      ];
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
          after = [ "postgresql.service" "nats.service" ];
          requires = [ "postgresql.service" "nats.service" ];
          execStart = "${cfg.package}/bin/sinex-ingestd --nats-url ${cfg.satellite.nats.servers} --batch-size ${toString cfg.satellite.ingestd.batchSize} --log-level ${cfg.satellite.logLevel}";
          environment = [
            "DATABASE_URL=${cfg.satellite.database.url}"
            "RUST_LOG=${cfg.satellite.logLevel}"
          ];
          memoryLimit = "1G";
          cpuQuota = "100%";
          tasksMax = 200;
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

      # Event Source Satellites with Hot Standby Support
      eventSourceServices = 
        let
          ingestDependencies = lib.optionals (cfg.satellite.coreServices.enable or false) [ "sinex-ingestd.service" ];

          mkEventSource = name: sourceConfig:
            let
              derivedExtraArgs =
                if name == "fs-watcher" then
                  let
                    watchPaths = cfg.satellite.eventSources.filesystem.watchPaths;
                  in
                  if watchPaths == [] then
                    ""
                  else
                    let
                      processorConfig = builtins.toJSON {
                        filesystem = {
                          watch_paths = watchPaths;
                        };
                    };
                  in "--processor-config ${lib.escapeShellArg processorConfig}"
                else "";

              combinedExtraArgs =
                lib.concatStringsSep " " (lib.filter (arg: arg != "") [
                  derivedExtraArgs
                  sourceConfig.extraArgs
                ]);
            in
            mkIf sourceConfig.enable (
            # Generate multiple instances for hot standby pattern
            lib.listToAttrs (map (instanceNum: 
              let instanceId = "${name}-${toString instanceNum}"; in
              lib.nameValuePair "sinex-${instanceId}" (mkSatelliteService "sinex-${name}" {
                description = "${sourceConfig.description} Event Source";
                after = lib.unique ([ "nats.service" ] ++ ingestDependencies ++ (sourceConfig.after or []));
                requires = lib.unique ([ "nats.service" ] ++ ingestDependencies ++ (sourceConfig.requires or []));
                wants = (sourceConfig.wants or []) ++ ingestDependencies;
                execStart = "${cfg.package}/bin/sinex-${name} --service-name sinex-${name} --nats-url ${cfg.satellite.nats.servers} --verbose 1 service" +
                  (if combinedExtraArgs != "" then " ${combinedExtraArgs}" else "");
                environment = [
                  "RUST_LOG=${cfg.satellite.logLevel}"
                  "DATABASE_URL=${cfg.satellite.database.url}"
                ] ++ sourceConfig.environment;
                memoryLimit = sourceConfig.memoryLimit;
                serviceConfigOverrides = sourceConfig.serviceConfigOverrides or {};
              } instanceId)
            ) (lib.range 1 sourceConfig.instances))
          );
        in
          (mkEventSource "fs-watcher" cfg.satellite.eventSources.filesystem) //
          (mkEventSource "terminal-satellite" cfg.satellite.eventSources.terminal) //
          (mkEventSource "desktop-satellite" cfg.satellite.eventSources.desktop) //
          (mkEventSource "system-satellite" cfg.satellite.eventSources.system);

      # Automaton Satellites
      automatonServices = 
        let
          mkAutomaton = name: automatonConfig: mkIf automatonConfig.enable (mkSatelliteService "sinex-${name}" {
            description = "${automatonConfig.description} Automaton";
            after = [ "postgresql.service" "nats.service" ];
            requires = [ "postgresql.service" "nats.service" ];
            execStart = "${cfg.package}/bin/sinex-${name} --service-name sinex-${name} --nats-url ${cfg.satellite.nats.servers} --verbose 1 service";
            environment = [
              "DATABASE_URL=${cfg.satellite.database.url}"
              "RUST_LOG=${cfg.satellite.logLevel}"
            ] ++ automatonConfig.environment;
            memoryLimit = automatonConfig.memoryLimit;
            cpuQuota = automatonConfig.cpuQuota;
          });
        in {
          # Terminal command canonicalizer
          sinex-terminal-command-canonicalizer = mkAutomaton "terminal-command-canonicalizer" cfg.satellite.automata.canonicalCommandSynthesizer;
          
          # Health aggregator
          sinex-health-aggregator = mkAutomaton "health-aggregator" cfg.satellite.automata.healthAggregator;
          
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

    environment = mkOption {
      type = types.listOf (types.strMatching "^[^=]+=.+$");
      default = [];
      example = [ "SINEX_NATS_TOKEN=env:prod/nats/token" ];
      description = "Additional environment variables applied to every satellite unit (KEY=value format).";
    };

    environmentFiles = mkOption {
      type = types.listOf (types.oneOf [ types.path types.str ]);
      default = [];
      description = "Environment files to include for each satellite unit (use for secrets or TLS material).";
    };

    database = {
      url = mkOption {
        type = types.str;
        default = "postgresql:///sinex_dev?host=/run/postgresql";
        description = "Database URL for satellite services";
      };
    };


    nats = {
      port = mkOption {
        type = types.port;
        default = 4222;
        description = "Port for the embedded NATS server";
      };

      monitoringPort = mkOption {
        type = types.port;
        default = 8222;
        description = "HTTP monitoring/metrics port for NATS";
      };

      servers = mkOption {
        type = types.str;
        default = "nats://127.0.0.1:4222";
        description = "NATS server URLs (comma-separated)";
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

    # Coordination system configuration
    coordination = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable satellite coordination system with hot standby";
      };

      heartbeatInterval = mkOption {
        type = types.int;
        default = 30;
        description = "Heartbeat interval in seconds";
      };

      leadershipTimeout = mkOption {
        type = types.int;
        default = 120;
        description = "Leadership acquisition timeout in seconds";
      };

      handoffTimeout = mkOption {
        type = types.int;
        default = 60;
        description = "Graceful handoff timeout in seconds";
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

        instances = mkOption {
          type = types.ints.positive;
          default = 2;
          description = "Number of instances for hot standby (2-3 recommended)";
        };

        watchPaths = mkOption {
          type = types.listOf types.str;
          default = [ "~/" ];
          description = "Paths to monitor for filesystem events.";
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

        instances = mkOption {
          type = types.ints.positive;
          default = 2;
          description = "Number of instances for hot standby (2-3 recommended)";
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

        instances = mkOption {
          type = types.ints.positive;
          default = 2;
          description = "Number of instances for hot standby (2-3 recommended)";
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

        instances = mkOption {
          type = types.ints.positive;
          default = 2;
          description = "Number of instances for hot standby (2-3 recommended)";
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
          description = "NATS consumer group";
        };

        subjects = mkOption {
          type = types.listOf types.str;
          default = [ "events.terminal.*" "events.filesystem.*" ];
          description = "NATS JetStream subjects to consume";
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

      healthAggregator = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable health aggregator automaton";
        };

        description = mkOption {
          type = types.str;
          default = "Health Aggregator";
          readOnly = true;
        };

        consumerGroup = mkOption {
          type = types.str;
          default = "health-aggregators";
          description = "NATS consumer group";
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

    generatedUnits = mkOption {
      type = types.listOf types.str;
      internal = true;
      readOnly = true;
      description = "Systemd unit names generated by the satellite module.";
    };
  };

  config = mkIf (cfg.enable && cfg.satellite.enable && (cfg.serviceManagement.serviceGroups.core or true)) {
    services.sinex.satelliteUser = mkDefault cfg.database.user;
    services.sinex.satellite.database.url =
      mkDefault "postgresql:///${cfg.database.name}?host=/run/postgresql";

    services.sinex.database.additionalUsers = mkIf cfg.database.autoSetup (mkAfter [
      {
        name = cfg.satelliteUser;
        ensureDBOwnership = true;
      }
    ]);

    # NATS JetStream service configuration
    services.nats = {
      enable = true;
      serverName = "sinex-nats";
      jetstream = true;
      port = cfg.satellite.nats.port;
      settings = {
        server_name = "sinex-nats";
        host = "127.0.0.1";
        http = "127.0.0.1:${toString cfg.satellite.nats.monitoringPort}";
        jetstream = {
          store_dir = lib.mkDefault "/var/lib/nats/jetstream";
          max_memory_store = "1G";
          max_file_store = "10G";
        };
      };
    };

    services.sinex.satellite.nats.servers = mkDefault "nats://127.0.0.1:${toString cfg.satellite.nats.port}";

    # Generate systemd services for all satellites and supporting setup jobs
    systemd.services =
      satelliteConfigs
      // optionalAttrs cfg.satellite.coordination.enable {
        sinex-coordination-setup = {
          description = "Setup Sinex Coordination Database Tables";
          wantedBy = [ "multi-user.target" ];
          after = [ "postgresql.service" ];
          requires = [ "postgresql.service" ];
          serviceConfig = {
            Type = "oneshot";
            User = cfg.satelliteUser;
            Group = cfg.satelliteUser;
            RemainAfterExit = true;
          };
          script = ''
            ${pkgs.postgresql}/bin/psql "${cfg.satellite.database.url}" <<'EOF'
            CREATE SCHEMA IF NOT EXISTS core;

            -- Create satellite coordination tables
            CREATE TABLE IF NOT EXISTS core.satellite_instances (
                instance_id UUID PRIMARY KEY,
                service_name TEXT NOT NULL,
                version TEXT NOT NULL,
                start_time TIMESTAMPTZ NOT NULL,
                last_heartbeat TIMESTAMPTZ NOT NULL,
                host_name TEXT NOT NULL,
                metadata JSONB DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS core.satellite_signals (
                id SERIAL PRIMARY KEY,
                target_instance TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                message TEXT,
                payload JSONB DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                processed_at TIMESTAMPTZ,
                processed_by UUID
            );

            CREATE TABLE IF NOT EXISTS core.service_leadership (
                service_name TEXT PRIMARY KEY,
                instance_id UUID NOT NULL,
                acquired_at TIMESTAMPTZ NOT NULL,
                last_heartbeat TIMESTAMPTZ NOT NULL,
                version TEXT NOT NULL,
                metadata JSONB DEFAULT '{}'
            );

            -- Create indexes for performance
            CREATE INDEX IF NOT EXISTS idx_satellite_instances_service_version 
                ON core.satellite_instances(service_name, version DESC, start_time ASC);

            CREATE INDEX IF NOT EXISTS idx_satellite_signals_target_unprocessed 
                ON core.satellite_signals(target_instance, created_at) 
                WHERE processed_at IS NULL;

            CREATE INDEX IF NOT EXISTS idx_service_leadership_heartbeat 
                ON core.service_leadership(last_heartbeat);
            EOF
          '';
        };
      };

    services.sinex.satellite.generatedUnits = lib.attrNames satelliteConfigs;

    # Assertions
    assertions = [
      {
        assertion = cfg.satellite.enable -> cfg.satellite.database.url != "";
        message = "Database URL must be configured for satellite services";
      }
      {
        assertion = cfg.satellite.enable -> cfg.satellite.nats.servers != "";
        message = "NATS servers must be configured for satellite services";
      }
    ];
  };
}
