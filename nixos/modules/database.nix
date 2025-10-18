# Database configuration module
{ lib, config, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  ensureUserOption = types.submodule ({ name, ... }: {
    options = {
      name = mkOption {
        type = types.str;
        description = "Database role name";
      };

      ensureDBOwnership = mkOption {
        type = types.bool;
        default = false;
        description = "Grant ownership of the Sinex database to this role";
      };

      ensureClauses = mkOption {
        type = types.attrsOf types.bool;
        default = {};
        description = "Additional role clauses (e.g. { login = true; createdb = true; })";
      };
    };

    config = {
      ensureClauses.login = mkDefault true;
    };
  });
in
{
  options.services.sinex.database = {
    host = mkOption {
      type = types.str;
      default = "localhost";
      description = "PostgreSQL host";
    };

    port = mkOption {
      type = types.port;
      default = 5432;
      description = "PostgreSQL port";
    };

    listenAddress = mkOption {
      type = types.str;
      default = "127.0.0.1";
      description = "PostgreSQL listen address";
    };

    name = mkOption {
      type = types.str;
      default = "sinex";
      description = "Database name";
    };

    user = mkOption {
      type = types.str;
      default = cfg.database.name;  # Derive from database name
      defaultText = literalExpression "cfg.database.name";
      description = "Database user (defaults to database name)";
    };

    passwordFile = mkOption {
      type = types.nullOr types.path;
      default = null;
      description = "Path to file containing database password";
    };

    autoSetup = mkOption {
      type = types.bool;
      default = true;
      description = "Automatically setup database user and permissions";
    };

    package = mkOption {
      type = types.package;
      default = pkgs.postgresql_16;
      defaultText = literalExpression "pkgs.postgresql_16";
      description = "PostgreSQL package to deploy";
    };

    extraExtensions = mkOption {
      type = types.listOf types.package;
      default = [];
      description = "Additional PostgreSQL extension packages to load alongside the defaults";
    };

    extraSharedPreloadLibraries = mkOption {
      type = types.listOf types.str;
      default = [];
      description = "Extra entries appended to shared_preload_libraries";
    };

    monotonicUlids = mkOption {
      type = types.bool;
      default = true;
      description = "Enable pgx_ulid in shared_preload_libraries to support monotonic ULIDs";
    };

    additionalUsers = mkOption {
      type = types.listOf ensureUserOption;
      default = [];
      description = "Additional database roles to ensure (appended to services.postgresql.ensureUsers)";
    };

    additionalSettings = mkOption {
      type = types.attrsOf types.anything;
      default = {};
      description = "Extra PostgreSQL configuration settings merged onto the managed defaults";
    };

    authentication = mkOption {
      type = types.lines;
      default = ''
        local   all             all                                     peer
        host    all             all             0.0.0.0/0               reject
        host    all             all             ::/0                    reject
      '';
      description = "pg_hba.conf rules applied when autoSetup is enabled";
    };

    # Connection pool with sensible defaults
    connectionPool = {
      maxConnections = mkOption {
        type = types.int;
        default = 20;
        description = ''
          Maximum database connections per service.
          
          Calculation guidelines:
          - Base: 5-10 connections for low-traffic satellites
          - Scale: 20-30 for high-traffic services (ingestd, gateway)
          - Total PostgreSQL max_connections should be:
            (number_of_services * avg_maxConnections) + 50 overhead
          
          Example with 10 satellites:
          - 8 satellites @ 10 connections = 80
          - 2 high-traffic @ 30 connections = 60
          - Overhead for admin/monitoring = 50
          - Total max_connections = 190 (round to 200)
        '';
      };

      minConnections = mkOption {
        type = types.int;
        default = 5;
        description = ''
          Minimum database connections to maintain.
          
          Trade-offs:
          - Higher: Faster response for bursts, more idle resources
          - Lower: Better resource efficiency, slower burst response
          - Recommended: 25% of maxConnections
        '';
      };

      connectionTimeout = mkOption {
        type = types.int;
        default = 30;
        description = ''
          Connection timeout in seconds.
          
          When to adjust:
          - Increase (60s): High-latency networks, overloaded DB
          - Decrease (10s): Fast failure detection needed
          - Default (30s): Good for local/LAN PostgreSQL
        '';
      };

      idleTimeout = mkOption {
        type = types.int;
        default = 600;
        description = ''
          Idle connection timeout in seconds.
          
          Balancing act:
          - Shorter (300s): Aggressive resource reclaim, more reconnects
          - Longer (1800s): Fewer reconnects, more idle resources
          - Default (600s/10min): Good balance for most workloads
          
          Set to 0 to disable idle timeouts (not recommended).
        '';
      };
    };

    # Health check configuration
    healthCheck = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable database health checks";
      };

      interval = mkOption {
        type = types.int;
        default = 30;
        description = "Health check interval in seconds";
      };

      timeout = mkOption {
        type = types.int;
        default = 5;
        description = "Health check timeout in seconds";
      };
    };

    # Migration configuration
    migration = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable automatic database migrations";
      };

      package = mkOption {
        type = types.package;
        default = cfg.package; # Use the main sinex package which includes migration binary
        defaultText = literalExpression "cfg.package";
        description = "Package containing the migration binary (sinex-db-migration)";
      };

      binary = mkOption {
        type = types.str;
        default = "sinex-db-migration";
        description = "Name of the migration binary";
      };

      timeout = mkOption {
        type = types.int;
        default = 300;
        description = "Migration timeout in seconds";
      };
    };
  };

  config =
    let
      perServiceConnections = lib.max 1 cfg.database.connectionPool.maxConnections;

      satelliteEnabled = cfg.satellite.enable or false;

      coreServiceCount =
        if satelliteEnabled && cfg.satellite.coreServices.enable then 2 else 0;

      eventSourceCount =
        if satelliteEnabled then
          (if cfg.satellite.eventSources.filesystem.enable then cfg.satellite.eventSources.filesystem.instances else 0)
          + (if cfg.satellite.eventSources.terminal.enable then cfg.satellite.eventSources.terminal.instances else 0)
          + (if cfg.satellite.eventSources.desktop.enable then cfg.satellite.eventSources.desktop.instances else 0)
          + (if cfg.satellite.eventSources.system.enable then cfg.satellite.eventSources.system.instances else 0)
        else 0;

      automataCount =
        if satelliteEnabled then
          (if cfg.satellite.automata.canonicalCommandSynthesizer.enable then 1 else 0)
          + (if cfg.satellite.automata.healthAggregator.enable then 1 else 0)
        else 0;

      totalServiceCount = coreServiceCount + eventSourceCount + automataCount;

      computedMaxConnections =
        let baseline = totalServiceCount * perServiceConnections + 50;
        in lib.max (perServiceConnections + 10) baseline;
      postgresqlPackages =
        if cfg.database.package ? pkgs then cfg.database.package.pkgs
        else pkgs.postgresql16Packages;

      defaultExtensionPackages =
        [ postgresqlPackages.timescaledb ]
        ++ lib.optional (postgresqlPackages ? pg_jsonschema) postgresqlPackages.pg_jsonschema
        ++ [
          postgresqlPackages.pgx_ulid
          postgresqlPackages.pgvector
        ];

      extensionPackages =
        lib.unique (defaultExtensionPackages ++ cfg.database.extraExtensions);

      sharedPreloadBase =
        [ "timescaledb" ]
        ++ lib.optionals cfg.database.monotonicUlids [ "pgx_ulid" ];

      sharedPreloadLibraries =
        lib.concatStringsSep "," (lib.unique (sharedPreloadBase ++ cfg.database.extraSharedPreloadLibraries));

      ensuredUsers =
        [{ name = cfg.database.user; ensureDBOwnership = true; }]
        ++ cfg.database.additionalUsers;

      baseSettings = {
        # Connection and timeout settings
        statement_timeout = mkDefault "60s";
        lock_timeout = mkDefault "30s";
        idle_in_transaction_session_timeout = mkDefault "300s";

        # Performance settings
        shared_buffers = mkDefault "256MB";
        effective_cache_size = mkDefault "1GB";
        maintenance_work_mem = mkDefault "256MB";
        checkpoint_completion_target = mkDefault "0.9";

        # Prepared statements
        max_prepared_transactions = mkDefault 256;

        # Logging for monitoring
        log_statement = mkDefault "mod";
        log_duration = mkDefault true;
        log_min_duration_statement = mkDefault "1000ms";

        # Connection limits
        max_connections = mkDefault computedMaxConnections;

        # Extension requirements
        shared_preload_libraries = mkDefault sharedPreloadLibraries;
        listen_addresses = mkDefault cfg.database.listenAddress;
      };

      finalSettings = mkMerge [
        baseSettings
        cfg.database.additionalSettings
      ];
    in
    mkIf cfg.database.autoSetup {
      services.postgresql = {
        enable = true;
        package = lib.mkForce cfg.database.package;
        extensions = extensionPackages;
        ensureDatabases = mkDefault [ cfg.database.name ];
        ensureUsers = mkDefault ensuredUsers;
        authentication = mkDefault cfg.database.authentication;
        settings = finalSettings;
      };
    };
}
