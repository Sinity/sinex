# Complete NixOS Configuration Example for Sinex
#
# This file demonstrates ALL available configuration options with their default values.
# Most options can be omitted for sensible defaults.
#
# For implementation details, see:
# - modules/sinex-config.nix      - Core configuration and PostgreSQL setup
# - modules/database.nix          - Database connection pooling and health checks
# - modules/satellite-services.nix - Individual satellite service configurations
# - modules/monitoring.nix        - Monitoring and alerting setup
# - modules/preflight-verification.nix - Pre-deployment checks
#
# Key architectural decisions are documented at implementation points:
# - PostgreSQL extension setup: modules/sinex-config.nix (lines 285-305)
# - TimescaleDB configuration: migrations/00000000000002_create_core_tables.sql
# - ULID implementation: crate/sinex-ulid/src/lib.rs

{ config, lib, pkgs, ... }:

{
  # Import the Sinex modules
  imports = [
    ./modules
  ];

  services.sinex = {
    # Basic required configuration
    enable = true;
    targetUser = "myuser";  # REQUIRED: Replace with your username

    # Package configuration (defaults to auto-detected packages)
    package = pkgs.sinex or (import ../. { }).packages.${pkgs.system}.default;
    cliPackage = pkgs.python3;  # Temporary default

    # Directory configuration (optional overrides)
    directories = {
      state = "/var/lib/sinex";  # Persistent state data
      logs = "/var/log/sinex";   # Log files
    };

    # Database configuration
    database = {
      host = "localhost";
      port = 5432;
      name = "sinex";
      user = "sinex";  # Defaults to database name
      passwordFile = null;  # Path to password file (optional)
      autoSetup = true;     # Auto-create database and user

      # Connection pool settings (correct defaults from module)
      connectionPool = {
        maxConnections = 20;     # Module default
        minConnections = 5;
        connectionTimeout = 30;  # Module default (not 10)
        idleTimeout = 600;
      };

      # Health monitoring (correct defaults from module)
      healthCheck = {
        enable = true;
        interval = 30;           # Module default (not 60)
        timeout = 5;             # Module default (not 10)
        retryAttempts = 3;
      };

      # Migration settings
      migration = {
        enable = true;
        # package defaults to cfg.package which includes migration binary
        # binary = "sinex-db-migration"; # defaults to this
        timeout = 300;
      };
    };

    # Satellite Architecture (NEW - recommended)
    satellite = {
      enable = true;
      logLevel = "info";  # Options: "trace", "debug", "info", "warn", "error"
      
      # Database configuration for satellites
      database = {
        url = "postgresql:///sinex_dev?host=/run/postgresql";
      };
      
      # Redis configuration for event bus
      redis = {
        url = "redis://localhost:6379";
      };
      
      # Core hub services
      coreServices = {
        enable = true;
      };
      
      # Ingest daemon configuration
      ingestd = {
        batchSize = 1000;
        batchTimeout = 5;
      };
      
      # Event source satellites
      eventSources = {
        # Filesystem watcher
        filesystem = {
          enable = true;
          batchSize = 100;
          batchTimeout = 5;
          memoryLimit = "256M";
          environment = [];
          extraArgs = "";
        };
        
        # Terminal satellite
        terminal = {
          enable = true;
          batchSize = 100;
          batchTimeout = 5;
          memoryLimit = "256M";
          environment = [];
          extraArgs = "";
        };
        
        # Desktop satellite (clipboard, window manager)
        desktop = {
          enable = true;
          batchSize = 50;
          batchTimeout = 5;
          memoryLimit = "256M";
          environment = [];
          extraArgs = "";
        };
        
        # System satellite (dbus, journald)
        system = {
          enable = true;
          batchSize = 200;
          batchTimeout = 10;
          memoryLimit = "384M";
          environment = [];
          extraArgs = "";
        };
      };
      
      # Automaton satellites
      automata = {
        # Terminal command canonicalizer
        canonicalCommandSynthesizer = {
          enable = true;
          consumerGroup = "canonical-synthesizers";
          batchSize = 50;
          checkpointInterval = 30;
          memoryLimit = "512M";
          cpuQuota = "50%";
          environment = [];
        };
        
        # Health aggregator
        healthAggregator = {
          enable = true;
          consumerGroup = "health-aggregators";
          batchSize = 50;
          checkpointInterval = 30;
          memoryLimit = "512M";
          cpuQuota = "50%";
          environment = [];
        };
      };
    };

    # Promotion Worker configuration
    promoWorker = {
      enable = true;
      pollInterval = 5;
      batchSize = 100;

      # Health check configuration
      healthCheck = {
        enable = true;
        port = 8081;
        interval = 30;
        timeout = 5;
        retryAttempts = 3;
        enableProbes = true;
      };
    };

    # Blob Storage (git-annex) configuration (CORRECTED defaults)
    blobStorage = {
      enable = true;
      repositoryPath = "/realm/annex";  # CORRECTED: Module default, not "/var/lib/sinex/annex"
      autoInit = true;
      
      # Missing options from original example
      numCopies = 2;
      backend = "SHA256E";
      
      # Health check (partially corrected)
      healthCheck = {
        enable = true;
        interval = 1800;        # 30 minutes
        wantedSize = "100G";
        diskUsageWarning = 0.8;  # Missing from original
      };

      # Maintenance (corrected structure)
      maintenance = {
        enableAutoGc = true;     # CORRECTED: Module field name
        gcSchedule = "weekly";
        enablePeriodicFsck = true;  # Missing from original
        fsckSchedule = "monthly";
      };

      # REMOVED: sync section (doesn't exist in modules)
    };

    # Resource limits
    resources = {
      ingestd = {
        memoryMax = "1G";
        cpuQuota = "200%";
      };

      gateway = {
        memoryMax = "512M";
        cpuQuota = "100%";
      };

      defaultSatellite = {
        memoryMax = "256M";
        cpuQuota = "50%";
      };
    };

    # Security configuration
    security = {
      level = "balanced";  # Options: "minimal", "balanced", "strict"
      allowFileSystemAccess = true;
      allowSocketAccess = true;
      allowDeviceAccess = true;
    };

    # Coordinated update configuration
    update = {
      enable = true;
      gracePeriod = 30;
      healthCheckTimeout = 60;
      rollbackOnFailure = true;
      preserveData = true;
    };

    # Monitoring configuration (COMPLETELY REWRITTEN to match module)
    monitoring = {
      # Logging configuration (corrected to match module)
      logging = {
        retention = {
          maxFiles = 10;
          maxSize = "100M";
          maxAge = "30d";
        };
        
        performance = {
          slowQueryThreshold = 1000;
          traceRequests = false;
        };
      };

      # Prometheus configuration (corrected structure)
      prometheus = {
        centralCollector = {
          enable = false;
          port = 2114;
          endpoints = [];
        };
      };

      # Alerting configuration (completely different from original)
      alerting = {
        healthAlerts = {
          serviceDown = {
            enable = true;
            threshold = "2m";
          };
          highErrorRate = {
            enable = true;
            threshold = 0.05;
          };
          databaseConnections = {
            enable = true;
            maxConnectionsPercent = 0.8;
          };
        };
        
        resourceAlerts = {
          highMemoryUsage = {
            enable = true;
            threshold = 0.9;
          };
          highCpuUsage = {
            enable = true;
            threshold = 0.8;
          };
          diskSpaceUsage = {
            enable = true;
            threshold = 0.85;
          };
        };
      };

      # REMOVED: observabilityStack, dashboards.grafana (don't exist in monitoring module)
    };

    # Preflight verification configuration (EXPANDED)
    preflightVerification = {
      enable = true;
      timeout = 120;
      
      # Corrected phase structure
      phases = {
        databaseConnectivity = true;
        extensions = true;
        migrations = true;
        resources = true;
        configuration = true;
        services = true;
        integration = true;
      };
      
      # Missing options from original
      skipPhases = [];
      failureAction = "abort";
      recordResults = true;
      
      notifications = {
        enable = false;
        onFailure = true;
        onSuccess = false;
      };
    };
  };

  # Optional: PostgreSQL setup (if not already configured)
  services.postgresql = {
    enable = true;
    package = pkgs.postgresql_16;
    
    # Required extensions for Sinex
    extraPlugins = with pkgs.postgresql16Packages; [
      timescaledb
      pgx_ulid        # For ULID type support (available in nixpkgs)
      pgvector        # For vector embeddings support
      # Note: pg_jsonschema is provided by the sinex overlay
    ];
    
    settings = {
      shared_preload_libraries = "timescaledb";
      max_connections = 200;
      shared_buffers = "256MB";
      effective_cache_size = "1GB";
      maintenance_work_mem = "64MB";
      checkpoint_completion_target = 0.9;
      wal_buffers = "16MB";
      default_statistics_target = 100;
      random_page_cost = 1.1;
      effective_io_concurrency = 200;
    };
  };

  # Optional: System packages that may be useful
  environment.systemPackages = with pkgs; [
    asciinema       # For terminal recording
    git-annex       # For blob storage
    postgresql      # For database administration
  ];
}
