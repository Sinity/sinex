# Complete NixOS Configuration Example for Sinex
# This shows ALL available configuration options with their correct default values
# Most options can be omitted for sensible defaults

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
        package = pkgs.sqlx-cli;
        directory = "../migrations";
        timeout = 300;           # Missing from original example
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

    # Unified Collector configuration (LEGACY - consider migrating to satellite architecture)
    unifiedCollector = {
      enable = false;  # Disabled in favor of satellite architecture
      logLevel = "info";  # Options: "trace", "debug", "info", "warn", "error"
      dryRun = false;     # If true, events are logged but not stored

      # Health check configuration (EXPANDED - was incomplete)
      healthCheck = {
        enable = true;
        port = 8080;
        interval = 30;
        timeout = 5;
        retryAttempts = 3;
        enableProbes = true;

        # Advanced probe configurations (missing from original)
        path = "/health";
        readinessPath = "/ready";
        livenessPath = "/alive";

        startupProbe = {
          enable = true;
          initialDelay = 30;
          periodSeconds = 5;
          timeoutSeconds = 3;
          failureThreshold = 12;
        };

        readinessProbe = {
          enable = true;
          initialDelay = 5;
          periodSeconds = 10;
          timeoutSeconds = 3;
          failureThreshold = 3;
        };

        livenessProbe = {
          enable = true;
          initialDelay = 60;
          periodSeconds = 30;
          timeoutSeconds = 5;
          failureThreshold = 3;
        };
      };

      # Restart policy
      restart = {
        policy = "on-failure";
        baseDelay = 5;
        maxDelay = 300;
        maxRetries = 5;
      };

      # Dead Letter Queue (DLQ) configuration
      dlq = {
        enable = true;
        failureStoragePath = "/var/lib/sinex/failures";
        maxRetries = 3;
        retryDelaySecs = 60;

        cleanup = {
          enable = true;
          maxAge = "7d";
          maxFiles = 10000;
        };
      };

      # Event Sources - Individual configuration
      sources = {
        # Shell history sources
        atuin = {
          enable = true;
          databasePath = "~/.local/share/atuin/history.db";
          pollInterval = 3;  # Module default (not 5)
        };

        shellHistory = {
          enable = true;
          zshPath = "~/.zsh_history";
          bashPath = "~/.bash_history";
        };

        # Terminal sources
        asciinema = {
          enable = false;  # Disabled by default
          path = "~/.local/share/asciinema";
          autoRecord = false;
          autoAnnex = true;
        };

        kitty = {
          enable = true;
          pollInterval = 2;
          socketPath = "/tmp/kitty";
          autoConfigureShellIntegration = true;
          enableCommandCompletion = true;
          scrollbackSafetyNetInterval = 60;
          maxScrollbackLines = 10000;
          
          shellIntegrationConfig = {
            "shell_integration" = "enabled";
            "allow_remote_control" = "socket-only";
            "listen_on" = "unix:/tmp/kitty-\${USER}";
          };
          
          autoModifyUserConfig = true;
          userConfigPath = "~/.config/kitty/kitty.conf";
        };

        # Legacy alias for backward compatibility
        kittyScrollback = {
          enable = false;  # Use 'kitty' source instead
          pollInterval = 60;
          socketPath = "/tmp/kitty";
          maxScrollbackLines = 10000;
          captureInterval = 60;
        };

        # Filesystem monitoring (with correct exclude patterns structure)
        filesystem = {
          enable = true;
          watchPaths = [ "~/Documents" "~/Projects" "~/Downloads" ];
          excludePatterns = [];  # Additional patterns (sensible defaults always applied)
          overrideDefaultExcludes = false;  # Advanced: ignore default excludes
          
          # Filesystem-specific options (missing from original)
          debounceMs = 100;
          maxDepth = null;
        };

        # D-Bus event monitoring (FIXED - extractAll default is true)
        dbus = {
          enable = true;
          monitorSession = true;
          monitorSystem = true;
          logAllSignals = false;
          extractAll = true;  # CORRECTED: Module default is true, not false
          
          # Individual extraction options (when extractAll = false)
          extractNotifications = true;
          extractMedia = true;
          extractPower = true;
          extractHardware = true;
          extractSession = true;
          extractPolicykit = true;
          extractBluetooth = true;
          extractNetwork = true;
          extractScreensaver = true;
          extractMounts = true;
        };

        # Clipboard monitoring
        clipboard = {
          enable = true;
          pollInterval = 500;  # Module default is 500ms, not 1000
          monitorClipboard = true;
          monitorPrimary = true;
          monitorSecondary = false;
          maxPreviewLength = 100;
          enableHistory = true;
          maxHistoryEntries = 1000;
          hashFileContent = true;
          maxContentSize = 10485760;  # 10MB
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
      unifiedCollector = {
        memoryMax = "1G";
        cpuQuota = "200%";
        tasksMax = 1000;
        ioWeight = 100;
      };

      promoWorker = {
        memoryMax = "512M";
        cpuQuota = "100%";
        tasksMax = 500;
        ioWeight = 100;
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