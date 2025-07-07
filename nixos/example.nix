# Complete NixOS Configuration Example for Sinex
# This shows all available configuration options with their default values
# Most options can be omitted for sensible defaults

{ config, lib, pkgs, ... }:

{
  # Import the Sinex modules
  imports = [
    ./modules  # Import the entire module set
  ];

  services.sinex = {
    # Basic required configuration
    enable = true;
    targetUser = "myuser";  # REQUIRED: Replace with your username

    # Package configuration (defaults to auto-detected packages)
    package = pkgs.sinex or (import ../. { }).packages.${pkgs.system}.default;
    cliPackage = pkgs.python3;  # Temporary default

    # Preset selection (affects all subsystem defaults)
    preset = "normal";  # Options: "lite", "normal", "max"

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

      # Connection pool settings
      connectionPool = {
        maxConnections = 30;     # Adjusted per preset
        minConnections = 5;
        connectionTimeout = 10;
        idleTimeout = 600;
      };

      # Health monitoring
      healthCheck = {
        enable = true;
        interval = 60;
        timeout = 10;
        retryAttempts = 3;
      };

      # Migration settings
      migration = {
        enable = true;
        package = pkgs.sqlx-cli;
        directory = "../migrations";  # Relative to project root
        autoRun = true;
      };
    };

    # Unified Collector configuration
    unifiedCollector = {
      enable = true;
      logLevel = "info";  # Options: "trace", "debug", "info", "warn", "error"
      dryRun = false;     # If true, events are logged but not stored

      # Health check configuration
      healthCheck = {
        enable = true;
        port = 8080;
        interval = 30;
        timeout = 5;
        retryAttempts = 3;
        enableProbes = true;
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
          maxAge = "7d";      # Adjusted per preset
          maxFiles = 10000;   # Adjusted per preset
        };
      };

      # Event Sources - Individual configuration
      sources = {
        # Shell history sources
        atuin = {
          enable = true;
          databasePath = "~/.local/share/atuin/history.db";
          pollInterval = 5;  # Adjusted per preset
        };

        shellHistory = {
          enable = true;
          zshPath = "~/.zsh_history";
          bashPath = "~/.bash_history";
        };

        # Terminal sources
        asciinema = {
          enable = false;  # Disabled by default except in "max" preset
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

        # Filesystem monitoring
        filesystem = {
          enable = true;
          watchPaths = [ "~/Documents" "~/Projects" "~/Downloads" ];  # Adjusted per preset
          excludePatterns = [];  # Additional patterns (sensible defaults always applied)
          overrideDefaultExcludes = false;  # Advanced: ignore default excludes
        };

        # D-Bus event monitoring
        dbus = {
          enable = true;
          monitorSession = true;
          monitorSystem = true;
          logAllSignals = false;
          extractAll = false;  # Individual options below take precedence
          extractNotifications = true;
          extractMedia = true;             # Adjusted per preset
          extractPower = true;
          extractHardware = true;          # Adjusted per preset
          extractSession = true;           # Adjusted per preset
          extractPolicykit = true;         # Adjusted per preset
          extractBluetooth = true;         # Adjusted per preset
          extractNetwork = true;           # Adjusted per preset
          extractScreensaver = true;       # Adjusted per preset
          extractMounts = true;            # Adjusted per preset
        };

        # Clipboard monitoring
        clipboard = {
          enable = true;
          pollInterval = 1000;  # milliseconds, adjusted per preset
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
      pollInterval = 5;      # Adjusted per preset
      batchSize = 100;       # Adjusted per preset

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

    # Blob Storage (git-annex) configuration
    blobStorage = {
      enable = true;
      repositoryPath = "/var/lib/sinex/annex";
      autoInit = true;
      
      healthCheck = {
        enable = true;
        interval = 1800;        # 30 minutes, adjusted per preset
        wantedSize = "100G";    # Adjusted per preset
        checkFsck = true;
        alertOnIssues = true;
      };

      maintenance = {
        gcSchedule = "weekly";     # Adjusted per preset
        fsckSchedule = "monthly";  # Adjusted per preset
        enableAutoCommit = true;
        enableAutoSync = false;
      };

      sync = {
        enable = false;
        remotes = [];
        schedule = "daily";
        batchMode = true;
      };
    };

    # Resource limits (automatically set based on preset)
    resources = {
      unifiedCollector = {
        memoryMax = "1G";      # Adjusted per preset
        cpuQuota = "200%";     # Adjusted per preset
        tasksMax = 1000;       # Adjusted per preset
        ioWeight = 100;
      };

      promoWorker = {
        memoryMax = "512M";    # Adjusted per preset
        cpuQuota = "100%";     # Adjusted per preset
        tasksMax = 500;        # Adjusted per preset
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
      gracePeriod = 30;               # Seconds to wait for graceful shutdown
      healthCheckTimeout = 60;        # Seconds to wait for health checks
      rollbackOnFailure = true;       # Auto-rollback on health check failure
      preserveData = true;            # Preserve DLQ data during updates
    };

    # Monitoring configuration
    monitoring = {
      logging = {
        level = "info";
        enableStructured = true;
        enableColors = true;
        enableTimestamps = true;
      };

      prometheus = {
        enable = false;  # Enabled in "normal" and "max" presets
        port = 9090;
        retentionTime = "15d";
        scrapeInterval = "15s";
      };

      alerting = {
        enable = false;  # Enabled in "normal" and "max" presets
        webhookUrl = null;
        slackChannel = null;
        emailRecipients = [];
        
        rules = {
          highMemoryUsage = true;
          diskSpaceLow = true;
          serviceDown = true;
          databaseConnectivity = true;
          eventProcessingLag = true;
        };
      };

      observabilityStack = {
        enable = false;  # Enabled in "normal" and "max" presets
        prometheus = true;
        grafana = true;
        loki = false;
        jaeger = false;
      };

      dashboards = {
        grafana = {
          enable = false;  # Enabled in "normal" and "max" presets
          port = 3000;
          adminPassword = "admin";
          datasources = {
            prometheus = true;
            postgres = true;
          };
        };
      };
    };

    # Preflight verification configuration
    preflightVerification = {
      enable = true;
      timeout = 120;
      phases = {
        databaseConnectivity = true;
        extensions = true;
        migrations = true;
        resources = true;
        configuration = true;
        services = true;
        integration = true;
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
      # Note: pgx_ulid and pg_jsonschema need to be built/installed separately
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