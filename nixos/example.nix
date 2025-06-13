# Example NixOS configuration using the Sinex module
{ config, pkgs, ... }:

{
  imports = [
    # Import the Sinex module from the flake
    # In a real system, this would be:
    # inputs.sinex.nixosModules.default
  ];

  # Basic Sinex configuration
  services.sinex = {
    enable = true;

    # Database configuration (defaults to local PostgreSQL)
    database = {
      name = "sinex_prod";
      user = "sinex_user";
      # passwordFile = /run/secrets/sinex-db-password;
    };

    # Unified collector configuration
    unifiedCollector = {
      enable = true;
      logLevel = "debug"; # For initial setup

      # Configure event sources
      sources = {
        # Shell history from Atuin
        atuin = {
          enable = true;
          pollInterval = 5; # Check every 5 seconds
          # databasePath = "~/.local/share/atuin/history.db"; # default
        };

        # Traditional shell history files
        shellHistory = {
          enable = true;
          # zshPath = "~/.zsh_history"; # default
          # bashPath = "~/.bash_history"; # default
        };

        # Terminal session recordings
        asciinema = {
          enable = true;
          autoRecord = true; # Automatically record all terminal sessions!
          recordingsPath = "/home/sinity/.local/share/asciinema";
        };

        # Kitty terminal scrollback
        kittyScrollback = {
          enable = true;
          captureInterval = 600; # Every 10 minutes
          maxScrollbackLines = 50000; # Capture more history
        };

        # Filesystem monitoring
        filesystem = {
          enable = true;
          watchPaths = [
            "~/Documents"
            "~/Projects"
            "~/Obsidian"
            "/realm/knowledgebase"
          ];
          excludePatterns = [
            "*.tmp"
            "*.swp"
            ".git/*"
            "node_modules/*"
            "__pycache__/*"
          ];
        };
        
        # D-Bus event monitoring
        dbus = {
          enable = true;
          monitorSession = true;    # User session bus
          monitorSystem = true;     # System bus
          logAllSignals = false;    # Set to true for debugging
          extractNotifications = true;  # Desktop notifications
          extractMedia = true;          # Media playback (MPRIS)
          extractPower = true;          # Sleep/wake events
        };
      };

      # Dead letter queue settings
      dlq = {
        maxRetries = 5;
        retryDelaySecs = 120;
        enableFileDlq = true;
      };

      # Prometheus metrics
      metricsPort = 2112;
    };

    # Event promotion worker
    promoWorker = {
      enable = true;
      batchSize = 200; # Process more events per batch
      pollInterval = 3; # Check queue more frequently
    };

    # Git-annex blob storage
    blobStorage = {
      enable = true;
      repositoryPath = "/realm/sinex-annex/blobs";
      autoInit = true;
      numCopies = 3; # Keep 3 copies for important data
    };

    # Observability configuration
    observability = {
      enablePrometheus = true; # Scrape metrics
      enableGrafana = true; # Deploy dashboards
      logToDatabase = true; # Also store logs as events
      metricsToDatabase = false; # Keep metrics in Prometheus only
    };
  };

  # Additional system configuration for Sinex
  services.postgresql = {
    enable = true;
    package = pkgs.postgresql_16;
    
    # TimescaleDB extension for time-series data
    extraPlugins = with pkgs.postgresql16Packages; [
      timescaledb
      pgvector # For embeddings
      # pg_jsonschema # When available in nixpkgs
    ];
    
    settings = {
      shared_preload_libraries = "timescaledb,pgvector";
    };
  };

  # Prometheus configuration to scrape Sinex
  services.prometheus = {
    enable = true;
    # Sinex module automatically adds scrape configs
  };

  # Grafana with Sinex dashboards
  services.grafana = {
    enable = true;
    # Sinex module automatically provisions dashboards
  };

  # User environment integration
  users.users.sinity = {
    extraGroups = [ "sinex" ]; # If using non-dynamic user
    
    # Shell configuration for better integration
    packages = with pkgs; [
      atuin # For shell history
      asciinema # For terminal recording
      jq # For querying events
      postgresql # For psql access
    ];
  };

  # Environment variables
  environment.variables = {
    SINEX_DATABASE_URL = config.services.sinex.database.url;
    SINEX_ANNEX_PATH = config.services.sinex.blobStorage.repositoryPath;
  };

  # Shell aliases for convenience
  programs.bash.shellAliases = {
    exo = "${pkgs.sinex}/bin/exo";
    exo-query = "${pkgs.sinex}/bin/exo query";
    exo-stats = "${pkgs.sinex}/bin/exo stats";
    exo-blob = "${pkgs.sinex}/bin/exo blob";
  };

  programs.zsh.shellAliases = config.programs.bash.shellAliases;
}