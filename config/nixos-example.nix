# Example NixOS Configuration for Sinex
# This shows the direct, clear configuration approach without presets

{ config, lib, pkgs, ... }:

{
  # Import the Sinex module
  imports = [
    ../nixos/modules/sinex-config.nix
  ];

  services.sinex = {
    enable = true;
    
    # REQUIRED: User to monitor
    targetUser = "sinity";
    
    # REQUIRED: Git-annex repository (no defaults to prevent mistakes)
    annexRepo = "/home/sinity/sinex-data";
    
    # Database settings (sensible defaults)
    database = {
      name = "sinex";
      autoSetup = true;
      connectionPoolSize = 25;
    };

    # Event sources (full-featured defaults, easy to disable)
    eventSources = {
      filesystem = true;          # Monitor file changes
      terminal = true;            # Capture shell commands
      windowManager = true;       # Window focus and workspace changes  
      clipboard = true;           # Clipboard content changes
      systemEvents = true;        # D-Bus, journal, system events
      
      # Advanced features (disabled by default, enable when needed)
      processMonitoring = false;  # All process launches (needs root)
      networkMonitoring = false;  # Network connections (needs root)
      screenCapture = false;      # Screenshots with OCR (privacy sensitive)
    };

    # Observability: simple on/off (on = full monitoring stack)
    observability = {
      enable = true;              # Prometheus + Grafana + dashboards + alerts
      grafanaPort = 3000;         # Default web interface port
      prometheusPort = 9090;      # Default metrics port
    };

    # Storage settings
    storage = {
      dataRetention = null;       # Infinite retention (data volumes are minimal)
      compressionLevel = "balanced";
      blobThreshold = "10MB";     # Store large content in git-annex
    };

    # Service configuration
    services = {
      collector.memoryLimit = "512M";
      worker.concurrency = 4;
      updateService.gracePeriod = 30;
    };

    # Pre-flight verification (safety feature)
    preflightVerification = {
      enable = true;
      timeout = 120;
      failureAction = "abort";    # Fail safely on verification errors
    };
  };

  # Optional: Enable PostgreSQL with required extensions
  services.postgresql = {
    enable = true;
    package = pkgs.postgresql_16;
    extensions = with pkgs.postgresql16Packages; [
      timescaledb
      # TODO: Add pg_jsonschema when available
    ];
  };
}