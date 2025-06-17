# Sinex with preset + common customizations
# Shows how to use presets while overriding specific settings

{ config, lib, pkgs, ... }:

{
  imports = [
    ../modules  # Import the modularized Sinex module
  ];

  services.sinex = {
    enable = true;
    
    # Start with a preset for sensible defaults
    preset = "normal";  # or "lite", "max"
    
    # Then customize what you need to change
    targetUser = "myuser";  # Your actual username
    
    # Custom database configuration
    database = {
      name = "my_exocortex";
      host = "localhost";
      # user auto-derived from database name = "my_exocortex"
    };
    
    # Custom git-annex location
    blobStorage = {
      enable = true;
      repositoryPath = "/storage/sinex-annex";  # Your preferred location
      autoInit = true;
      # Override size limits from preset
      healthCheck.wantedSize = "200G";  # Bigger than preset's 100G
      # Custom maintenance schedule
      maintenance = {
        gcSchedule = "daily";      # More frequent than preset's weekly
        fsckSchedule = "weekly";   # More frequent than preset's monthly
      };
    };
    
    # Customize filesystem monitoring paths
    unifiedCollector.sources.filesystem = {
      enable = true;
      watchPaths = [
        "~/Documents"
        "~/Projects" 
        "~/Research"
        "~/Downloads"
        "/media/important-data"  # External drive
      ];
      excludePatterns = [
        "*.tmp"
        "*.cache"
        ".git/*"
        "node_modules/*"
        ".venv/*"
        "__pycache__/*"
      ];
    };
    
    # Privacy: disable clipboard monitoring
    unifiedCollector.sources.clipboard.enable = false;
    
    # Faster shell history monitoring
    unifiedCollector.sources.atuin.pollInterval = 2;  # Override preset's 5s
    
    # Custom DLQ retention
    unifiedCollector.dlq.cleanup = {
      enable = true;
      maxAge = "30d";    # Longer than preset's 14d
      maxFiles = 50000;  # More than preset's 25000
    };
    
    # Database connection tuning
    database.connectionPool = {
      maxConnections = 40;  # Override preset's 30
      minConnections = 10;
      connectionTimeout = 45;
      idleTimeout = 900;
    };
    
    # Custom health check intervals
    unifiedCollector.healthCheck = {
      enable = true;
      interval = 30;  # More frequent than preset
      timeout = 10;
    };
    
    # Enable observability but customize ports
    monitoring = {
      enable = true;
      observabilityStack = {
        enable = true;
        prometheusPort = 9091;  # Custom port
        grafanaPort = 3001;     # Custom port
        retentionTime = "60d";  # Longer retention
        listenAddress = "0.0.0.0";  # Allow network access (be careful!)
      };
      dashboards.grafana.enable = true;
      
      # Custom alerting thresholds
      alerting = {
        enable = true;
        healthAlerts.serviceDown.threshold = "1m";  # Faster alerts
        resourceAlerts.highMemoryUsage.threshold = 0.85;  # 85% instead of 90%
      };
      
      # Custom logging retention
      logging = {
        level = "info";  # Override any preset setting
        retention = {
          maxFiles = 20;   # More log files
          maxSize = "200M"; # Bigger log files
          maxAge = "60d";   # Longer retention
        };
      };
    };
    
    # Custom directory locations
    directories = {
      state = "/var/lib/my-sinex";
      cache = "/var/cache/my-sinex"; 
      logs = "/var/log/my-sinex";
    };
    
    # Promotion worker tuning
    promoWorker = {
      enable = true;
      pollInterval = 2;     # Faster than preset's 3s
      batchSize = 500;      # Larger batches than preset's 300
      metricsPort = 2114;   # Custom port
    };
  };
  
  # PostgreSQL with your preferred settings
  services.postgresql = {
    enable = true;
    package = pkgs.postgresql_16;
    extraPlugins = with pkgs.postgresql16Packages; [
      timescaledb
      pgvector
    ];
    settings = {
      shared_preload_libraries = "timescaledb,pg_stat_statements";
      max_connections = 150;  # Match your connection pool settings
      shared_buffers = "512MB";  # More than auto-defaults
      effective_cache_size = "2GB";
      work_mem = "32MB";
    };
  };
  
  # Custom firewall if exposing monitoring to network
  networking.firewall = {
    allowedTCPPorts = [ 
      9091  # Prometheus
      3001  # Grafana
    ];
  };
}