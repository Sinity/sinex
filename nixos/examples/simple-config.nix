# Simple Sinex configuration using modularized structure
# This replaces the huge full.nix with a much more manageable configuration

{ config, lib, pkgs, ... }:

{
  imports = [
    ../modules  # Import the modularized Sinex module
  ];

  # Enable Sinex with max preset - this handles most configuration automatically
  services.sinex = {
    enable = true;
    
    # Use max preset for maximum data capture
    preset = "max";
    
    # Target user for file monitoring
    targetUser = "sinity";
    
    # Database configuration (auto-derives user from database name)
    database = {
      name = "sinex";
      autoSetup = true;
      # user = "sinex" is automatically derived from database name
    };
    
    # Git-annex blob storage at /realm/annex
    blobStorage = {
      enable = true;
      repositoryPath = "/realm/annex";
      autoInit = true;
    };
    
    # Event sources - most are enabled by default with aggressive preset
    unifiedCollector.sources = {
      # Override specific settings as needed
      filesystem.watchPaths = [ 
        "~/Documents" 
        "~/Projects" 
        "~/Downloads"
      ];
      
      atuin = {
        enable = true;
        pollInterval = 1;  # Very aggressive for shell history
      };
      
      clipboard = {
        enable = true;
        pollInterval = 100;  # Very aggressive clipboard monitoring
        maxHistoryEntries = 5000;
      };
    };
    
    # Enhanced monitoring and logging
    monitoring = {
      enable = true;
      logging = {
        level = "debug";  # Comprehensive logging
        structured = true;
      };
      prometheus.enable = true;
    };
  };
  
  # Enable PostgreSQL with TimescaleDB (handled automatically by module)
  services.postgresql = {
    enable = true;
    package = pkgs.postgresql_16;
    extraPlugins = with pkgs.postgresql16Packages; [
      timescaledb
      pgvector
    ];
    settings = {
      shared_preload_libraries = "timescaledb,pg_stat_statements";
      max_connections = 100;
      work_mem = "256MB";
      maintenance_work_mem = "1GB";
    };
  };
}