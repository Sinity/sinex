# Sinex with full observability stack
# This example shows how to enable Prometheus + Grafana monitoring

{ config, lib, pkgs, ... }:

{
  imports = [
    ../modules  # Import the modularized Sinex module
  ];

  # Enable Sinex with observability
  services.sinex = {
    enable = true;
    preset = "normal";
    
    # Target user for file monitoring
    targetUser = "sinity";
    
    # Database configuration
    database = {
      name = "sinex";
      autoSetup = true;
    };
    
    # Git-annex blob storage
    blobStorage = {
      enable = true;
      repositoryPath = "/realm/annex";
      autoInit = true;
    };
    
    # Enable full observability stack
    monitoring = {
      enable = true;
      observabilityStack = {
        enable = true;
        prometheusPort = 9090;
        grafanaPort = 3000;
        retentionTime = "30d";
        listenAddress = "127.0.0.1";  # localhost only for security
      };
      dashboards.grafana.enable = true;
      alerting.enable = true;
    };
  };
  
  # PostgreSQL with required extensions
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
    };
  };
}