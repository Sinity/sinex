# Shared VM configuration for Sinex tests
{ config, pkgs, lib, ... }:

{
  # Basic system configuration
  networking.hostName = "sinex-test";
  
  # PostgreSQL with TimescaleDB
  services.postgresql = {
    enable = true;
    package = pkgs.postgresql_16;
    ensureDatabases = [ "sinex_test" ];
    ensureUsers = [{
      name = "sinex";
      ensureDBOwnership = true;
    }];
    
    settings = {
      shared_preload_libraries = "timescaledb";
      max_connections = 100;
    };
    
    extraPlugins = with pkgs.postgresql16Packages; [
      timescaledb
    ];
  };
  
  # Environment setup
  environment.systemPackages = with pkgs; [
    # Sinex binaries (will be built from flake)
    # For now, we'll use placeholders
    postgresql_16
    jq
    ripgrep
  ];
  
  # Test user
  users.users.test = {
    isNormalUser = true;
    home = "/home/test";
    createHome = true;
  };
  
  # Create watched directories
  systemd.tmpfiles.rules = [
    "d /home/test/watched 0755 test users -"
    "d /tmp/sinex-test 0755 test users -"
  ];
  
  # Environment variables
  environment.variables = {
    DATABASE_URL = "postgresql:///sinex_test?host=/run/postgresql";
    SINEX_TEST_MODE = "true";
  };
  
  # Ensure PostgreSQL is ready before tests
  systemd.services.postgresql.postStart = lib.mkAfter ''
    ${pkgs.postgresql_16}/bin/psql -U postgres -d sinex_test -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
    ${pkgs.postgresql_16}/bin/psql -U postgres -d sinex_test -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"
  '';
}