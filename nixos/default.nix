{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinnix-exocortex;
  
  # Get packages from the flake's overlay or build them
  hyprlandIngestor = pkgs.sinnixExocortex.hyprlandIngestor or (pkgs.callPackage ../ingestors/hyprland {});
  
  # Schema file location
  schemaFile = ../schema/mvp_schema.sql;

  # Database initialization script
  initDbScript = pkgs.writeShellScript "init-exocortex-db" ''
    set -e
    
    # Wait for PostgreSQL to be available
    until ${pkgs.postgresql}/bin/pg_isready -h localhost -U postgres; do
      echo "Waiting for PostgreSQL to be ready..."
      sleep 2
    done
    
    # Create database if it doesn't exist
    ${pkgs.postgresql}/bin/psql -h localhost -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = '${cfg.database.name}'" | grep -q 1 || \
      ${pkgs.postgresql}/bin/psql -h localhost -U postgres -c "CREATE DATABASE \"${cfg.database.name}\""
    
    # Run schema migrations
    ${pkgs.postgresql}/bin/psql -h localhost -U postgres -d "${cfg.database.name}" -f ${schemaFile} || true
  '';

in {
  options.services.sinnix-exocortex = {
    enable = mkEnableOption "Sinnix Exocortex universal data capture and query system";
    
    database = {
      url = mkOption {
        type = types.str;
        default = "postgresql://localhost/${cfg.database.name}";
        description = "PostgreSQL database URL";
      };
      
      name = mkOption {
        type = types.str;
        default = "exocortex";
        description = "Database name";
      };
      
      user = mkOption {
        type = types.str;
        default = "exocortex";
        description = "Database user";
      };
      
      ensureExists = mkOption {
        type = types.bool;
        default = true;
        description = "Ensure database exists and is initialized";
      };
    };
    
    ingestors = {
      hyprland = {
        enable = mkEnableOption "Hyprland window activity ingestor";
        
        interval = mkOption {
          type = types.int;
          default = 5;
          description = "Polling interval in seconds";
        };
      };
      
      # Future ingestors can be added here
      # browser.enable = mkEnableOption "Browser activity ingestor";
      # filesystem.enable = mkEnableOption "Filesystem activity ingestor";
    };
  };
  
  config = mkIf cfg.enable {
    # Ensure PostgreSQL user exists
    users.users.${cfg.database.user} = mkIf (cfg.database.user != "postgres") {
      isSystemUser = true;
      group = cfg.database.user;
    };
    
    users.groups.${cfg.database.user} = mkIf (cfg.database.user != "postgres") {};
    
    # Database initialization
    systemd.services.sinnix-exocortex-init = mkIf cfg.database.ensureExists {
      description = "Initialize Sinnix Exocortex database";
      wantedBy = [ "multi-user.target" ];
      after = [ "postgresql.service" ];
      requires = [ "postgresql.service" ];
      
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        ExecStart = initDbScript;
        User = "postgres";
      };
    };
    
    # Hyprland ingestor service
    systemd.user.services.sinnix-exocortex-hyprland = mkIf cfg.ingestors.hyprland.enable {
      description = "Sinnix Exocortex Hyprland activity ingestor";
      wantedBy = [ "graphical-session.target" ];
      after = [ "graphical-session.target" ];
      
      environment = {
        DATABASE_URL = cfg.database.url;
        RUST_LOG = "info";
      };
      
      serviceConfig = {
        ExecStart = "${hyprlandIngestor}/bin/hyprland-ingestor";
        Restart = "on-failure";
        RestartSec = 10;
      };
    };
    
    # Ensure services are restarted on configuration changes
    system.activationScripts.sinnix-exocortex = mkIf cfg.enable ''
      # Restart user services if configuration changed
      ${pkgs.systemd}/bin/systemctl --user daemon-reload || true
    '';
  };
}