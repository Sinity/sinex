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
    
    systemUser = mkOption {
      type = types.str;
      description = ''
        The system user that should run the exocortex services.
        This should be the user running the graphical session (e.g., the one running Hyprland).
      '';
      example = "sinity";
    };
    
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
    # Don't create users - assume they already exist
    # The database user should be created by PostgreSQL configuration
    
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
    systemd.services.sinnix-exocortex-hyprland = mkIf cfg.ingestors.hyprland.enable {
      description = "Sinnix Exocortex Hyprland activity ingestor";
      wantedBy = [ "multi-user.target" ];
      after = [ "sinnix-exocortex-init.service" "sinnix-exocortex-grant-permissions.service" ];
      
      environment = {
        DATABASE_URL = cfg.database.url;
        RUST_LOG = "info";
      };
      
      serviceConfig = {
        Type = "simple";
        User = cfg.systemUser;
        ExecStart = "${pkgs.writeShellScript "hyprland-ingestor-wrapper" ''
          #!/usr/bin/env bash
          # Wait for Hyprland to be available
          while ! pgrep -x Hyprland > /dev/null; do
            echo "Waiting for Hyprland to start..."
            sleep 5
          done
          
          # Get the user's runtime directory
          export XDG_RUNTIME_DIR="/run/user/$(id -u)"
          
          # Find the Hyprland instance socket
          export HYPRLAND_INSTANCE_SIGNATURE=$(ls -t "$XDG_RUNTIME_DIR"/hypr/ 2>/dev/null | grep -v '\.lock$' | head -n1)
          
          if [ -z "$HYPRLAND_INSTANCE_SIGNATURE" ]; then
            echo "Error: Could not find Hyprland instance"
            exit 1
          fi
          
          echo "Found Hyprland instance: $HYPRLAND_INSTANCE_SIGNATURE"
          
          # Start the actual ingestor
          exec ${hyprlandIngestor}/bin/hyprland-ingestor
        ''}";
        Restart = "always";
        RestartSec = 10;
        
        # Run in user's login session context
        PAMName = "login";
      };
    };
    
    # Grant database permissions to the system user
    systemd.services.sinnix-exocortex-grant-permissions = mkIf cfg.database.ensureExists {
      description = "Grant Sinnix Exocortex database permissions";
      wantedBy = [ "multi-user.target" ];
      after = [ "sinnix-exocortex-init.service" ];
      requires = [ "sinnix-exocortex-init.service" ];
      
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        ExecStart = pkgs.writeShellScript "grant-exocortex-permissions" ''
          set -e
          # Create role if it doesn't exist
          ${pkgs.postgresql}/bin/psql -h localhost -U postgres -tc "SELECT 1 FROM pg_roles WHERE rolname = '${cfg.systemUser}'" | grep -q 1 || \
            ${pkgs.postgresql}/bin/psql -h localhost -U postgres -c "CREATE ROLE \"${cfg.systemUser}\" WITH LOGIN"
          
          # Grant permissions
          ${pkgs.postgresql}/bin/psql -h localhost -U postgres -d "${cfg.database.name}" -c "
            GRANT CONNECT ON DATABASE \"${cfg.database.name}\" TO \"${cfg.systemUser}\";
            GRANT USAGE ON SCHEMA public TO \"${cfg.systemUser}\";
            GRANT CREATE ON SCHEMA public TO \"${cfg.systemUser}\";
            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"${cfg.systemUser}\";
            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"${cfg.systemUser}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO \"${cfg.systemUser}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO \"${cfg.systemUser}\";
          " || true
        '';
        User = "postgres";
      };
    };
  };
}