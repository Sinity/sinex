{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  
  # Get packages from the flake's overlay
  # The overlay is applied by the flake, so these packages should be available
  hyprlandIngestor = pkgs.sinex.hyprlandIngestor;
  filesystemIngestor = pkgs.sinex.filesystemIngestor;
  kittyIngestor = pkgs.sinex.kittyIngestor;
  promoWorker = pkgs.sinex.promoWorker;
  
  # Sinex monitoring script
  sinexMonitor = pkgs.writeShellScriptBin "sinex-monitor" (builtins.readFile ../scripts/sinex-monitor.sh);
  
  # Database initialization script using sqlx migrations
  initDbScript = pkgs.writeShellScript "init-sinex-db" ''
    set -e
    
    # Wait for PostgreSQL to be available
    until ${pkgs.postgresql}/bin/pg_isready -h localhost -U postgres; do
      echo "Waiting for PostgreSQL to be ready..."
      sleep 2
    done
    
    # Create database if it doesn't exist (should be handled by ensureDatabases)
    ${pkgs.postgresql}/bin/psql -h localhost -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = '${cfg.database.name}'" | grep -q 1 || \
      ${pkgs.postgresql}/bin/psql -h localhost -U postgres -c "CREATE DATABASE \"${cfg.database.name}\""
    
    # Create required extensions
    ${pkgs.postgresql}/bin/psql -h localhost -U postgres -d "${cfg.database.name}" -c "CREATE EXTENSION IF NOT EXISTS ulid;"
    ${pkgs.postgresql}/bin/psql -h localhost -U postgres -d "${cfg.database.name}" -c "CREATE EXTENSION IF NOT EXISTS vector;"
    ${pkgs.postgresql}/bin/psql -h localhost -U postgres -d "${cfg.database.name}" -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
    
    # Run migrations using sqlx
    export DATABASE_URL="postgresql://${cfg.systemUser}@localhost/${cfg.database.name}"
    cd ${../.}
    ${pkgs.sqlx-cli}/bin/sqlx migrate run
  '';

in {
  options.services.sinex = {
    enable = mkEnableOption "Sinex universal data capture and query system";
    
    systemUser = mkOption {
      type = types.str;
      description = ''
        The system user that should run the sinex services.
        This should be the user running the graphical session (e.g., the one running Hyprland).
      '';
      example = "sinity";
    };
    
    autoConfigureSystem = mkOption {
      type = types.bool;
      default = true;
      description = ''
        Automatically configure system requirements for Sinex ingestors:
        - Configure kitty terminal for remote control
        - Set up shell integration for command tracking
        - Increase inotify limits for filesystem monitoring
      '';
    };
    
    database = {
      url = mkOption {
        type = types.str;
        default = "postgresql://${cfg.systemUser}@localhost/${cfg.database.name}";
        description = "PostgreSQL database URL";
      };
      
      name = mkOption {
        type = types.str;
        default = "sinex";
        description = "Database name";
      };
      
      user = mkOption {
        type = types.str;
        default = "sinex";
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
      
      filesystem = {
        enable = mkEnableOption "Filesystem activity ingestor";
        
        watchDirectories = mkOption {
          type = types.listOf types.str;
          default = [ "~" ];
          description = "Directories to watch recursively";
        };
        
        excludePatterns = mkOption {
          type = types.listOf types.str;
          default = [ "*.tmp" "*.log" "*.cache" ".git/**" "node_modules/**" "__pycache__/**" ];
          description = "Glob patterns to exclude from monitoring";
        };
        
        debounceMs = mkOption {
          type = types.int;
          default = 500;
          description = "Debounce delay in milliseconds";
        };
      };
      
      kitty = {
        enable = mkEnableOption "Kitty terminal activity ingestor";
        
        captureCommands = mkOption {
          type = types.bool;
          default = true;
          description = "Capture command execution events";
        };
        
        captureOutput = mkOption {
          type = types.bool;
          default = false;
          description = "Capture command output (privacy consideration)";
        };
        
        shellIntegration = mkOption {
          type = types.bool;
          default = true;
          description = "Enable shell integration for better command tracking";
        };
      };
    };
  };
  
  config = mkIf cfg.enable {
    # System tuning for filesystem monitoring
    boot.kernel.sysctl = mkIf cfg.ingestors.filesystem.enable {
      "fs.inotify.max_user_watches" = mkDefault 524288;
      "fs.inotify.max_user_instances" = mkDefault 256;
    };
    
    # Ensure PostgreSQL is enabled with required extensions
    services.postgresql = {
      enable = mkDefault true;
      package = mkDefault pkgs.postgresql_16;
      extensions = ps: with ps; [
        timescaledb
        pgvector  
        pgx_ulid
      ];
      
      # Ensure basic authentication and database setup
      authentication = mkOverride 999 ''
        # TYPE  DATABASE        USER            ADDRESS                 METHOD
        local   all             all                                     trust
        host    all             all             127.0.0.1/32            trust
        host    all             all             ::1/128                 trust
      '';
      
      # Ensure our database exists
      ensureDatabases = [ cfg.database.name ];
      
      # Ensure database user exists
      ensureUsers = [
        {
          name = cfg.systemUser;
          ensureClauses = {
            login = true;
          };
        }
      ];
      
      # Basic performance settings
      settings = mkDefault {
        shared_preload_libraries = "timescaledb";
        max_connections = 100;
        shared_buffers = "256MB";
        effective_cache_size = "1GB";
      };
    };
    # Don't create users - assume they already exist
    # The database user should be created by PostgreSQL configuration
    
    # Database initialization
    systemd.services.sinex-init = mkIf cfg.database.ensureExists {
      description = "Initialize Sinex database";
      wantedBy = [ "multi-user.target" ];
      after = [ "postgresql.service" ];
      requires = [ "postgresql.service" ];
      
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        ExecStart = initDbScript;
        User = "postgres";
        Environment = "PATH=${pkgs.postgresql}/bin:${pkgs.sqlx-cli}/bin";
      };
    };
    
    # Install monitoring tools system-wide
    environment.systemPackages = [ sinexMonitor ];
    
    # Enhanced logging configuration for journald
    services.journald.extraConfig = mkIf cfg.enable ''
      # Increase retention for Sinex services
      SystemMaxUse=1G
      SystemKeepFree=500M
      MaxRetentionSec=1month
    '';
    
    # Hyprland ingestor service
    systemd.services.sinex-hyprland = mkIf cfg.ingestors.hyprland.enable {
      description = "Sinex Hyprland activity ingestor";
      wantedBy = [ "multi-user.target" ];
      after = [ "sinex-init.service" "sinex-grant-permissions.service" ];
      
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
    
    # Filesystem ingestor service
    systemd.services.sinex-filesystem = mkIf cfg.ingestors.filesystem.enable {
      description = "Sinex Filesystem activity ingestor";
      wantedBy = [ "multi-user.target" ];
      after = [ "sinex-init.service" "sinex-grant-permissions.service" ];
      
      environment = {
        DATABASE_URL = cfg.database.url;
        RUST_LOG = "info";
      };
      
      serviceConfig = {
        Type = "simple";
        User = cfg.systemUser;
        ExecStart = pkgs.writeShellScript "filesystem-ingestor-wrapper" ''
          #!/usr/bin/env bash
          
          # Create config directory
          mkdir -p ~/.config/sinex
          
          # Generate configuration file
          cat > ~/.config/sinex/filesystem-ingestor.toml <<EOF
[database]
url = "${cfg.database.url}"
max_connections = 5

[logging]
level = "info"
format = "json"

[filesystem]
watch_directories = [${builtins.concatStringsSep ", " (map (dir: "\"${dir}\"") cfg.ingestors.filesystem.watchDirectories)}]
exclude_patterns = [${builtins.concatStringsSep ", " (map (pattern: "\"${pattern}\"") cfg.ingestors.filesystem.excludePatterns)}]
debounce_ms = ${toString cfg.ingestors.filesystem.debounceMs}
batch_size_events = 50
batch_timeout_ms = 5000
hash_files = true
max_hash_size_bytes = 10485760
heartbeat_interval_secs = 60
max_retries = 3
retry_delay_secs = 5
EOF
          
          # Start the actual ingestor
          exec ${filesystemIngestor}/bin/filesystem-ingestor run
        '';
        Restart = "always";
        RestartSec = 10;
        
        # Run in user's login session context
        PAMName = "login";
      };
    };
    
    # Kitty ingestor service
    systemd.services.sinex-kitty = mkIf cfg.ingestors.kitty.enable {
      description = "Sinex Kitty terminal activity ingestor";
      wantedBy = [ "multi-user.target" ];
      after = [ "sinex-init.service" "sinex-grant-permissions.service" ];
      
      environment = {
        DATABASE_URL = cfg.database.url;
        RUST_LOG = "info";
      };
      
      serviceConfig = {
        Type = "simple";
        User = cfg.systemUser;
        ExecStart = pkgs.writeShellScript "kitty-ingestor-wrapper" ''
          #!/usr/bin/env bash
          
          # Create config directory
          mkdir -p ~/.config/sinex
          
          # Generate configuration file
          cat > ~/.config/sinex/kitty-ingestor.toml <<EOF
[database]
url = "${cfg.database.url}"
max_connections = 5

[logging]
level = "info"
format = "json"

[kitty]
socket_path = "/tmp/kitty-*"
polling_interval_secs = 5
command_timeout_secs = 30
heartbeat_interval_secs = 60
capture_commands = ${if cfg.ingestors.kitty.captureCommands then "true" else "false"}
capture_output = ${if cfg.ingestors.kitty.captureOutput then "true" else "false"}
shell_integration = ${if cfg.ingestors.kitty.shellIntegration then "true" else "false"}
EOF
          
          # Start the actual ingestor
          exec ${kittyIngestor}/bin/kitty-ingestor run
        '';
        Restart = "always";
        RestartSec = 10;
        
        # Run in user's login session context
        PAMName = "login";
      };
    };
    
    # Grant database permissions to the system user
    systemd.services.sinex-grant-permissions = mkIf cfg.database.ensureExists {
      description = "Grant Sinex database permissions";
      wantedBy = [ "multi-user.target" ];
      after = [ "sinex-init.service" ];
      requires = [ "sinex-init.service" ];
      
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        ExecStart = pkgs.writeShellScript "grant-sinex-permissions" ''
          set -e
          # Create role if it doesn't exist
          ${pkgs.postgresql}/bin/psql -h localhost -U postgres -tc "SELECT 1 FROM pg_roles WHERE rolname = '${cfg.systemUser}'" | grep -q 1 || \
            ${pkgs.postgresql}/bin/psql -h localhost -U postgres -c "CREATE ROLE \"${cfg.systemUser}\" WITH LOGIN"
          
          # Grant permissions
          ${pkgs.postgresql}/bin/psql -h localhost -U postgres -d "${cfg.database.name}" -c "
            GRANT CONNECT ON DATABASE \"${cfg.database.name}\" TO \"${cfg.systemUser}\";
            GRANT USAGE ON SCHEMA public TO \"${cfg.systemUser}\";
            GRANT CREATE ON SCHEMA public TO \"${cfg.systemUser}\";
            GRANT USAGE ON SCHEMA raw TO \"${cfg.systemUser}\";
            GRANT USAGE ON SCHEMA sinex_schemas TO \"${cfg.systemUser}\";
            GRANT USAGE ON SCHEMA core TO \"${cfg.systemUser}\";
            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"${cfg.systemUser}\";
            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"${cfg.systemUser}\";
            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO \"${cfg.systemUser}\";
            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO \"${cfg.systemUser}\";
            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA sinex_schemas TO \"${cfg.systemUser}\";
            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA sinex_schemas TO \"${cfg.systemUser}\";
            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA core TO \"${cfg.systemUser}\";
            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA core TO \"${cfg.systemUser}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO \"${cfg.systemUser}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO \"${cfg.systemUser}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO \"${cfg.systemUser}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON SEQUENCES TO \"${cfg.systemUser}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA sinex_schemas GRANT ALL ON TABLES TO \"${cfg.systemUser}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA sinex_schemas GRANT ALL ON SEQUENCES TO \"${cfg.systemUser}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT ALL ON TABLES TO \"${cfg.systemUser}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT ALL ON SEQUENCES TO \"${cfg.systemUser}\";
          " || true
        '';
        User = "postgres";
      };
    };
    
    # User information about manual configuration
    warnings = mkIf (cfg.enable && !cfg.autoConfigureSystem) [
      ''
        Sinex auto-configuration is disabled. You may need to manually configure:
        1. Kitty terminal: allow_remote_control = "yes" and listen_on = "unix:/tmp/kitty"
        2. Shell integration for command tracking (see Sinex documentation)
        3. Filesystem monitoring: increase inotify limits if needed
      ''
    ];
  };
}