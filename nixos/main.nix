{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  
  # Import the flake packages for this module
  # These should be provided by the system that imports this module
  hyprlandIngestor = pkgs.sinex-hyprland-ingestor or (throw "sinex-hyprland-ingestor package not found");
  filesystemIngestor = pkgs.sinex-filesystem-ingestor or (throw "sinex-filesystem-ingestor package not found"); 
  kittyIngestor = pkgs.sinex-kitty-ingestor or (throw "sinex-kitty-ingestor package not found");
  promoWorker = pkgs.sinex-promo-worker or (throw "sinex-promo-worker package not found");
  
  
  # Database initialization script using sqlx migrations
  initDbScript = pkgs.writeShellScript "init-sinex-db" ''
    set -e
    
    # Wait for PostgreSQL to be available
    until ${pkgs.postgresql}/bin/pg_isready -h /run/postgresql; do
      echo "Waiting for PostgreSQL to be ready..."
      sleep 2
    done
    
    # Create database if it doesn't exist (should be handled by ensureDatabases)
    ${pkgs.postgresql}/bin/psql -h /run/postgresql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = '${cfg.database.name}'" | ${pkgs.gnugrep}/bin/grep -q 1 || \
      ${pkgs.postgresql}/bin/psql -h /run/postgresql -U postgres -c "CREATE DATABASE \"${cfg.database.name}\""
    
    # Create required extensions
    ${pkgs.postgresql}/bin/psql -h /run/postgresql -U postgres -d "${cfg.database.name}" -c "CREATE EXTENSION IF NOT EXISTS ulid;"
    ${pkgs.postgresql}/bin/psql -h /run/postgresql -U postgres -d "${cfg.database.name}" -c "CREATE EXTENSION IF NOT EXISTS vector;"
    ${pkgs.postgresql}/bin/psql -h /run/postgresql -U postgres -d "${cfg.database.name}" -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
    ${pkgs.postgresql}/bin/psql -h /run/postgresql -U postgres -d "${cfg.database.name}" -c "CREATE EXTENSION IF NOT EXISTS pg_jsonschema;"
    
    # Run migrations using sqlx as postgres user
    export DATABASE_URL="postgresql://postgres/${cfg.database.name}?host=/run/postgresql"
    cd ${../.}
    ${pkgs.sqlx-cli}/bin/sqlx migrate run
  '';

in {
  options.services.sinex = {
    enable = mkEnableOption "Sinex universal data capture and query system";
    
    systemUser = mkOption {
      type = types.str;
      default = "sinex";
      description = ''
        The system user that should run the sinex services.
        Defaults to dedicated 'sinex' user for security isolation.
      '';
      example = "sinex";
    };
    
    autoConfigureSystem = mkOption {
      type = types.bool;
      default = true;
      description = ''
        Automatically configure system requirements for Sinex ingestors:
        - Configure kitty terminal for remote control
        - Set up shell integration for command tracking
        - Increase inotify limits for filesystem monitoring
        - Configure system-level settings for optimal performance
      '';
    };
    
    database = {
      url = mkOption {
        type = types.str;
        default = "postgresql:///${cfg.database.name}?host=/run/postgresql&user=${cfg.database.user}";
        description = "PostgreSQL database URL using local peer authentication";
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
    # Create sinex system user and group
    users.users.sinex = {
      isSystemUser = true;
      group = "sinex";
      home = "/var/lib/sinex";
      createHome = true;
      description = "Sinex data capture system user";
      extraGroups = [ "users" ]; # Need access to user runtime directories
    };
    
    users.groups.sinex = {};
    
    # Create necessary directories for Sinex services
    systemd.tmpfiles.rules = [
      "d /var/lib/sinex 0755 ${cfg.systemUser} sinex -"
      "d /var/lib/sinex/dlq 0755 ${cfg.systemUser} sinex -"
      "d /var/lib/sinex/dlq/filesystem-ingestor 0755 ${cfg.systemUser} sinex -"
      "d /var/lib/sinex/dlq/hyprland-ingestor 0755 ${cfg.systemUser} sinex -"
      "d /var/lib/sinex/dlq/kitty-ingestor 0755 ${cfg.systemUser} sinex -"
      "d /var/log/sinex 0755 ${cfg.systemUser} sinex -"
      "d /var/log/sinex/filesystem-ingestor 0755 ${cfg.systemUser} sinex -"
      "d /var/log/sinex/hyprland-ingestor 0755 ${cfg.systemUser} sinex -"
      "d /var/log/sinex/kitty-ingestor 0755 ${cfg.systemUser} sinex -"
    ];
    
    # System tuning for filesystem monitoring
    boot.kernel.sysctl = mkIf cfg.ingestors.filesystem.enable {
      "fs.inotify.max_user_watches" = mkForce 1048576;
      "fs.inotify.max_user_instances" = mkForce 512;
    };
    
    # Ensure PostgreSQL is enabled with required extensions
    services.postgresql = {
      enable = mkDefault true;
      package = mkDefault pkgs.postgresql_16;
      extraPlugins = with pkgs.postgresql16Packages; [
        timescaledb
        pgvector  
        pgx_ulid
        # Note: pg_jsonschema needs to be installed separately:
        # https://github.com/supabase/pg_jsonschema#installation
        # Or add to postgresql16Packages in your system configuration
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
          name = cfg.database.user;
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
    
    # Monitoring available via flake apps: nix run .#monitor
    
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
      after = [ "sinex-init.service" "sinex-grant-permissions.service" "sinex-fix-ownership.service" ];
      
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
          while ! pgrep -f "Hyprland" > /dev/null; do
            echo "Waiting for Hyprland to start..."
            sleep 5
          done
          
          # Find the actual user running Hyprland
          HYPRLAND_USER=$(pgrep -f "Hyprland" -o | xargs ps -o user= -p | tr -d ' ')
          HYPRLAND_UID=$(id -u "$HYPRLAND_USER" 2>/dev/null || echo "1000")
          
          # Get the user's runtime directory  
          export XDG_RUNTIME_DIR="/run/user/$HYPRLAND_UID"
          
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
      };
    };
    
    # Filesystem ingestor service
    systemd.services.sinex-filesystem = mkIf cfg.ingestors.filesystem.enable {
      description = "Sinex Filesystem activity ingestor";
      wantedBy = [ "multi-user.target" ];
      after = [ "sinex-init.service" "sinex-grant-permissions.service" "sinex-fix-ownership.service" ];
      
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
      };
    };
    
    # Kitty ingestor service
    systemd.services.sinex-kitty = mkIf cfg.ingestors.kitty.enable {
      description = "Sinex Kitty terminal activity ingestor";
      wantedBy = [ "multi-user.target" ];
      after = [ "sinex-init.service" "sinex-grant-permissions.service" "sinex-fix-ownership.service" ];
      
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
      };
    };
    
    # Fix ownership of existing directories to sinex user
    systemd.services.sinex-fix-ownership = mkIf cfg.enable {
      description = "Fix ownership of Sinex directories";
      wantedBy = [ "multi-user.target" ];
      after = [ "local-fs.target" ];
      
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        ExecStart = pkgs.writeShellScript "fix-sinex-ownership" ''
          set -e
          # Fix ownership of existing directories if they exist
          for dir in /var/lib/sinex /var/log/sinex; do
            if [ -d "$dir" ]; then
              echo "Fixing ownership of $dir"
              chown -R sinex:sinex "$dir" || true
            fi
          done
        '';
        User = "root";
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
          ${pkgs.postgresql}/bin/psql -h /run/postgresql -U postgres -tc "SELECT 1 FROM pg_roles WHERE rolname = '${cfg.database.user}'" | ${pkgs.gnugrep}/bin/grep -q 1 || \
            ${pkgs.postgresql}/bin/psql -h /run/postgresql -U postgres -c "CREATE ROLE \"${cfg.database.user}\" WITH LOGIN"
          
          # Grant permissions
          ${pkgs.postgresql}/bin/psql -h /run/postgresql -U postgres -d "${cfg.database.name}" -c "
            GRANT CONNECT ON DATABASE \"${cfg.database.name}\" TO \"${cfg.database.user}\";
            GRANT USAGE ON SCHEMA public TO \"${cfg.database.user}\";
            GRANT CREATE ON SCHEMA public TO \"${cfg.database.user}\";
            GRANT USAGE ON SCHEMA raw TO \"${cfg.database.user}\";
            GRANT USAGE ON SCHEMA sinex_schemas TO \"${cfg.database.user}\";
            GRANT USAGE ON SCHEMA core TO \"${cfg.database.user}\";
            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"${cfg.database.user}\";
            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"${cfg.database.user}\";
            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO \"${cfg.database.user}\";
            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO \"${cfg.database.user}\";
            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA sinex_schemas TO \"${cfg.database.user}\";
            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA sinex_schemas TO \"${cfg.database.user}\";
            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA core TO \"${cfg.database.user}\";
            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA core TO \"${cfg.database.user}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO \"${cfg.database.user}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO \"${cfg.database.user}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO \"${cfg.database.user}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON SEQUENCES TO \"${cfg.database.user}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA sinex_schemas GRANT ALL ON TABLES TO \"${cfg.database.user}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA sinex_schemas GRANT ALL ON SEQUENCES TO \"${cfg.database.user}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT ALL ON TABLES TO \"${cfg.database.user}\";
            ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT ALL ON SEQUENCES TO \"${cfg.database.user}\";
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