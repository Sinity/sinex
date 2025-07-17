# Sinex NixOS Module - Modularized Structure
{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  
  # Import utility modules
  healthChecks = import ./health-checks.nix { inherit lib; };
  
  # Simple config file generation (Rust handles the actual config conversion)
  collectorConfigFile = pkgs.writeText "collector-placeholder.toml" ''
    # Placeholder config - Sinex uses environment variables and NixOS module options directly
    # The Rust collector reads configuration via nixos_config.rs, not TOML files
  '';
  
in
{
  imports = [
    # Core configuration modules
    ./database.nix
    ./event-sources.nix
    ./blob-storage.nix
    ./monitoring.nix
    ./preflight-verification.nix
    ./kitty-shell-integration.nix
    
    # New consolidated service management (recommended)
    ./services
    
    # Satellite architecture services
    ./satellite-services.nix
  ];

  options.services.sinex = {
    enable = mkEnableOption "Sinex Exocortex event capture system";

    package = mkOption {
      type = types.package;
      default = pkgs.sinex or (import ../. { }).packages.${pkgs.system}.default;
      defaultText = literalExpression "pkgs.sinex";
      description = "Sinex package to use";
    };

    cliPackage = mkOption {
      type = types.package;
      default = pkgs.python3;  # Temporary default to fix VM tests
      defaultText = literalExpression "pkgs.sinex-cli";
      description = "Sinex CLI package to use";
    };

    # Simplified target user configuration
    targetUser = mkOption {
      type = types.str;
      description = "Username whose files to monitor for events";
      example = "myuser";
    };

    # Satellite architecture user
    satelliteUser = mkOption {
      type = types.str;
      default = "sinex";
      description = "System user for satellite services";
    };

    # Simplified directories - monitoring.nix compatibility
    directories = {
      state = mkOption {
        type = types.path;
        default = "/var/lib/sinex";
        description = "Directory for persistent state data";
      };

      logs = mkOption {
        type = types.path;
        default = "/var/log/sinex";
        description = "Directory for log files";
      };
    };

    # Log level configuration (applies to all services)
    logLevel = mkOption {
      type = types.enum [ "trace" "debug" "info" "warn" "error" ];
      default = "info";
      description = "Global log level for Sinex services";
    };

    # DLQ configuration (used by satellite services)
    dlq = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable Dead Letter Queue for failed events";
      };

      failureStoragePath = mkOption {
        type = types.str;
        default = "/var/lib/sinex/failures";
        description = ''
          Directory for DLQ files and critical failure logs when database is down.
          Contains both failed event files and critical meta-failure logs.
        '';
      };

      maxRetries = mkOption {
        type = types.int;
        default = 3;
        description = "Maximum retry attempts for failed events";
      };

      retryDelaySecs = mkOption {
        type = types.int;
        default = 60;
        description = "Delay between retry attempts in seconds";
      };

      cleanup = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable automatic DLQ file cleanup";
        };

        maxAge = mkOption {
          type = types.str;
          default = "7d";
          description = "Maximum age of DLQ files before cleanup";
        };

        maxFiles = mkOption {
          type = types.int;
          default = 10000;
          description = "Maximum number of DLQ files before cleanup";
        };
      };
    };


    # Resource limits configuration for satellite services
    resources = {
      # Core services
      ingestd = {
        memoryMax = mkOption {
          type = types.str;
          default = "1G";
          description = "Maximum memory for ingestion daemon";
        };

        cpuQuota = mkOption {
          type = types.str;
          default = "100%";
          description = "CPU quota for ingestion daemon";
        };
      };

      gateway = {
        memoryMax = mkOption {
          type = types.str;
          default = "512M";
          description = "Maximum memory for API gateway";
        };

        cpuQuota = mkOption {
          type = types.str;
          default = "50%";
          description = "CPU quota for API gateway";
        };
      };

      # Default resources for satellites
      defaultSatellite = {
        memoryMax = mkOption {
          type = types.str;
          default = "256M";
          description = "Default maximum memory for satellite services";
        };

        cpuQuota = mkOption {
          type = types.str;
          default = "50%";
          description = "Default CPU quota for satellite services";
        };
      };
    };

    # Security configuration
    security = {
      level = mkOption {
        type = types.enum [ "minimal" "balanced" "strict" ];
        default = "balanced";
        description = ''
          Security level for SystemD hardening:
          - minimal: Basic security, maximum functionality
          - balanced: Reasonable security with event monitoring capabilities
          - strict: Maximum security, may restrict some monitoring features
        '';
      };

      allowFileSystemAccess = mkOption {
        type = types.bool;
        default = true;
        description = "Allow filesystem monitoring access (disabling may break file events)";
      };

      allowSocketAccess = mkOption {
        type = types.bool;
        default = true;
        description = "Allow access to Unix sockets for terminal and window manager monitoring";
      };

      allowDeviceAccess = mkOption {
        type = types.bool;
        default = true;
        description = "Allow device access for hardware event monitoring";
      };
    };

    # Update configuration
    update = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable coordinated update process";
      };

      gracePeriod = mkOption {
        type = types.int;
        default = 30;
        description = "Grace period in seconds for services to complete work before update";
      };

      healthCheckTimeout = mkOption {
        type = types.int;
        default = 60;
        description = "Maximum time to wait for health checks after update";
      };

      rollbackOnFailure = mkOption {
        type = types.bool;
        default = true;
        description = "Automatically rollback if health checks fail";
      };

      preserveData = mkOption {
        type = types.bool;
        default = true;
        description = "Preserve DLQ and failure data during updates";
      };
    };

  };

  config = mkIf cfg.enable {
    # Environment packages
    environment.systemPackages = with pkgs; [ 
      asciinema 
      cfg.cliPackage
    ];


    # Satellite architecture is now the default
    # Legacy collector/worker services have been removed
    # Use satellite services via satellite-services.nix module


    # User and group creation
    users.users.${cfg.database.user} = {
      isSystemUser = true;
      group = cfg.database.user;
      description = "Sinex database user";
      home = "/var/lib/${cfg.database.user}";
      createHome = true;
    };

    users.groups.${cfg.database.user} = {};

    # Satellite services user
    users.users.${cfg.satelliteUser} = mkIf cfg.satellite.enable {
      isSystemUser = true;
      group = cfg.satelliteUser;
      description = "Sinex satellite services user";
      home = "/var/lib/sinex";
      createHome = true;
    };

    users.groups.${cfg.satelliteUser} = mkIf cfg.satellite.enable {};

    # Directory setup and configuration
    systemd.tmpfiles.rules = [
      # Basic directories for monitoring.nix compatibility  
      "d ${cfg.directories.state} 0755 ${cfg.database.user} ${cfg.database.user} -"
      "d ${cfg.directories.logs} 0755 ${cfg.database.user} ${cfg.database.user} -"
      # Configuration directory
      "d /etc/sinex 0755 root root -"
    ] ++ lib.optionals cfg.dlq.enable [
      # DLQ failure storage directory
      "d ${cfg.dlq.failureStoragePath} 0755 ${cfg.database.user} ${cfg.database.user} -"
    ];
    
    # Place generated configuration file in standard location
    environment.etc."sinex/collector.toml".source = collectorConfigFile;

    # Database setup (if enabled)
    services.postgresql = mkIf cfg.database.autoSetup {
      enable = true;
      ensureDatabases = [ cfg.database.name ];
      ensureUsers = [
        {
          name = cfg.database.user;
          ensureDBOwnership = true;
        }
      ];
    };

    
    # Terminal auto-recording for all users
    programs.bash.promptInit = mkIf cfg.eventSources.asciinema.autoRecord ''
      # Automatic asciinema recording for Sinex
      if [[ ! -n "$ASCIINEMA_REC" ]] && command -v asciinema >/dev/null 2>&1; then
        export ASCIINEMA_REC=1
        ASCIINEMA_DIR="$HOME/.local/share/asciinema"
        mkdir -p "$ASCIINEMA_DIR"
        exec asciinema rec --quiet --idle-time-limit 3600 --command "$SHELL" \
          "$ASCIINEMA_DIR/$(hostname)-$(date +%Y%m%d-%H%M%S)-$$.cast"
      fi
    '';

    programs.zsh.promptInit = mkIf cfg.eventSources.asciinema.autoRecord ''
      # Automatic asciinema recording for Sinex
      if [[ ! -n "$ASCIINEMA_REC" ]] && command -v asciinema >/dev/null 2>&1; then
        export ASCIINEMA_REC=1
        ASCIINEMA_DIR="$HOME/.local/share/asciinema"
        mkdir -p "$ASCIINEMA_DIR"
        exec asciinema rec --quiet --idle-time-limit 3600 --command "$SHELL" \
          "$ASCIINEMA_DIR/$(hostname)-$(date +%Y%m%d-%H%M%S)-$$.cast"
      fi
    '';

    # Assertions for configuration validation
    assertions = [
      {
        assertion = cfg.enable -> cfg.targetUser != "";
        message = "services.sinex.targetUser must be set when Sinex is enabled";
      }
      {
        assertion = cfg.monitoring.observabilityStack.enable -> cfg.database.autoSetup || config.services.postgresql.enable;
        message = "PostgreSQL must be enabled for Sinex observability stack";
      }
      {
        assertion = cfg.monitoring.dashboards.grafana.enable -> cfg.monitoring.observabilityStack.enable;
        message = "Grafana dashboards require the observability stack to be enabled";
      }
    ];
  };
}