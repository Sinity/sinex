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
    #
    # The Sinex security model implements defense-in-depth with multiple layers:
    #
    # ## Process Isolation
    # Each satellite runs with minimal privileges through systemd hardening:
    # - NoNewPrivileges: Prevents privilege escalation
    # - ProtectSystem: Read-only system directories
    # - SystemCallFilter: Restricts available system calls
    # - PrivateTmp: Isolated temporary directories
    #
    # ## Trust Boundaries
    # Clear separation between components:
    # - Satellites → ingestd: gRPC with Unix socket permissions
    # - ingestd → PostgreSQL: Database role separation
    # - Automata → Redis: Consumer group isolation
    # - User → Gateway: API authentication (future)
    #
    # ## Resource Limits
    # Prevents resource exhaustion attacks:
    # - Memory limits per service (MemoryMax)
    # - CPU quotas (CPUQuota)
    # - File descriptor limits
    # - Rate limiting on event ingestion
    #
    # ## Data Sanitization
    # Multiple layers of privacy protection:
    # - Input sanitization: Redact secrets before storage
    # - Environment variable filtering
    # - Command argument scrubbing
    # - Access control: Source-based permissions
    #
    security = {
      level = mkOption {
        type = types.enum [ "minimal" "balanced" "strict" ];
        default = "balanced";
        description = ''
          Security level for SystemD hardening:
          - minimal: Basic security, maximum functionality
          - balanced: Reasonable security with event monitoring capabilities
          - strict: Maximum security, may restrict some monitoring features
          
          Each level applies different systemd hardening options:
          
          Minimal:
          - Basic sandboxing (PrivateTmp, ProtectHome)
          - No network isolation
          - Full filesystem access for monitoring
          
          Balanced (recommended):
          - Process isolation (NoNewPrivileges, ProtectSystem)
          - Limited system call filtering
          - Restricted but functional device access
          - Memory and CPU limits enforced
          
          Strict:
          - Full sandboxing (PrivateDevices, ProtectKernelTunables)
          - Aggressive system call filtering
          - Minimal filesystem access
          - May break some event sources
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

      # Privacy features configuration
      sanitization = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable automatic sanitization of sensitive data";
        };

        secretPatterns = mkOption {
          type = types.listOf types.str;
          default = [
            "password[=:]\\s*\\S+"
            "token[=:]\\s*\\S+"
            "api[_-]?key[=:]\\s*\\S+"
            "secret[=:]\\s*\\S+"
          ];
          description = "Regex patterns for detecting secrets to redact";
        };

        envVarFilter = mkOption {
          type = types.listOf types.str;
          default = [
            "PASSWORD"
            "TOKEN"
            "SECRET"
            "API_KEY"
            "PRIVATE_KEY"
          ];
          description = "Environment variables to filter from command captures";
        };
      };

      # Access control
      accessControl = {
        enable = mkOption {
          type = types.bool;
          default = false;
          description = "Enable source-based access control";
        };

        rules = mkOption {
          type = types.listOf (types.submodule {
            options = {
              source = mkOption {
                type = types.str;
                description = "Event source pattern to match";
              };
              allow = mkOption {
                type = types.listOf types.str;
                default = [ "*" ];
                description = "Allowed users/roles for this source";
              };
            };
          });
          default = [];
          description = "Access control rules by event source";
        };
      };

      # Audit configuration
      audit = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable security audit logging";
        };

        logLevel = mkOption {
          type = types.enum [ "info" "warn" "error" ];
          default = "warn";
          description = "Security audit log level";
        };

        retentionDays = mkOption {
          type = types.int;
          default = 90;
          description = "Days to retain security audit logs";
        };
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