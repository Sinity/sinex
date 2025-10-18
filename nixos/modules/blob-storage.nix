# Git-annex blob storage configuration module
{ lib, config, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  repositoryUser =
    if cfg.satellite.enable then cfg.satelliteUser else cfg.database.user;
in
{
  options.services.sinex.blobStorage = {
    enable = mkOption {
      type = types.bool;
      default = true;
      description = "Enable git-annex blob storage integration";
    };

    repositoryPath = mkOption {
      type = types.path;
      default = "/realm/annex";
      description = "Path to git-annex repository";
    };

    autoInit = mkOption {
      type = types.bool;
      default = true;
      description = "Automatically initialize git-annex repository";
    };

    numCopies = mkOption {
      type = types.int;
      default = 2;
      description = "Minimum number of copies for git-annex";
    };

    backend = mkOption {
      type = types.str;
      default = "SHA256E";
      description = "Git-annex backend to use for new files";
    };

    # Simplified maintenance options
    maintenance = {
      enableAutoGc = mkOption {
        type = types.bool;
        default = true;
        description = "Enable automatic garbage collection";
      };

      gcSchedule = mkOption {
        type = types.str;
        default = "weekly";
        description = "Schedule for garbage collection";
      };

      enablePeriodicFsck = mkOption {
        type = types.bool;
        default = true;
        description = "Enable periodic file system consistency checks";
      };

      fsckSchedule = mkOption {
        type = types.str;
        default = "monthly";
        description = "Schedule for periodic fsck";
      };
    };

    # Health monitoring
    healthCheck = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable git-annex repository health checks";
      };

      interval = mkOption {
        type = types.int;
        default = 3600;
        description = "Health check interval in seconds";
      };

      wantedSize = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = ''
          Repository size monitoring limit (e.g., "50G", "1T", "500M"). 
          This is NOT preallocation - just logs warnings when exceeded.
          Set to null for unlimited (no size monitoring).
        '';
      };

      diskUsageWarning = mkOption {
        type = types.float;
        default = 0.8;
        description = "Warn when repository uses this fraction of wantedSize (0.8 = 80%)";
      };
    };
  };

  config = mkIf (cfg.enable && cfg.blobStorage.enable) {
    # Ensure repository directory exists
    systemd.tmpfiles.rules = [
      "d ${cfg.blobStorage.repositoryPath} 0755 ${repositoryUser} ${repositoryUser} -"
    ];

    # Git-annex initialization service
    systemd.services.sinex-git-annex-init = mkIf cfg.blobStorage.autoInit {
      description = "Initialize Sinex Git-Annex Repository";
      wantedBy = [ "multi-user.target" ];
      after = [ "local-fs.target" ];
      
      serviceConfig = {
        Type = "oneshot";
        User = repositoryUser;
        Group = repositoryUser;
        RemainAfterExit = true;
        
        ExecStart = pkgs.writeShellScript "sinex-git-annex-init" ''
          set -euo pipefail
          
          REPO_PATH="${cfg.blobStorage.repositoryPath}"
          
          # Create directory if it doesn't exist
          mkdir -p "$REPO_PATH"
          cd "$REPO_PATH"
          
          # Initialize git repo if not already done
          if [ ! -d .git ]; then
            ${pkgs.git}/bin/git init
            ${pkgs.git}/bin/git config user.name "Sinex System"
            ${pkgs.git}/bin/git config user.email "sinex@localhost"
          fi
          
          # Initialize git-annex if not already done
          if [ ! -d .git/annex ]; then
            ${pkgs.git-annex}/bin/git annex init "Sinex Blob Storage"
            ${pkgs.git-annex}/bin/git annex numcopies ${toString cfg.blobStorage.numCopies}
            ${pkgs.git-annex}/bin/git annex config annex.backend ${cfg.blobStorage.backend}
            
            # Create initial commit
            echo "# Sinex Blob Storage Repository" > README.md
            ${pkgs.git}/bin/git add README.md
            ${pkgs.git}/bin/git commit -m "Initial commit"
          fi
          
          echo "Git-annex repository initialized successfully"
        '';
      };
    };

    # Maintenance services and timers are supplied by services/maintenance-services.nix
    # to ensure a single implementation for git-annex lifecycle tasks.
  };
}
