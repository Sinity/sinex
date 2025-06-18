# Git-annex blob storage configuration module
{ lib, config, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
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
      "d ${cfg.blobStorage.repositoryPath} 0755 ${cfg.database.user} ${cfg.database.user} -"
    ];

    # Git-annex initialization service
    systemd.services.sinex-git-annex-init = mkIf cfg.blobStorage.autoInit {
      description = "Initialize Sinex Git-Annex Repository";
      wantedBy = [ "multi-user.target" ];
      after = [ "local-fs.target" ];
      
      serviceConfig = {
        Type = "oneshot";
        User = cfg.database.user;
        Group = cfg.database.user;
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

    # Git-annex maintenance timers
    systemd.timers = {
      sinex-git-annex-gc = mkIf cfg.blobStorage.maintenance.enableAutoGc {
        description = "Sinex Git-Annex Garbage Collection";
        wantedBy = [ "timers.target" ];
        timerConfig = {
          OnCalendar = cfg.blobStorage.maintenance.gcSchedule;
          Persistent = true;
          RandomizedDelaySec = "1h";
        };
      };

      sinex-git-annex-fsck = mkIf cfg.blobStorage.maintenance.enablePeriodicFsck {
        description = "Sinex Git-Annex File System Check";
        wantedBy = [ "timers.target" ];
        timerConfig = {
          OnCalendar = cfg.blobStorage.maintenance.fsckSchedule;
          Persistent = true;
          RandomizedDelaySec = "2h";
        };
      };
      
      sinex-blob-storage-metrics = mkIf cfg.monitoring.enable {
        description = "Emit Sinex Blob Storage Metrics";
        wantedBy = [ "timers.target" ];
        timerConfig = {
          OnBootSec = "5m";
          OnUnitActiveSec = "30s";
          AccuracySec = "5s";
        };
      };
    };

    systemd.services = {
      sinex-git-annex-gc = mkIf cfg.blobStorage.maintenance.enableAutoGc {
        description = "Sinex Git-Annex Garbage Collection";
        serviceConfig = {
          Type = "oneshot";
          User = cfg.database.user;
          Group = cfg.database.user;
          WorkingDirectory = cfg.blobStorage.repositoryPath;
          
          ExecStart = pkgs.writeShellScript "sinex-git-annex-gc" ''
            set -euo pipefail
            echo "Starting git-annex garbage collection..."
            ${pkgs.git}/bin/git gc --aggressive
            ${pkgs.git-annex}/bin/git annex unused
            echo "Garbage collection completed"
          '';
        };
      };

      sinex-git-annex-fsck = mkIf cfg.blobStorage.maintenance.enablePeriodicFsck {
        description = "Sinex Git-Annex File System Check";
        serviceConfig = {
          Type = "oneshot";
          User = cfg.database.user;
          Group = cfg.database.user;
          WorkingDirectory = cfg.blobStorage.repositoryPath;
          
          ExecStart = pkgs.writeShellScript "sinex-git-annex-fsck" ''
            set -euo pipefail
            echo "Starting git-annex file system check..."
            ${pkgs.git-annex}/bin/git annex fsck --fast
            echo "File system check completed"
          '';
        };
      };
      
      sinex-blob-storage-metrics = mkIf cfg.monitoring.enable {
        description = "Emit Sinex Blob Storage Metrics";
        serviceConfig = {
          Type = "oneshot";
          User = cfg.database.user;
          Group = cfg.database.user;
          WorkingDirectory = cfg.blobStorage.repositoryPath;
          
          # This is a placeholder - in reality, you'd call a Rust binary that uses BlobManager::emit_storage_stats
          ExecStart = pkgs.writeShellScript "sinex-blob-metrics" ''
            set -euo pipefail
            
            # For now, just log that metrics would be emitted
            # In production, this would call a Rust tool that uses BlobManager::emit_storage_stats()
            echo "Blob storage metrics emission placeholder - implement sinex-blob-metrics binary"
            
            # Could also gather basic stats via git-annex info and emit them
            if [ -d "${cfg.blobStorage.repositoryPath}/.git/annex" ]; then
              ${pkgs.git-annex}/bin/git annex info --fast || true
            fi
          '';
        };
      };
    };
  };
}