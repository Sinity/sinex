{ config, lib, pkgs, ... }:

with lib;

let
  systemdHardening = import ./lib/systemd-hardening.nix { inherit lib; };
  inherit (systemdHardening) mkHelperServiceConfig;
  cfg = config.services.sinex;
  blob = cfg.storage.blob;
  maintenanceCfg = cfg.lifecycle.maintenance;
  repoPath = blob.repositoryPath;
  repositoryUser = cfg.users.nodes;

  maintenanceEnabled = cfg.lifecycle.maintenance.enable;
  legacyAnnexEnabled = blob.enable && blob.legacyAnnexData;
  runBlobGc = maintenanceEnabled && maintenanceCfg.tasks.blobGc && blob.maintenance.gc.enable && legacyAnnexEnabled;
  runBlobFsck = maintenanceEnabled && maintenanceCfg.tasks.blobFsck && blob.maintenance.fsck.enable && legacyAnnexEnabled;
  healthEnabled = maintenanceEnabled && blob.health.enable;

  gcSchedule = blob.maintenance.gc.schedule;
  fsckSchedule = blob.maintenance.fsck.schedule;
  healthInterval = blob.health.intervalSec;

  gitAnnex = "${pkgs.git-annex}/bin/git-annex";
  gitBin = "${pkgs.git}/bin/git";
  duBin = "${pkgs.coreutils}/bin/du";

  initScript = pkgs.writeShellScript "sinex-blob-init" ''
    set -euo pipefail
    REPO_PATH="${repoPath}"

    mkdir -p "$REPO_PATH"
    cd "$REPO_PATH"

    if [ ! -d .git ]; then
      ${gitBin} init
      ${gitBin} config user.name "Sinex System"
      ${gitBin} config user.email "sinex@localhost"
    fi

    if [ ! -d .git/annex ]; then
      ${gitAnnex} init "Sinex Blob Storage"
      ${gitAnnex} numcopies ${toString blob.numCopies}
      echo "# Sinex Blob Storage" > README.md
      ${gitBin} add README.md
      ${gitBin} commit -m "Initial commit"
    fi
  '';

  gcScript = pkgs.writeShellScript "sinex-blob-gc" ''
    set -euo pipefail
    cd "${repoPath}"

    ${gitAnnex} unused || true
    # Drop all unused content in one pass; no artificial limit.
    ${gitAnnex} dropunused --force all || true
    ${gitBin} gc --aggressive || ${gitBin} gc
  '';

  fsckScript = pkgs.writeShellScript "sinex-blob-fsck" ''
    set -euo pipefail
    cd "${repoPath}"

    ${gitAnnex} fsck --incremental --fast || ${gitAnnex} fsck --fast
  '';

  healthScript = pkgs.writeShellScript "sinex-blob-health" ''
    set -euo pipefail
    repo_size=$(${duBin} -sb "${repoPath}" | cut -f1)
    warn_at_bytes=${toString (if blob.health.warnAtBytes != null then blob.health.warnAtBytes else 0)}
    warn_at_percent=${toString blob.health.warnAtPercent}

    if [ "$warn_at_bytes" -gt 0 ]; then
      threshold=$(printf '%.0f' "$(echo "$warn_at_bytes * $warn_at_percent" | ${pkgs.bc}/bin/bc -l)")
      if [ "$repo_size" -ge "$threshold" ]; then
        echo "Sinex blob repository warning: usage $repo_size bytes (threshold $threshold bytes)"
      fi
    fi
  '';

in
{
  config = mkMerge [
    (mkIf (blob.enable && blob.legacyAnnexData && blob.autoInit) {
      systemd.services.sinex-blob-init = {
        description = "Initialize Sinex blob repository";
        wantedBy = [ "multi-user.target" ];
        after = [ "local-fs.target" ];
        path = [ pkgs.git pkgs.git-annex ];
        serviceConfig = {
          ExecStart = initScript;
        } // mkHelperServiceConfig {
          user = repositoryUser;
          group = repositoryUser;
          remainAfterExit = true;
          readWritePaths = [ repoPath ];
        };
      };
    })

    (mkIf (blob.enable && blob.legacyAnnexData && runBlobGc) {
      systemd.services.sinex-blob-gc = {
        description = "Sinex blob garbage collection";
        after = [ "sinex-blob-init.service" ];
        requires = [ "sinex-blob-init.service" ];
        path = [ pkgs.git pkgs.git-annex ];
        serviceConfig = {
          WorkingDirectory = repoPath;
          ExecStart = gcScript;
        } // mkHelperServiceConfig {
          user = repositoryUser;
          group = repositoryUser;
          readWritePaths = [ repoPath ];
        };
      };

      systemd.timers.sinex-blob-gc = {
        description = "Timer for Sinex blob garbage collection";
        wantedBy = [ "timers.target" ];
        timerConfig = {
          OnCalendar = gcSchedule;
          Persistent = true;
        };
      };
    })

    (mkIf (blob.enable && blob.legacyAnnexData && runBlobFsck) {
      systemd.services.sinex-blob-fsck = {
        description = "Sinex blob fsck";
        after = [ "sinex-blob-init.service" ];
        requires = [ "sinex-blob-init.service" ];
        path = [ pkgs.git pkgs.git-annex ];
        serviceConfig = {
          WorkingDirectory = repoPath;
          ExecStart = fsckScript;
          TimeoutStartSec = 3600;
        } // mkHelperServiceConfig {
          user = repositoryUser;
          group = repositoryUser;
          readWritePaths = [ repoPath ];
        };
      };

      systemd.timers.sinex-blob-fsck = {
        description = "Timer for Sinex blob fsck";
        wantedBy = [ "timers.target" ];
        timerConfig = {
          OnCalendar = fsckSchedule;
          Persistent = true;
        };
      };
    })

    (mkIf (blob.enable && blob.legacyAnnexData && healthEnabled) {
      systemd.services.sinex-blob-health = {
        description = "Sinex blob health check";
        after = [ "sinex-blob-init.service" ];
        requires = [ "sinex-blob-init.service" ];
        path = [ pkgs.git pkgs.git-annex ];
        serviceConfig = {
          WorkingDirectory = repoPath;
          ExecStart = healthScript;
        } // mkHelperServiceConfig {
          user = repositoryUser;
          group = repositoryUser;
          readWritePaths = [ repoPath ];
        };
      };

      systemd.timers.sinex-blob-health = {
        description = "Timer for Sinex blob health check";
        wantedBy = [ "timers.target" ];
        timerConfig = {
          OnUnitActiveSec = toString healthInterval;
          Persistent = true;
        };
      };
    })
  ];
}
