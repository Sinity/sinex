# One coordinator acquires the native cut; Borg owns archive durability.
{ cfg, pkgs }:
let
  root = cfg.backup.artifactDir;
  control = "/run/sinex-backup";
  asUser = "${pkgs.util-linux}/bin/runuser -u ${pkgs.lib.escapeShellArg cfg.user} --";
  client = "${cfg.package}/bin/sinexctl --endpoint ${pkgs.lib.escapeShellArg "unix:${cfg.api.socket}"} --token-file ${pkgs.lib.escapeShellArg "${cfg.stateDir}/tokens/operator.token"} --json";
  job = component: repo: {
    paths = [ "." ];
    inherit repo;
    doInit = false;
    encryption = { mode = "repokey-blake2"; passCommand = cfg.backup.passCommand; };
    compression = "auto,zstd";
    startAt = [ ];
    failOnWarnings = true;
    appendFailedSuffix = true;
    preHook = ''
      sinexBackupRun=$(cat ${pkgs.lib.escapeShellArg "${control}/active"})
      run=$(basename -- "$sinexBackupRun")
      test "$sinexBackupRun" = ${pkgs.lib.escapeShellArg root}/"$run"
      [[ "$run" =~ ^[0-9a-f-]{36}$ ]]
      if [[ "''${SINEX_BACKUP_CWD:-}" != "$sinexBackupRun/${component}" ]]; then
        exec ${pkgs.python3}/bin/python3 ${./backup-reader.py} \
          ${pkgs.lib.escapeShellArg root} "$run" ${component} ${pkgs.lib.escapeShellArg cfg.user} "$0"
      fi
      unset SINEX_BACKUP_CWD
      hold=$(${asUser} ${pkgs.jq}/bin/jq -er '.boundary.id' "$sinexBackupRun/manifest.json")
      [[ "$hold" =~ ^[0-9a-f-]{36}$ ]]
      # Repository aliases must not put key bytes in the main repository,
      # even as a .failed archive. Initialization is a separate explicit act.
      borgWrapper info "''${extraArgs[@]}" --json ${pkgs.lib.escapeShellArg cfg.backup.repo} > ${pkgs.lib.escapeShellArg "${control}/main.repository.json"}
      borgWrapper info "''${extraArgs[@]}" --json ${pkgs.lib.escapeShellArg cfg.backup.keysRepo} > ${pkgs.lib.escapeShellArg "${control}/keys.repository.json"}
      mainId=$(${pkgs.jq}/bin/jq -er '.repository.id | strings | select(length > 0)' ${pkgs.lib.escapeShellArg "${control}/main.repository.json"})
      keyId=$(${pkgs.jq}/bin/jq -er '.repository.id | strings | select(length > 0)' ${pkgs.lib.escapeShellArg "${control}/keys.repository.json"})
      test "$mainId" != "$keyId"
      archiveName="sinex-core-$hold"
      # The root Borg walk starts at the descriptor-pinned cwd, not a mutable parent path.
    '';
    # Runs after a successful create and .failed rename. A successful sibling
    # cannot manufacture this repository's receipt. Root writes only its own
    # control directory; writes through daemon-owned paths run as that user.
    postCreate = ''
      borgWrapper info "''${extraArgs[@]}" --json "::$archiveName" > ${pkgs.lib.escapeShellArg "${control}/${component}.borg-info.json.tmp"}
      ${pkgs.jq}/bin/jq -e --arg name "$archiveName" \
        '.archives[0] | .name == $name and (.id | type == "string" and length > 0)' \
        ${pkgs.lib.escapeShellArg "${control}/${component}.borg-info.json.tmp"} >/dev/null
      mv -- ${pkgs.lib.escapeShellArg "${control}/${component}.borg-info.json.tmp"} ${pkgs.lib.escapeShellArg "${control}/${component}.borg-info.json"}
      ${asUser} ${pkgs.coreutils}/bin/tee "$sinexBackupRun/${component}.borg-info.json" < ${pkgs.lib.escapeShellArg "${control}/${component}.borg-info.json"} >/dev/null
      sync -f ${pkgs.lib.escapeShellArg "${control}/${component}.borg-info.json"}
    '';
  };
in {
  service = {
    description = "Sinex immutable core backup and paired Borg archives";
    after = [ "sinexd.service" ];
    requires = [ "sinexd.service" ];
    startAt = cfg.backup.startAt;
    path = [ pkgs.systemd pkgs.coreutils pkgs.jq pkgs.util-linux ];
    serviceConfig = {
      Type = "oneshot";
      UMask = "0077";
      RuntimeDirectory = "sinex-backup";
      RuntimeDirectoryMode = "0700";
      RuntimeDirectoryPreserve = "yes";
    };
    script = ''
      set -euo pipefail
      # systemd serializes the coordinator. Refuse to coalesce with a manual
      # sibling that may already be copying a different immutable cut.
      for unit in borgbackup-job-sinex.service borgbackup-job-sinex-keys.service; do
        if systemctl is-active --quiet "$unit"; then
          echo "$unit is already running; retain its current backup cut" >&2
          exit 1
        fi
      done
      run=${pkgs.lib.escapeShellArg root}/$(uuidgen)
      ${asUser} ${client} backup prepare --output "$run" > ${pkgs.lib.escapeShellArg "${control}/prepare.json"}
      ${asUser} test -s "$run/manifest.json"
      hold=$(jq -er '.result.artifact.boundary.id' ${pkgs.lib.escapeShellArg "${control}/prepare.json"})
      [[ "$hold" =~ ^[0-9a-f-]{36}$ ]]
      test "$hold" = "$(${asUser} jq -er '.boundary.id' "$run/manifest.json")"
      ${asUser} tee "$run/prepare.json" < ${pkgs.lib.escapeShellArg "${control}/prepare.json"} >/dev/null
      printf '%s\n' "$run" > ${pkgs.lib.escapeShellArg "${control}/active.tmp"}
      mv -- ${pkgs.lib.escapeShellArg "${control}/active.tmp"} ${pkgs.lib.escapeShellArg "${control}/active"}
      # A missing participant retains the native artifact and hold. It cannot
      # become a completed nightly backup through successful core archives.
      jq -e '.result.artifact.producer_custody == "complete"' ${pkgs.lib.escapeShellArg "${control}/prepare.json"} >/dev/null
      systemctl start --wait borgbackup-job-sinex.service
      systemctl start --wait borgbackup-job-sinex-keys.service
      # Both receipts came from actual archive reads, never unrelated latest
      # entries or solely a successful systemctl transport.
      jq -e --arg name "sinex-core-$hold" '.archives[0].name == $name and (.archives[0].id | length > 0)' ${pkgs.lib.escapeShellArg "${control}/main.borg-info.json"} >/dev/null
      jq -e --arg name "sinex-core-$hold" '.archives[0].name == $name and (.archives[0].id | length > 0)' ${pkgs.lib.escapeShellArg "${control}/keys.borg-info.json"} >/dev/null
      jq -n --arg hold "$hold" \
        --slurpfile main ${pkgs.lib.escapeShellArg "${control}/main.borg-info.json"} \
        --slurpfile keys ${pkgs.lib.escapeShellArg "${control}/keys.borg-info.json"} \
        --slurpfile prepared ${pkgs.lib.escapeShellArg "${control}/prepare.json"} \
        '{hold:$hold,main:$main[0].archives[0],keys:$keys[0].archives[0],producer_custody:$prepared[0].result.artifact.producer_custody,required_producers:$prepared[0].result.artifact.required_producers,producer_cuts:$prepared[0].result.artifact.producer_cuts}' \
        > ${pkgs.lib.escapeShellArg "${control}/complete.json"}
      sync -f ${pkgs.lib.escapeShellArg "${control}/complete.json"}
      ${asUser} tee "$run/complete.json" < ${pkgs.lib.escapeShellArg "${control}/complete.json"} >/dev/null
      # A warning or failed sibling retains both artifact and hold. Admission
      # stayed online; only this successfully archived cut is released.
      ${asUser} ${client} backup release "$hold" | ${asUser} tee "$run/release.json" >/dev/null
      ${asUser} ln -s -- "$(basename "$run")/complete.json" "$run/latest-complete.tmp"
      ${asUser} mv -Tf -- "$run/latest-complete.tmp" ${pkgs.lib.escapeShellArg "${root}/latest-complete.json"}
      # These copies belong solely to the completed run. Retain failed runs
      # and receipts; never perform privileged cleanup through daemon paths.
      ${asUser} rm -rf -- "$run/main" "$run/keys"
      rm -- ${pkgs.lib.escapeShellArg "${control}/active"}
    '';
  };
  jobs = {
    sinex = job "main" cfg.backup.repo;
    sinex-keys = job "keys" cfg.backup.keysRepo;
  };
}
