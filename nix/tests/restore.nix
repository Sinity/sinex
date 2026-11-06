# The online core restore drill, including a post-cut write and a partial pair.
self: {
  name = "sinex-restore";
  nodes.machine = { pkgs, lib, ... }: {
    imports = [ self.nixosModules.default ];
    virtualisation = { memorySize = 3072; cores = 2; diskSize = 4096; };
    time.timeZone = "Etc/UTC";
    environment.etc."sinex-test/nats-daemon".text = "daemon-secret";
    environment.etc."sinex-test/borg-pass".text = "borg-pass";
    environment.systemPackages = [ pkgs.borgbackup pkgs.jq ];
    systemd.tmpfiles.rules = [
      "d /backup 0700 root root -"
      "f /var/lib/sinex/zsh_history 0644 sinex sinex - : 1700000000:0;ls -la\\n: 1700000060:2;cargo test\\n: 1700000200:0;git status\\n"
    ];
    services.sinex = {
      enable = true;
      database.sharedBuffers = "128MB";
      nats.daemonPasswordFile = "/etc/sinex-test/nats-daemon";
      # Only explicit synthetic stages enter this fixture. Acquiring unrelated
      # VM telemetry is not part of the independent restore oracle.
      exclude = lib.listToAttrs (map (source: {
        name = source.name;
        value.reason = "The restore fixture acquires only its explicit synthetic stages.";
      }) (lib.importJSON ../catalog.json).sources);
      backup = {
        repo = "/backup/sinex";
        keysRepo = "/backup/sinex-keys";
        passCommand = "cat /etc/sinex-test/borg-pass";
        startAt = [ ];
      };
    };
    services.borgbackup.jobs.sinex.preHook = lib.mkMerge [ (lib.mkBefore ''
      if test -e /run/poison-backup-path; then
        selected=$(cat /run/sinex-backup/active)
        ${pkgs.util-linux}/bin/runuser -u sinex -- mv "$selected/main" "$selected/main.original"
        ${pkgs.util-linux}/bin/runuser -u sinex -- ln -s /root "$selected/main"
        rm /run/poison-backup-path
      fi
    '') (lib.mkAfter ''
      if test -e /run/hold-backup-copy; then
        touch /run/backup-artifact-ready
        while ! test -e /run/release-backup-copy; do sleep 0.05; done
      fi
    '') ];
    services.borgbackup.jobs.sinex-keys.preHook = lib.mkAfter ''
      test ! -e /run/fail-key-create
    '';
  };
  testScript = ''
    import json
    ctl = "sinexctl --endpoint unix:/run/sinex/sinexd.sock --token-file /var/lib/sinex/tokens/operator.token --json"

    def events():
        return machine.succeed(
            "sudo -u postgres psql -d sinex -tAc \"select count(*), md5(string_agg(id::text || payload::text, ',' order by id)) from core.events\""
        ).strip()

    def files(root):
        return machine.succeed(f"cd {root} && find . -type f | sort | xargs -r sha256sum").strip()

    machine.wait_for_unit("sinexd.service")
    machine.succeed(f"{ctl} stage /var/lib/sinex/zsh_history --parser zsh --follow")
    machine.succeed("BORG_PASSCOMMAND='cat /etc/sinex-test/borg-pass' borg init --encryption=repokey-blake2 /backup/sinex")
    machine.succeed("BORG_PASSCOMMAND='cat /etc/sinex-test/borg-pass' borg init --encryption=repokey-blake2 /backup/sinex-keys")
    before = events()
    assert not before.startswith("0|"), before

    with subtest("a failed key archive retains the cut without completing the pair"):
        machine.succeed("touch /run/fail-key-create")
        machine.fail("systemctl start sinex-backup.service")
        failed_run = machine.succeed("cat /run/sinex-backup/active").strip()
        failed_hold = machine.succeed(f"jq -r .boundary.id {failed_run}/manifest.json").strip()
        machine.succeed(f"test -s {failed_run}/main.borg-info.json; test ! -e {failed_run}/complete.json")
        open_hold = machine.succeed(f"sudo -u postgres psql -d sinex -tAc \"SELECT released_at IS NULL FROM core.backup_holds WHERE operation_id='{failed_hold}'\"").strip()
        assert open_hold == "t", open_hold
        machine.succeed("rm /run/fail-key-create")

    with subtest("an artifact parent symlink cannot redirect the privileged Borg reader"):
        machine.succeed("touch /run/poison-backup-path")
        machine.fail("systemctl start sinex-backup.service")
        poisoned = machine.succeed("cat /run/sinex-backup/active").strip()
        machine.succeed(f"test -L {poisoned}/main; test ! -e {poisoned}/main.borg-info.json; test ! -e {poisoned}/complete.json")
        machine.succeed(f"sudo -u sinex rm {poisoned}/main; sudo -u sinex mv {poisoned}/main.original {poisoned}/main")

    with subtest("admission advances while the immutable cut is being archived"):
        machine.succeed("touch /run/hold-backup-copy")
        machine.succeed("systemctl start --no-block sinex-backup.service")
        machine.wait_until_succeeds("test -e /run/backup-artifact-ready")
        run = machine.succeed("cat /run/sinex-backup/active").strip()
        manifest = json.loads(machine.succeed(f"cat {run}/manifest.json"))
        assert manifest["boundary"]["producer_custody"] == "incomplete"
        # Independently read exactly the original protected prefix. A growing
        # live segment's later bytes are not part of this cut.
        prefix = next(f["bytes"] for f in manifest["files"] if f["path"] == "main/segments/current.hb")
        machine.succeed("mkdir -p /run/expected-store/segments")
        machine.succeed(f"head -c {prefix} /var/lib/sinex/store/segments/current.hb > /run/expected-store/segments/current.hb")
        before_files = files("/run/expected-store")
        assert events() == before, "the fixture admitted unrequested input before the controlled post-cut write"
        machine.succeed("printf ': 1700000300:0;after backup cut\\n' > /var/lib/sinex/after-cut")
        machine.succeed(f"{ctl} stage /var/lib/sinex/after-cut --parser zsh --follow")
        assert events() != before, "post-cut admission must remain available"
        machine.succeed("touch /run/release-backup-copy")
        machine.succeed("systemctl start --wait sinex-backup.service")
        pair = json.loads(machine.succeed("cat /run/sinex-backup/complete.json"))
        assert pair["hold"] == manifest["boundary"]["id"]
        assert pair["producer_custody"] == "incomplete"
        assert pair["main"]["name"] == pair["keys"]["name"]
        assert pair["main"]["id"] and pair["keys"]["id"]
        machine.succeed(f"test ! -d {run}/main; test ! -d {run}/keys")

    with subtest("lose the database, source files and content store"):
        machine.succeed("systemctl stop sinexd.service")
        machine.succeed("sudo -u postgres dropdb sinex")
        machine.succeed("rm -rf /var/lib/sinex/store/* /var/lib/sinex/backups/*")
        machine.succeed("rm /var/lib/sinex/zsh_history /var/lib/sinex/after-cut")

    with subtest("restore this exact main and key archive pair alone"):
        machine.succeed("mkdir -p /restore/main /restore/keys")
        main_archive = pair["main"]["name"]
        key_archive = pair["keys"]["name"]
        machine.succeed(f"cd /restore/main && BORG_PASSCOMMAND='cat /etc/sinex-test/borg-pass' borg extract /backup/sinex::{main_archive}")
        machine.succeed(f"cd /restore/keys && BORG_PASSCOMMAND='cat /etc/sinex-test/borg-pass' borg extract /backup/sinex-keys::{key_archive}")
        main_manifest = json.loads(machine.succeed("cat /restore/main/manifest.json"))
        key_manifest = json.loads(machine.succeed("cat /restore/keys/manifest.json"))
        assert main_manifest == key_manifest == manifest
        machine.succeed("cp -a /restore/main/segments /var/lib/sinex/store/")
        machine.succeed("sudo -u postgres createdb -O sinex sinex")
        machine.succeed("systemctl restart sinex-db-setup.service")
        machine.succeed("sudo -u postgres psql -d sinex -c 'SELECT timescaledb_pre_restore()'")
        machine.succeed("sudo -u postgres pg_restore -d sinex --exit-on-error < /restore/main/sinex.dump")
        machine.succeed("sudo -u postgres pg_restore -d sinex --data-only --exit-on-error < /restore/keys/data-keys.dump")
        machine.succeed("sudo -u postgres psql -d sinex -c 'SELECT timescaledb_post_restore()'")
        machine.succeed("systemctl start sinexd.service")
        machine.wait_for_unit("sinexd.service")

    after = events()
    after_files = files("/var/lib/sinex/store")
    assert after == before, f"restored events differ: {before} vs {after}"
    assert after_files == before_files, f"restored exact segment bytes differ: {before_files!r} vs {after_files!r}"
  '';
}
