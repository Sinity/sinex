# The restore drill: data in, dump and Borg backup, both
# lost, restored from the backups alone, and the same events and bytes back.
self: {
  name = "sinex-restore";
  nodes.machine = { pkgs, ... }: {
    imports = [ self.nixosModules.default ];
    virtualisation = { memorySize = 3072; cores = 2; diskSize = 4096; };
    environment.etc."sinex-test/nats-daemon".text = "daemon-secret";
    environment.etc."sinex-test/borg-pass".text = "borg-pass";
    environment.systemPackages = [ pkgs.borgbackup ];
    systemd.tmpfiles.rules = [
      "d /backup 0700 root root -"
      "f /var/lib/sinex/zsh_history 0644 sinex sinex - : 1700000000:0;ls -la\\n: 1700000060:2;cargo test\\n: 1700000200:0;git status\\n"
    ];
    services.sinex = {
      enable = true;
      nats.daemonPasswordFile = "/etc/sinex-test/nats-daemon";
      backup = {
        repo = "/backup/sinex";
        keysRepo = "/backup/sinex-keys";
        passCommand = "cat /etc/sinex-test/borg-pass";
        startAt = "*-*-* 00:00:00";
      };
    };
  };
  testScript = ''
    url = "postgresql:///sinex?host=/run/postgresql"
    sinex = "sudo -u sinex"

    def state():
        events = machine.succeed(
            "sudo -u postgres psql -d sinex -tAc \"select count(*), md5(string_agg(id::text || payload::text, ',' order by id)) from core.events\""
        ).strip()
        blobs = machine.succeed("cd /var/lib/sinex/store && find . -type f | sort | xargs -r sha256sum | sha256sum").strip()
        return events, blobs

    machine.wait_for_unit("sinexd.service")
    # A backfill takes the writer, so the daemon stops for it.
    machine.succeed("systemctl stop sinexd.service")
    machine.succeed(f"{sinex} sinexd backfill --database '{url}' --store /var/lib/sinex/store --read zsh=/var/lib/sinex/zsh_history")
    machine.succeed("systemctl start sinexd.service")
    machine.wait_for_unit("sinexd.service")
    before = state()
    assert not before[0].startswith("0|"), before

    with subtest("dump and back up"):
        machine.succeed("systemctl start sinex-dump.service")
        machine.succeed("test -s /var/lib/sinex/dumps/sinex.dump")
        machine.succeed("test -s /var/lib/sinex/key-dumps/data-keys.dump")
        # The jobs are simple services: wait for them to finish.
        machine.succeed("systemctl start --wait borgbackup-job-sinex.service borgbackup-job-sinex-keys.service")

    with subtest("lose the database and the content store"):
        machine.succeed("systemctl stop sinexd.service")
        machine.succeed("sudo -u postgres dropdb sinex")
        machine.succeed("rm -rf /var/lib/sinex/store/* /var/lib/sinex/dumps/* /var/lib/sinex/key-dumps/*")

    with subtest("restore from the backups alone"):
        machine.succeed("cd / && BORG_PASSCOMMAND='cat /etc/sinex-test/borg-pass' borg extract /backup/sinex::$(BORG_PASSCOMMAND='cat /etc/sinex-test/borg-pass' borg list --last 1 --format '{archive}' /backup/sinex)")
        machine.succeed("cd / && BORG_PASSCOMMAND='cat /etc/sinex-test/borg-pass' borg extract /backup/sinex-keys::$(BORG_PASSCOMMAND='cat /etc/sinex-test/borg-pass' borg list --last 1 --format '{archive}' /backup/sinex-keys)")
        machine.succeed("sudo -u postgres createdb -O sinex sinex")
        machine.succeed("systemctl restart sinex-db-setup.service")
        machine.succeed("sudo -u postgres psql -d sinex -c 'SELECT timescaledb_pre_restore()'")
        # The dumps belong to sinex; root's shell hands them to pg_restore.
        machine.succeed("sudo -u postgres pg_restore -d sinex --exit-on-error < /var/lib/sinex/dumps/sinex.dump")
        machine.succeed("sudo -u postgres pg_restore -d sinex --data-only --exit-on-error < /var/lib/sinex/key-dumps/data-keys.dump")
        machine.succeed("sudo -u postgres psql -d sinex -c 'SELECT timescaledb_post_restore()'")
        machine.succeed("systemctl start sinexd.service")
        machine.wait_for_unit("sinexd.service")

    after = state()
    assert after == before, f"restored state differs: {before} vs {after}"
  '';
}
