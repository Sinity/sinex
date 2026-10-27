# The deployed stack on one host: PostgreSQL, NATS, sinexd, the journald
# capture and the session service, with a file change reaching the database
# through the session's NATS ingress, a source read from a private home
# directory, and a declared backfill.
self: {
  name = "sinex-stack";
  nodes.machine = { pkgs, ... }: {
    imports = [ self.nixosModules.default ];
    virtualisation = { memorySize = 3072; cores = 2; diskSize = 4096; };
    environment.etc."sinex-test/nats-daemon".text = "daemon secret: with @ and /";
    environment.etc."sinex-test/nats-session".text = "session-secret";
    environment.systemPackages = [ pkgs.curl ];
    users.users.alice = { isNormalUser = true; linger = true; };
    environment.etc."sinex-test/zsh_history".text = ''
      : 1700000000:0;echo hello
      : 1700000005:0;ls
    '';
    environment.etc."sinex-test/library_export.csv".text = ''
      Book Id,Title,Author,ISBN,ISBN13,My Rating,Average Rating,Date Read,Date Added,Bookshelves,Exclusive Shelf
      185,T,A,,,4,4.1,2020/01/02,2019/12/01,"x, y",read
    '';
    systemd.tmpfiles.rules = [
      "d /home/alice/notes 0755 alice users -"
      # A shell history only its owner can read.
      "C /home/alice/.zsh_history 0600 alice users - /etc/sinex-test/zsh_history"
    ];
    services.sinex = {
      enable = true;
      database.sharedBuffers = "128MB";
      nats.daemonPasswordFile = "/etc/sinex-test/nats-daemon";
      journald.enable = true;
      readAnyFile = true;
      sources."terminal.zsh-history" = {
        enable = true;
        paths = [ "/home/alice/.zsh_history" ];
      };
      backfills.books = [ "goodreads=/etc/sinex-test/library_export.csv" ];
      session = {
        enable = true;
        user = "alice";
        passwordFile = "/etc/sinex-test/nats-session";
        watch = [ "/home/alice/notes" ];
        wantedBy = [ "default.target" ];
      };
    };
  };
  testScript = ''
    def count(where):
        return int(machine.succeed(
            f"sudo -u postgres psql -d sinex -tAc \"select count(*) from core.events where {where}\""
        ).strip())

    machine.wait_for_unit("sinex-nats.service")
    machine.wait_for_unit("sinex-db-setup.service")
    machine.wait_for_unit("sinexd.service")
    machine.wait_for_unit("sinex-capture-journald.service")
    machine.wait_for_unit("sinex-session.service", user="alice")

    with subtest("NATS passwords are only hashes in the server config and never in arguments"):
        machine.succeed("grep -q 'password: \"\\$2a\\$' /run/sinex-nats/nats.conf")
        for secret in ["/etc/sinex-test/nats-daemon", "/etc/sinex-test/nats-session"]:
            machine.fail(f"grep -qFf {secret} /run/sinex-nats/nats.conf")
            machine.fail(f"cat /proc/[0-9]*/cmdline | tr '\\0' ' ' | grep -qFf {secret}")

    with subtest("PostgreSQL is set up as the module says"):
        machine.succeed("sudo -u postgres psql -tAc 'show wal_compression' | grep -qx lz4")
        machine.succeed("sudo -u postgres psql -tAc 'show full_page_writes' | grep -qx on")
        for setting, expected in {
            "shared_buffers": "128MB", "effective_cache_size": "12GB",
            "work_mem": "64MB", "maintenance_work_mem": "512MB",
            "max_wal_size": "16GB", "checkpoint_timeout": "30min",
            "checkpoint_completion_target": "0.9", "jit": "off",
            "synchronous_commit": "on", "fsync": "on",
            "max_connections": "64", "shared_preload_libraries": "timescaledb",
        }.items():
            shown = machine.succeed(f"sudo -u postgres psql -tAc 'show {setting}'").strip()
            assert shown == expected, f"{setting}: {shown!r} != {expected!r}"
        # PostgreSQL 18 made log_connections a list; off may show as empty.
        shown = machine.succeed("sudo -u postgres psql -tAc 'show log_connections'").strip()
        assert shown in ("", "off", "false", "no"), f"log_connections is {shown!r}"
        machine.succeed("sudo -u postgres psql -d sinex -tAc \"select 1 from pg_extension where extname='timescaledb'\" | grep -qx 1")

    with subtest("NATS refuses a producer outside its own subjects"):
        machine.succeed("grep -q 'session-machine' /run/sinex-nats/nats.conf")
        machine.fail("grep -rq session-secret /etc/systemd/system/sinex-nats.service /etc/systemd/system/sinexd.service")

    with subtest("a file change reaches the database through the ingress"):
        machine.succeed("su alice -c 'echo hello > /home/alice/notes/a.txt'")
        machine.wait_until_succeeds(
            "test $(sudo -u postgres psql -d sinex -tAc \"select count(*) from core.events where event_type = 'file.changed'\") -gt 0",
            timeout=180,
        )

    with subtest("the API answers with a token, and refuses without one"):
        machine.succeed(
            "curl -sf -H \"Authorization: Bearer $(cat /var/lib/sinex/tokens/read.token)\" http://127.0.0.1:7171/v1/commands"
        )
        machine.fail("curl -sf http://127.0.0.1:7171/v1/commands -X POST -d '{}'")

    with subtest("sinexd reads a file only the operator can read"):
        machine.succeed("test $(stat -c %a /home/alice) = 700")
        machine.wait_until_succeeds(
            "test $(sudo -u postgres psql -d sinex -tAc \"select count(*) from core.events where event_type = 'command.imported'\") -gt 0",
            timeout=120,
        )

    with subtest("the operator's login shell reaches the daemon and its private-mode switch"):
        machine.succeed("su - alice -c 'sinexctl status'")
        machine.succeed("su - alice -c 'sinexctl private on'")
        machine.succeed("test -e /run/sinex/private-mode")
        machine.succeed("su - alice -c 'sinexctl private off'")

    with subtest("a declared backfill stops sinexd, imports, and starts it again"):
        machine.succeed("systemctl start sinex-backfill-books.service")
        assert count("event_type = 'book.status'") == 2
        machine.wait_until_succeeds("systemctl is-active sinexd.service", timeout=120)

    with subtest("sinexd comes back after a crash"):
        machine.succeed("systemctl kill -s KILL sinexd.service")
        machine.wait_until_succeeds("systemctl is-active sinexd.service", timeout=120)
  '';
}
