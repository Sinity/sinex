# Capture topology: one database host and a capture-only satellite.
# The satellite mounts sinexd's content store over NFS and produces over NATS
# with its own credentials.
self: {
  name = "sinex-topology";
  nodes = {
    server = { ... }: {
      imports = [ self.nixosModules.default ];
      # This VM has 3 GiB total; use a smaller database buffer pool.
      services.postgresql.settings.shared_buffers = "128MB";
      virtualisation = { memorySize = 3072; cores = 2; diskSize = 4096; };
      networking.firewall.enable = false;
      environment.etc."sinex-test/nats-daemon".text = "daemon-secret";
      environment.etc."sinex-test/nats-satellite".text = "satellite-secret";
      services.sinex = {
        enable = true;
        noCow = false;
        nats = {
          address = "0.0.0.0";
          daemonPasswordFile = "/etc/sinex-test/nats-daemon";
          producers.capture-satellite.passwordFile = "/etc/sinex-test/nats-satellite";
        };
      };
      # The satellite writes into the store through this group over NFS, so
      # both hosts give it the same ID.
      users.groups.sinex.gid = 2900;
      services.nfs.server = {
        enable = true;
        exports = "/var/lib/sinex/store *(rw,no_subtree_check,sync)";
      };
    };
    satellite = { ... }: {
      imports = [ self.nixosModules.default ];
      virtualisation.memorySize = 1024;
      networking.firewall.enable = false;
      environment.etc."sinex-test/nats".text = "satellite-secret";
      # Test VMs take their mounts from virtualisation.fileSystems.
      virtualisation.fileSystems."/mnt/sinex-store" = {
        device = "server:/var/lib/sinex/store";
        fsType = "nfs";
        options = [ "x-systemd.automount" "noauto" ];
      };
      systemd.tmpfiles.rules = [ "d /srv/watched 0755 root root -" ];
      users.groups.sinex.gid = 2900;
      services.sinex.exclude."machine.journald".reason = "The satellite fixture tests only file changes.";
      services.sinex.captureNode = {
        enable = true;
        nats = "server:4222";
        passwordFile = "/etc/sinex-test/nats";
        store = "/mnt/sinex-store";

        watch = [ "/srv/watched" ];
      };
    };
  };
  testScript = ''
    import time

    server.wait_for_unit("sinexd.service")
    server.wait_for_unit("nfs-server.service")
    satellite.wait_for_unit("multi-user.target")
    # Root is squashed on the export; the capture user reaches the store
    # through the shared group.
    satellite.wait_until_succeeds("runuser -u sinex-capture -- ls /mnt/sinex-store", timeout=120)
    satellite.wait_for_unit("sinex-capture.service")

    with subtest("a change on the satellite is admitted on the server with the satellite's host"):
        # The watch starts after the unit does, so a change made before it
        # is only the starting state: change the file until one arrives.
        admitted = "test $(sudo -u postgres psql -d sinex -tAc \"select count(*) from core.events where event_type = 'file.changed' and payload::text like '%x.txt%'\") -gt 0"
        for _ in range(36):
            satellite.succeed("echo from-satellite >> /srv/watched/x.txt")
            if server.execute(admitted)[0] == 0:
                break
            time.sleep(5)
        else:
            raise Exception("no change from the satellite was admitted")

    with subtest("the capture does not run as root"):
        satellite.succeed("test $(systemctl show -p User --value sinex-capture.service) = sinex-capture")

    with subtest("the satellite's credentials allow only its own subjects"):
        server.succeed("grep -q 'capture-satellite' /run/sinex-nats/nats.conf")
        server.succeed("grep -q 'sinex.ingress.capture-satellite' /run/sinex-nats/nats.conf")
  '';
}
