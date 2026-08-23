# Production-shaped PostgreSQL local authentication proof.
#
# The shared VM base uses `trust` to keep unrelated integration scenarios cheap.
# This case restores the configured SCRAM boundary and proves that only the
# postgres bootstrap identity retains peer access over the Unix socket.
{ pkgs
, pg_jsonschema
, sinex ? null
, sinexCli ? null
, ...
}:

let
  inherit (pkgs) lib;
  databasePassword = "sinex-vm-local-auth-password";
in
pkgs.testers.nixosTest {
  name = "sinex-postgres-local-auth";

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib pg_jsonschema sinex sinexCli;
        postgresAuth = "scram-sha-256";
      })
    ];

    environment.etc."sinex/db-password".text = databasePassword;

    services.sinex.database = {
      localAuth = lib.mkForce "scram-sha-256";
      passwordFile = "/etc/sinex/db-password";
    };

  };

  testScript = ''
    database_password = "${databasePassword}"

    start_all()

    with subtest("PostgreSQL setup retains its bootstrap peer route"):
        machine.wait_for_unit("postgresql.service", timeout=60)
        machine.wait_for_unit("postgresql-setup.service", timeout=120)
        machine.succeed("systemctl is-active postgresql-setup.service")
        machine.succeed(
            "su - postgres -s /bin/sh -c "
            + "'psql -d postgres -At -c \"SELECT current_user\"' | grep '^postgres$'"
        )

    with subtest("Rendered HBA keeps postgres peer before application SCRAM"):
        machine.succeed(
            "hba=$(su - postgres -s /bin/sh -c 'psql -d postgres -At -c \"SHOW hba_file\"'); "
            + "awk '"
            + "/^[[:space:]]*local[[:space:]]+all[[:space:]]+postgres[[:space:]]+peer$/ { peer = NR } "
            + "/^[[:space:]]*local[[:space:]]+all[[:space:]]+all[[:space:]]+scram-sha-256$/ { scram = NR } "
            + "END { exit !(peer && scram && peer < scram) }' \"$hba\""
        )

    with subtest("Application socket route requires and accepts its configured password"):
        machine.succeed(
            "su - test -s /bin/sh -c "
            + "'PGPASSWORD=" + database_password
            + " psql -h /run/postgresql -U sinex -d sinex_dev -At -c \"SELECT current_user\"' "
            + "| grep '^sinex$'"
        )
        machine.fail(
            "su - test -s /bin/sh -c "
            + "'PGPASSWORD= psql -w -h /run/postgresql -U sinex -d sinex_dev -c \"SELECT 1\"'"
        )
  '';
}
