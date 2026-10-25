# The Sinex NixOS module. It deploys sinexd with its
# PostgreSQL and NATS, the session service, the journald capture, a
# capture-only node for satellite hosts, and the backups. Source and derivation
# options come from the registries through the generated `catalog.json`.
#
# Deployment itself waits for the operator's Phase C go-ahead; this module is
# what that deployment will use.
self:
{ config, lib, pkgs, ... }:
let
  inherit (lib) mkOption mkEnableOption mkIf mkMerge types optional optionals optionalString
    concatStringsSep concatMapStringsSep mapAttrs mapAttrsToList filterAttrs escapeShellArg;

  cfg = config.services.sinex;
  node = config.services.sinex.captureNode;
  catalog = lib.importJSON ./catalog.json;
  system = pkgs.stdenv.hostPlatform.system;
  defaultPackage = self.packages.${system}.default;

  postgres = cfg.database.package;
  dbUrl = "postgresql:///sinex?host=/run/postgresql";
  natsListen = "${cfg.nats.address}:${toString cfg.nats.port}";

  # Producers allowed onto the ingress: those declared, plus the session
  # service on this host.
  producers = cfg.nats.producers
    // lib.optionalAttrs cfg.session.enable {
      ${cfg.session.producer} = { passwordFile = cfg.session.passwordFile; };
    };

  # A NATS URL with a user and a password read from a file at start; the
  # password is URL-encoded so any characters work.
  natsUrlScript = { user, passwordFile, address }: ''
    pw=$(${pkgs.jq}/bin/jq -rRs 'rtrimstr("\n") | @uri' < "${passwordFile}")
    nats_url="nats://${user}:$pw@${address}"
  '';

  # Sealed operations restart cleanly: three tries in ten minutes, 30 s apart
  #.
  restartPolicy = {
    Restart = "on-failure";
    RestartSec = "30s";
  };
  startLimit = {
    StartLimitBurst = 3;
    StartLimitIntervalSec = 600;
  };

  sourceOption = spec: {
    enable = mkEnableOption "the ${spec.name} source (${spec.driver}; read by ${spec.consumer})";
    paths = mkOption {
      type = types.listOf types.str;
      default = [ ];
      description = "Where ${spec.name} reads from on this host.";
    };
    class = mkOption {
      type = types.enum [ "open" "guarded" "sealed" ];
      default = spec.class;
      description = "Privacy class of ${spec.name}'s events.";
    };
    kind = mkOption {
      type = types.nullOr types.str;
      default = null;
      description = "The kind to read ${spec.name} with when several kinds read it (`sinnix-lake` for a lake lane); the first registered when null.";
    };
    zone = mkOption {
      type = types.nullOr types.str;
      default = null;
      description = "IANA zone ${spec.name}'s local times were written in; the system zone when null.";
    };
    options = mkOption {
      type = types.attrsOf (types.oneOf [ types.str types.int types.bool (types.listOf types.str) ]);
      default = { };
      description = "${spec.name}'s own options (a selection, a capture interval).";
    };
  };
in
{
  options.services.sinex = {
    enable = mkEnableOption "Sinex";
    package = mkOption {
      type = types.package;
      default = defaultPackage;
      description = "The package with sinexd, sinexctl, sinex-session and sinex-capture.";
    };
    user = mkOption { type = types.str; default = "sinex"; };
    group = mkOption { type = types.str; default = "sinex"; };
    stateDir = mkOption {
      type = types.path;
      default = "/var/lib/sinex";
      description = "Tokens, NATS storage, spools and dumps. On the operator's machine a /realm subvolume.";
    };
    storeDir = mkOption {
      type = types.path;
      default = "${cfg.stateDir}/store";
      description = ''
        The content store: the exact bytes events are read from. Producers on
        this host write their spool under it through the store's group; spool
        files or directories with other ownership or modes are refused.
      '';
    };
    noCow = mkOption {
      type = types.bool;
      default = true;
      description = "Turn copy-on-write off for the database and content store directories on btrfs; both checksum their own data.";
    };
    host = mkOption {
      type = types.str;
      default = config.networking.hostName;
      description = "This host's name, recorded with every input.";
    };
    zone = mkOption { type = types.nullOr types.str; default = null; description = "IANA zone for days and clock times; the system's when null."; };
    dayStart = mkOption { type = types.str; default = "04:00"; description = "When a logical day starts."; };

    api = {
      socket = mkOption { type = types.path; default = "/run/sinex/sinexd.sock"; };
      listen = mkOption {
        type = types.nullOr types.str;
        default = "127.0.0.1:7171";
        description = "Loopback address for the HTTP API, used with a token.";
      };
    };

    database = {
      package = mkOption {
        type = types.package;
        default = pkgs.postgresql_18.withPackages (p: [ p.timescaledb ]);
      };
      dataDir = mkOption {
        type = types.path;
        default = "/var/lib/postgresql/sinex";
        description = "PostgreSQL's data directory; postgres must be able to reach it, so not under the 0750 state directory.";
      };
      maxConnections = mkOption {
        type = types.int;
        default = 64;
        description = "Room for the writer, the readers, the API pool and admin commands.";
      };
    };

    nats = {
      address = mkOption {
        type = types.str;
        default = "127.0.0.1";
        description = "Where NATS listens. A satellite capture node needs an address it can reach (a tailnet address).";
      };
      port = mkOption { type = types.port; default = 4222; };
      subjects = mkOption { type = types.str; default = "sinex"; };
      daemonPasswordFile = mkOption {
        type = types.path;
        description = "The daemon user's NATS password (an agenix secret readable by the sinex user).";
      };
      producers = mkOption {
        type = types.attrsOf (types.submodule {
          options.passwordFile = mkOption {
            type = types.path;
            description = "This producer's NATS password (an agenix secret).";
          };
        });
        default = { };
        description = "Outside producers allowed onto the ingress, each limited to its own subjects.";
      };
      tls = {
        certFile = mkOption { type = types.nullOr types.path; default = null; };
        keyFile = mkOption { type = types.nullOr types.path; default = null; };
      };
    };

    sources = mapAttrs (_: sourceOption)
      (lib.listToAttrs (map (s: { name = s.name; value = s; }) catalog.sources));

    derivations = lib.listToAttrs (map
      (d: {
        name = d.name;
        value.enable = mkOption {
          type = types.bool;
          default = true;
          description = "The ${d.kind} derivation ${d.name} (${concatStringsSep ", " d.inputs} → ${concatStringsSep ", " d.outputs}); analytics are on by default.";
        };
      })
      catalog.derivations);

    journald.enable = mkOption {
      type = types.bool;
      default = cfg.sources."machine.journald".enable;
      defaultText = lib.literalExpression ''config.services.sinex.sources."machine.journald".enable'';
      description = "Capture the systemd journal into its spool.";
    };

    session = {
      enable = mkEnableOption "the session service for the operator's graphical session";
      user = mkOption { type = types.str; default = ""; description = "The operator's account."; };
      producer = mkOption { type = types.str; default = "session-${cfg.host}"; };
      passwordFile = mkOption {
        type = types.path;
        description = "The session producer's NATS password, readable by the operator's account.";
      };
      hyprland = mkOption { type = types.bool; default = false; };
      notifications = mkOption { type = types.bool; default = false; description = "Capture desktop notifications."; };
      players = mkOption { type = types.bool; default = false; description = "Capture media players (MPRIS)."; };
      clipboard = mkOption {
        type = types.bool;
        default = false;
        description = "Capture the clipboard and primary selection (content marked secret by its owner is not read).";
      };
      producers = mkOption {
        type = types.bool;
        default = false;
        description = "Listen on the producer socket for the Neovim and kitty integrations.";
      };
      screenEvery = mkOption {
        type = types.nullOr types.ints.positive;
        default = null;
        description = "Seconds between stills of every output; no screen capture when null.";
      };
      audioSegment = mkOption {
        type = types.nullOr types.ints.positive;
        default = null;
        description = "Seconds per audio segment of the default source; no audio capture when null.";
      };
      watch = mkOption {
        type = types.listOf types.str;
        default = [ ];
        description = "Home directories whose file changes to capture.";
      };
      wantedBy = mkOption {
        type = types.listOf types.str;
        default = [ "graphical-session.target" ];
      };
    };

    backup = {
      dumpDir = mkOption { type = types.path; default = "${cfg.stateDir}/dumps"; };
      keysDir = mkOption { type = types.path; default = "${cfg.stateDir}/key-dumps"; };
      startAt = mkOption { type = types.str; default = "daily"; };
      repo = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = "Borg repository for the content store and the database dump; no Borg job when null.";
      };
      keysRepo = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = "A separate Borg repository for the sealed key table.";
      };
      passCommand = mkOption {
        type = types.str;
        default = "";
        description = "Command printing the Borg passphrase (repokey-blake2 encryption).";
      };
    };
  };

  options.services.sinex.captureNode = {
    enable = mkEnableOption "a capture-only Sinex node for a satellite host";
    package = mkOption { type = types.package; default = defaultPackage; };
    nats = mkOption {
      type = types.str;
      example = "sinex-host.tailnet:4222";
      description = "The main host's NATS address.";
    };
    subjects = mkOption { type = types.str; default = "sinex"; };
    producer = mkOption { type = types.str; default = "capture-${config.networking.hostName}"; };
    passwordFile = mkOption { type = types.path; };
    store = mkOption {
      type = types.path;
      description = "sinexd's content store as mounted here (sending bytes over the link is not built yet).";
    };
    stateDir = mkOption { type = types.path; default = "/var/lib/sinex-capture"; };
    host = mkOption { type = types.str; default = config.networking.hostName; };
    journald = mkOption { type = types.bool; default = true; };
    watch = mkOption { type = types.listOf types.str; default = [ ]; };
  };

  config = mkMerge [
    (mkIf cfg.enable {
      users.users = {
        ${cfg.user} = {
          isSystemUser = true;
          group = cfg.group;
          home = cfg.stateDir;
        };
      } // lib.optionalAttrs cfg.session.enable {
        # Producers on this host write material bytes into the store.
        ${cfg.session.user}.extraGroups = [ cfg.group ];
      };
      assertions = [{
        assertion = !cfg.session.enable || cfg.session.user != "";
        message = "services.sinex.session.user must name the operator's account";
      }];
      users.groups.${cfg.group} = { };
      environment.systemPackages = [ cfg.package ];

      # Bindings from the registry-generated options, for the daemon's sources.
      # The journald capture's spool is the journald source's input.
      environment.etc."sinex/sources.json".text = builtins.toJSON {
        sources = filterAttrs (_: s: s.enable) (cfg.sources
          // lib.optionalAttrs cfg.journald.enable {
            "machine.journald" = cfg.sources."machine.journald" // {
              enable = true;
              paths = [ "${cfg.stateDir}/spool/journald" ];
            };
          });
        derivations = mapAttrs (_: d: d.enable) cfg.derivations;
      };

      systemd.tmpfiles.rules = [
        "d ${cfg.stateDir} 0750 ${cfg.user} ${cfg.group} -"
        # Group mode 2770 marks the store root as shared with producers on this
        # host; they write only under incoming/producers (the spool).
        "d ${cfg.storeDir} 2770 ${cfg.user} ${cfg.group} -"
        "d ${cfg.storeDir}/incoming 2770 ${cfg.user} ${cfg.group} -"
        "d ${cfg.storeDir}/incoming/producers 2770 ${cfg.user} ${cfg.group} -"
        "d ${cfg.stateDir}/tokens 0750 ${cfg.user} ${cfg.group} -"
        "d ${cfg.stateDir}/nats 0750 ${cfg.user} ${cfg.group} -"
        "d ${cfg.stateDir}/spool 0750 ${cfg.user} ${cfg.group} -"
        "d ${cfg.backup.dumpDir} 0750 ${cfg.user} ${cfg.group} -"
        "d ${cfg.backup.keysDir} 0700 ${cfg.user} ${cfg.group} -"
        "d ${cfg.database.dataDir} 0700 postgres postgres -"
      ] ++ optionals cfg.noCow [
        "h ${cfg.database.dataDir} - - - - +C"
        "h ${cfg.storeDir} - - - - +C"
      ];

      services.postgresql = {
        enable = true;
        package = postgres;
        dataDir = cfg.database.dataDir;
        enableTCPIP = false;
        ensureDatabases = [ "sinex" ];
        ensureUsers = [{ name = cfg.user; ensureDBOwnership = true; }];
        settings = {
          shared_preload_libraries = "timescaledb";
          "timescaledb.telemetry_level" = "off";
          "timescaledb.enable_chunk_skipping" = "on";
          full_page_writes = true;
          wal_compression = "lz4";
          password_encryption = "scram-sha-256";
          max_connections = cfg.database.maxConnections;
          # Connection logs would feed back into the journald source.
          log_connections = false;
          log_disconnections = false;
        };
        authentication = lib.mkBefore ''
          local sinex ${cfg.user} peer
          local all postgres peer
          host  sinex all 127.0.0.1/32 scram-sha-256
          host  sinex all ::1/128      scram-sha-256
        '';
      };

      # Extensions and roles need the superuser; the schema itself is applied
      # by the sinex user at daemon start.
      systemd.services.sinex-db-setup = {
        description = "Sinex database extensions and roles";
        after = [ "postgresql.service" "postgresql-setup.service" ];
        requires = [ "postgresql.service" ];
        wantedBy = [ "multi-user.target" ];
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
          User = "postgres";
        };
        script = ''
          ${postgres}/bin/psql -v ON_ERROR_STOP=1 -d sinex <<'SQL'
          CREATE EXTENSION IF NOT EXISTS timescaledb;
          CREATE EXTENSION IF NOT EXISTS pg_trgm;
          DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sinex_writer') THEN CREATE ROLE sinex_writer; END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sinex_reader') THEN CREATE ROLE sinex_reader; END IF;
          END $$;
          GRANT sinex_writer, sinex_reader TO ${cfg.user};
          SQL
        '';
      };

      systemd.services.sinex-nats = {
        description = "NATS JetStream for Sinex: the ingress and the confirmed stream";
        after = [ "network.target" ];
        wantedBy = [ "multi-user.target" ];
        unitConfig = startLimit;
        restartIfChanged = false;
        serviceConfig = restartPolicy // {
          User = cfg.user;
          Group = cfg.group;
          RuntimeDirectory = "sinex-nats";
          RuntimeDirectoryMode = "0700";
          LoadCredential = [ "daemon:${cfg.nats.daemonPasswordFile}" ]
            ++ mapAttrsToList (name: p: "producer-${name}:${p.passwordFile}") producers;
        };
        preStart = ''
          ${optionalString (cfg.nats.tls.certFile != null) ''
            # A missing or expired certificate stops here, by name.
            ${pkgs.openssl}/bin/openssl x509 -noout -checkend 0 -in ${escapeShellArg cfg.nats.tls.certFile} \
              || { echo "sinex-nats: TLS certificate ${cfg.nats.tls.certFile} is missing or expired" >&2; exit 1; }
            test -r ${escapeShellArg cfg.nats.tls.keyFile} \
              || { echo "sinex-nats: TLS key ${cfg.nats.tls.keyFile} is not readable" >&2; exit 1; }
          ''}
          conf="$RUNTIME_DIRECTORY/nats.conf"
          {
            echo 'listen: "${natsListen}"'
            echo 'jetstream { store_dir: "${cfg.stateDir}/nats" }'
            ${optionalString (cfg.nats.tls.certFile != null) ''
              echo 'tls { cert_file: "${cfg.nats.tls.certFile}", key_file: "${cfg.nats.tls.keyFile}" }'
            ''}
            ${cfg.package}/bin/sinexd admin nats-authorization \
              --subjects ${escapeShellArg cfg.nats.subjects} \
              --daemon-password-file "$CREDENTIALS_DIRECTORY/daemon" \
              ${concatMapStringsSep " " (name: "--producer ${escapeShellArg name}=\"$CREDENTIALS_DIRECTORY/producer-${name}\"") (lib.attrNames producers)}
          } > "$conf"
        '';
        script = ''exec ${pkgs.nats-server}/bin/nats-server -c "$RUNTIME_DIRECTORY/nats.conf"'';
      };

      systemd.services.sinexd = {
        description = "Sinex daemon";
        after = [ "sinex-db-setup.service" "sinex-nats.service" "network.target" ];
        requires = [ "sinex-db-setup.service" "sinex-nats.service" ];
        wantedBy = [ "multi-user.target" ];
        unitConfig = startLimit;
        # A deploy is verified by checking the running binary, not by a
        # restart on every switch.
        restartIfChanged = false;
        # Tesseract reads text from screen segments (screen.text).
        path = [ pkgs.tesseract pkgs.coreutils ];
        serviceConfig = restartPolicy // {
          User = cfg.user;
          Group = cfg.group;
          RuntimeDirectory = "sinex";
          RuntimeDirectoryMode = "0750";
          LoadCredential = [ "nats:${cfg.nats.daemonPasswordFile}" ];
          UMask = "0007";
        };
        preStart = ''
          # Tokens are made once; a missing file disables that level.
          for level in read agent operator; do
            f=${cfg.stateDir}/tokens/$level.token
            if [ ! -s "$f" ]; then
              (umask 0037; head -c 32 /dev/urandom | base64 | tr -d '=+/\n' > "$f")
            fi
          done
          ${cfg.package}/bin/sinexd admin schema --database ${escapeShellArg dbUrl}
        '';
        script = ''
          ${natsUrlScript { user = "sinexd"; passwordFile = "$CREDENTIALS_DIRECTORY/nats"; address = natsListen; }}
          exec ${cfg.package}/bin/sinexd run \
            --database ${escapeShellArg dbUrl} \
            --store ${escapeShellArg cfg.storeDir} \
            --nats "$nats_url" \
            --subjects ${escapeShellArg cfg.nats.subjects} \
            --host ${escapeShellArg cfg.host} \
            --socket ${escapeShellArg cfg.api.socket} \
            ${optionalString (cfg.api.listen != null) "--listen ${escapeShellArg cfg.api.listen}"} \
            --tokens ${cfg.stateDir}/tokens \
            ${optionalString (cfg.zone != null) "--zone ${escapeShellArg cfg.zone}"} \
            --day-start ${escapeShellArg cfg.dayStart} \
            --sources /etc/sinex/sources.json
        '';
      };

      systemd.services.sinex-capture-journald = mkIf cfg.journald.enable {
        description = "Sinex journald capture into its spool";
        after = [ "systemd-journald.service" ];
        wantedBy = [ "multi-user.target" ];
        unitConfig = startLimit;
        serviceConfig = restartPolicy // {
          User = cfg.user;
          Group = cfg.group;
          SupplementaryGroups = [ "systemd-journal" ];
        };
        path = [ config.systemd.package ];
        script = ''
          exec ${cfg.package}/bin/sinexd capture journald \
            --spool ${cfg.stateDir}/spool/journald --since=-1d --follow
        '';
      };

      # The session service runs in the operator's graphical session and reads
      # home-directory sources itself, so no ACLs on $HOME are needed.
      systemd.user.services.sinex-session = mkIf cfg.session.enable {
        description = "Sinex session sources";
        wantedBy = cfg.session.wantedBy;
        partOf = cfg.session.wantedBy;
        unitConfig = startLimit // { ConditionUser = cfg.session.user; };
        serviceConfig = restartPolicy;
        # busctl, wl-paste, grim, pw-record and ffmpeg run as capture children.
        path = with pkgs; [ systemd wl-clipboard grim pipewire ffmpeg-headless ];
        script = ''
          ${natsUrlScript { user = cfg.session.producer; passwordFile = cfg.session.passwordFile; address = natsListen; }}
          exec ${cfg.package}/bin/sinex-session run \
            --nats "$nats_url" \
            --subjects ${escapeShellArg cfg.nats.subjects} \
            --producer ${escapeShellArg cfg.session.producer} \
            --store ${escapeShellArg cfg.storeDir} \
            --state "$HOME/.local/state/sinex-session" \
            --host ${escapeShellArg cfg.host} \
            ${optionalString cfg.session.hyprland "--hyprland"} \
            ${optionalString cfg.session.notifications "--notifications"} \
            ${optionalString cfg.session.players "--players"} \
            ${optionalString cfg.session.clipboard "--clipboard"} \
            ${optionalString cfg.session.producers "--producers"} \
            ${optionalString (cfg.session.screenEvery != null) "--screen-every ${toString cfg.session.screenEvery}"} \
            ${optionalString (cfg.session.audioSegment != null) "--audio-segment ${toString cfg.session.audioSegment}"} \
            ${concatMapStringsSep " " (d: "--watch ${escapeShellArg d}") cfg.session.watch}
        '';
      };

      # Daily dump: everything except the sealed key table, which is dumped on
      # its own and backed up to a separate repository.
      systemd.services.sinex-dump = {
        description = "Sinex database dump";
        after = [ "postgresql.service" ];
        startAt = cfg.backup.startAt;
        serviceConfig = { Type = "oneshot"; User = cfg.user; Group = cfg.group; UMask = "0077"; };
        script = ''
          ${postgres}/bin/pg_dump -d sinex -Fc --exclude-table-data=privacy.material_keys \
            -f ${cfg.backup.dumpDir}/sinex.dump.tmp
          mv ${cfg.backup.dumpDir}/sinex.dump.tmp ${cfg.backup.dumpDir}/sinex.dump
          ${postgres}/bin/pg_dump -d sinex -Fc --data-only -t privacy.material_keys \
            -f ${cfg.backup.keysDir}/material-keys.dump.tmp
          mv ${cfg.backup.keysDir}/material-keys.dump.tmp ${cfg.backup.keysDir}/material-keys.dump
        '';
      };

      services.borgbackup.jobs = mkMerge [
        (mkIf (cfg.backup.repo != null) {
          sinex = {
            paths = [ cfg.storeDir cfg.backup.dumpDir ];
            repo = cfg.backup.repo;
            encryption = { mode = "repokey-blake2"; passCommand = cfg.backup.passCommand; };
            compression = "auto,zstd";
            startAt = cfg.backup.startAt;
            preHook = "systemctl start sinex-dump.service";
          };
        })
        (mkIf (cfg.backup.keysRepo != null) {
          sinex-keys = {
            paths = [ cfg.backup.keysDir ];
            repo = cfg.backup.keysRepo;
            encryption = { mode = "repokey-blake2"; passCommand = cfg.backup.passCommand; };
            startAt = cfg.backup.startAt;
            preHook = "systemctl start sinex-dump.service";
          };
        })
      ];
    })

    (mkIf node.enable {
      systemd.services.sinex-capture = {
        description = "Sinex capture-only node";
        after = [ "network-online.target" "remote-fs.target" ];
        wants = [ "network-online.target" ];
        wantedBy = [ "multi-user.target" ];
        unitConfig = startLimit;
        serviceConfig = restartPolicy // {
          StateDirectory = "sinex-capture";
          SupplementaryGroups = optional node.journald "systemd-journal";
        };
        path = [ config.systemd.package ];
        script = ''
          ${natsUrlScript { user = node.producer; passwordFile = node.passwordFile; address = node.nats; }}
          exec ${node.package}/bin/sinex-capture \
            --nats "$nats_url" \
            --subjects ${escapeShellArg node.subjects} \
            --producer ${escapeShellArg node.producer} \
            --store ${escapeShellArg node.store} \
            --state ${escapeShellArg node.stateDir} \
            --host ${escapeShellArg node.host} \
            ${optionalString node.journald "--journald"} \
            ${concatMapStringsSep " " (d: "--watch ${escapeShellArg d}") node.watch}
        '';
      };
    })
  ];
}
