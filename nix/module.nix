# The Sinex NixOS module. It deploys sinexd with its
# PostgreSQL and NATS, the session service, the journald capture, a
# capture-only node for satellite hosts, and the backups. Source and derivation
# options come from the registries through the generated `catalog.json`.
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
  # One private-mode switch for the daemon, the session service and the
  # operator's sinexctl; the operator reaches it through the sinex group.
  privateSwitch = "/run/sinex/private-mode";
  natsListen = "${cfg.nats.address}:${toString cfg.nats.port}";

  # Producers allowed onto the ingress: those declared, plus the session
  # service on this host.
  producers = cfg.nats.producers
    // lib.optionalAttrs cfg.session.enable {
      ${cfg.session.producer} = { passwordFile = cfg.session.passwordFile; };
    };

  # The NATS server and user, and the file the password is read from: a
  # password never appears in a process's arguments.
  natsArgs = { user, passwordFile, address }:
    "--nats ${escapeShellArg "nats://${user}@${address}"} --nats-password-file \"${passwordFile}\"";

  # A failing service is restarted three times in ten minutes, 30 s apart,
  # then left stopped.
  restartPolicy = {
    Restart = "on-failure";
    RestartSec = "30s";
  };
  startLimit = {
    StartLimitBurst = 3;
    StartLimitIntervalSec = 600;
  };

  sourceOption = spec: {
    enable = mkEnableOption "the ${spec.name} source (${concatStringsSep ", " spec.drivers}; for ${spec.purpose})";
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
    format = mkOption {
      type = types.nullOr types.str;
      default = null;
      description = "The format to read ${spec.name} with: one of ${concatStringsSep ", " spec.formats}; the first when null.";
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
        this host write their frame bytes under it through the store's group; frame
        files or directories with other ownership or modes are refused.
      '';
    };
    noCow = mkOption {
      type = types.bool;
      default = true;
      description = ''
        Turn copy-on-write off for the database directory on btrfs, where
        rewrites in place would fragment it; PostgreSQL checksums its own pages.
        The content store keeps copy-on-write and compression: its files are
        written once.
      '';
    };
    host = mkOption {
      type = types.str;
      default = config.networking.hostName;
      description = "This host's name, recorded with every input.";
    };
    readAnyFile = mkOption {
      type = types.bool;
      default = false;
      description = ''
        Let sinexd read every file on the host (CAP_DAC_READ_SEARCH), so
        sources in the operator's home and private directories need no
        permission changes. It can read them, never write them. Without it,
        every source path must be readable by the sinex user.
      '';
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
      subjectRoot = mkOption { type = types.str; default = "sinex"; description = "The token every NATS subject starts with."; };
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
      integrations = mkOption {
        type = types.bool;
        default = false;
        description = "Listen on the integration socket for the Neovim and kitty integrations.";
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

    backfills = mkOption {
      type = types.attrsOf (types.listOf types.str);
      default = { };
      example = { takeout-2025 = [ "takeout=/data/takeout-2025.tgz" ]; };
      description = ''
        One-shot imports of large exports, each a `sinex-backfill-NAME`
        service that is started by hand, never at activation. Each entry is a
        list of `format=path` inputs (`sinexd admin catalog` lists the formats).
        The database has one writer, so starting a backfill stops sinexd and
        starts it again when the backfill ends; live captures spool meanwhile.
        The inputs are mounted read-only for it.
      '';
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
    subjectRoot = mkOption { type = types.str; default = "sinex"; description = "The token every NATS subject starts with."; };
    producer = mkOption { type = types.str; default = "capture-${config.networking.hostName}"; };
    passwordFile = mkOption { type = types.path; };
    user = mkOption {
      type = types.str;
      default = "sinex-capture";
      description = "The account the capture runs as. It must be able to read the watched directories.";
    };
    group = mkOption {
      type = types.str;
      default = "sinex";
      description = ''
        The content store's group: the capture writes its frames' bytes through
        it. When the store is a network mount, the group's ID must match the
        store host's.
      '';
    };
    store = mkOption {
      type = types.path;
      description = "sinexd's content store as mounted here, or with `upload` this node's own store.";
    };
    upload = mkOption {
      type = types.bool;
      default = false;
      description = "Keep `store` on this node and send each material's bytes over the NATS link.";
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
      # sinexctl in a login shell talks to this daemon with the agent token
      # and flips the daemon's private-mode switch. The Unix socket is the
      # daemon user's own; other users reach the API over loopback HTTP.
      environment.sessionVariables = {
        SINEX_ENDPOINT = if cfg.api.listen != null then "http://${cfg.api.listen}" else "unix:${cfg.api.socket}";
        SINEX_TOKEN_FILE = "${cfg.stateDir}/tokens/agent.token";
        SINEX_PRIVATE_MODE = privateSwitch;
      };

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
        # The private-mode switch's directory exists before sinexd first runs,
        # so the session service can check it.
        "d /run/sinex 0770 ${cfg.user} ${cfg.group} -"
        # Group mode 2770 marks the store root as shared with producers on this
        # host; they write only under incoming/producers (frame directories).
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
          shared_buffers = lib.mkDefault "4GB";
          max_wal_size = lib.mkDefault "16GB";
          checkpoint_timeout = lib.mkDefault "30min";
          work_mem = lib.mkDefault "64MB";
          log_temp_files = lib.mkDefault "16MB";
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
      # by the sinex user at daemon start. Both come from sinex-schema.
      systemd.services.sinex-db-setup = {
        description = "Sinex database extensions and roles";
        after = [ "postgresql.service" "postgresql-setup.service" ];
        requires = [ "postgresql.service" ];
        wantedBy = [ "multi-user.target" ];
        # sinexd requires this unit, so restarting it on every package change
        # would restart sinexd too, which activation leaves to the operator.
        # Restart both by hand to apply new extensions or roles.
        restartIfChanged = false;
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
          User = "postgres";
        };
        script = ''
          exec ${cfg.package}/bin/sinexd admin prepare-database \
            --database ${escapeShellArg dbUrl} --owner ${escapeShellArg cfg.user}
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
              --subject-root ${escapeShellArg cfg.nats.subjectRoot} \
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
          # It holds the private-mode switch, which the operator sets through
          # the group and a backfill run while the daemon is stopped checks
          # too; a missing directory counts as on.
          RuntimeDirectoryMode = "0770";
          RuntimeDirectoryPreserve = "yes";
          LoadCredential = [ "nats:${cfg.nats.daemonPasswordFile}" ];
          UMask = "0007";
          # A schema upgrade can read every stored event; a start timeout
          # would roll it back and begin it again.
          TimeoutStartSec = "infinity";
        } // lib.optionalAttrs cfg.readAnyFile {
          AmbientCapabilities = [ "CAP_DAC_READ_SEARCH" ];
          CapabilityBoundingSet = [ "CAP_DAC_READ_SEARCH" ];
        };
        environment.SINEX_PRIVATE_MODE = privateSwitch;
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
          exec ${cfg.package}/bin/sinexd run \
            --database ${escapeShellArg dbUrl} \
            --store ${escapeShellArg cfg.storeDir} \
            ${natsArgs { user = "sinexd"; passwordFile = "$CREDENTIALS_DIRECTORY/nats"; address = natsListen; }} \
            --subject-root ${escapeShellArg cfg.nats.subjectRoot} \
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
          exec ${cfg.package}/bin/sinex-session capture journald \
            --spool ${cfg.stateDir}/spool/journald --option since=-1d --option follow=true
        '';
      };

      # The session service runs in the operator's graphical session for the
      # live sources that need it (Hyprland, D-Bus, the clipboard, file
      # changes) and produces them over the ingress. Files the sources read
      # (shell histories, databases, notes) are read by sinexd.
      systemd.user.services.sinex-session = mkIf cfg.session.enable {
        description = "Sinex session sources";
        wantedBy = cfg.session.wantedBy;
        partOf = cfg.session.wantedBy;
        unitConfig = { ConditionUser = cfg.session.user; StartLimitIntervalSec = 0; };
        serviceConfig = { Restart = "always"; RestartSec = "2s"; };
        environment.SINEX_PRIVATE_MODE = privateSwitch;
        # busctl, wl-paste, grim, pw-record and ffmpeg run as capture children.
        path = with pkgs; [ systemd wl-clipboard grim pipewire ffmpeg-headless ];
        script = ''
          exec ${cfg.package}/bin/sinex-session run \
            ${natsArgs { user = cfg.session.producer; passwordFile = cfg.session.passwordFile; address = natsListen; }} \
            --subject-root ${escapeShellArg cfg.nats.subjectRoot} \
            --producer ${escapeShellArg cfg.session.producer} \
            --store ${escapeShellArg cfg.storeDir} \
            --state "''${XDG_STATE_HOME:-$HOME/.local/state}/sinex-session" \
            --host ${escapeShellArg cfg.host} \
            ${optionalString cfg.session.hyprland "--hyprland"} \
            ${optionalString cfg.session.notifications "--notifications"} \
            ${optionalString cfg.session.players "--players"} \
            ${optionalString cfg.session.clipboard "--clipboard"} \
            ${optionalString cfg.session.integrations "--integrations"} \
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
          ${postgres}/bin/pg_dump -d sinex -Fc --exclude-table-data=privacy.data_keys \
            -f ${cfg.backup.dumpDir}/sinex.dump.tmp
          mv ${cfg.backup.dumpDir}/sinex.dump.tmp ${cfg.backup.dumpDir}/sinex.dump
          ${postgres}/bin/pg_dump -d sinex -Fc --data-only -t privacy.data_keys \
            -f ${cfg.backup.keysDir}/data-keys.dump.tmp
          mv ${cfg.backup.keysDir}/data-keys.dump.tmp ${cfg.backup.keysDir}/data-keys.dump
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

    # Declared backfills, apart from the attribute set above that names
    # services one by one.
    (mkIf cfg.enable {
      systemd.services = lib.mapAttrs'
        (name: inputs: lib.nameValuePair "sinex-backfill-${name}" {
          description = "Sinex backfill ${name}";
          after = [ "sinex-db-setup.service" "sinexd.service" ];
          requires = [ "sinex-db-setup.service" ];
          restartIfChanged = false;
          path = [ pkgs.tesseract pkgs.coreutils ];
          environment.SINEX_PRIVATE_MODE = privateSwitch;
          serviceConfig = {
            Type = "oneshot";
            User = cfg.user;
            Group = cfg.group;
            UMask = "0007";
            ReadOnlyPaths = map (input: lib.last (lib.splitString "=" input)) inputs;
            # The database has one writer: sinexd stops for the import and
            # starts after it however it ends, stopped by hand included. A
            # unit dependency (Conflicts) would cancel this start job when
            # sinexd starts again.
            ExecStartPre = "+${config.systemd.package}/bin/systemctl stop sinexd.service";
            ExecStopPost = "+${config.systemd.package}/bin/systemctl start --no-block sinexd.service";
          } // lib.optionalAttrs cfg.readAnyFile {
            AmbientCapabilities = [ "CAP_DAC_READ_SEARCH" ];
            CapabilityBoundingSet = [ "CAP_DAC_READ_SEARCH" ];
          };
          script = ''
            exec ${cfg.package}/bin/sinexd backfill \
              --database ${escapeShellArg dbUrl} \
              --store ${escapeShellArg cfg.storeDir} \
              --host ${escapeShellArg cfg.host} \
              ${optionalString (cfg.zone != null) "--zone ${escapeShellArg cfg.zone}"} \
              ${concatMapStringsSep " " (input: "--read ${escapeShellArg input}") inputs}
          '';
        })
        cfg.backfills;
    })

    (mkIf node.enable {
      users.users.${node.user} = {
        isSystemUser = true;
        group = node.group;
      };
      users.groups.${node.group} = { };
      systemd.services.sinex-capture = {
        description = "Sinex capture-only node";
        after = [ "network-online.target" "remote-fs.target" ];
        wants = [ "network-online.target" ];
        wantedBy = [ "multi-user.target" ];
        unitConfig = startLimit;
        serviceConfig = restartPolicy // {
          User = node.user;
          Group = node.group;
          UMask = "0007";
          StateDirectory = "sinex-capture";
          # The private-mode switch: a file here turns capture off. The
          # capture user cannot create the default /run/sinex, and a switch
          # it cannot check counts as on (the node logs why, once).
          RuntimeDirectory = "sinex-capture";
          RuntimeDirectoryPreserve = "yes";
          SupplementaryGroups = optional node.journald "systemd-journal";
        };
        environment.SINEX_PRIVATE_MODE = "/run/sinex-capture/private-mode";
        path = [ config.systemd.package ];
        script = ''
          exec ${node.package}/bin/sinex-capture \
            ${natsArgs { user = node.producer; passwordFile = node.passwordFile; address = node.nats; }} \
            --subject-root ${escapeShellArg node.subjectRoot} \
            --producer ${escapeShellArg node.producer} \
            --store ${escapeShellArg node.store} \
            --state ${escapeShellArg node.stateDir} \
            --host ${escapeShellArg node.host} \
            ${optionalString node.upload "--upload"} \
            ${optionalString node.journald "--journald"} \
            ${concatMapStringsSep " " (d: "--watch ${escapeShellArg d}") node.watch}
        '';
      };
    })
  ];
}
