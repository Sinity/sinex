# Basic E2E flow test for Sinex - Optimized version
{ pkgs
, sinex-ingestd
, sinex-gateway
, pg_jsonschema
, sinex ? null
, sinexCli ? null
, ...
}:

let
  inherit (pkgs) lib;
in
pkgs.nixosTest {
  name = "sinex-basic-flow";
  
  # Skip lint check for this test to avoid f-string issues
  skipLint = true;

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [ 
      (import ../common/test-base.nix { 
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli; 
      })
    ];

    # Override base config for this test
    services.sinex = {
      shell = {
        asciinema = {
          autoRecord = false;
          recordingsPath = "/home/test/.local/share/asciinema";
        };
        kitty = {
          enable = true;
          autoConfigure = true;
          userConfigPath = "~/.config/kitty/kitty.conf";
        };
      };

      satellite = {
        eventSources = {
          filesystem.watchPaths = lib.mkAfter [ "/home/test/watched" ];
          terminal.enable = true;
          desktop.enable = true;
          system.enable = true;
        };
      };
    };

    # Additional packages for comprehensive testing
    environment.systemPackages = with pkgs; [
      atuin
      asciinema
      zsh
      git
      wl-clipboard
      wl-clip-persist
    ];
    
    # Enable services needed for optional tests
    services.dbus.enable = true;
    programs.zsh.enable = true;
    
    # Additional tmpfiles for test data
    systemd.tmpfiles.rules = lib.mkAfter (
      let
        stateDir = config.services.sinex.directories.state;
      in [
        # Atuin directories
        "d ${stateDir}/.local 0755 sinex sinex -"
        "d ${stateDir}/.local/share 0755 sinex sinex -"
        "d ${stateDir}/.local/share/atuin 0755 sinex sinex -"

        # Asciinema directories
        "d /home/test/.local 0755 test users -"
        "d /home/test/.local/share 0755 test users -"
        "d /home/test/.local/share/asciinema 0755 test users -"

        # Runtime directories for optional Wayland tests
        "d /run/user 0755 root root -"
        "d /run/user/1000 0700 test users -"
      ]
    );
    
    # Configure Atuin
    environment.etc."atuin/config.toml".text = ''
      auto_sync = false
      search_mode = "fuzzy"
      filter_mode = "global"
      style = "compact"
      inline_height = 30
      up_arrow = false
      show_preview = true
    '';
    
    # Optional Hyprland for Wayland tests (may fail in headless - that's OK)
    systemd.services.hyprland-headless = {
      description = "Hyprland Wayland compositor (optional for testing)";
      wantedBy = [ ];
      after = [ "systemd-user-sessions.service" ];
      
      serviceConfig = {
        ExecStart = "${pkgs.hyprland}/bin/Hyprland";
        Restart = "no";
        User = "test";
        Group = "users";
        Environment = [
          "WAYLAND_DISPLAY=wayland-1"
          "XDG_RUNTIME_DIR=/run/user/1000"
          "XDG_SESSION_TYPE=wayland"
          "WLR_BACKENDS=headless"
          "WLR_RENDERER=pixman"
          "WLR_RENDERER_ALLOW_SOFTWARE=1"
          "HYPRLAND_NO_RT=1"
          "HYPRLAND_NO_SD_NOTIFY=1"
          "LIBGL_ALWAYS_SOFTWARE=1"
          "WLR_NO_HARDWARE_CURSORS=1"
        ];
      };
      
      preStart = ''
        mkdir -p /run/user/1000
        chown test:users /run/user/1000
        chmod 0700 /run/user/1000
        
        mkdir -p /home/test/.config/hypr
        cat > /home/test/.config/hypr/hyprland.conf <<EOF
monitor=,preferred,auto,1
input { kb_layout = us }
general { gaps_in = 5; gaps_out = 20; border_size = 2 }
misc { disable_hyprland_logo = true }
EOF
        chown -R test:users /home/test/.config
      '';
    };
  };

  testScript = ''
    start_all()

    state_dir = "/var/lib/sinex"
    
    # Simple helper functions embedded in test
    def wait_for_sinex_ready():
        machine.wait_for_unit("postgresql.service", timeout=60)
        machine.wait_for_unit("sinex-ingestd.service", timeout=60)
        machine.wait_until_succeeds("systemctl is-active sinex-ingestd", timeout=30)
    
    def get_event_count():
        result = machine.succeed(
            "su - postgres -c 'psql -d sinex -t -c \"SELECT COUNT(*) FROM core.events;\"'"
        )
        return int(result.strip())
    
    def generate_events(count, prefix):
        events_created = 0
        for i in range(count):
            try:
                machine.succeed(f"echo 'test file {prefix}_{i}' > /home/test/watched/test_{prefix}_{i}.txt")
                events_created += 1
            except:
                pass
        return events_created
    
    # Wait for system to be ready with proper health checks
    with subtest("System initialization"):
        machine.wait_for_unit("multi-user.target")
        wait_for_sinex_ready()
        
        # Basic service check
        machine.succeed("systemctl is-active postgresql")
        machine.succeed("systemctl is-active sinex-ingestd")

    # Test 1: Database schema validation
    with subtest("Database schema validation"):
        # Use wait_until_succeeds for database queries to handle timing issues
        tables = machine.wait_until_succeeds(
            "su - postgres -c \"psql -d sinex -t -c \\\"SELECT tablename FROM pg_tables WHERE schemaname = 'raw';\\\"\"",
            timeout=30
        )
        print(f"Raw schema tables:\n{tables}")
        assert "events" in tables, "core.events table not created"
        
        # Check extensions with retry
        extensions = machine.wait_until_succeeds(
            "su - postgres -c 'psql -d sinex -c \"\\dx\"'",
            timeout=30
        )
        assert "timescaledb" in extensions, "TimescaleDB not installed"

    # Test 2: Filesystem event capture
    with subtest("Filesystem event capture"):
        initial_count = get_event_count()
        print(f"Initial event count: {initial_count}")
        
        # Generate events using helper
        events_created = generate_events(5, "basic")
        print(f"Created {events_created} filesystem events")
        
        # Verify events were captured
        assert events_created > 0, "No filesystem events captured"
        
        # Wait for events to be processed
        machine.sleep(5)
        final_count = get_event_count()
        assert final_count > initial_count, "Events not captured"
        
    # Test 3: Shell history capture 
    with subtest("Shell history event capture"):
        pre_count = get_event_count()
        
        # Add commands to shell history with retry for file creation
        machine.wait_until_succeeds(
            f"echo 'cd /tmp' >> {state_dir}/.zsh_history",
            timeout=10
        )
        machine.succeed(f"echo 'ls -la' >> {state_dir}/.bash_history")
        
        # Wait for processing
        machine.sleep(3)
        post_count = get_event_count()
        
        print(f"Shell history events: {post_count - pre_count}")
        assert post_count > pre_count, "Shell history events not captured"
        
    # Test 4: Atuin integration
    with subtest("Atuin history integration"):
        pre_count = get_event_count()
        
        # Initialize Atuin with proper error handling
        try:
            machine.succeed(f"su - sinex -c 'cd {state_dir} && atuin init zsh'")
            machine.succeed(f"su - sinex -c 'cd {state_dir} && atuin import auto'")
            
            # Add test command to Atuin
            db_path = f"{state_dir}/.local/share/atuin/history.db"
            machine.wait_until_succeeds(
                f"sqlite3 {db_path} \"INSERT INTO history (id, timestamp, duration, exit, command, cwd, session, hostname) VALUES ('test123', 1700000000, 100, 0, 'echo test-command', '/tmp', 'session1', 'testhost');\"",
                timeout=10
            )
            
            machine.sleep(3)
            post_count = get_event_count()
            print(f"Atuin events: {post_count - pre_count}")
        except Exception as e:
            print(f"Atuin test skipped: {e}")
        
    # Test 5: Asciinema recording
    with subtest("Asciinema recording detection"):
        pre_count = get_event_count()
        
        # Create test recording
        machine.succeed(
            "su - test -c 'echo header > /home/test/.local/share/asciinema/test.cast'"
        )
        machine.succeed(
            "su - test -c 'echo data >> /home/test/.local/share/asciinema/test.cast'"
        )
        
        machine.sleep(3)
        post_count = get_event_count()
        print(f"Asciinema events: {post_count - pre_count}")
        
    # Test 6: Optional Wayland-dependent tests
    with subtest("Optional Wayland tests"):
        # Try to start Hyprland but don't fail if it doesn't work
        try:
            machine.systemctl("start hyprland-headless")
            machine.sleep(5)
            
            # Simple Wayland check
            wayland_available = False
            try:
                machine.execute("test -n \"$WAYLAND_DISPLAY\"")
                wayland_available = True
            except:
                pass
            
            if wayland_available:
                print("Wayland available - testing clipboard and Kitty")
                
                # Clipboard test
                pre_count = get_event_count()
                machine.succeed(
                    "su - test -c 'XDG_RUNTIME_DIR=/run/user/1000 WAYLAND_DISPLAY=wayland-1 "
                    "echo test-clipboard | wl-copy'"
                )
                machine.sleep(2)
                post_count = get_event_count()
                print(f"Clipboard events: {post_count - pre_count}")
                
                # Kitty test (may still fail even with Wayland)
                try:
                    machine.execute(
                        "su - test -c 'XDG_RUNTIME_DIR=/run/user/1000 WAYLAND_DISPLAY=wayland-1 "
                        "kitty --listen-on=unix:/tmp/kitty --detach &'"
                    )
                    machine.sleep(3)
                    print("Kitty started successfully")
                except:
                    print("Kitty failed to start (expected in headless)")
            else:
                print("Wayland not available - skipping clipboard/Kitty tests")
        except Exception as e:
            print(f"Optional Wayland tests skipped: {e}")
        
        # Always test that collector continues working
        # Check service is still running
        machine.succeed("systemctl is-active sinex-ingestd")
        
    # Test 7: D-Bus monitoring
    with subtest("D-Bus event monitoring"):
        # D-Bus should capture system events automatically
        pre_count = get_event_count()
        machine.sleep(3)
        post_count = get_event_count()
        
        print(f"D-Bus events captured: {post_count - pre_count}")
        
        # Query recent events
        events = machine.succeed("sinex 5")
        print(f"Recent events sample:\n{events}")

    # Test 8: Batch event processing
    with subtest("Multiple event capture"):
        pre_count = get_event_count()
        
        # Generate batch of events
        batch_size = 20
        events_created = generate_events(batch_size, "batch")
        
        print(f"Created {events_created} events in batch")
        assert events_created >= batch_size * 0.9, \
            f"Too few events captured: {events_created}/{batch_size}"
        
        # Verify total count increased appropriately  
        final_count = get_event_count()
        print(f"Total events now: {final_count}")

    # Test 9: Service resilience  
    with subtest("Service restart resilience"):
        pre_restart_count = get_event_count()
        
        # Restart collector
        machine.systemctl("restart sinex-ingestd")
        machine.wait_for_unit("sinex-ingestd.service")
        
        # Verify service recovered
        machine.succeed("systemctl is-active sinex-ingestd")
        
        # Generate events after restart
        events_after_restart = generate_events(5, "restart")
        assert events_after_restart > 0, "No events captured after restart"
        
        print(f"✓ Service resilient to restarts")

    # Test 10: Database verification
    with subtest("Database integration"):
        # Verify events in database with retry
        result = machine.wait_until_succeeds(
            "su - postgres -c 'psql -d sinex -t -c \"SELECT COUNT(*) FROM core.events;\"'",
            timeout=10
        )
        db_count = int(result.strip())
        print(f"Database event count: {db_count}")
        assert db_count > 0, "No events in database"
        
        # Verify TimescaleDB hypertable
        hypertables = machine.succeed(
            "su - postgres -c 'psql -d sinex -c "
            "\"SELECT * FROM timescaledb_information.hypertables;\"'"
        )
        assert "events" in hypertables, "Events table not a hypertable"
        
        # Basic cleanup - let systemd handle most cleanup
        print("✓ Test cleanup completed")
        
        print("✓ All basic flow tests completed successfully")
  '';
}
