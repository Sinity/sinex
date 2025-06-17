# Basic NixOS VM test for Sinex modular configuration
# Tests that the modular structure can boot and services start correctly

import <nixpkgs/nixos/tests/make-test-python.nix> ({ pkgs, ... }: {
  name = "sinex-basic";
  
  meta = with pkgs.lib.maintainers; {
    maintainers = [ ];
    description = "Basic functionality test for Sinex NixOS module";
  };

  nodes.machine = { config, pkgs, ... }: {
    imports = [
      ../modules  # Import our modular structure
    ];

    # Basic Sinex configuration with normal preset
    services.sinex = {
      enable = true;
      preset = "normal";
      targetUser = "testuser";
      
      database = {
        name = "sinex_test";
        autoSetup = true;
      };
      
      blobStorage = {
        enable = true;
        repositoryPath = "/tmp/sinex-annex";
        autoInit = true;
      };
      
      # Use minimal event sources for testing
      unifiedCollector.sources = {
        filesystem = {
          enable = true;
          watchPaths = [ "/tmp/test-watch" ];
          excludePatterns = [ "*.exclude-test" ];
        };
        clipboard.enable = false;  # Skip clipboard in VM
        dbus.enable = true;
        atuin.enable = false;  # Skip atuin in VM
        kittyScrollback.enable = false;  # Skip terminal capture
        asciinema.enable = false;
      };
    };

    # Create test user
    users.users.testuser = {
      isNormalUser = true;
      home = "/home/testuser";
      createHome = true;
    };

    # Minimal system configuration for VM
    boot.loader.grub.device = "/dev/vda";
    fileSystems."/" = {
      device = "/dev/vda1";
      fsType = "ext4";
    };
    
    # Enable git for git-annex functionality
    environment.systemPackages = with pkgs; [ git git-annex ];
  };

  testScript = ''
    start_all()
    
    # Wait for system to be ready
    machine.wait_for_unit("multi-user.target")
    
    # Test 1: Check that PostgreSQL started
    machine.wait_for_unit("postgresql.service")
    machine.succeed("systemctl is-active postgresql.service")
    
    # Test 2: Check that database was created
    machine.succeed("su - postgres -c 'psql -lqt' | grep -q sinex_test")
    
    # Test 3: Check that Sinex user was created
    machine.succeed("getent passwd sinex_test")
    machine.succeed("getent group sinex_test")
    
    # Test 4: Check that directories were created with correct permissions
    machine.succeed("test -d /var/lib/sinex")
    machine.succeed("test -d /var/log/sinex") 
    machine.succeed("test -d /var/cache/sinex")
    machine.succeed("test -d /run/sinex")
    
    # Test 5: Check directory ownership
    machine.succeed("stat -c '%U:%G' /var/lib/sinex | grep -q 'sinex_test:sinex_test'")
    machine.succeed("stat -c '%U:%G' /var/log/sinex | grep -q 'sinex_test:sinex_test'")
    
    # Test 6: Try to start Sinex unified collector (should not fail immediately)
    machine.succeed("systemctl start sinex-unified-collector.service")
    machine.sleep(5)  # Give it time to initialize
    
    # Test 7: Check if service is running or at least attempted to start
    # (might fail due to missing actual Sinex binaries, but systemd should try)
    result = machine.succeed("systemctl status sinex-unified-collector.service")
    print(f"Unified collector status: {result}")
    
    # Test 8: Check git-annex repository initialization
    machine.succeed("test -d /tmp/sinex-annex/.git")
    machine.succeed("test -d /tmp/sinex-annex/.git/annex")
    
    # Test 9: Create test file and verify filesystem monitoring setup
    machine.succeed("mkdir -p /tmp/test-watch")
    machine.succeed("touch /tmp/test-watch/test-file.txt")
    machine.succeed("touch /tmp/test-watch/should-exclude.exclude-test")
    
    # Test 10: Verify configuration was applied correctly
    # Check that the directories options are accessible
    machine.succeed("test -d /var/lib/sinex/monitoring")
    
    # Test 11: Check that cleanup timer was created
    machine.succeed("systemctl list-timers | grep -q sinex-cleanup")
    
    print("✅ All basic functionality tests passed!")
  '';
})