# VM configuration for snapshot-based testing
{ config, lib, pkgs, ... }:

{
  # Enable qcow2 disk format for snapshot support
  virtualisation = {
    # Use qcow2 format instead of raw disk image
    diskImage = "./${config.networking.hostName}.qcow2";
    
    # Additional QEMU options for snapshot management
    qemu.options = [
      "-enable-kvm"
      "-cpu host"
    ] ++ lib.optionals (config.virtualisation ? snapshotMode && config.virtualisation.snapshotMode) [
      # When running from snapshot, use copy-on-write
      "-snapshot"
    ] ++ lib.optionals (config.virtualisation ? baseSnapshot && config.virtualisation.baseSnapshot != null) [
      "-loadvm"
      config.virtualisation.baseSnapshot
    ];
    
    # Custom disk image creation with qcow2 format
    emptyDiskImages = lib.mkForce [ ];
    
    # Use a custom disk image builder
    diskImageSize = lib.mkDefault "4G";
  };
  
  # Options for snapshot management
  options.virtualisation = {
    snapshotMode = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Run VM in snapshot mode (changes are not persisted)";
    };
    
    baseSnapshot = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "Base snapshot name to restore from";
    };
  };
  
  # System configuration for faster snapshot operations
  boot = {
    # Reduce boot time
    loader.timeout = 0;
    
    # Minimal initrd for faster boot
    initrd = {
      # Only include necessary modules
      includeDefaultModules = false;
      availableKernelModules = [ "ahci" "xhci_pci" "virtio_pci" "sr_mod" "virtio_blk" ];
      kernelModules = [ ];
    };
    
    # Kernel parameters for faster boot
    kernelParams = [ 
      "quiet" 
      "loglevel=3" 
      "systemd.show_status=false"
      "rd.udev.log_level=3"
    ];
  };
  
  # Optimize systemd for faster startup
  systemd = {
    # Disable unnecessary services
    services = {
      "systemd-journal-catalog-update".enable = false;
      "systemd-update-done".enable = false;
    };
    
    # Speed up shutdown
    extraConfig = ''
      DefaultTimeoutStopSec=10s
      DefaultTimeoutStartSec=10s
    '';
  };
}
