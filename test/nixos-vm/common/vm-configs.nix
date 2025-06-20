# Predefined VM configurations for different test scenarios
{ lib, ... }:

{
  # Minimal VM for quick smoke tests
  minimal = {
    virtualisation = {
      memorySize = 1024;
      diskSize = 2048;
      cores = 1;
      # Disable unnecessary features
      graphics = false;
      writableStoreUseTmpfs = false;
    };
    
    # Aggressive service optimization
    services.journald.extraConfig = ''
      Storage=volatile
      RuntimeMaxUse=64M
    '';
  };
  
  # Standard VM for most tests
  standard = {
    virtualisation = {
      memorySize = 2048;
      diskSize = 4096;
      cores = 2;
      graphics = false;
      writableStoreUseTmpfs = false;
      # Better 9p performance
      qemu.options = [
        "-enable-kvm"
        "-cpu host"
      ];
    };
  };
  
  # Performance testing VM
  performance = {
    virtualisation = {
      memorySize = 4096;
      diskSize = 8192;
      cores = 4;
      graphics = false;
      writableStoreUseTmpfs = false;
      qemu.options = [
        "-enable-kvm"
        "-cpu host"
        "-smp 4"
      ];
    };
    
    # Tune kernel for performance
    boot.kernel.sysctl = {
      "vm.swappiness" = 10;
      "vm.dirty_ratio" = 15;
      "vm.dirty_background_ratio" = 5;
    };
  };
  
  # Large VM for stress/chaos testing
  large = {
    virtualisation = {
      memorySize = 8192;
      diskSize = 16384;
      cores = 8;
      graphics = false;
      writableStoreUseTmpfs = false;
      qemu.options = [
        "-enable-kvm"
        "-cpu host"
        "-smp 8"
      ];
    };
    
    boot.kernel.sysctl = {
      "vm.swappiness" = 10;
      "vm.dirty_ratio" = 20;
      "vm.dirty_background_ratio" = 10;
      "fs.file-max" = 1000000;
      "kernel.pid_max" = 4194304;
    };
  };
}
