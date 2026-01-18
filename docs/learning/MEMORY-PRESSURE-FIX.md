# Fixing Cargo/Rustc Memory Pressure

## Diagnosis

Your system:
- 32GB RAM, 24 cores
- swappiness=10 (very reluctant to swap)
- Cargo spawns 24 parallel rustc processes
- Each rustc for test binaries uses 800MB-2.1GB
- Total: ~36GB needed, but only 32GB available

**Result**: System hits RAM ceiling, reluctantly swaps, then immediately tries to swap back → thrashing.

---

## Immediate Fixes (pick one or combine)

### Option 1: Limit Parallelism (Recommended)

Add to `.cargo/config.toml`:

```toml
[build]
jobs = 8  # Or RAM_GB / 2
```

Or set environment variable:
```bash
export CARGO_BUILD_JOBS=8
```

**Why 8?** With ~2GB per rustc, 8 processes = 16GB, leaving headroom for OS/services.

### Option 2: Increase Swappiness

```bash
# Temporary (survives until reboot)
sudo sysctl vm.swappiness=60

# Permanent (NixOS)
# Add to configuration.nix:
boot.kernel.sysctl."vm.swappiness" = 60;
```

Higher swappiness lets the kernel proactively move cold pages to swap *before* hitting the wall, preventing thrashing.

### Option 3: Reduce Codegen Units for Debug

Your `.cargo/config.toml` or `Cargo.toml` profile has `codegen-units = 256`. For dev:

```toml
[profile.dev]
codegen-units = 16  # Fewer parallel threads per rustc
```

This reduces per-rustc memory usage at the cost of some parallelism.

---

## Quick Test

Kill current builds and try:

```bash
CARGO_BUILD_JOBS=6 cargo build --workspace 2>&1 | tee compilation.log
```

Watch memory with `watch -n1 free -h` in another terminal.

---

## Long-term: Cargo Config for 32GB Systems

```toml
# .cargo/config.toml

[build]
jobs = 8                    # Limit parallelism to RAM / 4GB
pipelining = true

[profile.dev]
codegen-units = 16          # Reduce from 256
split-debuginfo = "unpacked"
incremental = true

[profile.dev.package."*"]
codegen-units = 8           # Even fewer for dependencies
```

---

## Why swappiness=10 Hurts

Low swappiness means:
1. System uses all 32GB RAM
2. One more allocation → OOM pressure
3. Kernel *must* swap, but reluctantly
4. Swapped pages immediately needed again → swap back in
5. Thrashing: constant swap I/O, processes stall waiting for pages

With swappiness=60:
1. System proactively moves cold pages (old incremental cache, etc.) to swap
2. Hot pages stay in RAM
3. No sudden wall-hitting
4. Swap is used as "cold storage" not "emergency overflow"

---

## Check Current Pressure

```bash
# Memory pressure (should be low)
cat /proc/pressure/memory

# IO pressure (should be low)
cat /proc/pressure/io

# If "full avg10" > 5%, you're thrashing
```

---

## NixOS Permanent Fix

If using NixOS, add to your configuration:

```nix
{
  boot.kernel.sysctl = {
    "vm.swappiness" = 60;
    "vm.vfs_cache_pressure" = 50;  # Keep inode/dentry caches longer
  };

  # Optional: zram for compressed RAM swap (faster than SSD swap)
  zramSwap = {
    enable = true;
    memoryPercent = 50;  # Use up to 50% of RAM as compressed swap
  };
}
```

zram is particularly good for compilation - many pages compress well (debug info, repeated strings).
