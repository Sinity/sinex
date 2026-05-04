# Resource Scoping

## cgroup v2 Resource Scoping Mechanism

Sinex services run under systemd, which uses Linux cgroup v2 (unified hierarchy)
for resource control. Each systemd service gets its own cgroup, and NixOS module
options set per-service `MemoryMax`, `CPUQuota`, and `LimitNOFILE` via the
`mkBaseServiceConfig` helper in `nixos/modules/node-services.nix`.

### Per-Service Resource Limits

Every long-running Sinex service (ingestd, gateway, all ingestors, all automata)
inherits from `mkBaseServiceConfig`, which applies:

- `MemoryMax` — hard memory cap; the OOM killer fires when exceeded
- `CPUQuota` — CPU bandwidth limit as a percentage value (e.g. `200%` = 2 cores)
- `TimeoutStopSec` — graceful shutdown window before SIGKILL
- `LimitNOFILE` — file descriptor cap (set when the resource profile requests it)

These are configured through the NixOS module options at
`services.sinex.nodes.defaults.resources` (global defaults) and per-node
`services.sinex.nodes.<name>.resources` overrides.

### Runtime Resource Profiles

Resource profiles are defined in the NixOS module as:

```
services.sinex.nodes.defaults.resources = {
  memoryMax = "512M";
  cpuQuota = "200%";
  shutdownTimeoutSec = 30;
  openFilesLimit = 16384;
};
```

### Checking Resource Limits at Runtime

- `systemctl show <service> -p MemoryCurrent,MemoryMax,CPUUsageNSec` — live usage
- `systemd-cgtop` — interactive cgroup resource monitor
- `/sys/fs/cgroup/system.slice/<service>.service/` — raw cgroup v2 stats
- `xtask status` — workspace-level health summary (does not check cgroup limits)

### xtask devshell Resource Limits

The `xtask` binary itself runs within whatever cgroup the calling shell provides.
For heavy development commands (e.g. `xtask check --full`, `xtask test --heavy`),
the project `direnv` and `nix develop` environments do not currently impose
additional cgroup limits — the development shell inherits the user session cgroup.

When running heavy operations, use the `sinnix-scope` wrapper (available on
`sinnix-prime`) to route long-running commands into a resource-bounded slice:

```bash
sinnix-scope build -- xtask check --full
sinnix-scope background -- xtask test --heavy
```

These slices are configured in the NixOS system configuration (`sinnix` repo,
not `sinex`), with CPU and memory limits appropriate for background build work.

### cgroup v2 Availability Check

The `xtask doctor --runtime` command currently checks NATS connectivity,
PostgreSQL health, consumer lag, and batch latency, but does **not** verify
cgroup v2 availability or that the running services are actually constrained
by the configured resource limits. This is a known gap — a future `xtask doctor
--runtime` enhancement should:

1. Check that `/sys/fs/cgroup` is mounted as cgroup2
2. Verify each running Sinex service has `MemoryMax` set in its cgroup
3. Warn if any service is near its memory limit

### Pressure Stall Information (PSI)

Linux PSI metrics (`/proc/pressure/io`, `/proc/pressure/memory`) are exposed
through the `sinex_node_sdk::PressureMonitor` type. CAS write paths can check
pressure before large I/O operations and apply bounded backoff when the system
is under resource contention. See `crate/lib/sinex-node-sdk/src/pressure.rs`.
