---
created: "2026-06-29T22:04:00+02:00"
purpose: "RAM/IO pressure investigation notes for Sinnix/Sinex devloop"
status: "active"
project: "sinex/sinnix/lynchpin"
---

# RAM/IO Pressure Investigation

## Current Findings

- Live global memory at 2026-06-29 21:58 CEST was not under memory pressure:
  about 31 GiB total, 15 GiB used, 16 GiB available, 515 MiB swap used, memory
  PSI avg10/60/300 all zero.
- IO PSI remained nonzero around the same period, so input lag is more likely
  tied to IO contention and prior swap/cache churn than current anonymous RAM
  exhaustion.
- The current full Codex agent cgroup looked huge by `memory.current` (~9-10
  GiB), but cgroup `memory.stat` decomposed that into ~1.6-1.7 GiB anonymous
  memory plus ~7.3-8.9 GiB file cache.
- This file cache is clean and mostly buffered IO page cache, not mapped shared
  libraries: `file_mapped` was only ~81 MiB, `file_dirty` ~0.6 MiB, writeback 0.
- `fincore` identified `/realm/db/machine-telemetry/telemetry.sqlite` as a major
  resident file (~2.6 GiB resident out of a 5.8 GiB DB) after the analysis
  probes. Codex state/log SQLite files were much smaller residents by comparison.
- Process IO telemetry around 2026-06-29T20:01Z showed the investigation itself
  reading multiple GiB from machine telemetry SQLite. The observer can perturb
  cgroup page-cache accounting, so the tooling must distinguish anon vs file
  cache before attributing "memory use" to an agent.
- Borg jobs are real IO/memory contributors: recent `borgbackup-job-realm` ran
  ~5m18s, peaked at 1.9 GiB memory plus 516 MiB swap, and did ~6 GiB read / 2.1
  GiB written. `borgbackup-job-persist` similarly peaked at 1.9 GiB and did ~3.3
  GiB read / 1.6 GiB written.

## Instrumentation Change In Progress

In `/realm/project/sinnix/pkgs/machine-telemetry/collector.py`:

- Bumped schema version to 4.
- Added cgroup `memory.stat` capture for service rows:
  `memory_anon_bytes`, `memory_file_bytes`, `memory_kernel_bytes`,
  `memory_slab_bytes`, `memory_sock_bytes`, `memory_shmem_bytes`,
  `memory_swapcached_bytes`, `memory_zswap_bytes`, `memory_zswapped_bytes`.
- Added bounded `command_line` to top process IO delta rows, so future rows can
  identify expensive probes like `sqlite3 ...` instead of only `comm=sqlite3`.
- Verified with `python3 -m py_compile` and a short temp-DB collector loop:
  service rows included anon/file/kernel split; process IO rows included command
  lines.

## Interpretation

The phrase "file cache" was too broad. In this incident it mainly means clean
page-cache pages charged to the agent cgroup by large SQLite scans. It is not a
heap leak and not dirty writeback, but it can make `memory.current` look huge and
can drive reclaim/refault noise if repeated while other IO-heavy timers run.

## Next Work

- Deploy the collector change through Sinnix switch so new rows carry the split.
- Add/adjust Lynchpin/Sinnix reports to prefer anon/file/swap decomposition over
  raw `memory_current_bytes` when explaining workload memory.
- Avoid full-table SQLite introspection during pressure debugging; use bounded
  time windows and indexed predicates.
- Continue analyzing timer overlap and IO containment for Borg/btrbk/syslog vs
  agent experiments without disabling the workload.

## 2026-06-29 Live Deployment

- Sinnix switch completed successfully after removing the swap-free-percent
  rebuild preflight gate. The guard now keys on reclaim-aware `MemAvailable`
  only.
- `machine-telemetry.service` restarted on the new collector and inserted
  schema-v4 rows.
- New `metric_sample` rows populate global memory decomposition:
  `mem_total_mb`, `mem_used_mb`, `mem_avail_mb`, `mem_anon_mb`,
  `mem_file_cache_mb`, `mem_slab_reclaimable_mb`, `mem_dirty_mb`,
  `mem_writeback_mb`, and `swap_used_mb`.
- New `service_state` rows populate cgroup memory split fields. Example for
  `machine-telemetry.service`: `memory_current_bytes=64466944`,
  `memory_anon_bytes=34033664`, `memory_file_bytes=25055232`,
  `memory_kernel_bytes=2265088`.
- New `process_io_delta_sample` rows include bounded `command_line`, enabling
  later attribution of sqlite/chrome/agent/borg I/O bursts without relying on
  PID plus `comm` only.
- Noctalia source inspection showed `ram_used` and `ram_pct` both derive from
  `MemTotal - MemAvailable`, not raw `MemFree`. The durable Sinnix activation
  reconciliation now keeps `[widget.sysmon].stat = "ram_pct"` so the bar shows
  reclaim-aware pressure percent instead of a scary GiB-used label.
