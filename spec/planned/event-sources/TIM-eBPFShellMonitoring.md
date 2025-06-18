# TIM-eBPFShellMonitoring: Low-Level Shell/Terminal Monitoring via eBPF

*   **Relevant ADR:** (Part of ADR-008 as a specialized/future option)
*   **Original UG Context:** Section 8.3
*   **Security Warning:** eBPF programs for this purpose require significant privileges (`CAP_SYS_ADMIN` or `CAP_BPF`+`CAP_PERFMON`). The userspace agent collecting data also needs care. This is an advanced technique with security implications.

This TIM details the technical aspects of using eBPF (extended Berkeley Packet Filter) for low-level monitoring of shell and terminal activity by tracing system calls. This is considered a specialized or future enhancement, not part of the primary terminal capture strategy (ADR-008).

## 1. Rationale Summary

eBPF allows in-kernel execution of sandboxed programs, enabling efficient, low-overhead tracing of syscalls related to process execution (`execve`) and TTY I/O (`read`, `write`, `ioctl`). This can provide a very detailed, kernel-level view of terminal activity, complementing higher-level logging.

## 2. eBPF Program Types and Attachment [UG Sec 8.3.1, CR2]

*   **Key Program Types for Terminal Monitoring:**
    *   `BPF_PROG_TYPE_TRACEPOINT`: Attach to static kernel tracepoints (e.g., `syscalls:sys_enter_execve`, `sched:sched_process_exec`, `tty:tty_write`). Preferred for stability if suitable tracepoints exist.
    *   `BPF_PROG_TYPE_KPROBE`/`BPF_PROG_TYPE_KRETPROBE`: Attach to entry/exit of almost any kernel function (e.g., `do_execve`, TTY driver functions like `tty_perform_flush`). More flexible, but less stable across kernel versions.
*   **Loading eBPF Programs:** Typically done by a userspace control application (e.g., written in C/C++ using `libbpf`, or Go/Rust with eBPF libraries like `libbpf-rs`, `aya`).
*   **Data Transfer to Userspace:**
    *   **BPF Maps (e.g., `BPF_MAP_TYPE_PERF_EVENT_ARRAY`, `BPF_MAP_TYPE_HASH`):** eBPF programs write collected event data structures to these maps.
    *   **BPF Ring Buffer (`BPF_MAP_TYPE_RINGBUF` - Kernel 5.8+ recommended [CR2]):** More efficient, MPMC (multi-producer, multi-consumer) buffer for high-volume data transfer from kernel to userspace.
    *   The userspace application reads data from these maps/ring buffers.

## 3. Key Tracepoints and Syscalls for Monitoring [UG Sec 8.3.2, CR2]

### 3.1. Process Execution (Commands)

*   **Goal:** Capture commands executed by shells.
*   **Tracepoints:**
    *   `sched:sched_process_exec` (or `raw_syscalls:sys_enter_execve` / `raw_syscalls:sys_exit_execve`, or `syscalls:sys_enter_execve` / `syscalls:sys_exit_execve`).
    *   These tracepoints fire when a new program is executed via `execve` or `execveat`.
*   **Data to Collect in eBPF Program:**
    *   Timestamp (`bpf_ktime_get_ns()`).
    *   PID, PPID, UID, GID (`bpf_get_current_pid_tgid()`, `bpf_get_current_uid_gid()`).
    *   Command name (`bpf_get_current_comm()`).
    *   Arguments (`argv`) and environment variables (`envp`) can be read from syscall arguments (e.g., from `PT_REGS_PARM2_SYSCALL(ctx)` for `argv` in `sys_enter_execve`). Reading full `argv`/`envp` strings in kernel can be complex due to pointer chasing and size limits; often, only a few initial args or specific env vars are captured.
    *   Return value (on `sys_exit_execve`) to check for success/failure.
*   **Userspace Correlation:** The userspace agent links `execve` events to parent shell PIDs to identify commands run within specific terminal sessions. It can also resolve the full path of the executable from `/proc/[pid]/exe`.

### 3.2. TTY I/O (Input/Output within Terminal)

*   **Goal:** Capture text written to or read from a terminal.
*   **Tracepoints/Kprobes:**
    *   `tty:tty_write`, `tty:tty_read` (if kernel has these tracepoints and they provide useful data).
    *   Kprobes on kernel functions like `tty_write()`, `n_tty_receive_buf_common()`.
    *   Syscalls: `read (0)`, `write (1)`. Attach to `sys_enter_read/write` and `sys_exit_read/write`.
*   **Data to Collect in eBPF Program:**
    *   PID, UID, GID.
    *   File descriptor (FD) number.
    *   Buffer content (data being read/written). `bpf_probe_read_user_str()` or `bpf_probe_read()` to copy data from user-space buffers passed to syscalls. Subject to size limits (e.g., only capture first N bytes).
    *   Return value (bytes read/written).
*   **Userspace Filtering:** The userspace agent must filter these `read`/`write` events based on whether the FD corresponds to a TTY or PTY associated with a user terminal session. This requires the agent to:
    *   Track `open` syscalls for `/dev/ptmx` (PTY master allocation) or `/dev/tty*`.
    *   Inspect `/proc/[shell_pid]/fd/` to identify which FDs are connected to TTYs/PTYs.

### 3.3. PTY Control `ioctl`s [UG Sec 8.3.3, CR2]

*   **Goal:** Monitor terminal state changes.
*   **Syscall:** `ioctl (16)`. Attach eBPF program to `sys_enter_ioctl` / `sys_exit_ioctl`.
*   **Key `ioctl` Commands & Data to Capture:**
    *   `TIOCGWINSZ` (Get Window Size): Capture `struct winsize` argument (rows, cols).
    *   `TIOCSWINSZ` (Set Window Size): Capture `struct winsize`.
    *   `TIOCGPGRP` (Get Process Group ID): Captures foreground process group of TTY.
*   **Userspace Filtering:** Filter `ioctl` events by FD to target TTYs/PTYs.

## 4. Performance Overhead and Privilege Requirements [UG Sec 8.3.4, CR2]

*   **Performance Overhead:**
    *   Tracepoints: ~50-200 ns per event.
    *   Kprobes: ~100-500 ns per event.
    *   BPF Ring Buffer ops: ~20-50 ns.
    *   Overall impact is generally low if eBPF programs are efficient (minimal in-kernel work, careful filtering, batch data to userspace). Can become significant for very high-frequency syscalls if not filtered aggressively in kernel.
*   **Privilege Requirements:**
    *   Loading eBPF programs: Requires `CAP_SYS_ADMIN`. On newer kernels (Linux 5.8+), some operations might be possible with `CAP_BPF` + `CAP_PERFMON`, but `CAP_SYS_ADMIN` is often practically needed for full access to tracepoints/kprobes.
    *   The userspace agent controlling eBPF programs and reading data typically runs as root or with these capabilities. **This agent must be minimal and heavily sandboxed if possible.** Data should be passed via secure IPC to an unprivileged Exocortex processor agent.

## 5. Minimum Kernel Version Recommendations [UG Sec 8.3.5, CR2]

*   **BPF Ring Buffer:** Linux Kernel 5.8+ for `BPF_MAP_TYPE_RINGBUF` (more efficient than perf event arrays).
*   **CO-RE (Compile Once - Run Everywhere):** Kernels with BTF (BPF Type Format) support (common in modern distributions) allow writing eBPF programs portable across kernel versions without recompiling against specific kernel headers. Requires `libbpf` and Clang/LLVM for compilation.

## 6. Example eBPF Tooling for Development

*   **BCC (BPF Compiler Collection):** Python/Lua frontends for writing eBPF programs. Good for rapid prototyping and exploration.
*   **`libbpf-bootstrap` / `libbpf-rs` / `aya` (Rust):** Frameworks for writing CO-RE eBPF programs in C (with `libbpf`) or Rust. Preferred for production eBPF agents due to lower overhead and better deployment.

## 7. Exocortex Integration

*   A dedicated `agent/ebpf_terminal_monitor` (e.g., Rust using `libbpf-rs` or `aya`) would load the eBPF programs and run the userspace control loop.
*   This agent would:
    1.  Identify target shell PIDs and their TTY/PTY file descriptors.
    2.  Filter and correlate eBPF events (execs, TTY I/O, ioctls) related to these sessions.
    3.  Structure the data into appropriate `raw.events` payloads (e.g., `system.ebpf.process_exec_in_tty`, `system.ebpf.tty_output_captured`).
    4.  Send these events to the Exocortex `raw.events` table.
*   This provides a very detailed, low-level stream that complements Atuin, Asciinema, and Kitty RC data.

