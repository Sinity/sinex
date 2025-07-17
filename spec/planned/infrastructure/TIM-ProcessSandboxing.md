# TIM-ProcessSandboxing: `seccomp-bpf` and AppArmor

*   **Relevant ADR:** (N/A directly, core security hardening)
*   **Original UG Context:** Section 22.2

This TIM details process sandboxing techniques using Systemd's `SystemCallFilter` (seccomp-bpf) and AppArmor on NixOS to harden Exocortex agents and services.

## 1. Rationale Summary

Sandboxing limits the potential damage a compromised or buggy Exocortex process can inflict by restricting its access to system calls (seccomp) and resources like files/network (AppArmor). This is a key defense-in-depth measure.

## 2. Systemd `SystemCallFilter` (`seccomp-bpf`) [UG Sec 22.2.1, OR3, CR3]

Restricts allowed Linux system calls for a service process.

### 2.1. Configuration in NixOS Service Definition

In the `serviceConfig` block of a systemd service unit defined in NixOS:
```nix
# systemd.services.my_sinex_agent = {
//   serviceConfig = {
//     # ... other settings ...
//     NoNewPrivileges = true;  // ESSENTIAL: Must be true for seccomp filter to be effective.

//     # Option 1: Use Systemd's pre-defined syscall sets (recommended for simplicity if they fit)
//     # SystemCallFilter = [ "@system-service" ]; // Broad set for typical daemons
//     # SystemCallFilter = [ "@basic-io" "@file-system" "@network-io" "@process" ]; // Combine sets

//     # Option 2: Explicit whitelist (most secure, harder to maintain - needs syscall profiling)
//     # Example for a simple agent that reads/writes files and connects to PostgreSQL via local socket:
//     SystemCallFilter = [
//       "read" "write" "openat" "close" "fstat" "lseek" "mmap" "munmap"
//       "socket" "connect" "sendto" "recvfrom" "epoll_create1" "epoll_ctl" "epoll_wait"
//       "futex" "rt_sigaction" "rt_sigprocmask" "ioctl" "fcntl" "access" "statx"
//       "getrandom" "brk" "arch_prctl" "set_robust_list" "rseq"
//       "exit_group" "writev" "readv" "getsockname" "getpid" "gettid" "tgkill"
//       # Add more based on strace -c -f output for the specific agent
//     ];

//     SystemCallArchitectures = "native"; // Or ["amd64", "x86_64", "x32"] if supporting multiple ABIs
//     SystemCallErrorNumber = "EPERM";    // Return EPERM (Permission denied) on syscall violation.
//   };
// };
```
*   **`NoNewPrivileges = true;` is mandatory.**
*   **Syscall Sets:** `man systemd.exec` lists available sets (e.g., `@basic-io`, `@file-system`, `@network-io`, `@process`, `@clock`, `@cpu-emulation`, `@debug`, `@privileged`, `@system-service`). Whitelisting is generally more secure than blacklisting (e.g., `~@privileged`).
*   **Profiling for Whitelists:**
    1.  Start with minimal set (e.g., `exit_group`, basic I/O).
    2.  Run agent under `strace -c -f /path/to/agent_binary ...args...` during normal operation and test cases.
    3.  Observe syscalls used in `strace` summary. Add them to whitelist.
    4.  Test thoroughly. Denying a needed syscall will likely crash agent.
    5.  Tools like `oci-seccomp-bpf-hook` or `auditd` logs (`ausearch -m SYSCALL -p <PID>`) can also help generate profiles.

### 2.2. Effectiveness and Performance [CR3]

*   Reduces kernel attack surface significantly (e.g., typical agent might need 15-30 syscalls out of 300+, ~93% reduction [CR3]).
*   Performance overhead <0.1% (with kernel BPF JIT).

## 3. AppArmor on NixOS [UG Sec 22.2.2, OR3]

Provides Mandatory Access Control (MAC) via per-program profiles defining allowed resource access (files, network, capabilities).

### 3.1. NixOS Setup

```nix
# In configuration.nix
# security.apparmor = {
//   enable = true;
//   # Optional:
//   # killProcessOnViolation = true; // Default is log & deny. Kill is harsher.
//   # Define profiles directly or via packages/modules
//   # Example: Inline profile (less common for complex profiles)
//   # extraProfiles = ''
//   #   /nix/store/xxxx-my-agent-bin/bin/my_agent flags=(complain) {
//   #     #include <abstractions/base>
//   #     # Profile rules here
//   #   }
//   # '';
//   # Better: Use a dedicated NixOS module for AppArmor profiles for Sinex agents
// };
// # services.my_sinex_agent.serviceConfig.AppArmorProfile = "/path/to/profile_name_in_apparmor_d"; // Or just the profile name
```
*   Requires AppArmor-enabled kernel (common). Reboot often needed after first enable.
*   Profiles live in `/etc/apparmor.d/` (NixOS symlinks them from store). Name often `/path/to/bin` with `/` -> `.`.

### 3.2. Profile Writing and Management

1.  **Generate Initial Profile:** `sudo aa-genprof /path/to/nix_store_agent_binary` (run agent, exercise functionality).
2.  **Complain Mode:** `sudo aa-complain /path/to/nix_store_agent_binary` (or set `flags=(complain)` in profile).
3.  **Refine with `aa-logprof`:** Review audit logs (`/var/log/audit/audit.log` or journald if no `auditd`) and interactively allow/deny observed operations to build the profile.
4.  **Enforce Mode:** `sudo aa-enforce /path/to/nix_store_agent_binary` (or remove `flags=(complain)`).
5.  **NixOS Profile Definition (Example for `sinex-promo-worker`):**
    A dedicated NixOS module (e.g., `nixos/modules/security/apparmor-sinex-profiles.nix`) would define profiles:
    ```nix
    # { pkgs, config, ... }:
    # {
    //   security.apparmor.profiles."sinex-promo-worker-profile" = {
    //     name = "${config.services.sinex-promo-worker.package}/bin/sinex-promo-worker"; # Actual path in Nix store
    //     # mode = "complain"; # During development
    //     content = ''
    //       #include <tunables/global>
    //       profile sinex_promo_worker_profile ${config.services.sinex-promo-worker.package}/bin/sinex-promo-worker {
    //         #include <abstractions/base>
    //         #include <abstractions/nameservice> # For DNS if it makes external calls
    //         #include <abstractions/postgresql>  # For local PostgreSQL socket access

    //         capability net_bind_service, # For Prometheus exporter port

    //         network inet tcp, # Allow outgoing TCP for DB if remote
    //         network inet6 tcp,
    //         network inet stream bind addr=0.0.0.0 port=${toString config.services.sinex-promo-worker.settings.prometheus_port or 2112},

    //         # Read own executable and system libs
    //         owner "${config.services.sinex-promo-worker.package}/bin/sinex-promo-worker" mr,
    //         "/nix/store/**" rmix, # Allow read/execute from Nix store broadly (can be tightened)
            
    //         # PostgreSQL socket access
    //         "${config.services.postgresql.unixSocketDir}/.s.PGSQL.${toString config.services.postgresql.port}" w,

    //         # Log file access (if not solely using journald)
    //         "/var/log/sinex/promo-worker.log" rw,
    //         "/var/log/sinex/promo-worker.log.*" rw,

    //         # Deny by default
    //       }
    //     '';
    //   };
    //   # Link this profile to the service unit
    //   systemd.services.sinex-promo-worker.serviceConfig.AppArmorProfile = "sinex-promo-worker-profile";
    // }
    ```

### 3.3. AppArmor Commands

*   Load/reload: `sudo apparmor_parser -r /etc/apparmor.d/profile_name` or `sudo systemctl reload apparmor.service`.
*   Status: `sudo aa-status`.

## 4. `evdev` Keyboard Capture Specific Mitigations [UG Sec 22.3, SR1, CR4]

This is critical due to keylogging risk. Refer to UG Section 22.3 for the full strategy, summarized here:

1.  **Strict Privilege Separation (Mandatory):**
    *   Minimal `evdev` reader component (e.g., `interception-tools` plugin or small C/Rust binary using `libevdev`). Runs with minimal privileges (only access to specific keyboard `evdev` node).
    *   Heavily sandboxed with its own seccomp/AppArmor profile allowing only `read`/`poll`/`ioctl` on keyboard FD, `write` to its IPC output, `exit`.
    *   Forwards raw scancode data via secure local IPC (permissioned UNIX socket) to an unprivileged `EvdevEventProcessorAgent`.
    *   `EvdevEventProcessorAgent` (unprivileged) parses, (attempts context filtering), structures for `core.events`, inserts to DB. Has NO direct `evdev` access.
2.  **User Opt-In & Clear Persistent Notification (Mandatory).**
3.  **Prefer Higher-Level Input Capture (Default).**
4.  **Context-Aware Filtering (Best-Effort, Unreliable):** `EvdevEventProcessorAgent` attempts to suppress logging for password fields.

By combining seccomp-bpf for syscall filtering and AppArmor for resource access control, Exocortex agents can be significantly hardened against potential vulnerabilities or compromise.

