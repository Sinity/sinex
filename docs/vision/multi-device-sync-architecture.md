# TIM-MultiDeviceSyncArchitecture: Multi-Device Synchronization

> **Operational note (2025-10-23)**  
> JetStream ingestion is canonical. Any retired pipeline references here are historical context.


*   **Relevant ADR:** (N/A directly, enables Vision Doc Part VI.5 for personal multi-device coherence)
*   **Original UG Context:** Section 28

This TIM details the architecture and tools for synchronizing Exocortex data and relevant application state across multiple user devices (desktop, laptop, mobile), enabling offline operation and eventual consistency.

## 1. Rationale Summary

The Exocortex is designed for local-first operation on each device. To provide a coherent experience when the user moves between devices, or if data is captured on one device (e.g., mobile) and processed/queried on another (e.g., desktop), robust synchronization mechanisms are needed for different types of data.

## 2. Tools for File and SQLite Synchronization [UG Sec 28.1, CR5]

### 2.1. LiteFS (for SQLite Databases) [`openai_sinex_6.md` Sec 8]

*   **Mechanism:** Distributed replication for SQLite (by Fly.io). Intercepts filesystem calls.
    *   Primary node (leaseholder) for writes. Read-replicas stream changes (SQLite WAL pages as LTX files).
    *   Dynamic leader election (Consul, etcd) or static primary.
*   **Exocortex Use Case:** Synchronize SQLite DBs used for:
    *   Atuin command history DB (`~/.local/share/atuin/history.db`).
    *   Local PKM caches/indexes for faster UI on specific devices.
    *   Agent-specific state databases (if SQLite is used by an agent).
*   **Configuration (`litefs.yml`):**
    ```yaml
    # /etc/litefs.yml (managed by NixOS)
    fuse:
      dir: "/var/lib/sinex/litefs_mount" # Apps access DBs via this FUSE mount
    data:
      dir: "/var/lib/sinex/litefs_data"  # Original SQLite files here
    lease: # Example for a primary node
      type: "static"
      hostname: "exocortex-main-host" # Must be resolvable by other LiteFS nodes
      advertise-url: "http://exocortex-main-host:20202" # Default LiteFS sync port
      candidate: true # This node can be primary
    # proxy: { addr: ":<app_port>", target: "localhost:<app_port>", db: "db_name_in_data_dir" } # Optional proxy
    ```
*   **NixOS Service:** `services.litefs.enable = true;` with declarative settings.
*   **Conflict Handling:** LiteFS is single-writer (primary leaseholder). Does not perform logical merge of concurrent writes during network partitions ("split-brain"). One version typically wins on heal; changes on non-primaries during partition may be lost. Applications must tolerate or have higher-level conflict resolution if this is a risk. For Exocortex, if PostgreSQL is central truth, LiteFS is more for replicating read-heavy caches or specific single-writer agent state.

### 2.2. Syncthing (for General File Synchronization)

*   **Mechanism:** Decentralized P2P continuous file sync. End-to-end encrypted.
*   **Exocortex Use Case:**
    *   PKM Markdown vault (if a filesystem view is maintained alongside DB-native Yjs content, primarily for consumption by non-Exocortex-aware tools or user preference for file browsing).
    *   `git-annex` repositories (syncing `.git` metadata dir; content sync usually via `git annex sync` with remotes, but Syncthing can sync unlocked/wanted files directly between personal devices).
    *   Exocortex configuration files (if some are manually managed outside NixOS).
    *   User-managed data folders related to Exocortex projects.
*   **Optimization:**
    *   `.stignore` patterns to exclude temp files (`*.swp`), caches, build artifacts.
    *   File versioning (Syncthing can keep old versions).
    *   Conflict Handling: Creates `*.sync-conflict-...` files for user to manually resolve. (Less relevant for Yjs CRDT content files, as CRDT handles merge).

## 3. Clocks for Event Ordering and Causality (Multi-Device) [UG Sec 28.2, CR5, SA4]

Essential when events are generated on intermittently connected devices.

### 3.1. ULIDs and NTP (Network Time Protocol)

*   **ULIDs:** Provide time-ordering via embedded 48-bit ms timestamp.
*   **NTP Requirement [SA4]:** All Exocortex devices must run NTP clients and be reasonably synchronized (sub-second accuracy) for ULID timestamps to be meaningful for cross-device ordering.

### 3.2. Hybrid Logical Clocks (HLCs) [CR5]

*   **Mechanism:** Combine physical clock time with a logical counter: `(physical_time_i, logical_counter_i)`.
    *   Captures Lamport causality (A happened before B) while staying close to real time.
    *   On local event: Update HLC based on physical time and previous HLC (monotonic).
    *   On send message `m`: Timestamp `m` with `hlc_send`.
    *   On receive `m` with `hlc_send`: Update local HLC to `max(own_hlc, hlc_send, current_physical_time)`, then increment logical counter if physical parts were equal.
*   **Benefit:** Better causal ordering than plain timestamps with moderate clock skew. Good for distributed logs, CRDT operations.

### 3.3. Vector Clocks [CR5]

*   **Mechanism:** Each of `N` nodes has vector `VC` of `N` logical clocks. `VC_i[j]` = events node `i` knows from node `j`.
    *   Local event at `i`: Increment `VC_i[i]`.
    *   Send message `m` from `i`: Attach `VC_i`.
    *   Node `j` receives `m` with `VC_send`: `VC_j[k] = max(VC_j[k], VC_send[k])` for all `k`; then increment `VC_j[j]`.
*   **Causality:** Event A (`VC_A`) happened before B (`VC_B`) if `VC_A <= VC_B` component-wise and `VC_A != VC_B`. Concurrent if neither happened before other.
*   **Benefit:** Precisely captures causality.
*   **Overhead:** Vector size `O(N)`. Manageable for small number of personal devices.

## 4. CRDTs for Conflict-Free Merging [UG Sec 28.3, CR5]

Conflict-Free Replicated Data Types guarantee eventual consistency for concurrent operations.

*   **Yjs for Textual Content (PKM Notes, Living Document):** As per ADR-004.
    *   Yjs update blobs (binary diffs) exchanged between devices.
    *   Sync protocol for Yjs updates needs to carry HLC or Vector Clock timestamps from originating device to ensure causal ordering when applying updates.
*   **Other CRDT Types (for other synced state if needed):**
    *   Counters (G-Counter, PN-Counter): For shared stats.
    *   Sets (G-Set, 2P-Set, OR-Set): For shared tags, processed IDs.
    *   Registers (LWW-Register): For simple config flags (resolve concurrent updates by HLC).
*   **Synchronization Protocol for CRDTs:**
    *   Op-Based (send deltas/updates, like Yjs) is more efficient than State-Based (send full state).
    *   Transport: Central relay server (user's main Exocortex host), P2P (WebRTC, libp2p), or MQTT for small updates.

## 5. Offline Operation and Eventual Consistency [UG Sec 28.4, CR5]

*   **Local-First Principle:** Exocortex on each device functional offline.
*   **Outgoing Operation Queues:**
    *   Changes made offline (new `core.events` on mobile, Yjs updates for notes, new `git-annex` blobs) are queued persistently on device (e.g., SQLite queue on mobile, Yjs doc stores local ops).
*   **Synchronization on Reconnect:** Device sends queued changes, fetches incoming changes.
*   **Eventual Consistency:** All replicas eventually converge. CRDTs guarantee strong eventual consistency. Non-CRDT data conflicts resolved by LWW (HLCs) or manual merge.

## 6. Federation Topologies (Conceptual, for Future Inter-User Sync) [UG Sec 28.5, CR5]

Not primary for single-user multi-device, but informs design.
*   **Hub-and-Spoke:** User's main Exocortex host as personal hub. Other devices sync to it. Hubs might federate.
*   **Peer-to-Peer (Mesh):** Direct device-to-device or instance-to-instance sync (libp2p, DHT).
*   **Hybrid:** Personal hub, with P2P federation between hubs.
*   **Enablers:** Global ULIDs, NTP, `git-annex` for blobs, CRDTs. ACLs and cryptographic sharing for inter-user.
