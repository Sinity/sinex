# Mutex/RwLock Contention Map

Scope
- Identify shared locks that could become contention points in high-throughput paths.

Method
- rg for Mutex/RwLock usage; focus on production modules.

High-traffic locks
- ingestd keeps EventValidator behind Arc<RwLock<...>> and task handles behind Arc<Mutex<Vec<JoinHandle>>>; schema reload and validation both contend on the validator lock (crate/core/sinex-ingestd/src/service.rs:48-59).
- ConfirmationBuffer stores pending events in Arc<RwLock<HashMap<Ulid, ProvisionalEvent>>>; add/confirm/timeout checks all hit this lock (crate/lib/sinex-node-sdk/src/confirmation_handler.rs:72-107).
- MaterialAssembler keeps per-material state in DashMap<Ulid, Arc<Mutex<AssemblerState>>>; high event volume across many materials can lead to frequent lock acquisition (crate/core/sinex-ingestd/src/material_assembler/mod.rs:41-50).

Lower-risk locks
- HeartbeatEmitter uses parking_lot::Mutex for counters and status; lock hold times are short and local (crate/lib/sinex-node-sdk/src/heartbeat.rs:189-299).

Observations
- Most locks are coarse but short-lived; the most likely contention is in confirmation handling and material assembly under heavy ingest.

Follow-ups
- Add lock hold time metrics for the confirmation buffer and material assembler.
- Consider sharding confirmation buffers by event_id hash if contention is observed in production.
