# Connection Pool Usage

Scope
- Identify how DB connections are acquired and how long they may be held across awaits.

Method
- rg for pool.acquire/pool.begin in production modules; review key call sites.

Key patterns
- Pool creation sets per-connection statement_timeout in after_connect (crate/lib/sinex-core/src/db/mod.rs:251-268).
- AnalyticsService acquires a connection with a tight 40ms timeout to avoid pool exhaustion (crate/lib/sinex-services/src/analytics.rs:46-62).
- AdvisoryLock holds a PoolConnection inside a ResourceGuard for the lifetime of the lock; this intentionally ties up a pool slot while the lock is held (crate/lib/sinex-core/src/db/advisory_lock.rs:15-73).

Observations
- Tight timeouts are good for surfacing saturation but may cause false timeouts under bursty load.
- Long-lived advisory locks are safe but require capacity planning; each lock consumes a pool connection until release.

Follow-ups
- Consider metrics around acquire timeouts (count, latency) to catch saturation early.
- Document expected maximum concurrent advisory locks so pool sizing remains safe.
