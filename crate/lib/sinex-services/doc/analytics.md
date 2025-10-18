# Analytics Service

`AnalyticsService` computes rollups over the event stream to support dashboards
and alerting. It is responsible for:

- Counting events per source and event type using SQLx repositories from
  `sinex-core`.
- Applying time-range filters consistently so downstream callers can request
  partial windows.
- Translating raw database rows into ergonomic maps keyed by source or event
  type.

See `docs/architecture/SystemOperations_And_Integrity_Architecture.md` for the
system-level dashboards that consume these APIs.
