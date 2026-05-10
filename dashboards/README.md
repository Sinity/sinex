# Sinex Grafana Dashboards

This directory holds Grafana dashboards (JSON, file-provisioning format) that
read from the `sinex_telemetry` schema views in the operator-facing Postgres
database. They are part of the gateway-side deliverable for issue #1172
(AC-6); the NixOS-side wiring lives in `sinnix` and is a separate change.

## Dashboards

| File                         | Source view                                       |
| ---------------------------- | ------------------------------------------------- |
| `gateway-stats-1h.json`      | `sinex_telemetry.gateway_stats_1h`                |
| `ingestd-batch-stats-1h.json`| `sinex_telemetry.ingestd_batch_stats_1h`          |
| `node-stats-1h.json`         | `sinex_telemetry.node_stats_1h`                   |
| `recent-activity.json`       | `sinex_telemetry.recent_activity_summary`         |
| `current-health.json`        | `sinex_telemetry.current_health`                  |

Each dashboard expects a Postgres data source named `${DS_POSTGRES}` whose
connection has at least `SELECT` on the `sinex_telemetry` schema. None of
the dashboards mutate state; they are pure read-only views over the existing
telemetry surface (`crate/lib/sinex-schema/src/apply.rs`
`TELEMETRY_VIEW_RELATIONS`).

## Consumption pattern

The sinex repo ships JSON only. The deployment side — the NixOS module that
mounts a file-provisioned Grafana with these dashboards — lives in
`/realm/project/sinnix` (see `modules/services/sinex/dashboards.nix` once
that change lands). The module imports this directory verbatim as a
file-provisioning provider, gated on a `sinex.dashboards.enable` option.

The cross-repo split is intentional: the dashboard *content* belongs with
the views it queries, while the *deployment* belongs with the rest of the
sinex service definitions in sinnix. Coordinated change order:

1. Land this directory in sinex (current PR).
2. Land the matching NixOS provider in sinnix referencing the directory by
   absolute path (e.g. `${sinex}/dashboards`).
3. Rebuild the deployment.

## Authoring conventions

- One dashboard per JSON file — no multi-dashboard bundles.
- `uid` is stable and matches the file stem so links survive renames at the
  view level.
- Time range defaults are tight (last 7 days for hourly views, last 24h or
  1h for activity-style views) so first paint is fast even on a busy db.
- Avoid Grafana plugin dependencies; everything here is core
  `timeseries` / `table` panels.
- `${DS_POSTGRES}` is the templated data source name; do not hard-code
  data-source UIDs because they are environment-specific.
