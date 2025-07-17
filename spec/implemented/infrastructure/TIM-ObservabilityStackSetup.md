# TIM-ObservabilityStackSetup: Prometheus, Grafana, Loki

## Status Dashboard
**Maturity Level**: L2 - Ready for Implementation
**Implementation**: 70% (Prometheus and Grafana fully configured, Loki/Promtail pending)
**Dependencies**: NixOS services, Prometheus exporters, Grafana configuration, Loki setup
**Blocks**: System monitoring, performance analysis, debugging, operational visibility

## MVP Specification
- Prometheus metrics collection setup
- Basic Grafana dashboards for system health
- Loki log aggregation configuration
- Promtail log collection agents
- Essential system and application metrics

## Enhanced Features
- Advanced dashboard customization
- Alerting and notification systems
- Log analysis and pattern detection
- Performance trend analysis
- Custom metrics and exporters
- Multi-host monitoring coordination

## Implementation Checklist
- [x] Prometheus server configuration
- [x] Node exporter setup for system metrics
- [x] Grafana installation and initial dashboards
- [ ] Loki server configuration
- [ ] Promtail log collection setup
- [x] Application metrics integration
- [x] Alert rule configuration
- [x] Dashboard template development
- [x] Monitoring best practices documentation

*   **Relevant ADR:** (N/A directly, core operational infrastructure)
*   **Original UG Context:** Section 24
*   **Vision Document Reference:** Part VI.1

This TIM details the setup and configuration of the core observability stack for the Exocortex: Prometheus for metrics, Grafana for dashboards, and Loki/Promtail for centralized logging, primarily using NixOS service modules.

## 1. Rationale Summary

A comprehensive observability stack is crucial for monitoring Exocortex health, performance, resource usage, and for debugging issues. Prometheus, Grafana, and Loki form a popular, powerful, and well-integrated open-source solution.

## 2. Stack Components [UG Sec 24.1, OR3]

*   **Prometheus:** Time-series database, collects metrics via HTTP scraping of `/metrics` endpoints. Uses PromQL for querying.
*   **Grafana:** Visualization and dashboarding. Queries Prometheus, Loki, PostgreSQL, etc.
*   **Loki:** Log aggregation system (by Grafana Labs). Stores logs indexed by labels.
*   **Promtail:** Loki's log collection agent. Scrapes logs (files, journald), attaches labels, ships to Loki.
*   **Exporters:**
    *   `node_exporter`: Exposes host system metrics (CPU, RAM, disk, network).
    *   `postgres_exporter`: Exposes PostgreSQL metrics.
    *   Exocortex agents/services expose their own `/metrics` endpoints.

## 3. NixOS Service Configuration [UG Sec 24.2, OR3, SA4]

Example configuration in `configuration.nix`.

### 3.1. Prometheus

```nix
# services.prometheus = {
//   enable = true;
//   listenAddress = "0.0.0.0"; # Or "127.0.0.1"
//   port = 9090;
//   retentionTime = "30d"; # Or longer, e.g., "1y"
//   # externalLabels = { monitor = "exocortex-main-host"; }; # Add global labels

//   scrapeConfigs = [
//     { job_name = "prometheus"; static_configs = [{ targets = ["localhost:9090"]; }]; }
//     { job_name = "node_exporter"; static_configs = [{ targets = ["localhost:${toString config.services.prometheus.exporters.node.port}"]; }]; }
//     { job_name = "postgres_exporter"; static_configs = [{ targets = ["localhost:${toString config.services.prometheus.exporters.postgres.port}"]; }]; }
//     # Add scrape_configs for each Sinex agent/service exposing /metrics
//     { job_name = "sinex_promo_worker"; metrics_path = "/metrics"; static_configs = [{ targets = ["localhost:2112"]; }]; }
//     { job_name = "sinex_web_archiver"; metrics_path = "/metrics"; static_configs = [{ targets = ["localhost:2113"]; }]; }
//     # ... etc. ...
//   ];

//   exporters = {
//     node = {
//       enable = true;
//       listenAddress = "0.0.0.0";
//       port = 9100;
//       # enabledCollectors = [ "systemd" ]; # Optionally enable specific collectors like systemd
//     };
//     postgres = { # wrouesnel/postgres_exporter
//       enable = true;
//       listenAddress = "0.0.0.0";
//       port = 9187;
//       # Connection string usually inferred if running on same host as default PG.
//       # If specific DB or user needed:
//       # connectionString = "postgresql://exporter_user:pass@localhost/sinex_db";
//       # Or set DATA_SOURCE_NAME environment variable for the service.
//       # Ensure exporter_user has SELECT grants on pg_stat_* views.
//       # For custom queries (e.g., DLQ size, queue sizes):
//       # queryFiles."sinex_custom_pg_metrics".path = ./path/to/sinex_pg_custom_queries.yaml;
//       # disableDefaultMetrics = false;
//       # disableSettingsMetrics = true; # Optional: disable some default metrics if too noisy
//     };
//   };
//   # ruleFiles = [ ./path/to/prometheus_alert_rules.yml ]; # For Prometheus alerting rules
// };
```
**Custom PostgreSQL Queries for `postgres_exporter` (`sinex_pg_custom_queries.yaml`):**
(From UG Sec 24.2)
```yaml
sinex_events_total: # Metric name prefix
  query: "SELECT source, event_type, count(*) AS count FROM core.events GROUP BY source, event_type;"
  metrics:
    - source: { usage: "LABEL", description: "Event source (processor name)" }
    - event_type: { usage: "LABEL", description: "Type of event" }
    - count:  { usage: "GAUGE", description: "Number of events by source and type" }

sinex_processor_checkpoints:
  query: "SELECT automaton_name, processed_count FROM core.automaton_checkpoints;"
  metrics:
    - automaton_name: { usage: "LABEL", description: "Automaton processor name" }
    - processed_count: { usage: "GAUGE", description: "Number of events processed by automaton" }
```

### 3.2. Grafana

```nix
# services.grafana = {
//   enable = true;
//   settings = {
//     server = {
//       http_addr = "0.0.0.0"; # Or "127.0.0.1"
//       http_port = 3000;
//       # domain = "exocortex-grafana.local"; # If using a custom domain
//       # root_url = "%(protocol)s://%(domain)s:%(http_port)s/";
//     };
//     auth.anonymous = {
//       enabled = true;
//       org_name = "Main Org."; # Or your user's org
//       org_role = "Admin";    # Set to Viewer for read-only anonymous access initially
//     };
//     # For admin user (important if anonymous is not Admin)
//     # "auth.basic" = { enabled = true; };
//     # admin = {
//     #   user = "admin";
//     #   passwordFile = config.age.secrets.grafana_admin_password.path; # Manage Grafana admin pass with agenix
//     # };
//   };
//   # Declarative provisioning of datasources and dashboards
//   provision = {
//     enable = true;
//     datasources = [{
//       name = "Prometheus-Exocortex";
//       type = "prometheus";
//       access = "proxy"; # Grafana server proxies requests
//       url = "http://localhost:${toString config.services.prometheus.port}";
//       isDefault = true;
//       jsonData = { httpMethod = "POST"; }; # Recommended for Prometheus
//     }];
//     # dashboards = [{ # Example dashboard provider
//     #   name = "exocortex-dashboards";
//     #   options = {
//     #     path = ./path/to/exocortex_grafana_dashboards_dir; # Dir containing dashboard JSON files
//     #     foldersFromFilesStructure = true;
//     #   };
//     # }];
//   };
// };
// # age.secrets.grafana_admin_password.file = ./secrets/grafana_admin_password.age; # If managing admin pass
```

### 3.3. Loki and Promtail (Optional, for Logs)

```nix
# services.loki = {
//   enable = true;
//   # Default config usually fine for single host. Listens on :3100.
//   # configFile = "/path/to/loki-config.yaml"; # For custom config
// };

// services.promtail = {
//   enable = true;
//   # Configuration is typically provided as a Nix attrset matching Promtail's config structure
//   configuration = {
//     server = {
//       http_listen_port = 9080; # Promtail's own HTTP port (for /metrics etc.)
//       grpc_listen_port = 0;    # Disable gRPC listener if not used
//     };
//     clients = [ # Loki server(s) to send logs to
//       { url = "http://localhost:${toString config.services.loki.configuration.server.http_listen_port}/loki/api/v1/push"; }
//     ];
//     scrape_configs = [
//       { # Scrape systemd journal for all units
//         job_name = "system_journal";
//         journal = {
//           max_age = "168h"; # How far back to read on startup (e.g., 7 days)
//           # path = "/var/log/journal"; # Or /run/log/journal
//           labels = {
//             job = "systemd-journal";
//             host = config.networking.hostName;
//           };
//         };
//         relabel_configs = [
//           { source_labels = ["__journal__systemd_unit"]; target_label = "unit"; }
//           { source_labels = ["__journal_syslog_identifier"]; target_label = "ident"; }
//           { source_labels = ["__journal_priority_keyword"]; target_label = "level"; }
//           # Add more relabel_configs to extract other useful fields from journal entries as labels
//         ];
//       }
//       # Example: Scrape specific Sinex application log files if they don't log to journal
//       # {
//       #   job_name = "sinex_app_logs";
//       #   static_configs = [
//       #     {
//       #       targets = ["localhost"]; # Promtail runs locally
//       #       labels = {
//       #         job = "sinex_applogs";
//       #         host = config.networking.hostName;
//       #         __path__ = "/var/log/sinex/**/*.log"; # Glob for Sinex log files
//       #       };
//       #     }
//       #   ];
//       #   # pipeline_stages = [ { /* docker = {} or json = {} or regex = {} ... */ } ]; # For parsing log lines
//       # }
//     ];
//   };
// };
```

## 4. Application Instrumentation (`/metrics` Endpoints) [UG Sec 24.3, OR3, SA4]

Exocortex services (Rust, Python agents) expose an HTTP `/metrics` endpoint serving Prometheus text format.
*   **Rust Example (using `prometheus` crate and `actix-web` - from UG Sec 24.3):**
    ```rust
    // use actix_web::{get, App, HttpResponse, HttpServer, Responder};
    // use prometheus::{Encoder, TextEncoder, IntCounterVec, HistogramVec, Opts, Registry};
    // use once_cell::sync::Lazy;

    // pub static SİNEX_REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

    // pub static PROMOTIONS_PROCESSED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    //     let opts = Opts::new("promotions_processed_total", "Total promotions processed.")
    //         .namespace("sinex").subsystem("promo_worker");
    //     IntCounterVec::new(opts, &["target_agent_name", "outcome"]).expect("metric can be created")
    // });
    // // Register with SİNEX_REGISTRY in main or a setup function:
    // // SİNEX_REGISTRY.register(Box::new(PROMOTIONS_PROCESSED_TOTAL.clone())).expect("collector can be registered");


    // #[get("/metrics")]
    // async fn metrics_handler() -> impl Responder {
    //     let encoder = TextEncoder::new();
    //     let mut buffer = Vec::new();
    //     if let Err(e) = encoder.encode(&SİNEX_REGISTRY.gather(), &mut buffer) { // Use custom registry
    //         eprintln!("Failed to encode metrics: {}", e);
    //         return HttpResponse::InternalServerError().body(format!("Failed to encode metrics: {}", e));
    //     }
    //     HttpResponse::Ok().content_type(encoder.format_type()).body(buffer)
    // }

    // In main application setup (e.g., for sinex-promo-worker):
    // async fn start_metrics_server(port: u16) -> std::io::Result<()> {
    //     // Important: Register metrics before starting server
    //     SİNEX_REGISTRY.register(Box::new(PROMOTIONS_PROCESSED_TOTAL.clone())).unwrap();
    //     // ... register other metrics ...

    //     HttpServer::new(move || App::new().service(metrics_handler))
    //         .bind(("0.0.0.0", port))?
    //         .run()
    //         .await
    // }
    // // Call start_metrics_server(2112).await in main.
    ```
*   **Key Metrics:** For each agent: items processed, errors, latency histograms, queue depths (if applicable), specific business logic counters.

## 5. Grafana Dashboard Design [UG Sec 24.5, OR3, SA4]

*   Dashboards are JSON models. Create via UI then export, or provision declaratively.
*   **Key Panels/Visualizations:**
    *   Event Ingestion Rates (`rate(sinex_events_total[5m])`).
    *   Processor Stats (processed rates, checkpoint progress, latency histograms).
    *   Redis Stream Metrics (stream length, consumer group lag).
    *   LLM Usage (calls, tokens, cost - from `sinex.agent.llm_api_call` derived metrics).
    *   System Resources (CPU, RAM, Disk, Network from `node_exporter`).
    *   PostgreSQL Health (QPS, connections, locks, cache hits from `postgres_exporter`).
    *   Personal Analytics (Time in app, task completion, mood correlations - requires agents to emit these as metrics).
*   Use variables in Grafana dashboards for filtering by `host`, `agent_name`, `event_type`, etc.

## 6. Distributed Tracing with OpenTelemetry and Jaeger [UG Sec 24.7, CR5]

For end-to-end request tracing across Exocortex components.
*   **OpenTelemetry (OTel):** Instrument Rust/Python services with OTel SDKs. Use auto-instrumentation for common libraries (HTTP clients, DB drivers) and manual instrumentation for custom logic (create spans for key operations). Propagate W3C Trace Context.
*   **Jaeger:** Backend for storing and visualizing traces.
    *   Run Jaeger All-In-One Docker image for local/single-host: `jaegertracing/all-in-one:latest`.
    *   Configure OTel SDKs to export traces via OTLP to Jaeger Collector (e.g., `http://localhost:4317` for gRPC, `http://localhost:4318` for HTTP).
*   **NixOS for Jaeger (from UG Sec 24.7):**
    ```nix
    # virtualisation.oci-containers.containers.jaeger = {
    //   image = "jaegertracing/all-in-one:1.53"; // Use specific version
    //   ports = [
    //     "16686:16686", # Jaeger UI HTTP
    //     "4317:4317",   # OTLP gRPC receiver
    //     "4318:4318",   # OTLP HTTP receiver
    //   ];
    //   # environment = { "COLLECTOR_OTLP_ENABLED" = "true"; };
    // };
    ```
*   **Use Cases:** Debug latency, understand dependencies, visualize complex workflows (e.g., LLM agent chains).

