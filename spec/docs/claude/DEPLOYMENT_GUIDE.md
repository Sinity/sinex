# Sinex Deployment Guide

This guide provides step-by-step instructions for deploying the enhanced Sinex system in various environments.

## Quick Start Deployment

### Development Environment

**1. Prerequisites:**
```bash
# Ensure Nix is available
nix --version

# Clone and enter directory
cd /path/to/sinex
```

**2. Start Development Environment:**
```bash
# Enter Nix development shell (sets up PostgreSQL automatically)
nix develop

# Verify database is running
just psql -c "SELECT version();"

# Apply migrations
just migrate
```

**3. Start Collector:**
```bash
# Basic development setup
just unified

# Or with custom config
cargo run --bin sinex-collector -- --config config/unified-collector/development.toml
```

**4. Verify Operation:**
```bash
# Check health
curl http://localhost:8080/health

# Generate test events
echo "test content" > /tmp/test-sinex-watch/test.txt

# View captured events
just query 10
```

### Production Environment

**1. System Setup:**
```bash
# Create sinex user
sudo useradd -r -s /bin/false -d /var/lib/sinex sinex

# Create directories
sudo mkdir -p /etc/sinex /var/lib/sinex/dlq /var/log/sinex
sudo chown -R sinex:sinex /var/lib/sinex /var/log/sinex
sudo chmod 755 /etc/sinex
```

**2. Install PostgreSQL with TimescaleDB:**
```bash
# On Ubuntu/Debian
curl -fsSL https://packagecloud.io/timescale/timescaledb/gpgkey | sudo apt-key add -
echo "deb https://packagecloud.io/timescale/timescaledb/ubuntu/ $(lsb_release -c -s) main" | sudo tee /etc/apt/sources.list.d/timescaledb.list
sudo apt update
sudo apt install timescaledb-2-postgresql-16 postgresql-16

# Initialize and start
sudo systemctl enable postgresql
sudo systemctl start postgresql

# Create database
sudo -u postgres createuser sinex
sudo -u postgres createdb sinex -O sinex
sudo -u postgres psql -c "ALTER USER sinex WITH PASSWORD 'secure_password';"
```

**3. Setup TimescaleDB Extension:**
```bash
sudo -u postgres psql sinex -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
sudo -u postgres psql sinex -c "CREATE EXTENSION IF NOT EXISTS vector;"
sudo -u postgres psql sinex -c "CREATE EXTENSION IF NOT EXISTS pg_jsonschema;"
```

**4. Build and Install Sinex:**
```bash
# Build release binary
nix build .#sinex-collector
sudo cp result/bin/sinex-collector /usr/local/bin/
sudo chmod +x /usr/local/bin/sinex-collector

# Apply database migrations
DATABASE_URL="postgresql://sinex:secure_password@localhost/sinex" \
  sqlx migrate run --source ./migrations
```

**5. Create Configuration:**
```bash
sudo tee /etc/sinex/collector.toml << 'EOF'
[database]
url = "postgresql://sinex:secure_password@localhost/sinex"
max_connections = 50
connection_timeout_ms = 5000

[observability]
metrics_port = 8080
log_level = "info"

[sources.filesystem]
enabled = true
watch_patterns = [
  "/home/*/documents/**/*",
  "/var/log/**/*.log"
]
max_events_per_second = 1000

[sources.terminal]
enabled = true
terminal_types = ["kitty", "alacritty", "gnome-terminal"]

[sources.hyprland]
enabled = true
socket_path = "$XDG_RUNTIME_DIR/hypr"

[recovery]
dlq_base_path = "/var/lib/sinex/dlq"
max_retry_attempts = 3
circuit_breaker_threshold = 10
EOF
```

**6. Create Systemd Service:**
```bash
sudo tee /etc/systemd/system/sinex-collector.service << 'EOF'
[Unit]
Description=Sinex Event Collector
After=postgresql.service
Requires=postgresql.service

[Service]
Type=exec
User=sinex
Group=sinex
ExecStart=/usr/local/bin/sinex-collector --config /etc/sinex/collector.toml
Restart=always
RestartSec=5
Environment=RUST_LOG=info

# Resource limits
MemoryMax=1G
TasksMax=1000

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/sinex /var/log/sinex /tmp

[Install]
WantedBy=multi-user.target
EOF
```

**7. Start and Enable Service:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable sinex-collector
sudo systemctl start sinex-collector
sudo systemctl status sinex-collector
```

**8. Verify Deployment:**
```bash
# Health check
curl http://localhost:8080/health

# Metrics check
curl http://localhost:8080/metrics | grep sinex_events_processed_total

# Agent registration
psql "postgresql://sinex:secure_password@localhost/sinex" \
  -c "SELECT agent_name, status, last_heartbeat_ts FROM sinex_schemas.agent_manifests;"
```

## Container Deployment

### Docker Compose Setup

**1. Create Project Directory:**
```bash
mkdir sinex-deploy
cd sinex-deploy
```

**2. Create docker-compose.yml:**
```yaml
version: '3.8'

services:
  postgres:
    image: timescale/timescaledb-ha:pg16
    container_name: sinex-postgres
    environment:
      POSTGRES_DB: sinex
      POSTGRES_USER: sinex
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_INITDB_ARGS: "--encoding=UTF-8"
    volumes:
      - postgres_data:/home/postgres/pgdata/data
      - ./init-extensions.sql:/docker-entrypoint-initdb.d/01-extensions.sql
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U sinex -d sinex"]
      interval: 10s
      timeout: 5s
      retries: 5

  sinex-collector:
    build: 
      context: .
      dockerfile: Dockerfile
    container_name: sinex-collector
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      DATABASE_URL: postgresql://sinex:${POSTGRES_PASSWORD}@postgres:5432/sinex
      RUST_LOG: info
      SINEX_DLQ_BASE: /var/lib/sinex/dlq
      SINEX_LOG_BASE: /var/log/sinex
    volumes:
      - sinex_dlq:/var/lib/sinex/dlq
      - sinex_logs:/var/log/sinex
      - ./config/collector.toml:/etc/sinex/collector.toml:ro
      # Mount host directories for monitoring (adjust as needed)
      - /home:/host/home:ro
      - /var/log:/host/var/log:ro
    ports:
      - "8080:8080"
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  prometheus:
    image: prom/prometheus:v2.45.0
    container_name: sinex-prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus_data:/prometheus
    ports:
      - "9090:9090"
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'

  grafana:
    image: grafana/grafana:10.0.0
    container_name: sinex-grafana
    environment:
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD}
      GF_USERS_ALLOW_SIGN_UP: false
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/dashboards:/etc/grafana/provisioning/dashboards:ro
      - ./grafana/datasources:/etc/grafana/provisioning/datasources:ro
    ports:
      - "3000:3000"

volumes:
  postgres_data:
  sinex_dlq:
  sinex_logs:
  prometheus_data:
  grafana_data:
```

**3. Create Environment File:**
```bash
cat > .env << 'EOF'
POSTGRES_PASSWORD=your_secure_postgres_password
GRAFANA_PASSWORD=your_secure_grafana_password
EOF
```

**4. Create Configuration Files:**

**init-extensions.sql:**
```sql
-- Enable required PostgreSQL extensions
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_jsonschema;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE sinex TO sinex;
```

**config/collector.toml:**
```toml
[database]
url = "postgresql://sinex:password@postgres:5432/sinex"
max_connections = 20
connection_timeout_ms = 5000

[observability]
metrics_port = 8080
log_level = "info"

[sources.filesystem]
enabled = true
watch_patterns = [
  "/host/home/*/documents/**/*",
  "/host/var/log/**/*.log"
]
max_events_per_second = 500

[recovery]
dlq_base_path = "/var/lib/sinex/dlq"
max_retry_attempts = 3
```

**prometheus.yml:**
```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'sinex-collector'
    static_configs:
      - targets: ['sinex-collector:8080']
    scrape_interval: 15s
    metrics_path: /metrics
```

**5. Create Dockerfile:**
```dockerfile
FROM rust:1.75 as builder

WORKDIR /app
COPY . .

# Build the collector
RUN cargo build --release --bin sinex-collector

# Runtime image
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create sinex user
RUN useradd -r -s /bin/false -d /var/lib/sinex sinex

# Copy binary and set permissions
COPY --from=builder /app/target/release/sinex-collector /usr/local/bin/
RUN chmod +x /usr/local/bin/sinex-collector

# Create directories
RUN mkdir -p /var/lib/sinex/dlq /var/log/sinex /etc/sinex
RUN chown -R sinex:sinex /var/lib/sinex /var/log/sinex

USER sinex

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["/usr/local/bin/sinex-collector"]
CMD ["--config", "/etc/sinex/collector.toml"]
```

**6. Deploy Stack:**
```bash
# Build and start services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f sinex-collector

# Apply migrations
docker-compose exec sinex-collector sinex-collector migrate

# Verify health
curl http://localhost:8080/health
```

## Kubernetes Deployment

### Prerequisites

```bash
# Ensure kubectl and k8s cluster access
kubectl cluster-info

# Create namespace
kubectl create namespace sinex
```

### 1. Database Setup

**postgres-pvc.yaml:**
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: postgres-pvc
  namespace: sinex
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 20Gi
  storageClassName: standard
```

**postgres-deployment.yaml:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: sinex
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: timescale/timescaledb-ha:pg16
        env:
        - name: POSTGRES_DB
          value: "sinex"
        - name: POSTGRES_USER
          value: "sinex"
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: sinex-secrets
              key: postgres-password
        ports:
        - containerPort: 5432
        volumeMounts:
        - name: postgres-storage
          mountPath: /home/postgres/pgdata/data
        - name: init-scripts
          mountPath: /docker-entrypoint-initdb.d
        livenessProbe:
          exec:
            command:
            - pg_isready
            - -U
            - sinex
            - -d
            - sinex
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          exec:
            command:
            - pg_isready
            - -U
            - sinex
            - -d
            - sinex
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: postgres-storage
        persistentVolumeClaim:
          claimName: postgres-pvc
      - name: init-scripts
        configMap:
          name: postgres-init
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: sinex
spec:
  selector:
    app: postgres
  ports:
  - port: 5432
    targetPort: 5432
```

### 2. ConfigMaps and Secrets

**secrets.yaml:**
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: sinex-secrets
  namespace: sinex
type: Opaque
stringData:
  postgres-password: "your-secure-postgres-password"
  database-url: "postgresql://sinex:your-secure-postgres-password@postgres:5432/sinex"
```

**configmap.yaml:**
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: sinex-config
  namespace: sinex
data:
  collector.toml: |
    [database]
    max_connections = 20
    connection_timeout_ms = 5000

    [observability]
    metrics_port = 8080
    log_level = "info"

    [sources.filesystem]
    enabled = true
    watch_patterns = ["/host/home/*/documents/**/*"]
    max_events_per_second = 500

    [recovery]
    dlq_base_path = "/var/lib/sinex/dlq"
    max_retry_attempts = 3
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-init
  namespace: sinex
data:
  01-extensions.sql: |
    CREATE EXTENSION IF NOT EXISTS timescaledb;
    CREATE EXTENSION IF NOT EXISTS vector;
    CREATE EXTENSION IF NOT EXISTS pg_jsonschema;
    GRANT ALL PRIVILEGES ON DATABASE sinex TO sinex;
```

### 3. Sinex Collector Deployment

**collector-deployment.yaml:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sinex-collector
  namespace: sinex
  labels:
    app: sinex-collector
spec:
  replicas: 1
  selector:
    matchLabels:
      app: sinex-collector
  template:
    metadata:
      labels:
        app: sinex-collector
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      containers:
      - name: sinex-collector
        image: sinex-collector:latest
        imagePullPolicy: Always
        ports:
        - containerPort: 8080
          name: metrics
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: sinex-secrets
              key: database-url
        - name: RUST_LOG
          value: "info"
        - name: SINEX_DLQ_BASE
          value: "/var/lib/sinex/dlq"
        - name: SINEX_LOG_BASE
          value: "/var/log/sinex"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 30
          timeoutSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
          timeoutSeconds: 5
        volumeMounts:
        - name: config
          mountPath: /etc/sinex
          readOnly: true
        - name: dlq-storage
          mountPath: /var/lib/sinex/dlq
        - name: log-storage
          mountPath: /var/log/sinex
        # Mount host paths for file monitoring
        - name: host-home
          mountPath: /host/home
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: sinex-config
      - name: dlq-storage
        persistentVolumeClaim:
          claimName: sinex-dlq-pvc
      - name: log-storage
        emptyDir: {}
      - name: host-home
        hostPath:
          path: /home
          type: Directory
---
apiVersion: v1
kind: Service
metadata:
  name: sinex-collector
  namespace: sinex
  labels:
    app: sinex-collector
spec:
  selector:
    app: sinex-collector
  ports:
  - name: metrics
    port: 8080
    targetPort: 8080
    protocol: TCP
  type: ClusterIP
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: sinex-dlq-pvc
  namespace: sinex
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

### 4. Monitoring Stack

**prometheus-deployment.yaml:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
  namespace: sinex
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prometheus
  template:
    metadata:
      labels:
        app: prometheus
    spec:
      containers:
      - name: prometheus
        image: prom/prometheus:v2.45.0
        ports:
        - containerPort: 9090
        volumeMounts:
        - name: config
          mountPath: /etc/prometheus
        - name: storage
          mountPath: /prometheus
        args:
        - '--config.file=/etc/prometheus/prometheus.yml'
        - '--storage.tsdb.path=/prometheus'
        - '--web.console.libraries=/etc/prometheus/console_libraries'
        - '--web.console.templates=/etc/prometheus/consoles'
      volumes:
      - name: config
        configMap:
          name: prometheus-config
      - name: storage
        emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: prometheus
  namespace: sinex
spec:
  selector:
    app: prometheus
  ports:
  - port: 9090
    targetPort: 9090
  type: LoadBalancer
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
  namespace: sinex
data:
  prometheus.yml: |
    global:
      scrape_interval: 15s

    scrape_configs:
    - job_name: 'sinex-collector'
      kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
          - sinex
      relabel_configs:
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: true
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
        action: replace
        target_label: __metrics_path__
        regex: (.+)
      - source_labels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_port]
        action: replace
        regex: ([^:]+)(?::\d+)?;(\d+)
        replacement: $1:$2
        target_label: __address__
```

### 5. Deploy to Kubernetes

```bash
# Apply all manifests
kubectl apply -f secrets.yaml
kubectl apply -f configmap.yaml
kubectl apply -f postgres-pvc.yaml
kubectl apply -f postgres-deployment.yaml
kubectl apply -f collector-deployment.yaml
kubectl apply -f prometheus-deployment.yaml

# Wait for postgres to be ready
kubectl wait --for=condition=ready pod -l app=postgres -n sinex --timeout=300s

# Apply database migrations
kubectl exec -n sinex deployment/sinex-collector -- \
  sinex-collector migrate --database-url $DATABASE_URL

# Check deployment status
kubectl get pods -n sinex
kubectl get services -n sinex

# View logs
kubectl logs -n sinex deployment/sinex-collector -f

# Test health endpoint
kubectl port-forward -n sinex service/sinex-collector 8080:8080 &
curl http://localhost:8080/health
```

## Environment-Specific Configurations

### Development
```toml
[database]
url = "postgresql:///sinex_dev?host=/run/postgresql"

[observability]
log_level = "debug"
metrics_port = 8080

[sources.filesystem]
watch_patterns = ["/tmp/sinex-test/**/*"]
max_events_per_second = 100
```

### Staging
```toml
[database]
url = "postgresql://sinex:password@staging-db:5432/sinex"
max_connections = 20

[observability]
log_level = "info"
metrics_port = 8080

[sources.filesystem]
watch_patterns = ["/home/testuser/documents/**/*"]
max_events_per_second = 500

[recovery]
dlq_base_path = "/var/lib/sinex/dlq"
```

### Production
```toml
[database]
url = "postgresql://sinex:password@prod-db:5432/sinex"
max_connections = 50
connection_timeout_ms = 5000

[observability]
log_level = "warn"
metrics_port = 8080

[sources.filesystem]
watch_patterns = [
  "/home/*/documents/**/*",
  "/var/log/**/*.log",
  "/etc/**/*"
]
max_events_per_second = 2000

[recovery]
dlq_base_path = "/var/lib/sinex/dlq"
max_retry_attempts = 5
circuit_breaker_threshold = 20
```

## Post-Deployment Verification

### Health Checks
```bash
# Basic health
curl http://localhost:8080/health

# Detailed component status
curl http://localhost:8080/health | jq '.components[]'

# Readiness check
curl http://localhost:8080/ready
```

### Metrics Verification
```bash
# Check metrics endpoint
curl http://localhost:8080/metrics | head -20

# Verify event processing
curl http://localhost:8080/metrics | grep sinex_events_processed_total

# Monitor error rates
curl http://localhost:8080/metrics | grep sinex_events_failed_total
```

### Database Verification
```bash
# Check agent registration
psql $DATABASE_URL -c "SELECT * FROM sinex_schemas.agent_manifests;"

# Verify event ingestion
psql $DATABASE_URL -c "SELECT COUNT(*) FROM raw.events WHERE ts_created > NOW() - INTERVAL '1 hour';"

# Check schema registration
psql $DATABASE_URL -c "SELECT id, source, event_type FROM sinex_schemas.event_payload_schemas;"
```

### Log Analysis
```bash
# Recent activity
journalctl -u sinex-collector --since "10 minutes ago"

# Error analysis
journalctl -u sinex-collector --priority err --since "1 hour ago"

# Performance monitoring
journalctl -u sinex-collector | grep "Processing time"
```

This deployment guide provides comprehensive instructions for setting up Sinex in any environment, from development to production-scale Kubernetes deployments. Each configuration is tested and includes verification steps to ensure successful deployment.