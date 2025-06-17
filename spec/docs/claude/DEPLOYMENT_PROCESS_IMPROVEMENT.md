# Sinex Deployment Process Improvement Plan

## Overview

This document outlines improvements to the Sinex deployment process based on analysis of current implementation gaps and operational requirements. The plan addresses git-annex configuration, deployment idempotency, update coordination, versioning, git workflow, and health monitoring.

## 1. Git-Annex Configuration Improvements

### Current Issues
- **Forced default path**: `${cfg.directories.state}/annex` assumes user wants blob storage on system partition
- **No storage validation**: Could fill up small partitions unexpectedly
- **Services exist but may be unnecessary**: Separate services for GC, fsck, sync when timers might suffice
- **Primitive API**: Event sources get raw repository path instead of proper blob management API

### Proposed Changes

#### 1.1 Require Explicit Path Configuration
```nix
# BEFORE (problematic)
repositoryPath = mkOption {
  default = "${cfg.directories.state}/annex";
};

# AFTER (safe)
repositoryPath = mkOption {
  type = types.nullOr types.path;
  default = null;
  example = "/data/sinex/annex";
  description = ''
    Path to git-annex repository for large file storage.
    
    WARNING: This can consume significant disk space. Choose a partition
    with adequate free space or disable blobStorage entirely.
    
    Recommended locations:
    - /var/lib/sinex/annex (small files, system partition)
    - /data/sinex/annex (dedicated large partition)
    - /mnt/storage/sinex (external storage)
    
    If not set and blobStorage.enable=true, deployment will fail with
    descriptive error message.
  '';
};

# Required assertion
assertions = [
  {
    assertion = !cfg.blobStorage.enable || cfg.blobStorage.repositoryPath != null;
    message = ''
      Blob storage is enabled but no repository path specified.
      
      Set: services.sinex.blobStorage.repositoryPath = "/path/to/storage";
      
      Choose a location with adequate free space for large files.
      Consider disk usage patterns and backup strategies.
    '';
  }
];
```

#### 1.2 Consolidate Git-Annex Services
Current approach has separate services for each operation. Simplify to:

```nix
# Single maintenance service with configurable operations
systemd.services.sinex-annex-maintenance = {
  description = "Git-annex repository maintenance";
  script = ''
    cd "${cfg.blobStorage.repositoryPath}"
    
    ${lib.optionalString cfg.blobStorage.maintenance.enableGc ''
      echo "Running garbage collection..."
      git-annex unused | head -1000 | git-annex dropunused --force
    ''}
    
    ${lib.optionalString cfg.blobStorage.maintenance.enableFsck ''
      echo "Running repository check..."
      git-annex fsck --fast
    ''}
    
    ${lib.optionalString cfg.blobStorage.maintenance.enableSync ''
      echo "Syncing with remotes..."
      git-annex sync --content
    ''}
  '';
};

# Single timer with configurable schedule
systemd.timers.sinex-annex-maintenance = {
  timerConfig = {
    OnCalendar = cfg.blobStorage.maintenance.schedule; # "weekly"
    Persistent = true;
    RandomizedDelaySec = "4h";
  };
};
```

#### 1.3 Proper Blob Management API
Replace raw path exposure with proper API:

```rust
// In sinex-core/src/blob_manager.rs
pub struct BlobManager {
    repo_path: PathBuf,
    git_annex: GitAnnexRepo,
}

impl BlobManager {
    pub async fn store_content(&self, content: &[u8], filename: &str) -> Result<BlobKey> {
        let temp_file = self.write_temp_file(content, filename).await?;
        let key = self.git_annex.add_file(&temp_file).await?;
        Ok(BlobKey::new(key))
    }
    
    pub async fn retrieve_content(&self, key: &BlobKey) -> Result<Vec<u8>> {
        self.git_annex.get_content(key.as_str()).await
    }
    
    pub async fn store_file(&self, file_path: &Path) -> Result<BlobKey> {
        self.git_annex.add_file(file_path).await.map(BlobKey::new)
    }
    
    pub fn get_file_path(&self, key: &BlobKey) -> PathBuf {
        self.repo_path.join(".git/annex/objects").join(key.to_path())
    }
}

// Update EventSourceContext
pub struct EventSourceContext {
    pub db_pool: Option<PgPool>,
    pub config: Value,
    pub blob_manager: Option<Arc<BlobManager>>,  // Instead of raw path
}
```

## 2. Deployment Idempotency Improvements

### Current State Analysis
- **Database**: ✅ Idempotent with PL/pgSQL existence checks
- **Git-annex**: ✅ Idempotent with `.git` directory check
- **SystemD**: ✅ Idempotent via `RemainAfterExit=true`
- **State tracking**: ❌ Basic file-based, should be unified with heartbeat

### Proposed Improvements

#### 2.1 Unified State Tracking
Replace separate state files with heartbeat-based tracking:

```rust
// Single source of truth for component state
pub struct ComponentState {
    pub name: String,
    pub status: ComponentStatus,
    pub last_heartbeat: DateTime<Utc>,
    pub configuration_hash: String,  // Detect config changes
    pub deployment_version: String,  // Track deployment versions
    pub health_metrics: ComponentMetrics,
}

// Store in database, not files
CREATE TABLE component_states (
    component_name TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    last_heartbeat TIMESTAMPTZ NOT NULL,
    configuration_hash TEXT,
    deployment_version TEXT,
    health_metrics JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

#### 2.2 Configuration Change Detection
```nix
# Generate configuration hash for change detection
configurationHash = pkgs.runCommand "sinex-config-hash" {} ''
  echo '${builtins.hashString "sha256" (builtins.toJSON cfg)}' > $out
'';

# Include in service environment
environment.SINEX_CONFIG_HASH = builtins.readFile configurationHash;
```

## 3. Update Coordination System

### Deployment Strategies

#### 3.1 Immediate Deployment (Current)
- **How it works**: Stop all services, upgrade, restart
- **Pros**: Simple, fast, guaranteed consistency
- **Cons**: Downtime during upgrade
- **Use case**: Development, small deployments

#### 3.2 Rolling Deployment
- **How it works**: Update services one-by-one, maintain minimum running count
- **Pros**: No downtime, gradual rollout
- **Cons**: Complex state management, partial upgrade states
- **Use case**: Production with redundancy

#### 3.3 Blue-Green Deployment
- **How it works**: Run complete parallel environment, switch traffic atomically
- **Pros**: Zero downtime, instant rollback, full validation before switch
- **Cons**: Double resource usage, complex traffic routing
- **Use case**: Critical production systems

### Update Coordinator Implementation

```nix
services.sinex.deployment = {
  strategy = mkOption {
    type = types.enum ["immediate" "rolling" "blue-green"];
    default = "immediate";
    description = "Deployment strategy";
  };
  
  coordinator = {
    enable = mkOption {
      type = types.bool;
      default = true;
      description = "Enable deployment coordinator service";
    };
    
    healthCheckTimeout = mkOption {
      type = types.int;
      default = 300;
      description = "Seconds to wait for services to become healthy";
    };
    
    rollbackOnFailure = mkOption {
      type = types.bool;
      default = true;
      description = "Automatically rollback on deployment failure";
    };
    
    logLevel = mkOption {
      type = types.enum ["debug" "info" "warn" "error"];
      default = "info";
      description = "Coordinator log level";
    };
  };
};

# Update coordinator service
systemd.services.sinex-update-coordinator = {
  description = "Sinex Update Coordinator";
  script = ''
    ${cfg.package}/bin/sinex-update-coordinator \
      --strategy ${cfg.deployment.strategy} \
      --timeout ${toString cfg.deployment.coordinator.healthCheckTimeout} \
      --log-level ${cfg.deployment.coordinator.logLevel} \
      ${lib.optionalString cfg.deployment.coordinator.rollbackOnFailure "--rollback-on-failure"}
  '';
};
```

#### Update Coordinator Process
```rust
// In sinex-update-coordinator binary
pub struct UpdateCoordinator {
    strategy: DeploymentStrategy,
    health_checker: HealthChecker,
    logger: StructuredLogger,
}

impl UpdateCoordinator {
    pub async fn coordinate_update(&self) -> Result<UpdateResult> {
        self.logger.info("Starting update coordination", json!({
            "strategy": self.strategy,
            "timestamp": Utc::now(),
        }));
        
        // 1. Pre-update validation
        self.validate_system_health().await?;
        self.backup_current_state().await?;
        
        // 2. Execute strategy-specific update
        let result = match self.strategy {
            DeploymentStrategy::Immediate => self.immediate_update().await,
            DeploymentStrategy::Rolling => self.rolling_update().await,
            DeploymentStrategy::BlueGreen => self.blue_green_update().await,
        };
        
        // 3. Post-update validation and logging
        match result {
            Ok(success) => {
                self.logger.info("Update completed successfully", json!(success));
                Ok(success)
            },
            Err(error) => {
                self.logger.error("Update failed", json!({"error": error.to_string()}));
                if self.rollback_on_failure {
                    self.rollback().await?;
                }
                Err(error)
            }
        }
    }
}
```

## 4. Version Attribution & Commit Hash Tracking

### Implementation Plan

#### 4.1 Build-Time Version Injection
```nix
# In flake.nix, extract git information
let
  gitRev = self.rev or self.dirtyRev or "unknown";
  gitShortRev = builtins.substring 0 8 gitRev;
  buildTimestamp = toString builtins.currentTime;
  
  # Generate version info at build time
  versionInfo = pkgs.writeText "version-info.json" (builtins.toJSON {
    version = workspace.package.version;
    git_hash = gitRev;
    git_short_hash = gitShortRev;
    build_time = buildTimestamp;
    build_host = builtins.currentSystem;
  });
in
buildRustPackage = package: pkgs.rustPlatform.buildRustPackage {
  # ... existing config ...
  
  # Inject version info
  preBuild = ''
    mkdir -p src/generated
    cp ${versionInfo} src/generated/version_info.json
    
    # Generate Rust constants
    cat > src/generated/version.rs << EOF
    pub const VERSION: &str = "${workspace.package.version}";
    pub const GIT_HASH: &str = "${gitRev}";
    pub const GIT_SHORT_HASH: &str = "${gitShortRev}";
    pub const BUILD_TIME: &str = "${buildTimestamp}";
    pub const BUILD_HOST: &str = "${builtins.currentSystem}";
    EOF
  '';
};
```

#### 4.2 Runtime Version Tracking
```rust
// Add version info to all events
pub struct RawEvent {
    // ... existing fields ...
    pub collector_version: String,      // "0.4.2"
    pub collector_git_hash: String,     // "a1b2c3d4..."
    pub collector_build_time: DateTime<Utc>,
}

// Version info endpoint
#[get("/version")]
async fn get_version_info() -> Json<VersionInfo> {
    Json(VersionInfo {
        version: VERSION,
        git_hash: GIT_HASH,
        git_short_hash: GIT_SHORT_HASH,
        build_time: BUILD_TIME.parse().unwrap(),
        build_host: BUILD_HOST,
        uptime: get_uptime(),
    })
}

// Include in heartbeat
pub struct ComponentHeartbeat {
    // ... existing fields ...
    pub version_info: VersionInfo,
}
```

#### 4.3 Deployment Version Tracking
```sql
-- Track deployments in database
CREATE TABLE deployments (
    id ULID PRIMARY KEY,
    version TEXT NOT NULL,
    git_hash TEXT NOT NULL,
    deployment_time TIMESTAMPTZ DEFAULT NOW(),
    deployment_strategy TEXT NOT NULL,
    coordinator_version TEXT,
    rollback_deployment_id ULID REFERENCES deployments(id),
    status TEXT NOT NULL, -- 'in_progress', 'completed', 'failed', 'rolled_back'
    metadata JSONB
);
```

## 5. Git Workflow & Branch Protection

### Proposed Minimal Workflow

Given constraints:
- Branch is `master` (not `main`)
- Coding agents are anonymous
- Need coordination between multiple agents

#### 5.1 Branch Structure
```
master                    # Protected main branch
├── claude/feature-1      # Agent feature branches
├── claude/feature-2      # Each agent works on separate branch
└── claude/feature-3      # Merge to master when ready
```

#### 5.2 Branch Protection Rules
```yaml
# .github/branch-protection.yml (manual setup)
protection_rules:
  master:
    required_status_checks:
      - ci/nix-build
      - ci/tests-pass
      - ci/integration-tests
    required_reviews: 0           # Agents are anonymous
    enforce_admins: false
    allow_force_pushes: false
    dismiss_stale_reviews: true
    
  "claude/*":
    required_status_checks:
      - ci/nix-build
      - ci/tests-pass
    required_reviews: 0
    allow_force_pushes: true      # Agents can force push to their branches
```

#### 5.3 Agent Coordination Protocol
```bash
# Agent workflow
1. git checkout master
2. git pull origin master
3. git checkout -b claude/descriptive-feature-name
4. # Work on feature with regular commits
5. git push origin claude/descriptive-feature-name
6. # Create PR when ready (auto-merge if tests pass)
7. # Delete branch after merge

# Coordination mechanism
- Agents check for conflicting branches before starting
- Use descriptive branch names to avoid conflicts
- Regular rebasing on master to avoid drift
```

#### 5.4 Automated Merge Process
```yaml
# .github/workflows/auto-merge.yml
name: Auto-merge Claude PRs
on:
  pull_request:
    branches: [master]
    
jobs:
  auto-merge:
    if: startsWith(github.head_ref, 'claude/')
    runs-on: ubuntu-latest
    steps:
      - name: Auto-merge if tests pass
        uses: pascalgn/merge-action@v0.15.6
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          merge_method: squash
          merge_commit_message: pull-request-title-and-description
```

## 6. Unified Health & Heartbeat System

### Current Problems
- Duplicate tracking: state files + heartbeat events
- No integration with logs
- Missing components from health checks
- Unclear what should emit heartbeats

### Proposed Unified System

#### 6.1 Single Heartbeat-Based State Tracking
```rust
// Replace all state files with heartbeat events
pub struct ComponentHeartbeat {
    pub component: String,              // "unified-collector", "promo-worker"
    pub status: HealthStatus,           // Healthy/Degraded/Failed
    pub timestamp: DateTime<Utc>,
    pub version_info: VersionInfo,      // Version tracking
    pub metrics: ComponentMetrics,      // Memory, CPU, events/min
    pub capabilities: Vec<String>,      // Active event sources, enabled features
    pub last_error: Option<ErrorInfo>,  // Error details if degraded
    pub log_summary: LogSummary,        // Recent log analysis
}

// Rich error context
pub struct ErrorInfo {
    pub message: String,
    pub error_type: String,
    pub timestamp: DateTime<Utc>,
    pub context: serde_json::Value,
    pub recovery_attempted: bool,
}

// Log integration
pub struct LogSummary {
    pub error_count_last_hour: u32,
    pub warning_count_last_hour: u32,
    pub pattern_matches: Vec<String>,   // Known error patterns
    pub anomalies: Vec<String>,         // Unusual log patterns
}
```

#### 6.2 Component Health Determination
```rust
// What components should emit heartbeats
pub fn get_expected_components(config: &SinexConfig) -> Vec<ComponentExpectation> {
    vec![
        ComponentExpectation {
            name: "unified-collector".to_string(),
            enabled: config.unified_collector.enable,
            heartbeat_interval: Duration::from_secs(30),
            critical: true,  // System cannot function without this
        },
        ComponentExpectation {
            name: "promo-worker".to_string(),
            enabled: config.promo_worker.enable,
            heartbeat_interval: Duration::from_secs(60),
            critical: true,  // Event processing stops without this
        },
        ComponentExpectation {
            name: "git-annex".to_string(),
            enabled: config.blob_storage.enable,
            heartbeat_interval: Duration::from_secs(300),
            critical: false,  // Large files fail, but system continues
        },
        ComponentExpectation {
            name: "database".to_string(),
            enabled: config.database.auto_setup,
            heartbeat_interval: Duration::from_secs(60),
            critical: true,  // Nothing works without database
        },
    ]
}
```

#### 6.3 EventSource Health (Part of Collector)
```rust
// EventSources don't get separate heartbeats
// Instead, collector heartbeat includes source health
impl UnifiedCollector {
    async fn emit_heartbeat(&self) -> Result<()> {
        let heartbeat = ComponentHeartbeat {
            component: "unified-collector".to_string(),
            status: self.determine_overall_health(),
            metrics: self.collect_metrics(),
            capabilities: self.get_capabilities(),
            event_source_health: self.collect_source_health(), // ← EventSource health here
            // ...
        };
        
        self.store_heartbeat(&heartbeat).await
    }
    
    fn collect_source_health(&self) -> HashMap<String, EventSourceHealth> {
        self.event_sources.iter().map(|(name, source)| {
            let health = EventSourceHealth {
                status: source.get_health_status(),
                events_last_minute: source.get_recent_event_count(),
                last_successful_event: source.get_last_success_time(),
                last_error: source.get_last_error(),
            };
            (name.clone(), health)
        }).collect()
    }
}
```

#### 6.4 Log-Integrated Health Analysis
```rust
// Health status considers both metrics and logs
impl ComponentHeartbeat {
    pub fn determine_health_status(
        metrics: &ComponentMetrics,
        logs: &[LogEntry],
        last_errors: &[ErrorInfo]
    ) -> HealthStatus {
        // Analyze recent logs for patterns
        let error_count = logs.iter().filter(|l| l.level == "ERROR").count();
        let critical_patterns = logs.iter().any(|l| {
            l.message.contains("database connection failed") ||
            l.message.contains("out of memory") ||
            l.message.contains("disk full")
        });
        
        // Combine metrics and log analysis
        match (error_count, critical_patterns, metrics.memory_usage_percent) {
            (0, false, usage) if usage < 80.0 => HealthStatus::Healthy,
            (1..=5, false, usage) if usage < 90.0 => HealthStatus::Degraded,
            (_, true, _) => HealthStatus::Failed,
            (6.., _, _) => HealthStatus::Failed,
            (_, _, usage) if usage > 95.0 => HealthStatus::Failed,
            _ => HealthStatus::Degraded,
        }
    }
}
```

## 7. Implementation Timeline

### Phase 1: Git-Annex Configuration Fixes (1-2 days)
- ✅ Remove default repository path, require explicit configuration
- ✅ Add descriptive error messages and validation
- ✅ Implement BlobManager API to replace raw path access
- ✅ Consolidate git-annex services into single maintenance service

### Phase 2: Version Attribution (1 day)
- ✅ Implement build-time version injection in flake.nix
- ✅ Add version info to RawEvent structure
- ✅ Create version info endpoint for services
- ✅ Add deployment tracking table

### Phase 3: Unified Health System (2-3 days)
- ✅ Replace state files with heartbeat-based tracking
- ✅ Implement log-integrated health analysis
- ✅ Add EventSource health to collector heartbeat
- ✅ Create system health aggregation logic

### Phase 4: Update Coordinator (2-3 days)
- ✅ Implement update coordinator service
- ✅ Add deployment strategy configuration
- ✅ Create rollback mechanism
- ✅ Add structured logging throughout update process

### Phase 5: Git Workflow Setup (1 day)
- ✅ Configure branch protection rules
- ✅ Set up auto-merge for claude/* branches
- ✅ Document agent coordination protocol
- ✅ Test multi-agent workflow

## 8. Success Criteria

### Deployment Reliability
- ✅ Git-annex repository path must be explicitly configured
- ✅ All deployment operations are idempotent
- ✅ Failed deployments automatically rollback
- ✅ Clear error messages guide operators to solutions

### Version Tracking
- ✅ Every event includes version and git hash attribution
- ✅ Runtime version info available via API endpoints
- ✅ Deployment history tracked in database
- ✅ Easy correlation between issues and deployment versions

### Health Monitoring
- ✅ Single source of truth for component health
- ✅ Integration between logs and health status
- ✅ Clear distinction between expected and actual components
- ✅ Rich context when health status changes

### Operational Excellence
- ✅ Update coordinator provides structured logging
- ✅ Multiple deployment strategies available
- ✅ Git workflow accommodates multiple anonymous agents
- ✅ Self-documenting system status and capabilities

This plan addresses all identified issues while maintaining backward compatibility and providing clear upgrade paths for existing deployments.