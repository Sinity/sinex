# Sinex Deployment Improvements - Specific Implementation Plan

## Overview

This document provides a concrete, actionable implementation plan for Sinex deployment improvements based on simplified, realistic requirements:

1. **Update Coordination** - Graceful shutdown, health checks, simple rollback
2. **Git Workflow** - Feature branches, manual merge, good hygiene (no complex automation)
3. **Unified Health Monitoring** - Heartbeat-based tracking, remove duplicate state files

**Total Timeline:** 7 days of focused development
**Approach:** 3 phases with independent rollback capability

---

## Phase 1: Foundation (Days 1-2)

**Goal:** Establish version tracking and enhanced heartbeat infrastructure

### 1.1 Git Workflow Documentation

**File:** `spec/docs/GIT_WORKFLOW.md` (new)

```markdown
# Git Workflow for Sinex

## Branch Strategy
- `master` - Main development branch
- `claude/YYYY-MM-DD-feature-name` - Agent feature branches  
- `feature/description` - Manual feature work
- `hotfix/issue-name` - Critical fixes

## Process
1. `git checkout master && git pull`
2. `git checkout -b claude/2024-01-15-health-endpoints`
3. Work in small, focused commits
4. `git push origin branch-name`
5. Create PR when ready (manual process)
6. Squash merge to master, delete branch

## Commit Standards
- Use conventional commits: `feat:`, `fix:`, `docs:`, `refactor:`
- Include context: "why" not just "what"
- Reference issues: `Fixes #123`

## Conflict Resolution
- Rebase on master before merge: `git rebase master`
- Use timestamp in branch names if coordination needed
- Keep features small to minimize conflicts
```

### 1.2 Build-Time Version Injection

**File:** `flake.nix` (update lines 40-62)

```nix
let
  # Extract git information for version tracking
  gitRev = self.rev or self.dirtyRev or "unknown";
  gitShortRev = builtins.substring 0 8 gitRev;
  version = "0.1.0"; # TODO: Extract from workspace
  buildTime = toString builtins.currentTime;
in
buildRustPackage = package: pkgs.rustPlatform.buildRustPackage {
  pname = package;
  version = version;
  src = ./.;
  cargoLock.lockFile = ./Cargo.lock;
  buildInputs = with pkgs; [ openssl dbus systemd ];
  nativeBuildInputs = with pkgs; [ pkg-config ];
  cargoBuildFlags = [ "-p" package ];
  auditable = false;
  doCheck = false;
  SQLX_OFFLINE = "true";
  
  # Inject version information at build time
  preBuild = ''
    if [ ! -d ".sqlx" ]; then
      echo "ERROR: .sqlx directory not found. Run 'cargo sqlx prepare' first."
      exit 1
    fi
    
    # Create build info for version tracking
    mkdir -p src/generated
    cat > src/generated/build_info.rs << EOF
    pub const VERSION: &str = "${version}";
    pub const GIT_HASH: &str = "${gitRev}";
    pub const GIT_SHORT_HASH: &str = "${gitShortRev}";
    pub const BUILD_TIME: &str = "${buildTime}";
    pub const BUILD_HOST: &str = "${builtins.currentSystem}";
    EOF
  '';
  
  postInstall = ''
    # Include migrations in the package
    mkdir -p $out/share/sinex
    cp -r migrations $out/share/sinex/
  '';
};
```

### 1.3 Enhanced Heartbeat Database Schema

**File:** `migrations/XXX_enhanced_heartbeats.sql` (new)

```sql
-- Enhanced heartbeat tracking for unified health monitoring
CREATE TABLE IF NOT EXISTS component_heartbeats (
    id ULID PRIMARY KEY,
    component_name TEXT NOT NULL,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    status TEXT NOT NULL CHECK (status IN ('healthy', 'degraded', 'failed')),
    
    -- Basic system metrics
    uptime_seconds BIGINT,
    memory_usage_mb INTEGER,
    cpu_usage_percent FLOAT,
    
    -- Component-specific metrics  
    events_processed_last_minute INTEGER DEFAULT 0,
    errors_last_hour INTEGER DEFAULT 0,
    last_error_message TEXT,
    
    -- Version tracking
    binary_version TEXT,
    git_hash TEXT,
    
    -- Extensible metrics storage
    metrics JSONB DEFAULT '{}'::jsonb
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_heartbeats_component_time 
ON component_heartbeats (component_name, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_heartbeats_recent 
ON component_heartbeats (timestamp DESC) 
WHERE timestamp > NOW() - INTERVAL '1 hour';

-- View for latest component status
CREATE OR REPLACE VIEW latest_component_health AS
SELECT DISTINCT ON (component_name)
    component_name,
    timestamp,
    status,
    uptime_seconds,
    memory_usage_mb,
    events_processed_last_minute,
    errors_last_hour,
    binary_version,
    git_hash
FROM component_heartbeats
ORDER BY component_name, timestamp DESC;
```

### Phase 1 Success Criteria
- [ ] Version info correctly appears in service builds (`nix build` includes git hash)
- [ ] Enhanced heartbeat table receives data within 1 minute of service start
- [ ] Git workflow successfully tested with sample feature branch
- [ ] Migration runs without errors and creates expected schema

---

## Phase 2: Health Consolidation (Days 3-5)

**Goal:** Replace state files with heartbeat-based health tracking

### 2.1 Service Heartbeat Integration

**Updates needed:** Add heartbeat emission to collector and worker services

### 2.2 Remove State Files from NixOS Module

**File:** `nixos/full.nix` (remove state file creation)

```nix
# REMOVE state file operations from service preStart
systemd.services.sinex-unified-collector = {
  preStart = ''
    mkdir -p ${cfg.directories.logs}
    mkdir -p ${cfg.directories.state}
    # State tracking now handled by heartbeat events, not files
  '';
};
```

### 2.3 Health Aggregation Service

**New crate:** `crate/sinex-health-aggregator/` with HTTP API for system health status

### Phase 2 Success Criteria
- [ ] Zero state files created after deployment
- [ ] Services emit heartbeats every 30-45 seconds
- [ ] Health aggregation service returns accurate system status
- [ ] Health API responds in <100ms

---

## Phase 3: Update Coordination (Days 6-7)

**Goal:** Implement graceful updates with health verification and rollback

### 3.1 Update Coordination Script

**File:** `script/update-sinex.sh` (new)

Key features:
- Graceful service shutdown
- Health verification via heartbeat API
- Automatic rollback on failure
- Structured logging throughout process

### 3.2 NixOS Service Dependencies

**File:** `nixos/full.nix` (add proper service dependencies)

```nix
systemd.services.sinex-unified-collector = {
  after = [ "postgresql.service" "network.target" ];
  requires = [ "postgresql.service" ];
  
  serviceConfig = {
    TimeoutStopSec = 30;
    KillMode = "mixed";
    KillSignal = "SIGTERM";
  };
};

systemd.services.sinex-promo-worker = {
  after = [ "postgresql.service" "sinex-unified-collector.service" ];
  requires = [ "postgresql.service" ];
  wants = [ "sinex-unified-collector.service" ];
};
```

### Phase 3 Success Criteria
- [ ] Update script completes successfully in <5 minutes
- [ ] Health checks correctly identify broken deployments
- [ ] Rollback mechanism completes in <2 minutes when needed
- [ ] Services start in correct dependency order

---

## Implementation Strategy

### Daily Breakdown

**Day 1:** Git workflow docs + version injection
**Day 2:** Heartbeat schema + basic structure
**Day 3:** Service heartbeat integration
**Day 4:** Remove state files + health aggregator
**Day 5:** Health system testing + integration
**Day 6:** Update script + service dependencies
**Day 7:** End-to-end testing + rollback validation

### Risk Mitigation

- Each phase independently revertible
- Existing systems remain during transition
- Extensive development environment testing
- Manual processes as fallback options

### Key Technical Decisions

1. **Heartbeat over state files** - Single source of truth for component status
2. **Health API aggregation** - Centralized health status endpoint
3. **Simple git workflow** - Manual merge process, no complex automation
4. **Graceful shutdown focus** - Proper signal handling and timeouts
5. **Basic rollback** - Simple version restoration without complex orchestration

---

## Success Metrics

### Deployment Reliability
- ✅ Deployments complete without manual intervention
- ✅ Failed deployments automatically rollback
- ✅ Update process completes reliably in <5 minutes
- ✅ Zero data loss during updates

### System Visibility
- ✅ Single source of truth for component health
- ✅ Real-time health monitoring via API
- ✅ Version tracking for all deployed components
- ✅ Clear audit trail for deployment events

### Operational Benefits
- **Reduced deployment risk** - Automatic failure detection and rollback
- **Better troubleshooting** - Rich health context and version tracking  
- **Simplified operations** - Coordinated update process
- **Clear status visibility** - Centralized health monitoring

This plan addresses your concerns about complexity while providing concrete, implementable improvements to Sinex deployment infrastructure.