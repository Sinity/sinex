# Sinex NixOS Module Cleanup & Refactoring Plan

This document outlines critical issues discovered in the NixOS module structure and the plan to address them.

## 🚨 Critical Issues Found

### 1. **Fixed: Broken Modular Structure** ✅
- **Issue**: `monitoring.nix` referenced undefined `cfg.directories` options
- **Status**: **FIXED** - Added missing directories options to `modules/default.nix`
- **Impact**: Modular structure is now functional

### 2. **Critical: Security Vulnerabilities** 🔴
- **Issue**: Complete lack of systemd security hardening
- **Impact**: Production deployment security risk
- **Details**:
  - Database credentials exposed in process lists
  - No syscall filtering, privilege restrictions, or filesystem isolation
  - All services run as same user with excessive permissions
  - No restart rate limiting (infinite restart loops possible)

### 3. **Missing: Comprehensive Testing** 🟡 → ✅
- **Issue**: No VM testing to verify module actually works
- **Status**: **FIXED** - Created comprehensive VM test suite
- **Added**:
  - Basic functionality tests (`vm-basic.nix`)
  - Preset validation tests (`vm-presets.nix`) 
  - Exclude patterns tests (`vm-exclude-patterns.nix`)
  - Test runner script (`run-tests.sh`)

### 4. **Technical Debt: Dual Implementation** 🔴
- **Issue**: Both `full.nix` (7,196 lines) and modular structure exist
- **Impact**: Maintenance burden, confusion, outdated examples
- **Status**: Migration 80% complete but not finalized

### 5. **Reliability: Systemd Integration Issues** 🔴
- **Issue**: Incomplete dependencies, race conditions, inconsistent resource limits
- **Impact**: Service reliability problems in production

## 📋 Immediate Action Plan

### Phase 1: Security Hardening (High Priority) 🔴

#### 1.1 Fix Credential Exposure
```nix
# Before (INSECURE):
Environment = [ "DATABASE_URL=postgresql://..." ];

# After (SECURE):
EnvironmentFile = "/etc/sinex/credentials.env";
```

#### 1.2 Add Systemd Security Hardening
Add to all services:
```nix
serviceConfig = {
  # Basic hardening
  PrivateTmp = true;
  ProtectSystem = "strict";
  ProtectHome = true;
  NoNewPrivileges = true;
  RestrictSUIDSGID = true;
  RemoveIPC = true;
  
  # Syscall filtering
  SystemCallFilter = [ "@system-service" "~@privileged" ];
  
  # Restart controls
  StartLimitIntervalSec = "60s";
  StartLimitBurst = 3;
};
```

#### 1.3 Fix Directory Permissions
```nix
# Change from 0755 to 0750 for sensitive directories
directories.permissions.state = "0750";
directories.permissions.logs = "0750";
```

### Phase 2: Reliability Improvements (High Priority) 🟡

#### 2.1 Fix Service Dependencies
```nix
after = [ "postgresql.service" "network-online.target" "sinex-git-annex-init.service" ];
wants = [ "network-online.target" ];
requires = [ "postgresql.service" ];
```

#### 2.2 Add Resource Limits to All Services
```nix
# Add to oneshot services:
MemoryMax = "256M";
TasksMax = 50;
IOWeight = 100;
```

#### 2.3 Implement Health Checks
- Add service readiness checks
- Implement graceful shutdown handling
- Add startup timeouts

### Phase 3: Technical Debt Cleanup (Medium Priority) 🟡

#### 3.1 Complete Migration from full.nix
- [ ] Deprecate `full.nix` with clear migration path
- [ ] Update all examples to use modular structure
- [ ] Remove `config-gen.nix` (or integrate utilities)
- [ ] Add validation that examples actually work

#### 3.2 Documentation Updates
- [ ] Update all documentation to reference modular structure
- [ ] Create migration guide for existing users
- [ ] Add troubleshooting guide for common issues

### Phase 4: Testing & Validation (Medium Priority) ✅

#### 4.1 VM Test Suite (COMPLETED)
- [x] Basic functionality tests
- [x] Preset validation tests  
- [x] Exclude patterns tests
- [x] Test runner automation

#### 4.2 Integration Testing
- [ ] Add tests for security hardening
- [ ] Add tests for service dependencies
- [ ] Add performance/resource limit tests
- [ ] Add upgrade/migration tests

### Phase 5: Advanced Improvements (Low Priority) 🟢

#### 5.1 Service Separation
- Split services by function for better isolation
- Create separate users for different components
- Implement principle of least privilege

#### 5.2 Configuration Validation
- Add comprehensive option validation
- Implement configuration generation utilities
- Add runtime configuration checks

#### 5.3 Monitoring & Observability
- Standardize health check endpoints
- Implement structured logging
- Add service dependency visualization

## 🎯 Success Criteria

### Security
- [ ] No credentials in process lists or logs
- [ ] All services hardened with systemd security features
- [ ] Directory permissions follow principle of least privilege
- [ ] Services restart safely with rate limiting

### Reliability  
- [ ] All service dependencies correct
- [ ] No race conditions during startup/shutdown
- [ ] Resource limits prevent resource exhaustion
- [ ] Services recover gracefully from failures

### Maintainability
- [ ] Single source of truth (remove `full.nix`)
- [ ] All examples work and are tested
- [ ] Documentation is current and accurate
- [ ] VM tests validate all functionality

### Production Readiness
- [ ] Services start reliably in production
- [ ] Performance is predictable and bounded
- [ ] Failures are contained and recoverable
- [ ] Monitoring and alerting work correctly

## 📊 Priority Matrix

| Issue | Impact | Effort | Priority |
|-------|--------|---------|----------|
| Security Hardening | High | Medium | **P0** |
| Credential Exposure | High | Low | **P0** |
| Service Dependencies | High | Low | **P1** |
| Resource Limits | Medium | Low | **P1** |
| Remove full.nix | Medium | Medium | **P2** |
| Documentation | Low | Medium | **P3** |

## 🚀 Next Steps

1. **Immediate (This Week)**: Implement security hardening and fix credential exposure
2. **Short Term (Next Sprint)**: Fix service dependencies and add resource limits  
3. **Medium Term (Next Month)**: Complete migration from full.nix and update documentation
4. **Long Term (Next Quarter)**: Advanced improvements and monitoring

## 📝 Implementation Notes

- Each phase should include testing to verify improvements
- Security changes should be tested in VM environment first
- Migration should maintain backward compatibility where possible
- All changes should be documented with examples

---

**Status**: Plan created, Phase 1 ready to begin
**Owner**: Development team
**Review Date**: Weekly during active phases