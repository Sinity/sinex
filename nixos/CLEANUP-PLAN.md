# Sinex NixOS Module Cleanup & Refactoring Plan

This document outlines critical issues discovered in the NixOS module structure and the plan to address them.

## 🚨 Critical Issues Found

### 1. **Fixed: Broken Modular Structure** ✅
- **Issue**: `monitoring.nix` referenced undefined `cfg.directories` options
- **Status**: **FIXED** - Added missing directories options to `modules/default.nix`
- **Impact**: Modular structure is now functional

### 2. **Fixed: Security Vulnerabilities** ✅
- **Issue**: Complete lack of systemd security hardening
- **Status**: **FIXED** - Comprehensive systemd security hardening implemented
- **Details**:
  - Added syscall filtering (`SystemCallFilter`)
  - Filesystem isolation (`ProtectSystem=strict`, `ProtectHome=true`)
  - Privilege restrictions (`NoNewPrivileges`, `RestrictSUIDSGID`)
  - Private temp directories (`PrivateTmp=true`)
  - Restart rate limiting (`StartLimitBurst=3`, `StartLimitIntervalSec=60s`)
  - Resource limits on all services (`MemoryMax`, `TasksMax`, `IOWeight`)

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

### 5. **Fixed: Systemd Integration Issues** ✅  
- **Issue**: Incomplete dependencies, race conditions, inconsistent resource limits
- **Status**: **FIXED** - Proper service dependencies and resource limits
- **Details**:
  - Added proper service dependencies (`requires`, `wants`, `after`)
  - Network readiness waiting (`network-online.target`)
  - Consistent resource limits on all services
  - Fixed restart policies with rate limiting

## 📋 Remaining Tasks

### Phase 1: Credential Management (Optional) 🟡
Since we have agenix in sinnix, we could integrate that for DATABASE_URL:
```nix
# Instead of:
Environment = [ "DATABASE_URL=postgresql://..." ];

# Use agenix secret:
EnvironmentFile = config.age.secrets.sinex-database-url.path;
```

### Phase 2: Health Checks (Optional) 🟢
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