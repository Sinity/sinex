#!/usr/bin/env bash

# Fully Automated Git History Squashing
# Based on comprehensive analysis of 781-commit history
# No user interaction required - just works

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
BACKUP_BRANCH="backup-before-squash-$(date +%Y%m%d-%H%M%S)"
LOG_FILE="/tmp/automated_squash_$(date +%Y%m%d-%H%M%S).log"

# Color output functions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $*" | tee -a "$LOG_FILE"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $*" | tee -a "$LOG_FILE"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $*" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${RED}[ERROR]${NC} $*" | tee -a "$LOG_FILE"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*" | tee -a "$LOG_FILE"
}

# High-confidence squash sequences from comprehensive analysis
declare -A SEQUENCES=(
    ["test_compilation_marathon"]="52e322a:92f5d12:18:fix: resolve comprehensive test suite compilation errors after major refactoring"
    ["test_infrastructure_migration"]="768f539:2a3dbd6:46:feat: implement comprehensive test infrastructure migration to sinex_test framework"  
    ["vm_test_infrastructure"]="287bf5e:20bef71:21:feat: implement comprehensive VM snapshot infrastructure for parallel testing"
    ["flake_database_refactor"]="e194d60:7e21511:19:feat: implement comprehensive flake and database infrastructure refactoring"
    ["test_automation_tooling"]="ecfd82a:6896409:17:feat: implement comprehensive test automation tooling for systematic error resolution"
    ["ulid_integration_fixes"]="577ee95:2452331:12:feat: implement comprehensive ULID/UUID type system integration"
    ["database_schema_migration"]="3a01870:86e86a9:8:feat: implement ULID-only primary key migration with TimescaleDB optimization"
)

check_prerequisites() {
    info "Checking prerequisites..."
    
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        error "Not in a git repository"
        exit 1
    fi
    
    if ! git diff-index --quiet HEAD --; then
        error "Working directory is not clean. Please commit or stash changes first."
        git status --porcelain
        exit 1
    fi
    
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    info "Current branch: $CURRENT_BRANCH"
    
    # Robust commit count check (allow ±50 commits)
    COMMIT_COUNT=$(git rev-list --count HEAD)
    info "Total commits in current branch: $COMMIT_COUNT"
    
    if [[ $COMMIT_COUNT -lt 731 || $COMMIT_COUNT -gt 831 ]]; then
        warn "Expected ~781 commits, found $COMMIT_COUNT. History may have changed significantly."
        warn "Continuing anyway - commit hash validation will catch issues."
    fi
    
    success "Prerequisites check passed"
}

create_backup() {
    info "Creating backup branch: $BACKUP_BRANCH"
    git branch "$BACKUP_BRANCH"
    git tag "pre-squash-$(date +%Y%m%d-%H%M%S)" HEAD
    success "Backup branch created: $BACKUP_BRANCH"
}

validate_commit_exists() {
    local commit="$1"
    if ! git rev-parse --verify "$commit" > /dev/null 2>&1; then
        return 1
    fi
    return 0
}

perform_squash() {
    local sequence_name="$1"
    local start_hash="$2"
    local end_hash="$3"
    local count="$4" 
    local message="$5"
    
    info "Squashing sequence: $sequence_name ($count commits)"
    info "Range: ${end_hash}..${start_hash}"
    
    # Validate commits exist
    if ! validate_commit_exists "$start_hash"; then
        warn "Start commit $start_hash not found, skipping $sequence_name"
        return 1
    fi
    
    if ! validate_commit_exists "$end_hash"; then
        warn "End commit $end_hash not found, skipping $sequence_name"
        return 1
    fi
    
    # Show what we're squashing
    info "Commits to be squashed:"
    git log --oneline --reverse "${end_hash}..${start_hash}" | tee -a "$LOG_FILE"
    
    # Create rebase script
    local parent_hash
    parent_hash=$(git rev-parse "${end_hash}~1")
    
    local rebase_script="/tmp/rebase_${sequence_name}_$$.txt"
    
    # Get commits in reverse order (oldest to newest for rebase)
    git log --format="%H" --reverse "${end_hash}..${start_hash}" > "/tmp/commits_${sequence_name}.txt"
    
    # Build rebase script: pick first, squash rest
    {
        local first_commit
        first_commit=$(head -n1 "/tmp/commits_${sequence_name}.txt")
        echo "pick $first_commit"
        
        tail -n+2 "/tmp/commits_${sequence_name}.txt" | while read -r commit; do
            echo "squash $commit"
        done
    } > "$rebase_script"
    
    # Perform automated rebase
    export GIT_SEQUENCE_EDITOR="cp $rebase_script"
    export GIT_EDITOR="echo '$message' > "
    
    if git rebase -i "$parent_hash"; then
        # Ensure commit message is set correctly
        if ! git log -1 --pretty=format:"%s" | grep -q "$(echo "$message" | head -c 20)"; then
            git commit --amend -m "$message"
        fi
        
        success "Squashed $sequence_name: $count commits → 1 commit"
        rm -f "$rebase_script" "/tmp/commits_${sequence_name}.txt"
        return 0
    else
        error "Failed to squash $sequence_name"
        git rebase --abort
        rm -f "$rebase_script" "/tmp/commits_${sequence_name}.txt"
        return 1
    fi
}

verify_build() {
    info "Verifying build..."
    
    if command -v nix > /dev/null; then
        if timeout 180 nix develop --command cargo check --workspace > /tmp/build_check.log 2>&1; then
            success "Build verification passed"
            return 0
        else
            error "Build verification failed"
            warn "Check /tmp/build_check.log for details"
            return 1
        fi
    else
        if timeout 180 cargo check --workspace > /tmp/build_check.log 2>&1; then
            success "Build verification passed (without nix)"
            return 0
        else
            error "Build verification failed"
            warn "Check /tmp/build_check.log for details"
            return 1
        fi
    fi
}

main() {
    info "Starting Automated Git History Squashing"
    info "Log file: $LOG_FILE"
    
    check_prerequisites
    create_backup
    
    local sequences_processed=0
    local sequences_failed=0
    local total_commits_squashed=0
    
    # Process sequences in order of safety (smallest/safest first)
    local sequence_order=(
        "database_schema_migration"     # 8 commits - smallest
        "ulid_integration_fixes"        # 12 commits
        "test_automation_tooling"       # 17 commits  
        "test_compilation_marathon"     # 18 commits - very safe
        "flake_database_refactor"       # 19 commits
        "vm_test_infrastructure"        # 21 commits
        "test_infrastructure_migration" # 46 commits - largest
    )
    
    for sequence_name in "${sequence_order[@]}"; do
        if [[ -v "SEQUENCES[$sequence_name]" ]]; then
            local sequence_data="${SEQUENCES[$sequence_name]}"
            local start_hash end_hash count message
            IFS=':' read -r start_hash end_hash count message <<< "$sequence_data"
            
            info "Processing sequence $((sequences_processed + 1))/7: $sequence_name"
            
            if perform_squash "$sequence_name" "$start_hash" "$end_hash" "$count" "$message"; then
                ((sequences_processed++))
                total_commits_squashed=$((total_commits_squashed + count - 1))
                
                # Verify build after each squash
                if ! verify_build; then
                    error "Build failed after squashing $sequence_name"
                    warn "You may need to investigate. Backup: $BACKUP_BRANCH"
                    exit 1
                fi
            else
                warn "Failed to process $sequence_name, continuing..."
                ((sequences_failed++))
            fi
        fi
    done
    
    # Final statistics
    local final_count=$(git log --oneline --no-merges | wc -l)
    local original_count=$((final_count + total_commits_squashed))
    local reduction_percent=$(( (total_commits_squashed * 100) / original_count ))
    
    echo
    success "=== SQUASHING COMPLETE ==="
    success "Sequences processed: $sequences_processed/7"
    success "Sequences failed: $sequences_failed"
    success "Commits squashed: $total_commits_squashed"
    success "Original commit count: $original_count"
    success "Final commit count: $final_count"
    success "History reduction: $reduction_percent%"
    success "Backup available: $BACKUP_BRANCH"
    success "Log file: $LOG_FILE"
    
    if [[ $sequences_failed -eq 0 ]]; then
        success "All sequences processed successfully!"
        info "Your git history has been optimized while preserving all meaningful development context."
    else
        warn "$sequences_failed sequences failed - check log for details"
    fi
    
    info "Run 'git log --oneline -20' to see the cleaned history"
}

# Initialize log
echo "=== Automated Git Squash Log - $(date) ===" > "$LOG_FILE"

main "$@"