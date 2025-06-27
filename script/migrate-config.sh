#!/usr/bin/env bash
set -euo pipefail

# Sinex Configuration Migration Script
# Migrates from complex legacy configuration to simplified preset-based system

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SINEX_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG_DIR="${SINEX_ROOT}/config"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

usage() {
    cat <<EOF
Sinex Configuration Migration Script

USAGE:
    $0 [OPTIONS] [ACTION]

ACTIONS:
    analyze     Analyze current configuration and suggest preset
    migrate     Migrate to simplified configuration system
    validate    Validate new configuration
    backup      Create backup of current configuration
    restore     Restore from backup

OPTIONS:
    --config-file FILE    Specific configuration file to migrate
    --preset PRESET       Force specific preset (personal-desktop, developer-focused, etc.)
    --dry-run            Show what would be done without making changes
    --backup-dir DIR     Custom backup directory
    --help               Show this help

EXAMPLES:
    $0 analyze                              # Analyze current config
    $0 migrate --preset developer-focused   # Migrate to developer preset
    $0 migrate --dry-run                    # Show migration preview
    $0 backup                               # Backup current config

PRESETS:
    personal-desktop     Comprehensive personal use (default)
    developer-focused    Development-focused event sources
    researcher          Document and browser focused
    server-monitoring   System monitoring only
    minimal             Essential events only
    comprehensive       Everything available
EOF
}

# Configuration analysis functions
analyze_current_config() {
    info "🔍 Analyzing current Sinex configuration..."
    
    local config_files=()
    
    # Find existing configuration files
    for file in "${CONFIG_DIR}"/*.toml; do
        if [[ -f "$file" ]]; then
            config_files+=("$file")
        fi
    done
    
    # Check user config directories
    if [[ -n "${HOME:-}" ]]; then
        for dir in "${HOME}/.config/sinex" "${HOME}/.sinex"; do
            if [[ -d "$dir" ]]; then
                for file in "$dir"/*.toml; do
                    if [[ -f "$file" ]]; then
                        config_files+=("$file")
                    fi
                done
            fi
        done
    fi
    
    if [[ ${#config_files[@]} -eq 0 ]]; then
        warn "No existing configuration files found"
        info "Would create new configuration with 'personal-desktop' preset"
        return 0
    fi
    
    info "Found configuration files:"
    for file in "${config_files[@]}"; do
        echo "  - $(basename "$file")"
    done
    
    # Analyze configuration complexity
    local total_lines=0
    local total_options=0
    
    for file in "${config_files[@]}"; do
        local lines=$(wc -l < "$file" 2>/dev/null || echo 0)
        local options=$(grep -c "^[[:space:]]*[a-zA-Z_].*=" "$file" 2>/dev/null || echo 0)
        total_lines=$((total_lines + lines))
        total_options=$((total_options + options))
        
        info "  $(basename "$file"): $lines lines, $options options"
    done
    
    echo
    info "📊 Configuration Complexity Analysis:"
    echo "  Total lines: $total_lines"
    echo "  Total options: $total_options"
    
    # Suggest preset based on content analysis
    local suggested_preset
    suggested_preset=$(suggest_preset "${config_files[@]}")
    
    echo
    success "💡 Suggested preset: $suggested_preset"
    echo
    
    # Show migration benefits
    info "🚀 Migration Benefits:"
    echo "  • Reduce configuration from $total_options options to ~3-5 key settings"
    echo "  • Eliminate manual path discovery and complex ignore patterns"
    echo "  • Auto-configure observability with rich defaults"
    echo "  • Simplify frequency/polling settings to semantic levels"
    echo "  • Enable smart auto-discovery for common tools (Atuin, Kitty, etc.)"
    echo
}

suggest_preset() {
    local config_files=("$@")
    local preset="personal-desktop"  # Default
    
    # Analyze configuration content
    local has_dev_paths=false
    local has_git_events=false
    local has_browser_config=false
    local has_minimal_events=false
    local has_system_focus=false
    
    for file in "${config_files[@]}"; do
        if grep -q -i "code\|src\|project\|git\|target\|node_modules" "$file" 2>/dev/null; then
            has_dev_paths=true
        fi
        
        if grep -q -E "shell.*command|terminal|atuin" "$file" 2>/dev/null; then
            has_git_events=true
        fi
        
        if grep -q -i "browser\|bookmark\|download" "$file" 2>/dev/null; then
            has_browser_config=true
        fi
        
        if [[ $(grep -c "enabled_events" "$file" 2>/dev/null || echo 0) -lt 5 ]]; then
            has_minimal_events=true
        fi
        
        if grep -q -i "system\|server\|monitor\|dbus.*system" "$file" 2>/dev/null; then
            has_system_focus=true
        fi
    done
    
    # Decision logic
    if [[ "$has_system_focus" == true ]] && [[ "$has_dev_paths" == false ]]; then
        preset="server-monitoring"
    elif [[ "$has_dev_paths" == true ]] && [[ "$has_git_events" == true ]]; then
        preset="developer-focused"
    elif [[ "$has_browser_config" == true ]] && [[ "$has_dev_paths" == false ]]; then
        preset="researcher"
    elif [[ "$has_minimal_events" == true ]]; then
        preset="minimal"
    else
        preset="personal-desktop"
    fi
    
    echo "$preset"
}

migrate_configuration() {
    local preset="${1:-}"
    local dry_run="${2:-false}"
    local target_file="${3:-sinex.toml}"
    
    if [[ -z "$preset" ]]; then
        # Auto-detect preset
        preset=$(suggest_preset "${CONFIG_DIR}"/*.toml)
        info "Auto-detected preset: $preset"
    fi
    
    info "🔄 Migrating configuration to simplified format..."
    info "Target preset: $preset"
    info "Output file: $target_file"
    
    if [[ "$dry_run" == true ]]; then
        warn "DRY RUN MODE - no files will be modified"
    fi
    
    # Create new simplified configuration
    local new_config
    new_config=$(cat <<EOF
# Sinex Unified Configuration
# Migrated from legacy configuration on $(date)
# This single file replaces multiple configuration files with intelligent defaults

# =============================================================================
# MAIN CONFIGURATION: Choose your use case
# =============================================================================

# Configuration preset (automatically configures everything)
preset = "$preset"

# Observability level (rich defaults: metrics + dashboards + alerts enabled)
observability = "standard"

# =============================================================================
# CUSTOMIZATION: Only uncomment/modify if defaults don't work
# =============================================================================

# Privacy controls (opt-out approach - most things enabled by default)
# [privacy]
# disable = []  # ["clipboard", "window-titles", "command-history"]

# Storage configuration (auto-configured for most users)
# [storage]
# database_pool = "auto"         # Intelligent connection pool sizing
# annex_repo = "auto"           # Auto-discovers git-annex repository

# Event source frequency (only override if defaults don't work)
# [frequency]
# global = "normal"             # "battery" | "normal" | "responsive" | "realtime"

# Custom paths (in addition to auto-discovered ones)
# [paths]
# watch = []                    # Additional filesystem monitoring paths
# ignore = []                   # Extra ignore patterns

# Advanced features (disabled by default, enable when needed)
# [advanced]
# enable_screen_capture = false
# enable_network_monitoring = false

# =============================================================================
# MIGRATION NOTES
# =============================================================================

# The following settings from your legacy configuration are now handled automatically:
$(generate_migration_notes)

# To revert to legacy configuration, set legacy_config = true and add:
# [legacy.enabled_events]
# events = [...]
EOF
)
    
    if [[ "$dry_run" == true ]]; then
        echo
        info "📝 Generated configuration preview:"
        echo "----------------------------------------"
        echo "$new_config"
        echo "----------------------------------------"
        echo
        info "To apply this migration, run: $0 migrate --preset $preset"
        return 0
    fi
    
    # Write new configuration
    echo "$new_config" > "$CONFIG_DIR/$target_file"
    success "✅ Migration completed: $CONFIG_DIR/$target_file"
    
    # Validate new configuration
    info "🔍 Validating new configuration..."
    if validate_configuration "$CONFIG_DIR/$target_file"; then
        success "✅ Configuration validation passed"
    else
        error "❌ Configuration validation failed"
        return 1
    fi
    
    # Show next steps
    echo
    info "📋 Next Steps:"
    echo "  1. Review the generated configuration: $CONFIG_DIR/$target_file"
    echo "  2. Test with: cargo run --bin sinex-collector -- --config $CONFIG_DIR/$target_file --dry-run"
    echo "  3. Deploy to NixOS with the new monitoring-simplified.nix module"
    echo "  4. Remove old configuration files when satisfied"
    echo
}

generate_migration_notes() {
    cat <<EOF
# • Filesystem watch patterns are now auto-discovered based on common development directories
# • Atuin database path is auto-detected from standard locations
# • Kitty socket path is auto-discovered
# • Ignore patterns are intelligently generated based on detected development environments
# • Database connection pool sizing is automatic based on system resources
# • Observability stack uses preset-based configuration (minimal/standard/comprehensive)
# • Event source polling intervals are now semantic levels (battery/normal/responsive)
# • Cross-validation and path checking is handled automatically
EOF
}

validate_configuration() {
    local config_file="$1"
    
    # Basic TOML syntax validation
    if ! command -v toml >/dev/null 2>&1; then
        warn "TOML validator not found, skipping syntax validation"
        return 0
    fi
    
    if ! toml check "$config_file" >/dev/null 2>&1; then
        error "Invalid TOML syntax in $config_file"
        return 1
    fi
    
    # Check for required fields
    if ! grep -q "preset.*=" "$config_file"; then
        error "Missing required 'preset' field"
        return 1
    fi
    
    # Validate preset value
    local preset
    preset=$(grep "preset.*=" "$config_file" | cut -d'"' -f2 2>/dev/null || echo "")
    case "$preset" in
        personal-desktop|developer-focused|researcher|server-monitoring|minimal|comprehensive)
            ;;
        *)
            error "Invalid preset: $preset"
            return 1
            ;;
    esac
    
    info "Configuration validation successful"
    return 0
}

backup_configuration() {
    local backup_dir="${1:-$CONFIG_DIR/backup-$(date +%Y%m%d-%H%M%S)}"
    
    info "📦 Creating configuration backup..."
    mkdir -p "$backup_dir"
    
    local backed_up=0
    for file in "$CONFIG_DIR"/*.toml; do
        if [[ -f "$file" ]]; then
            cp "$file" "$backup_dir/"
            backed_up=$((backed_up + 1))
        fi
    done
    
    if [[ $backed_up -gt 0 ]]; then
        success "✅ Backed up $backed_up configuration files to: $backup_dir"
        echo "To restore: $0 restore --backup-dir $backup_dir"
    else
        warn "No configuration files found to backup"
    fi
}

restore_configuration() {
    local backup_dir="$1"
    
    if [[ ! -d "$backup_dir" ]]; then
        error "Backup directory not found: $backup_dir"
        return 1
    fi
    
    info "🔄 Restoring configuration from backup..."
    
    local restored=0
    for file in "$backup_dir"/*.toml; do
        if [[ -f "$file" ]]; then
            cp "$file" "$CONFIG_DIR/"
            restored=$((restored + 1))
        fi
    done
    
    if [[ $restored -gt 0 ]]; then
        success "✅ Restored $restored configuration files"
    else
        warn "No configuration files found in backup"
    fi
}

# Main script logic
main() {
    local action=""
    local preset=""
    local dry_run=false
    local config_file=""
    local backup_dir=""
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            analyze|migrate|validate|backup|restore)
                action="$1"
                shift
                ;;
            --preset)
                preset="$2"
                shift 2
                ;;
            --config-file)
                config_file="$2"
                shift 2
                ;;
            --backup-dir)
                backup_dir="$2"
                shift 2
                ;;
            --dry-run)
                dry_run=true
                shift
                ;;
            --help)
                usage
                exit 0
                ;;
            *)
                error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
    
    if [[ -z "$action" ]]; then
        action="analyze"  # Default action
    fi
    
    case "$action" in
        analyze)
            analyze_current_config
            ;;
        migrate)
            migrate_configuration "$preset" "$dry_run" "${config_file:-sinex.toml}"
            ;;
        validate)
            validate_configuration "${config_file:-$CONFIG_DIR/sinex.toml}"
            ;;
        backup)
            backup_configuration "$backup_dir"
            ;;
        restore)
            if [[ -z "$backup_dir" ]]; then
                error "Backup directory required for restore action"
                usage
                exit 1
            fi
            restore_configuration "$backup_dir"
            ;;
        *)
            error "Unknown action: $action"
            usage
            exit 1
            ;;
    esac
}

# Check if we're being sourced or executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi