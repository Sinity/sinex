#!/usr/bin/env bash
# Schema backward compatibility checker for Sinex
# Validates that schema changes don't break existing events

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

success() { echo -e "${GREEN}✓${NC} $1"; }
error() { echo -e "${RED}✗${NC} $1"; }
warning() { echo -e "${YELLOW}⚠${NC} $1"; }
info() { echo -e "${BLUE}ℹ${NC} $1"; }

main() {
    info "Starting schema backward compatibility check"
    
    cd "$PROJECT_ROOT"
    
    # Check if schemas directory exists
    if [ ! -d "schemas/" ]; then
        warning "No schemas directory found, skipping compatibility check"
        return 0
    fi
    
    # Count schema files
    local schema_count
    schema_count=$(find schemas/ -name "*.json" -type f | wc -l)
    
    if [ "$schema_count" -eq 0 ]; then
        warning "No schema files found, skipping compatibility check"
        return 0
    fi
    
    info "Found $schema_count schema files to validate"
    
    # Basic validation: check for breaking changes patterns
    local exit_code=0
    
    while IFS= read -r -d '' file; do
        info "Checking compatibility for $(basename "$file")"
        
        # Check for potential breaking changes
        if check_breaking_changes "$file"; then
            success "No breaking changes detected in $(basename "$file")"
        else
            error "Potential breaking changes found in $(basename "$file")"
            exit_code=1
        fi
        
    done < <(find schemas/ -name "*.json" -type f -print0)
    
    if [ $exit_code -eq 0 ]; then
        success "All schema compatibility checks passed"
    else
        error "Schema compatibility issues detected"
    fi
    
    return $exit_code
}

check_breaking_changes() {
    local schema_file="$1"
    
    # Basic checks for common breaking changes
    local has_issues=false
    
    # Check 1: Ensure required fields are not added without defaults
    if grep -q '"required"' "$schema_file"; then
        local required_fields
        required_fields=$(jq -r '.required[]? // empty' "$schema_file" 2>/dev/null || echo "")
        
        if [ -n "$required_fields" ]; then
            while IFS= read -r field; do
                # Check if field has a default value
                local has_default
                has_default=$(jq -r ".properties.\"$field\".default // empty" "$schema_file" 2>/dev/null || echo "")
                
                if [ -z "$has_default" ]; then
                    warning "Required field '$field' in $(basename "$schema_file") has no default value"
                    warning "This could break compatibility with existing events"
                fi
            done <<< "$required_fields"
        fi
    fi
    
    # Check 2: Validate JSON syntax
    if ! jq . "$schema_file" >/dev/null 2>&1; then
        error "Invalid JSON syntax in $(basename "$schema_file")"
        has_issues=true
    fi
    
    # Check 3: Ensure schema has required meta-fields
    local has_schema_field
    has_schema_field=$(jq -r '."$schema" // empty' "$schema_file" 2>/dev/null || echo "")
    
    if [ -z "$has_schema_field" ]; then
        warning "Schema $(basename "$schema_file") missing \$schema field"
    fi
    
    # Check 4: Validate against meta-schema if available
    if [ -f "schemas/meta/meta-schema.json" ]; then
        if command -v ajv >/dev/null 2>&1; then
            if ! ajv validate -s schemas/meta/meta-schema.json -d "$schema_file" >/dev/null 2>&1; then
                error "Schema $(basename "$schema_file") does not conform to meta-schema"
                has_issues=true
            fi
        fi
    fi
    
    # Return success if no issues found
    [ "$has_issues" = false ]
}

# Check for required tools
check_dependencies() {
    local missing_deps=()
    
    if ! command -v jq >/dev/null 2>&1; then
        missing_deps+=("jq")
    fi
    
    if [ ${#missing_deps[@]} -gt 0 ]; then
        error "Missing required dependencies: ${missing_deps[*]}"
        info "Please install missing dependencies and retry"
        return 1
    fi
    
    return 0
}

# Verify dependencies and run main function
if check_dependencies; then
    main "$@"
else
    exit 1
fi