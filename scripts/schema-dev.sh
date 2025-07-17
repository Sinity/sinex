#!/usr/bin/env bash
set -euo pipefail

# Development helper script for schema management
# Provides common schema-related tasks for local development

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Show usage
usage() {
    cat <<EOF
${CYAN}Sinex Schema Development Tool${NC}

Usage: $0 <command> [options]

Commands:
  ${GREEN}generate${NC}     Generate schemas from Rust structs
  ${GREEN}validate${NC}     Validate all schemas in schemas/ directory
  ${GREEN}deploy${NC}       Deploy schemas to local database
  ${GREEN}check${NC}        Check backward compatibility against master
  ${GREEN}diff${NC}         Show differences between generated and committed schemas
  ${GREEN}clean${NC}        Remove generated schemas (careful!)
  ${GREEN}stats${NC}        Show schema statistics
  
Examples:
  $0 generate              # Generate schemas from code
  $0 validate              # Validate all schemas
  $0 check                 # Check compatibility with master branch
  $0 deploy                # Deploy to local database
  $0 diff                  # Show uncommitted schema changes

EOF
}

# Change to project root
cd "$PROJECT_ROOT"

# Command: generate schemas
cmd_generate() {
    echo "🔨 Generating schemas from Rust structs..."
    
    if ! nix develop --command cargo run --package sinex-events --bin generate-schemas; then
        echo -e "${RED}❌ Schema generation failed${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✅ Schemas generated successfully${NC}"
    echo
    echo "Run '$0 diff' to see changes"
}

# Command: validate schemas
cmd_validate() {
    echo "🔍 Validating JSON schemas..."
    
    # Check if ajv is installed
    if ! command -v ajv &> /dev/null; then
        echo -e "${YELLOW}Installing ajv-cli...${NC}"
        npm install -g ajv-cli ajv-formats
    fi
    
    local count=0
    local errors=0
    
    find schemas -name "*.json" -type f | sort | while read -r schema; do
        ((count++)) || true
        echo -n "  Validating ${schema#schemas/}... "
        
        if ajv compile -s "$schema" --spec=draft7 --strict=false > /tmp/ajv_output.txt 2>&1; then
            echo -e "${GREEN}✓${NC}"
        else
            echo -e "${RED}✗${NC}"
            cat /tmp/ajv_output.txt | sed 's/^/    /'
            ((errors++)) || true
        fi
    done
    
    echo
    if [ "$errors" -eq 0 ]; then
        echo -e "${GREEN}✅ All $count schemas are valid${NC}"
    else
        echo -e "${RED}❌ Found $errors invalid schemas${NC}"
        exit 1
    fi
}

# Command: deploy to database
cmd_deploy() {
    echo "🚀 Deploying schemas to local database..."
    "$SCRIPT_DIR/deploy-schemas.sh"
}

# Command: check compatibility
cmd_check() {
    echo "🔄 Checking backward compatibility..."
    "$SCRIPT_DIR/check-schema-compatibility.sh" "${1:-master}"
}

# Command: show diff
cmd_diff() {
    echo "📊 Schema changes:"
    echo
    
    if git diff --quiet schemas/; then
        echo "No changes detected"
    else
        git diff --stat schemas/
        echo
        echo -e "${CYAN}Detailed changes:${NC}"
        git diff schemas/
    fi
}

# Command: clean generated schemas
cmd_clean() {
    echo -e "${YELLOW}⚠️  This will remove all generated schemas!${NC}"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        find schemas -name "*.json" -type f -delete
        find schemas -type d -empty -delete
        echo -e "${GREEN}✅ Schemas cleaned${NC}"
    else
        echo "Cancelled"
    fi
}

# Command: show statistics
cmd_stats() {
    echo "📈 Schema Statistics"
    echo "━━━━━━━━━━━━━━━━━━━━━"
    
    local total=$(find schemas -name "*.json" -type f | wc -l)
    echo "Total schemas: $total"
    echo
    
    echo "By category:"
    for dir in schemas/v*/*/; do
        if [ -d "$dir" ]; then
            local category=$(basename "$dir")
            local count=$(find "$dir" -name "*.json" -type f | wc -l)
            printf "  %-20s %3d\n" "$category:" "$count"
        fi
    done
    
    echo
    echo "Schema sizes:"
    find schemas -name "*.json" -type f -exec wc -l {} + | sort -n | tail -5 | while read -r lines file; do
        printf "  %-40s %4d lines\n" "$(basename "$file"):" "$lines"
    done
    
    # If connected to database, show deployment status
    if psql "${DATABASE_URL:-postgresql:///sinex_dev?host=/run/postgresql}" -c "SELECT 1" > /dev/null 2>&1; then
        echo
        echo "Database deployment status:"
        local deployed=$(psql "${DATABASE_URL:-postgresql:///sinex_dev?host=/run/postgresql}" -t -c "SELECT COUNT(DISTINCT schema_id) FROM sinex_schemas.schema_registry WHERE is_active = true" 2>/dev/null || echo "0")
        echo "  Active schemas in DB: $deployed"
    fi
}

# Main command dispatcher
case "${1:-help}" in
    generate|gen|g)
        cmd_generate
        ;;
    validate|val|v)
        cmd_validate
        ;;
    deploy|dep|d)
        cmd_deploy
        ;;
    check|c)
        cmd_check "${2:-}"
        ;;
    diff)
        cmd_diff
        ;;
    clean)
        cmd_clean
        ;;
    stats|stat|s)
        cmd_stats
        ;;
    help|h|--help|-h)
        usage
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        echo
        usage
        exit 1
        ;;
esac