#!/usr/bin/env bash
#
# Sinex Test Automation Pipeline
# 
# Runs all standard code transformations in the correct order
# with verification at each step.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "🚀 Sinex Test Automation Pipeline"
echo "=================================="
echo "Project root: ${PROJECT_ROOT}"
echo ""

cd "${PROJECT_ROOT}"

# Verification function
verify_compilation() {
    echo "🧪 Verifying compilation..."
    if cargo check --workspace >/dev/null 2>&1; then
        echo "✅ Compilation successful"
        return 0
    else
        echo "❌ Compilation failed"
        cargo check --workspace
        return 1
    fi
}

# Initial verification
echo "📋 Step 0: Initial verification"
verify_compilation || {
    echo "💥 Initial compilation failed. Fix errors before running automation."
    exit 1
}

# Step 1: TempDir migration
echo ""
echo "📋 Step 1: TempDir migration"
echo "Converting TempDir::new().unwrap() to proper error handling..."

if command -v ast-grep >/dev/null 2>&1; then
    ast-grep run -c "${SCRIPT_DIR}/ast-grep/tempdir-migration.yml" test/ -U
    echo "✅ TempDir migration complete"
else
    echo "⚠ ast-grep not found, skipping TempDir migration"
fi

verify_compilation || {
    echo "💥 TempDir migration broke compilation"
    exit 1
}

# Step 2: EventSourceContext consolidation
echo ""
echo "📋 Step 2: EventSourceContext consolidation" 
echo "Converting EventSourceContext::new() to test helpers..."

if command -v ast-grep >/dev/null 2>&1; then
    ast-grep run -c "${SCRIPT_DIR}/ast-grep/eventsource-context.yml" test/ -U
    echo "✅ EventSourceContext consolidation complete"
else
    echo "⚠ ast-grep not found, skipping EventSourceContext consolidation"
fi

verify_compilation || {
    echo "💥 EventSourceContext consolidation broke compilation"
    exit 1
}

# Step 3: RawEvent pattern consolidation
echo ""
echo "📋 Step 3: RawEvent pattern consolidation"
echo "Converting manual RawEvent constructions to builders..."

if command -v ast-grep >/dev/null 2>&1; then
    ast-grep run -c "${SCRIPT_DIR}/ast-grep/rawevent-patterns.yml" test/ -U
    echo "✅ RawEvent pattern consolidation complete"
else
    echo "⚠ ast-grep not found, skipping RawEvent consolidation"
fi

verify_compilation || {
    echo "💥 RawEvent consolidation broke compilation"
    exit 1
}

# Step 4: Add missing Ok(()) returns
echo ""
echo "📋 Step 4: Ok(()) return insertion"
echo "Adding missing Ok(()) returns to Result functions..."

if command -v python3 >/dev/null 2>&1; then
    python3 "${SCRIPT_DIR}/python-scripts/ok-return-fixer.py" test/
    echo "✅ Ok(()) return insertion complete"
else
    echo "⚠ python3 not found, skipping Ok(()) insertion"
fi

verify_compilation || {
    echo "💥 Ok(()) insertion broke compilation"
    exit 1
}

# Step 5: Import consolidation
echo ""
echo "📋 Step 5: Import consolidation"
echo "Consolidating imports using test prelude..."

if command -v python3 >/dev/null 2>&1; then
    python3 "${SCRIPT_DIR}/python-scripts/bulk-import-consolidator.py" test/
    echo "✅ Import consolidation complete"
else
    echo "⚠ python3 not found, skipping import consolidation"
fi

verify_compilation || {
    echo "💥 Import consolidation broke compilation"
    exit 1
}

# Final verification and summary
echo ""
echo "🎯 Final Results"
echo "================"

# Count improvements
tempdir_remaining=$(rg -c "TempDir::new\(\)\.unwrap\(\)" test/ --type rust 2>/dev/null | wc -l || echo "0")
eventsource_remaining=$(rg -c "EventSourceContext::new" test/ --type rust 2>/dev/null | grep -v "common/mod.rs" | wc -l || echo "0")
rawevent_remaining=$(rg -c "RawEvent \{" test/ --type rust 2>/dev/null | wc -l || echo "0")

echo "📊 Automation Results:"
echo "  - TempDir::new().unwrap() remaining: ${tempdir_remaining}"
echo "  - EventSourceContext::new() remaining: ${eventsource_remaining}"  
echo "  - Manual RawEvent constructions remaining: ${rawevent_remaining}"

verify_compilation
echo ""
echo "🎉 All transformations completed successfully!"
echo ""
echo "💡 Next steps:"
echo "  1. Review the changes with 'git diff'"
echo "  2. Run tests with 'cargo test'"
echo "  3. Commit changes if satisfied"