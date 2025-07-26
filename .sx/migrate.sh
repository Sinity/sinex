#!/usr/bin/env bash
# Migrate from complex setup to simple sx

echo "🔄 Migrating to simplified sx environment..."

# Archive old scripts (don't delete, just move out of the way)
if [ -d "scripts" ] && [ ! -d ".sx/archive" ]; then
    echo "📦 Archiving old scripts..."
    mkdir -p .sx/archive
    mv scripts .sx/archive/
    echo "  Moved to .sx/archive/scripts"
fi

# Create symlink for backward compatibility
if [ ! -e "scripts" ]; then
    ln -s .sx/archive/scripts scripts
    echo "  Created compatibility symlink"
fi

# Simplify justfile - keep it but add sx notice
if [ -f "justfile" ] && ! grep -q "sx is the preferred" justfile; then
    cat > justfile.new << 'EOF'
# Sinex Development Commands
# NOTE: 'sx' is the preferred command. This justfile is kept for compatibility.
# 
# Instead of 'just foo', use:
#   sx dev    - Development cycle
#   sx test   - Run tests  
#   sx fix    - Fix issues
#   sx commit - Commit changes
#   sx ship   - Ship to production
#
# Original justfile content follows:

EOF
    cat justfile >> justfile.new
    mv justfile.new justfile
    echo "✅ Updated justfile with sx notice"
fi

# Update shell configuration
if grep -q "sinex-dev-aliases" ~/.bashrc 2>/dev/null; then
    echo "🔄 Updating shell aliases..."
    # Comment out old aliases
    sed -i '/# sinex-dev-aliases/,/^$/s/^/# /' ~/.bashrc
    
    # Add new minimal aliases
    cat >> ~/.bashrc << 'EOF'

# sx - Sinex simplified commands
alias sx='$(pwd)/sx'
export PATH="$(pwd):$PATH"  # Make sx available everywhere
EOF
    echo "✅ Updated shell configuration"
fi

# Create quick reference card
cat > .sx/quickref.md << 'EOF'
# Sinex Quick Reference

## The Only Commands You Need

```bash
sx         # Show smart menu
sx dev     # Format → Build → Test
sx test    # Run smart tests
sx fix     # Fix all issues
sx commit  # Commit changes
sx ship    # Ship to production
```

## Advanced (if needed)

```bash
sx monitor # View dashboard
sx health  # Check health
sx perf    # Performance score
sx clean   # Clean everything
```

## Everything Else

- Happens automatically
- No configuration needed
- Just works™
EOF

echo ""
echo "✅ Migration complete!"
echo ""
echo "🎯 Next steps:"
echo "  1. Run: source ~/.bashrc"
echo "  2. Then just use: sx"
echo ""
echo "All your old commands still work via 'just' if needed."