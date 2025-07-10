#!/usr/bin/env bash
# Migration script to switch exo CLI to RPC version

set -e

echo "Migrating exo CLI to use JSON-RPC..."

# Backup original
if [ -f "cli/exo.py" ] && [ ! -f "cli/exo_direct.py.bak" ]; then
    echo "Backing up original exo.py to exo_direct.py.bak"
    cp cli/exo.py cli/exo_direct.py.bak
fi

# Replace with RPC version
echo "Replacing exo.py with RPC version"
cp cli/exo_rpc.py cli/exo.py

echo "Migration complete!"
echo ""
echo "IMPORTANT: Make sure sinex-host is running:"
echo "  sinex-host rpc-server"
echo ""
echo "The original direct-DB version is saved as exo_direct.py.bak"