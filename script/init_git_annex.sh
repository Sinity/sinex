#!/usr/bin/env bash
set -euo pipefail

# Git-annex repository initialization script for Sinex
# Sets up git-annex repositories for blob storage

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Default configuration
DEFAULT_ANNEX_ROOT="/realm/sinex-annex"
DEFAULT_REPO_NAME="sinex-blobs"
DEFAULT_NUM_COPIES=2

# Parse command line arguments
ANNEX_ROOT="${1:-${DEFAULT_ANNEX_ROOT}}"
REPO_NAME="${2:-${DEFAULT_REPO_NAME}}"
NUM_COPIES="${3:-${DEFAULT_NUM_COPIES}}"

echo "🏗️  Initializing git-annex repository for Sinex blob storage"
echo "Repository path: ${ANNEX_ROOT}/${REPO_NAME}"
echo "Number of copies: ${NUM_COPIES}"

# Check if git-annex is available
if ! command -v git-annex &> /dev/null; then
    echo "❌ Error: git-annex is not installed or not in PATH"
    echo "Please install git-annex: nix-env -iA nixpkgs.git-annex"
    exit 1
fi

# Create directory structure
mkdir -p "${ANNEX_ROOT}"
cd "${ANNEX_ROOT}"

# Initialize repository if it doesn't exist
if [ ! -d "${REPO_NAME}" ]; then
    echo "📁 Creating new repository: ${REPO_NAME}"
    mkdir -p "${REPO_NAME}"
    cd "${REPO_NAME}"
    
    # Initialize git repository
    git init
    echo "✅ Initialized git repository"
    
    # Initialize git-annex
    git-annex init "Sinex Exocortex Blob Storage"
    echo "✅ Initialized git-annex"
    
    # Configure git-annex settings
    git config annex.numcopies "${NUM_COPIES}"
    git config annex.largefiles "anything"
    git config annex.backend "SHA256E"
    echo "✅ Configured git-annex settings"
    
    # Create .gitattributes for large files
    cat > .gitattributes << EOF
# Automatically annex all files
* annex.largefiles=anything
# But not .gitattributes itself
.gitattributes annex.largefiles=nothing
EOF
    
    git add .gitattributes
    git commit -m "Initial commit: add .gitattributes"
    echo "✅ Created .gitattributes"
    
else
    echo "📂 Repository already exists: ${REPO_NAME}"
    cd "${REPO_NAME}"
    
    # Verify it's a git-annex repository
    if [ ! -d ".git/annex" ]; then
        echo "❌ Error: Directory exists but is not a git-annex repository"
        exit 1
    fi
    
    echo "✅ Verified existing git-annex repository"
fi

# Display repository status
echo ""
echo "📊 Repository Status:"
echo "Repository path: $(pwd)"
echo "Git annex version: $(git-annex version | head -1)"
echo "Backend: $(git config annex.backend || echo 'default')"
echo "Number of copies: $(git config annex.numcopies || echo 'default')"

# Run initial fsck to verify integrity
echo ""
echo "🔍 Running initial integrity check..."
git-annex fsck --fast || echo "⚠️  Some issues found during fsck (this is normal for a new repository)"

echo ""
echo "🎉 Git-annex repository initialization complete!"
echo ""
echo "Next steps:"
echo "1. Set SINEX_ANNEX_PATH environment variable:"
echo "   export SINEX_ANNEX_PATH=\"${ANNEX_ROOT}/${REPO_NAME}\""
echo ""
echo "2. Test blob ingestion with the Sinex CLI:"
echo "   ./cli/exo.py blob ingest /path/to/test/file"
echo ""
echo "3. (Optional) Set up remote repositories for backup:"
echo "   git remote add backup /path/to/backup/repo"
echo "   git-annex init"
echo "   git-annex sync"