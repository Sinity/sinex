#!/usr/bin/env bash
# Smart watch that focuses on the crate you're actively editing

set -euo pipefail

# Function to detect which crate we're in based on current directory or recent changes
detect_active_crate() {
    # First, check if we're inside a crate directory
    current_dir=$(pwd)
    if [[ "$current_dir" =~ crate/([^/]+) ]]; then
        echo "${BASH_REMATCH[1]}"
        return
    fi
    
    # Otherwise, check recent git changes
    recent_changes=$(git diff --name-only HEAD 2>/dev/null || git diff --name-only --cached 2>/dev/null || echo "")
    if [ -n "$recent_changes" ]; then
        crate=$(echo "$recent_changes" | grep '^crate/' | head -1 | cut -d'/' -f2)
        if [ -n "$crate" ]; then
            echo "$crate"
            return
        fi
    fi
    
    # No specific crate detected
    echo ""
}

crate=$(detect_active_crate)

if [ -n "$crate" ]; then
    echo "🎯 Watching crate: $crate"
    echo "💡 Tip: This is faster than watching the whole workspace"
    cargo watch -x "check -p $crate"
else
    echo "📦 No specific crate detected, watching entire workspace"
    echo "💡 Tip: cd into a crate directory for faster checks"
    cargo watch -x "check --workspace"
fi