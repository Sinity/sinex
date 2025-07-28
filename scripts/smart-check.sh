#!/usr/bin/env bash
# Smart check that only checks crates with changes

set -euo pipefail

# Get list of changed files relative to last commit or index
changed_files=$(git diff --name-only HEAD 2>/dev/null || git diff --name-only --cached 2>/dev/null || echo "")

if [ -z "$changed_files" ]; then
    echo "No changes detected, running quick workspace check..."
    cargo check --workspace --all-targets
    exit 0
fi

# Extract unique crate names from changed files
changed_crates=$(echo "$changed_files" | grep '^crate/' | cut -d'/' -f2 | sort -u || echo "")

if [ -z "$changed_crates" ]; then
    echo "No changes in crate/, checking workspace..."
    cargo check --workspace --all-targets
else
    # Check each changed crate
    echo "Changes detected in: $(echo $changed_crates | tr '\n' ' ')"
    for crate in $changed_crates; do
        if [ -d "crate/$crate" ]; then
            echo "Checking $crate..."
            cargo check -p "$crate"
        fi
    done
fi