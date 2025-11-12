#!/usr/bin/env bash
# Smart watch that focuses on the crate you're actively editing

set -euo pipefail

PROJECT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

package_from_path() {
    local path="${1#./}"

    if [[ "$path" == crate/* ]]; then
        IFS='/' read -r -a parts <<< "$path"
        if [ "${#parts[@]}" -ge 3 ]; then
            echo "${parts[2]}"
            return
        fi
    fi

    case "$path" in
        tests/e2e*|tests/e2e/*)
            echo "sinex-e2e-tests"
            ;;
    esac
}

detect_active_crate() {
    local rel_path recent_changes pkg

    rel_path="${PWD#"$PROJECT_ROOT"/}"
    pkg="$(package_from_path "$rel_path")"
    if [ -n "$pkg" ]; then
        echo "$pkg"
        return
    fi

    recent_changes="$(git diff --name-only HEAD 2>/dev/null || git diff --name-only --cached 2>/dev/null || echo "")"
    if [ -n "$recent_changes" ]; then
        while IFS= read -r file; do
            pkg="$(package_from_path "$file")"
            if [ -n "$pkg" ]; then
                echo "$pkg"
                return
            fi
        done <<< "$recent_changes"
    fi

    echo ""
}

crate="$(detect_active_crate)"

if [ -n "$crate" ]; then
    echo "🎯 Watching crate: $crate"
    echo "💡 Tip: This is faster than watching the whole workspace"
    cargo watch -x "check -p $crate"
else
    echo "📦 No specific crate detected, watching entire workspace"
    echo "💡 Tip: cd into a crate directory for faster checks"
    cargo watch -x "check --workspace"
fi
