#!/usr/bin/env bash
# Smart check that only checks crates with changes

set -euo pipefail

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

collect_changed_packages() {
    local files="$1"
    local pkg
    while IFS= read -r file; do
        pkg="$(package_from_path "$file")"
        if [ -n "$pkg" ]; then
            echo "$pkg"
        fi
    done <<< "$files"
}

changed_files="$(git diff --name-only HEAD 2>/dev/null || git diff --name-only --cached 2>/dev/null || echo "")"

if [ -z "$changed_files" ]; then
    echo "No changes detected, running quick workspace check..."
    cargo check --workspace --all-targets
    exit 0
fi

mapfile -t changed_crates < <(collect_changed_packages "$changed_files" | sort -u)

if [ "${#changed_crates[@]}" -eq 0 ]; then
    echo "No crate-specific changes detected, checking workspace..."
    cargo check --workspace --all-targets
    exit 0
fi

echo "Changes detected in: ${changed_crates[*]}"
for crate in "${changed_crates[@]}"; do
    echo "Checking $crate..."
    cargo check -p "$crate"
done
