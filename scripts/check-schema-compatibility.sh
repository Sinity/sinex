#!/usr/bin/env bash
# Schema compatibility check script for CI
# Compares JSON schemas under schemas/ with the base branch using the
# rust-driven sinex-schema validator.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

DEFAULT_DB="postgresql:///sinex_dev?host=/run/postgresql"
export DATABASE_URL="${DATABASE_URL:-$DEFAULT_DB}"

if ! git rev-parse --git-dir >/dev/null 2>&1; then
  echo -e "${RED}Not inside a git repository${NC}"
  exit 1
fi

SCHEMA_CLI=(cargo run -p sinex-core --bin sinex-schema --features schema-manager --)

BASE_BRANCH=${CI_BASE_BRANCH:-$(git symbolic-ref refs/remotes/origin/HEAD 2>/dev/null | sed 's@^refs/remotes/origin/@@' || echo "master")}
SCHEMA_GLOB='schemas/**/*.json'
CHANGED_FILES=$(git diff --name-only "$BASE_BRANCH"...HEAD -- $SCHEMA_GLOB || true)

if [ -z "$CHANGED_FILES" ]; then
  echo -e "${GREEN}✅ No schema edits detected${NC}"
  exit 0
fi

echo "🔍 Checking compatibility for updated schemas:"
echo "$CHANGED_FILES"

ERRORS=0
for file in $CHANGED_FILES; do
  if [ ! -f "$file" ]; then
    echo -e "${YELLOW}⚠️  Skipping deleted schema $file${NC}"
    continue
  fi

  if ! git cat-file -e "$BASE_BRANCH:$file" 2>/dev/null; then
    echo -e "${GREEN}➕ New schema $file (no backward check required)${NC}"
    continue
  fi

  tmp_old=$(mktemp)
  git show "$BASE_BRANCH:$file" >"$tmp_old"

  echo "Comparing $file against $BASE_BRANCH..."
  if ! "${SCHEMA_CLI[@]}" validate "$tmp_old" "$file"; then
    echo -e "${RED}❌ Compatibility regression detected in $file${NC}"
    ERRORS=$((ERRORS + 1))
  else
    echo -e "${GREEN}✅ $file remains backward compatible${NC}"
  fi

  rm -f "$tmp_old"

done

if [ $ERRORS -gt 0 ]; then
  echo -e "${RED}Schema compatibility check failed (${ERRORS} issue(s))${NC}"
  exit 1
fi

echo -e "${GREEN}✅ Schema compatibility check passed${NC}"
