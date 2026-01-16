#!/usr/bin/env bash
set -euo pipefail

# Setup script for parallel Phase 1 & 2 execution
# Creates git worktrees and coordination infrastructure

SINEX_ROOT="/realm/project/sinex"
PARENT_DIR="/realm/project"

echo "==> Setting up parallel execution infrastructure"

# Create worktrees for each stream
echo "==> Creating git worktrees..."

cd "$SINEX_ROOT"

# Stream A: KV Coordination (1 week, CRITICAL)
if [ ! -d "$PARENT_DIR/sinex-stream-a" ]; then
    git worktree add "$PARENT_DIR/sinex-stream-a" -b feat/kv-coordination-completion
    echo "  ✓ Stream A worktree created: $PARENT_DIR/sinex-stream-a"
else
    echo "  ⚠ Stream A worktree already exists"
fi

# Stream B: Gateway RPC (2 weeks, HIGH)
if [ ! -d "$PARENT_DIR/sinex-stream-b" ]; then
    git worktree add "$PARENT_DIR/sinex-stream-b" -b feat/gateway-rpc-completeness
    echo "  ✓ Stream B worktree created: $PARENT_DIR/sinex-stream-b"
else
    echo "  ⚠ Stream B worktree already exists"
fi

# Stream C: Desktop Native APIs (1-2 weeks, HIGH, zero dependencies)
if [ ! -d "$PARENT_DIR/sinex-stream-c" ]; then
    git worktree add "$PARENT_DIR/sinex-stream-c" -b feat/desktop-native-apis
    echo "  ✓ Stream C worktree created: $PARENT_DIR/sinex-stream-c"
else
    echo "  ⚠ Stream C worktree already exists"
fi

# Create lock files for coordination
echo "==> Creating coordination lock files..."
touch /tmp/sinex-build.lock
touch /tmp/sinex-test.lock
echo "  ✓ Lock files created: /tmp/sinex-{build,test}.lock"

# Verify worktrees
echo ""
echo "==> Worktree status:"
git worktree list

echo ""
echo "==> Branch status:"
git branch -vv | grep -E "(feat/kv|feat/gateway|feat/desktop)"

echo ""
echo "==> Setup complete!"
echo ""
echo "Next steps:"
echo "1. Launch Stream A agent: cd $PARENT_DIR/sinex-stream-a"
echo "2. Launch Stream B agent: cd $PARENT_DIR/sinex-stream-b"
echo "3. Launch Stream C agent: cd $PARENT_DIR/sinex-stream-c"
echo ""
echo "Or use the agent launch script:"
echo "  bash $SINEX_ROOT/scripts/launch-parallel-agents.sh"
