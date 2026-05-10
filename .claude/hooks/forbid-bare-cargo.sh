#!/usr/bin/env bash
# PreToolUse Bash hook for /realm/project/sinex.
#
# Blocks bare `cargo <subcommand>` invocations in this xtask-wrapped workspace.
# The xtask wrapper is the canonical entrypoint — bare cargo bypasses
# preflight, history capture, the shared `.sinex/target` directory, and the
# structured-output contract downstream tooling relies on.
#
# The most common drift mode is `cargo nextest list ... --workspace`, which
# silently triggers a full workspace rebuild (10+ minutes after a cache
# invalidation). This hook prevents that class of mistake at the keystroke.
#
# Allowed:
#   - `cargo --version` / `cargo --help` / `cargo` with no subcommand
#     (info-only; matched by the `[a-z]` requirement after the space)
#   - Anything not preceded by a command boundary (so `# cargo ...` inside
#     heredocs / comments / quoted strings is unaffected)
#
# See `.agent/includes/patterns/toolchain.md` for the full xtask equivalents.

set -euo pipefail

INPUT=$(cat)
CMD=$(echo "$INPUT" | jq -r '.tool_input.command // ""')

# Match `cargo <subcommand>` only at real command boundaries — start of the
# command string, after a newline, or after `;`, `&&`, `||`, or `|` followed
# by at least one whitespace character. The whitespace requirement is what
# distinguishes a real shell pipeline (which has a space around the operator
# in agent-authored commands) from `cargo` appearing inside a quoted regex
# alternation like `grep -E "(cargo nextest|cargo check)"` — there the `|`
# has no surrounding whitespace and the segment is also inside double quotes.
#
# This is a heuristic; pathological cases (e.g. `echo "cargo build"`) can
# still trip it. The cost of a rare false positive (a denial that needs the
# agent to rephrase) is much lower than the cost of false negatives (a
# 10-minute workspace rebuild from `cargo nextest list --workspace`).
if echo "$CMD" | grep -qE '(^|\n)\s*([A-Z_][A-Z0-9_]*=\S+\s+)*cargo\s+(\+[A-Za-z0-9._-]+\s+)?[a-z]|(\s+(;|&&|\|\|?)\s+|;\s+)([A-Z_][A-Z0-9_]*=\S+\s+)*cargo\s+(\+[A-Za-z0-9._-]+\s+)?[a-z]'; then
  jq -n '{
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: "Bare cargo is forbidden in this workspace — use xtask (check / build / test / test --list / fix / docs ...). xtask test --list IS the cargo nextest list replacement. See .agent/includes/patterns/toolchain.md."
    }
  }'
  exit 0
fi

exit 0
