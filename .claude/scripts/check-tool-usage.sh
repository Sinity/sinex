#!/usr/bin/env bash
# Tool usage compliance checker
set -euo pipefail

input=$(cat)
command=$(echo "$input" | jq -r '.tool_input.command // ""')

[ -z "$command" ] && echo '{"continue": true}' && exit 0

# Block cat/head/tail -> suggest Read
if echo "$command" | grep -qE '^\s*(cat|head|tail)\s+'; then
    file_path=$(echo "$command" | sed -E 's/^\s*(cat|head|tail)\s+//' | awk '{print $1}')
    cat >&2 <<EOF
{
  "hookSpecificOutput": {"permissionDecision": "deny"},
  "systemMessage": "❌ Use Read tool instead of bash for reading files.\n\n**Wrong**: \`$command\`\n\n**Correct**: \`Read({ file_path: \"$file_path\" })\`\n\n**Why**: Read provides line numbers, pagination, proper encoding."
}
EOF
    exit 2
fi

# Block grep/rg -> suggest Grep
if echo "$command" | grep -qE '^\s*(grep|rg)\s+'; then
    pattern=$(echo "$command" | sed -E 's/^\s*(grep|rg)\s+//' | awk '{print $1}' | tr -d '"'"'"')
    cat >&2 <<EOF
{
  "hookSpecificOutput": {"permissionDecision": "deny"},
  "systemMessage": "❌ Use Grep tool instead of bash grep/rg.\n\n**Wrong**: \`$command\`\n\n**Correct**: \`Grep({ pattern: \"$pattern\", path: \"/realm/project/sinex\", output_mode: \"content\" })\`\n\n**Why**: Grep supports output modes, file filtering, better formatting."
}
EOF
    exit 2
fi

# Block find -> suggest Glob
if echo "$command" | grep -qE '^\s*find\s+'; then
    cat >&2 <<EOF
{
  "hookSpecificOutput": {"permissionDecision": "deny"},
  "systemMessage": "❌ Use Glob tool instead of find.\n\n**Wrong**: \`$command\`\n\n**Correct**: \`Glob({ pattern: \"**/*.rs\", path: \"/realm/project/sinex\" })\`\n\n**Why**: Faster, respects .gitignore, consistent syntax."
}
EOF
    exit 2
fi

# Block `xtask ... | tail` and `xtask ... | head`
# These patterns are forbidden because:
#   - `| tail -N` buffers ALL xtask output until EOF (no streaming), making the output
#     file appear empty while xtask runs. If xtask hangs, you see nothing forever.
#   - `| head -N` / `| tail -N` cause SIGPIPE when the pipe consumer exits, which
#     silently kills xtask mid-run, truncating output and leaving zombie bg processes.
# Correct patterns: use `--bg --json` to get a job ID, then `xtask jobs output ID`.
if echo "$command" | grep -qE 'xtask\s' && echo "$command" | grep -qE '\|\s*(tail|head)\b'; then
    cat >&2 <<'EOF'
{
  "hookSpecificOutput": {"permissionDecision": "deny"},
  "systemMessage": "❌ Piping xtask output through tail/head is FORBIDDEN.\n\nProblems:\n  1. tail -N buffers ALL output until EOF — if xtask hangs, you see nothing forever\n  2. When tail/head exits, SIGPIPE silently kills xtask mid-run\n  3. Output files appear empty (0 bytes) while xtask is running\n\nCorrect pattern:\n  xtask CMD --bg --json           # get job ID, returns immediately\n  xtask jobs wait ID              # block until done\n  xtask jobs output ID            # retrieve FULL output\n\nOr for quick jobs (< 5s), just run foreground without tail:\n  xtask CMD --json\n\nSee: do-dont.md anti-pattern: 'some_cmd | tail -N on xtask'"
}
EOF
    exit 2
fi

echo '{"continue": true}'
exit 0
