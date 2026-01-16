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

echo '{"continue": true}'
exit 0
