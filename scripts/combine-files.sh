#!/usr/bin/env bash
#
# combine-files.sh — Combine multiple text files into a single structured document
# Usage: ./combine-files.sh [options]

set -euo pipefail
IFS=$'\n\t'

# ------------------------
#  Dependencies check
# ------------------------
for cmd in fd fzf bat file stat date getopt; do
  command -v "$cmd" &>/dev/null || {
    echo "❌  '$cmd' is required but not installed." >&2
    exit 1
  }
done

# ------------------------
#  Defaults & help
# ------------------------
directory="."
output_file="combined.md"
output_format="markdown"

print_help() {
  cat <<EOF
Usage: $(basename "$0") [options]

Combine multiple text files into a single structured document.

Options:
  -d, --directory DIR   Directory to scan (default: current directory)
  -o, --output FILE     Output file (default: combined_configs.txt)
  -f, --format FORMAT   Output format: text, markdown (default: markdown)
  -h, --help            Show this help message
EOF
  exit 0
}

# ------------------------
#  Parse arguments
# ------------------------
OPTIONS=d:o:f:h
LONGOPTS=directory:,output:,format:,help

! PARSED=$(getopt --options=$OPTIONS --longoptions=$LONGOPTS --name "$0" -- "$@") && exit 2
eval set -- "$PARSED"

while true; do
  case "$1" in
  -d | --directory)
    directory="$2"
    shift 2
    ;;
  -o | --output)
    output_file="$2"
    shift 2
    ;;
  -f | --format)
    output_format="$2"
    shift 2
    ;;
  -h | --help) print_help ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

# ------------------------
#  Validate directory
# ------------------------
if [[ ! -d "$directory" ]]; then
  echo "Error: Directory '$directory' does not exist." >&2
  exit 1
fi

# ------------------------
#  Gather & filter files
# ------------------------
# fd will respect .gitignore and skip hidden by default; we re‑include hidden but then
# explicitly exclude a few infra dirs:
mapfile -t all_files < <(
  fd --type f --hidden \
    --exclude .git --exclude .obsidian --exclude node_modules --exclude vendor --exclude build \
    . "$directory"
)

# remove non-text files
files=()
for f in "${all_files[@]}"; do
  if file --mime-type -b "$f" | grep -q '^text/'; then
    files+=("$f")
  fi
done

if [[ ${#files[@]} -eq 0 ]]; then
  echo "No suitable text files found in '$directory'." >&2
  exit 1
fi

# ------------------------
#  fzf selection
# ------------------------
echo "Select files to include:"
mapfile -t selected_files < <(
  printf '%s\n' "${files[@]}" |
    fzf --multi --layout=reverse \
      --preview 'sz=$(stat -c%s {}); tk=$((sz/4)); \
                   printf "Size: %d bytes | Tokens: %d\n\n" "$sz" "$tk"; \
                   bat --style=numbers --color=always {}' \
      --preview-window=right:60%:wrap \
      --prompt="› "
)

if [[ ${#selected_files[@]} -eq 0 ]]; then
  echo "No files selected. Exiting."
  exit 0
fi

# ------------------------
#  Compute totals & header
# ------------------------
current_date=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
total_files=${#selected_files[@]}
total_tokens=0

# precompute size & token per file
declare -A size_map token_map
for f in "${selected_files[@]}"; do
  sz=$(stat -c%s "$f")
  tk=$((sz / 4))
  size_map["$f"]=$sz
  token_map["$f"]=$tk
  total_tokens=$((total_tokens + tk))
done

# ------------------------
#  Write output
# ------------------------
: >"$output_file"

if [[ "$output_format" == "markdown" ]]; then
  {
    echo '---'
    echo "generated: $current_date"
    echo "base_directory: $directory"
    echo "total_files: $total_files"
    echo "total_tokens_est: $total_tokens"
    echo '---'
    echo
    echo "## Table of Contents"
    echo
    i=1
    for f in "${selected_files[@]}"; do
      rel=${f#"$directory"/}
      echo "$i. [$rel](#file-$i)"
      i=$((i + 1))
    done
    echo
  } >>"$output_file"
else
  # plain‑text header
  {
    echo "COMBINED FILES"
    echo "Generated: $current_date"
    echo "Directory: $directory"
    echo
  } >>"$output_file"
fi

# ------------------------
#  Append each file
# ------------------------
i=1
for f in "${selected_files[@]}"; do
  rel=${f#"$directory"/}
  sz=${size_map["$f"]}
  tk=${token_map["$f"]}
  typ=$(file -b "$f" | cut -d, -f1)

  if [[ "$output_format" == "markdown" ]]; then
    echo "<a id=\"file-$i\"></a>" >>"$output_file"
    echo "## File: $rel" >>"$output_file"
    echo >>"$output_file"
    echo "- Size: $sz bytes" >>"$output_file"
    echo "- Tokens: $tk" >>"$output_file"
    echo "- Type: $typ" >>"$output_file"
    echo >>"$output_file"
    # code‑block language detection (unchanged)
    ext=${f##*.}
    case "$ext" in
    js | ts) lang=javascript ;;
    py) lang=python ;;
    rb) lang=ruby ;;
    sh | bash) lang=bash ;;
    nix) lang=nix ;;
    md) lang=markdown ;;
    html) lang=html ;;
    css) lang=css ;;
    json) lang=json ;;
    xml) lang=xml ;;
    lua) lang=lua ;;
    *) lang="" ;;
    esac
    echo '```'"$lang" >>"$output_file"
    cat "$f" >>"$output_file"
    echo '```' >>"$output_file"
    echo >>"$output_file"
  else
    # plain‑text
    echo "========================================" >>"$output_file"
    echo "FILE: $rel" >>"$output_file"
    echo "Size: $sz bytes | Tokens: $tk | Type: $typ" >>"$output_file"
    echo "========================================" >>"$output_file"
    echo >>"$output_file"
    cat "$f" >>"$output_file"
    echo -e "\n" >>"$output_file"
  fi

  i=$((i + 1))
done

echo "Done! Combined configuration saved to '$output_file'."
