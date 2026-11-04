# Sinex shell integration for zsh: one execution token per command.
#
# Source from an interactive zsh (the NixOS module does this). For each
# command the shell runs, preexec mints a UUIDv7 execution token, hands it to
# kitty as the `sinex_execution` user variable, and precmd reports the
# finished execution (token, command, cwd, start, duration, exit and the Atuin
# row started for it) through `sinex-session report-command`, in the
# background. Nothing here waits on Sinex.
[[ -o interactive ]] || return 0
(( ${+_sinex_loaded} )) && return 0
typeset -g _sinex_loaded=1
zmodload zsh/datetime 2>/dev/null || return 0

typeset -g _sinex_bin=${SINEX_SESSION_BIN:-sinex-session}
typeset -g _sinex_execution= _sinex_started_ns= _sinex_command= _sinex_atuin_id= _sinex_cwd=
typeset -gi _sinex_counter=0
typeset -g _sinex_base64='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'

# A UUIDv7 in REPLY: the millisecond, then a per-shell counter, the shell's
# process ID and random bits, so tokens minted on one host never repeat. It
# runs in the shell itself, so the counter advances.
_sinex_uuid7() {
  local -i ms=$(( $1 / 1000000 ))
  local -i counter=$(( _sinex_counter++ & 0xfff ))
  local -i rand=$(( ($$ & 0x3fffff) << 40 | RANDOM << 25 | RANDOM << 10 | (RANDOM & 0x3ff) ))
  printf -v REPLY '%08x-%04x-7%03x-%04x-%012x' $(( ms >> 16 )) $(( ms & 0xffff )) $counter \
    $(( 0x8000 | (rand >> 48 & 0x3fff) )) $(( rand & 0xffffffffffff ))
}

# Base64 of ASCII text, for kitty's user variables.
_sinex_b64() {
  local text=$1 out= ch
  local -i i a b c
  for (( i = 0; i < ${#text}; i += 3 )); do
    ch=${text[i+1]}; a=$(( #ch )); b=0; c=0
    if (( i + 2 <= ${#text} )); then ch=${text[i+2]}; b=$(( #ch )); fi
    if (( i + 3 <= ${#text} )); then ch=${text[i+3]}; c=$(( #ch )); fi
    out+=${_sinex_base64[$(( (a >> 2) + 1 ))]}
    out+=${_sinex_base64[$(( ((a & 3) << 4 | b >> 4) + 1 ))]}
    if (( i + 2 <= ${#text} )); then out+=${_sinex_base64[$(( ((b & 15) << 2 | c >> 6) + 1 ))]}; else out+='='; fi
    if (( i + 3 <= ${#text} )); then out+=${_sinex_base64[$(( (c & 63) + 1 ))]}; else out+='='; fi
  done
  print -rn -- $out
}

_sinex_preexec() {
  _sinex_started_ns=$(( epochtime[1] * 1000000000 + epochtime[2] ))
  _sinex_uuid7 $_sinex_started_ns
  _sinex_execution=$REPLY
  _sinex_command=$1
  _sinex_cwd=$PWD
  _sinex_atuin_id=${ATUIN_HISTORY_ID:-}
  if [[ -n ${KITTY_WINDOW_ID:-} ]]; then
    printf '\e]1337;SetUserVar=sinex_execution=%s\a' "$(_sinex_b64 $_sinex_execution)" > /dev/tty
  fi
}

_sinex_precmd() {
  local -i exit=$?
  [[ -n $_sinex_execution ]] || return 0
  local -i ended=$(( epochtime[1] * 1000000000 + epochtime[2] ))
  local atuin=${ATUIN_HISTORY_ID:-$_sinex_atuin_id}
  "$_sinex_bin" report-command --shell zsh --execution "$_sinex_execution" \
    --started-at-ns "$_sinex_started_ns" --ended-at-ns "$ended" --exit "$exit" \
    --cwd "$_sinex_cwd" --session "$$" --atuin-id "$atuin" -- "$_sinex_command" &>/dev/null &!
  _sinex_execution=
}

# Ours runs first in preexec, so kitty has the token before its own hook
# marks the command's start, and first in precmd, so Atuin's row ID (set by
# Atuin's preexec, cleared by its precmd) is still there to record.
_sinex_order() {
  preexec_functions=(_sinex_preexec ${preexec_functions:#_sinex_preexec})
  precmd_functions=(_sinex_precmd ${precmd_functions:#_sinex_precmd})
}
_sinex_first_prompt() {
  _sinex_order
  precmd_functions=(${precmd_functions:#_sinex_first_prompt})
}
typeset -ga preexec_functions precmd_functions
preexec_functions=(_sinex_preexec $preexec_functions)
precmd_functions=(_sinex_precmd $precmd_functions _sinex_first_prompt)
