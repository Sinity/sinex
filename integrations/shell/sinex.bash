# Sinex shell integration for bash: one execution token per command.
#
# Needs bash 5.1 or newer and bash-preexec (as Atuin's bash integration
# does); source it after both. See sinex.zsh for what it records.
[[ $- == *i* ]] || return 0
[[ -n ${_sinex_loaded:-} ]] && return 0
_sinex_loaded=1
(( BASH_VERSINFO[0] > 5 || (BASH_VERSINFO[0] == 5 && BASH_VERSINFO[1] >= 1) )) || return 0

_sinex_bin=${SINEX_SESSION_BIN:-sinex-session}
_sinex_execution= _sinex_started_ns= _sinex_command= _sinex_atuin_id= _sinex_cwd=
_sinex_counter=0
_sinex_base64='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'

_sinex_now_ns() {
  local now=${EPOCHREALTIME/[^0-9]/}
  printf '%s' "$(( 10#$now * 1000 ))"
}

# The token in REPLY, computed in the shell itself so the counter advances.
_sinex_uuid7() {
  local ms=$(( $1 / 1000000 ))
  local counter=$(( _sinex_counter++ & 0xfff ))
  local rand=$(( ($$ & 0x3fffff) << 40 | (SRANDOM & 0x7fff) << 25 | (SRANDOM & 0x7fff) << 10 | (SRANDOM & 0x3ff) ))
  printf -v REPLY '%08x-%04x-7%03x-%04x-%012x' $(( ms >> 16 )) $(( ms & 0xffff )) "$counter" \
    $(( 0x8000 | (rand >> 48 & 0x3fff) )) $(( rand & 0xffffffffffff ))
}

_sinex_b64() {
  local text=$1 out= i a b c
  for (( i = 0; i < ${#text}; i += 3 )); do
    printf -v a '%d' "'${text:i:1}"; b=0; c=0
    (( i + 1 < ${#text} )) && printf -v b '%d' "'${text:i+1:1}"
    (( i + 2 < ${#text} )) && printf -v c '%d' "'${text:i+2:1}"
    out+=${_sinex_base64:$(( a >> 2 )):1}
    out+=${_sinex_base64:$(( (a & 3) << 4 | b >> 4 )):1}
    if (( i + 1 < ${#text} )); then out+=${_sinex_base64:$(( (b & 15) << 2 | c >> 6 )):1}; else out+='='; fi
    if (( i + 2 < ${#text} )); then out+=${_sinex_base64:$(( c & 63 )):1}; else out+='='; fi
  done
  printf '%s' "$out"
}

_sinex_preexec() {
  _sinex_started_ns=$(_sinex_now_ns)
  _sinex_uuid7 "$_sinex_started_ns"
  _sinex_execution=$REPLY
  _sinex_command=$1
  _sinex_cwd=$PWD
  _sinex_atuin_id=${ATUIN_HISTORY_ID:-}
  if [[ -n ${KITTY_WINDOW_ID:-} ]]; then
    printf '\e]1337;SetUserVar=sinex_execution=%s\a' "$(_sinex_b64 "$_sinex_execution")" > /dev/tty
  fi
}

_sinex_precmd() {
  local exit=$?
  [[ -n $_sinex_execution ]] || return 0
  local ended
  ended=$(_sinex_now_ns)
  local atuin=${ATUIN_HISTORY_ID:-$_sinex_atuin_id}
  ( "$_sinex_bin" report-command --shell bash --execution "$_sinex_execution" \
      --started-at-ns "$_sinex_started_ns" --ended-at-ns "$ended" --exit "$exit" \
      --cwd "$_sinex_cwd" --session "$$" --atuin-id "$atuin" -- "$_sinex_command" &>/dev/null & )
  _sinex_execution=
}

# Attach at the first prompt, after every rc file ran, so bash-preexec and
# Atuin are loaded whichever order they were sourced in. Without
# bash-preexec there is no preexec hook to attach to.
_sinex_attach() {
  PROMPT_COMMAND=${PROMPT_COMMAND//_sinex_attach;/}
  declare -p preexec_functions &>/dev/null || return 0
  preexec_functions=(_sinex_preexec "${preexec_functions[@]}")
  precmd_functions=(_sinex_precmd "${precmd_functions[@]}")
}
PROMPT_COMMAND="_sinex_attach;${PROMPT_COMMAND:-}"
