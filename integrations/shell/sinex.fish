# Sinex shell integration for fish: one execution token per command.
# Source from an interactive fish; see sinex.zsh for what it records.
status is-interactive; or exit 0
set -q _sinex_loaded; and exit 0
set -g _sinex_loaded 1
set -q SINEX_SESSION_BIN; and set -g _sinex_bin $SINEX_SESSION_BIN; or set -g _sinex_bin sinex-session
set -g _sinex_counter 0

function _sinex_now_ns
    date +%s%N
end

function _sinex_uuid7 --argument-names started
    # The millisecond is cut from the nanosecond string, not divided: math
    # works in doubles, which cannot hold nanoseconds exactly.
    set -l ms (string sub -e -6 -- $started)
    set -l counter (math "$_sinex_counter % 4096")
    set -g _sinex_counter (math "$_sinex_counter + 1")
    set -l pid (math "$fish_pid % 4194304")
    set -l high (math "32768 + floor($pid / 256)")
    set -l low (math "($pid % 256) * 1099511627776 + "(random 0 1099511627775))
    printf '%08x-%04x-7%03x-%04x-%012x' (math --scale 0 "floor($ms / 65536)") (math "$ms % 65536") $counter $high $low
end

function _sinex_preexec --on-event fish_preexec
    set -g _sinex_started_ns (_sinex_now_ns)
    set -g _sinex_execution (_sinex_uuid7 $_sinex_started_ns)
    set -g _sinex_command $argv[1]
    set -g _sinex_cwd $PWD
    set -g _sinex_atuin_id "$ATUIN_HISTORY_ID"
    if set -q KITTY_WINDOW_ID
        printf '\e]1337;SetUserVar=sinex_execution=%s\a' (printf %s $_sinex_execution | base64 -w0) > /dev/tty
    end
end

function _sinex_postexec --on-event fish_postexec
    set -l exit $status
    set -q _sinex_execution; and test -n "$_sinex_execution"; or return 0
    set -l ended (_sinex_now_ns)
    set -l atuin "$ATUIN_HISTORY_ID"
    test -n "$atuin"; or set atuin $_sinex_atuin_id
    command $_sinex_bin report-command --shell fish --execution $_sinex_execution \
        --started-at-ns $_sinex_started_ns --ended-at-ns $ended --exit $exit \
        --cwd $_sinex_cwd --session $fish_pid --atuin-id "$atuin" -- $_sinex_command >/dev/null 2>&1 &
    disown 2>/dev/null
    set -g _sinex_execution ""
end
