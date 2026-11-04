# Integrations

Producers inside the operator's session that report to `sinex-session`'s
integration socket (`$SINEX_SESSION_SOCKET`, or
`$XDG_RUNTIME_DIR/sinex/session.sock`), which `sinex-session run --integrations`
listens on and produces over the NATS ingress (`session.integrations` source).
Each writes one JSON object per line; when nothing listens, events are dropped
and the program never waits.

- `neovim/`: `require("sinex").setup()` reports buffer, write, directory,
  focus and exit events (`editor.reported`).
- `kitty/sinex_watcher.py`: `watcher /path/to/sinex_watcher.py` in kitty.conf
  reports command start and end with the command's output (with kitty's
  shell integration), focus, title and close (`terminal.reported`), and each
  window's scrollback lines not sent before, every
  `$SINEX_KITTY_SCROLLBACK_SECONDS` (180) and when the window closes or kitty
  quits (`scrollback.captured`).

- `shell/sinex.{zsh,bash,fish}`: the shell integration. For each command
  it mints a UUIDv7 execution token in the shell's preexec hook, hands it to
  kitty as the `sinex_execution` user variable (kitty's watcher then records
  it on the command's start and end), and reports the finished execution
  (token, command line as received, cwd, start, duration, exit, the shell's
  process ID and the Atuin row started for it) through `sinex-session
  report-command` in the background (`command.reported`). The NixOS module
  loads it into interactive shells when `services.sinex.session.shellIntegration`
  is on (the default with a session user). Bash needs bash 5.1 and
  bash-preexec, as Atuin's bash integration does. Command fusion joins a
  report and the Atuin row it names by identifier, not by time.

Each new Kitty and Neovim record with a cwd includes a capture-time
`git_identity` (`common_dir`, all reachable `root_commits`, `head`, `branch`).
A failed or timed-out probe records an unknown identity. Historical cwd values
are never resolved against today's filesystem. Kitty also records its host,
process and native window identity; a compositor-to-terminal join still needs
an exact adapter link. Git probes have a 200 ms timeout each.

Run the synthetic worktree identity check with
`python3 integrations/kitty/test_watcher.py`.
