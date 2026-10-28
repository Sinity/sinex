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

Each new Kitty and Neovim record with a cwd includes a capture-time
`git_identity` (`common_dir`, all reachable `root_commits`, `head`, `branch`).
A failed or timed-out probe records an unknown identity. Historical cwd values
are never resolved against today's filesystem. Kitty also records its host,
process and native window identity; a compositor-to-terminal join still needs
an exact adapter link. Git probes have a 200 ms timeout each.

Run the synthetic worktree identity check with
`python3 integrations/kitty/test_watcher.py`.
