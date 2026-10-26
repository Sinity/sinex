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
