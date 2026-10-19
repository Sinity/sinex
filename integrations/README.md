# Integrations

Producers inside the operator's session that report to `sinex-session`'s
producer socket (`$SINEX_SESSION_SOCKET`, or
`$XDG_RUNTIME_DIR/sinex/session.sock`), which `sinex-session run --producers`
listens on and produces over the NATS ingress (`session.producers` source).
Each writes one JSON object per line; when nothing listens, events are dropped
and the program never waits.

- `neovim/`: `require("sinex").setup()` reports buffer, write, directory,
  focus and exit events (`editor.event`).
- `kitty/sinex_watcher.py`: `watcher /path/to/sinex_watcher.py` in kitty.conf
  reports command start and end (with kitty's shell integration), focus,
  title and close (`terminal.event`).
