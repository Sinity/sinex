"""Sinex: report kitty events to `sinex-session`'s integration socket.

In kitty.conf:

    watcher /path/to/sinex_watcher.py

Each event is one JSON line on the socket ($SINEX_SESSION_SOCKET, or
$XDG_RUNTIME_DIR/sinex/session.sock). When nothing listens the event is
dropped: kitty never waits on Sinex. Command start and end need kitty's shell
integration.
"""

import json
import os
import socket


def _path():
    p = os.environ.get("SINEX_SESSION_SOCKET")
    if p:
        return p
    run = os.environ.get("XDG_RUNTIME_DIR")
    return os.path.join(run, "sinex", "session.sock") if run else None


def _send(event, window, **fields):
    path = _path()
    if not path:
        return
    line = {"source": "kitty", "event": event, "window_id": getattr(window, "id", None)}
    try:
        line["cwd"] = window.cwd_of_child
    except Exception:
        pass
    line.update({k: v for k, v in fields.items() if v is not None})
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(0.2)
            s.connect(path)
            s.sendall((json.dumps(line) + "\n").encode())
    except OSError:
        pass


def on_focus_change(boss, window, data):
    if data.get("focused"):
        _send("focus", window, title=window.title)


def on_title_change(boss, window, data):
    _send("title", window, title=data.get("title"))


def on_close(boss, window, data):
    _send("close", window)


def on_cmd_startstop(boss, window, data):
    if data.get("is_start"):
        _send("cmd_start", window, cmdline=data.get("cmdline"))
    else:
        _send("cmd_end", window, cmdline=data.get("cmdline"), exit_status=data.get("exit_status"))
