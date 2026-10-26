"""Sinex: report kitty events to `sinex-session`'s integration socket.

In kitty.conf:

    watcher /path/to/sinex_watcher.py

Each event is one JSON line on the socket ($SINEX_SESSION_SOCKET, or
$XDG_RUNTIME_DIR/sinex/session.sock). When nothing listens the event is
dropped: kitty never waits on Sinex. Command start and end need kitty's shell
integration; a command's end carries what it printed.

Every window's scrollback is sent too, only lines not sent before: the rows
that scrolled into its history every $SINEX_KITTY_SCROLLBACK_SECONDS (180 by
default), and the rest, the screen included, when the window closes or kitty
quits. A window showing its alternate screen (an editor, a pager) has no
history to send then. What was sent is remembered only while kitty runs.
"""

import json
import os
import re
import socket

_HOST = socket.gethostname()
_ROW_END = re.compile(r"(\r\n|\r)")
_timer = None
# Window id -> kitty's count of rows added to that window's history when its
# scrollback was last sent.
_sent = {}
# Windows whose scrollback was sent in full as kitty quit.
_finished = set()


def _path():
    p = os.environ.get("SINEX_SESSION_SOCKET")
    if p:
        return p
    run = os.environ.get("XDG_RUNTIME_DIR")
    return os.path.join(run, "sinex", "session.sock") if run else None


def _write(line):
    path = _path()
    if not path:
        return
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(0.2)
            s.connect(path)
            s.sendall((json.dumps(line) + "\n").encode())
    except OSError:
        pass


def _send(event, window, **fields):
    line = {"source": "kitty", "event": event, "window_id": getattr(window, "id", None)}
    try:
        line["cwd"] = window.cwd_of_child
    except Exception:
        pass
    line.update({k: v for k, v in fields.items() if v is not None})
    _write(line)


def _rows(text):
    """Split `as_text(add_wrap_markers=True)` into rows, each with its end:
    `\\r\\n` ends a line, a lone `\\r` a row the line wraps from."""
    parts = _ROW_END.split(text)
    rows = [parts[i] + parts[i + 1] for i in range(0, len(parts) - 1, 2)]
    if parts[-1]:
        rows.append(parts[-1])
    return rows


def _scrollback(window, final):
    """Send the rows of `window` not sent before: new history rows, and the
    screen too when `final`."""
    if window.id in _finished:
        return
    if final:
        _finished.add(window.id)
    screen = window.screen
    if screen.is_using_alternate_linebuf():
        return
    added = screen.history_line_added_count
    last = _sent.get(window.id, 0)
    if added < last:
        last = added
    text = window.as_text(as_ansi=True, add_history=True, add_wrap_markers=True)
    rows = _rows(text)
    history = min(screen.historybuf.count, len(rows))
    start = history - min(added - last, history)
    end = len(rows) if final else history
    _sent[window.id] = added
    if end <= start:
        return
    _send(
        "scrollback",
        window,
        host=_HOST,
        kitty_pid=os.getpid(),
        title=window.title,
        lines=len(rows),
        from_line=start,
        text="".join(rows[start:end]),
    )


def _every_window(boss, final):
    for window in list(boss.all_windows):
        try:
            _scrollback(window, final)
        except Exception:
            pass


def on_load(boss, data):
    global _timer
    if _timer is not None:
        return
    from kitty.fast_data_types import add_timer

    try:
        seconds = float(os.environ.get("SINEX_KITTY_SCROLLBACK_SECONDS", "180"))
    except ValueError:
        seconds = 180.0
    _timer = add_timer(lambda timer_id: _every_window(boss, False), seconds, True)


def on_quit(boss, window, data):
    if data.get("confirmed"):
        _every_window(boss, True)


def on_focus_change(boss, window, data):
    if data.get("focused"):
        _send("focus", window, title=window.title)


def on_title_change(boss, window, data):
    _send("title", window, title=data.get("title"))


def on_close(boss, window, data):
    try:
        _scrollback(window, True)
    except Exception:
        pass
    _sent.pop(window.id, None)
    _finished.discard(window.id)
    _send("close", window)


def on_cmd_startstop(boss, window, data):
    if data.get("is_start"):
        _send("cmd_start", window, cmdline=data.get("cmdline"))
        return
    try:
        output = window.cmd_output(as_ansi=True)
    except Exception:
        output = None
    _send(
        "cmd_end",
        window,
        cmdline=data.get("cmdline"),
        exit_status=data.get("exit_status"),
        output=output,
    )
