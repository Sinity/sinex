# Kestrel

Synthetic shell history, book records and ActivityWatch observations for
Iris Vale on the fictional host `kestrel`, in September 2026. Display time is
Europe/Warsaw; logical days start at 04:00.

The binding-reader tests copy the shell files and book export into a temporary
home and translate the ActivityWatch response rows into the SQLite tables
its current reader consumes. They use real binding reads, admission, replay
and query execution. Expectations are handwritten from these inputs.

The away span first ends at 08:00 UTC and later at 09:27:30 UTC, under the
same bucket and row ID. A later response repeats two rows unchanged and
extends another. The zsh history has a Polish string, a multiline command,
and a 01:30 local command belonging to the previous logical day. Atuin also
observes those evening commands, including an immediate repeat of `git status`,
and has a deleted command row. The book export supplies three dated events from two books.
