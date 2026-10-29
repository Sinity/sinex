# Sinex

Sinex keeps a durable record of one person's digital life. It stores the
exact bytes of everything its sources produce (shell and terminal history,
window focus, D-Bus, input devices, browser history, mail, chat exports,
media, files, and the records of other programs such as
[Polylogue](https://github.com/Sinity/polylogue)), reads those bytes into
timestamped events, derives sessions, episodes, figures and answers from the
events in stored layers, and answers questions about all of it. Every event
points at the bytes it was read from or the events it was derived from, so
any result can be traced back to its source and read again when a parser or
rule changes. People and agents use it through the same commands.

## Run it locally

Sinex needs PostgreSQL with TimescaleDB, and NATS for producers outside the
daemon. With Nix, `nix develop` provides both, and `nix build` builds
`sinexd`, `sinexctl` and `sinex-schema`.

```sh
createdb sinex
sinex-schema apply 'postgresql:///sinex'        # create the schema
sinexd run                                      # the daemon: capture, admission, derivations, API
sinexctl stage ~/.local/share/atuin/history.db  # read an input into Sinex
sinexctl stage list                             # what has been staged and how far it has been read
```

Deployment configuration covers database and NATS endpoints, the content
store, the time zone and logical-day start, and exclusions. Every key
will be listed in [planned docs/reference/settings.md](docs/reference/README.md#planned-pages).
For options shipped today, use `sinexd run --help` and the
[Nix module declarations](nix/module.nix). Sources whose inputs exist at their usual locations are captured without further setup. On
NixOS, the flake's module sets all of this up
([docs/operations.md](docs/operations.md#deployment)).

## First questions

```sh
sinexctl query 'shell-sessions where cwds ^ ~/src/tide sort start desc'
sinexctl recall last --cwd ~/src/tide
sinexctl brief
sinexctl show sinex:ev/EVENT_ID
sinexctl trace sinex:ev/EVENT_ID
```

`query` runs the query language over events, materials, sources, runs and
failures; `recall` reads around a time, a day or the latest shell session;
`brief` is the daily summary; `show` and `trace` read one event and follow it
down to its bytes. `sinexctl --help` and the MCP tool list describe current
operations; [planned docs/reference/commands.md](docs/reference/README.md#planned-pages)
will render the operation declarations as a committed page.

## Read next

- [docs/architecture.md](docs/architecture.md): how Sinex is put together and why.
- [docs/README.md](docs/README.md): every page of the design.
- [AGENTS.md](AGENTS.md): how work is done in this repository.

Cloud setup and shell startup, including the explicit native backend, are described in
[the shared setup guide](https://github.com/Sinity/polylogue-sinex-common/blob/master/dev/cloud/README.md).
