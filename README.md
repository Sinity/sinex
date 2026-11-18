# Sinex

Sinex keeps a record of one person's digital life. It stores the
original bytes of everything
 its sources produce (shell and terminal history,
window focus, D-Bus, input devices, browser history, mail, chat exports,
media, files, and the records of other programs such as
[Polylogue](https://github.com/Sinity/polylogue)), reads those bytes into
timestamped events, derives sessions, episodes, figures and answers from the
events in stored layers, and answers questions about all of it. Every event
points at the bytes it was read from or the events it was derived from, so
any result can be traced back to its source and read again when a parser or
rule changes. People and agents use it through the same commands.

## Run it locally

The commands below use the current build, whose client is `sinexctl`.

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
store, the time zone and logical-day start, and exclusions.
For options shipped today, use `sinexd run --help` and the
[Nix module declarations](nix/module.nix). Sources whose inputs exist at their usual locations are captured without further setup. On
NixOS, the flake's module sets all of this up
([docs/operations.md](docs/operations.md#deployment)).

## First questions

```sh
sinexctl query 'shell-sessions where cwds ^ /home/me/src/tide | sort by start desc'
sinexctl recall last --cwd ~/src/tide
sinexctl brief
sinexctl show sinex:ev/EVENT_ID
sinexctl trace sinex:ev/EVENT_ID
```

`query` runs the query language over events, materials, sources, runs and
failures; `recall` reads around a time, a day or the latest shell session;
`brief` is the daily summary; `show` and `trace` read one event and follow it
down to its bytes. `sinexctl --help` and the MCP tool list describe current
operations.

## Decided command surface

The next client surface uses the product name, `sinex`, with one selected query,
time, path, URL or reference followed by verbs. It is a decided target, not
implemented by this documentation change:

```sh
sinex 'commands where cwd:/src/tide since:7d'
sinex yesterday recall
sinex e:EVENT_SUFFIX read --view trace
sinex here recall --cwd ~/src/tide
sinex stage ~/exports/history.zip
sinex stages
```

`sinex_read`, `sinex_write` and `sinex_act` share typed requests and answers,
with access checked by the daemon. Domain designs, including future evidence,
library and reader features, remain in their owning pages. See
[the complete grammar and current-build distinction](docs/interfaces.md#cli).

## Read next

- [docs/architecture.md](docs/architecture.md): how Sinex is put together and why.
- [docs/README.md](docs/README.md): every page of the design.
- [AGENTS.md](AGENTS.md): how work is done in this repository.
- [Development](docs/development.md): the declared Rust toolchain, shell and
  local overrides.

Cloud setup and shell startup, including the explicit native backend, are described in
[the shared setup guide](https://github.com/Sinity/polylogue-sinex-common/blob/master/dev/cloud/README.md).
