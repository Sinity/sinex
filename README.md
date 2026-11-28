# Sinex

Sinex keeps a record of one person's digital life. It retains source
material from shell and terminal history, the desktop, browsers, mail, chat
exports, files and other programs such as
[Polylogue](https://github.com/Sinity/polylogue). When a declared file policy
keeps location facts alone, the recorded listing is the source material.
Events cite the material they were read from or the events they were derived
from. Sessions, periods of activity and figures can be traced back to that
evidence and recomputed when a parser or rule changes. People and agents use
the same commands under their credentials and grants.

Media curation and personal analysis are designed as separate applications
over Sinex's public interfaces. The current build still serves their existing
commands through the daemon.

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
with access checked by the daemon. The [media library](docs/library.md) and [personal analysis](docs/evidence.md)
own their domain surfaces. Sinex owns its [core reader](docs/web.md). See
[the complete grammar and current-build distinction](docs/interfaces.md#cli).

## Read next

- [docs/architecture.md](docs/architecture.md): how Sinex is put together and why.
- [docs/README.md](docs/README.md): every page of the design.
- [AGENTS.md](AGENTS.md): how work is done in this repository.
- [Development](docs/development.md): the declared Rust toolchain, shell and
  local overrides.

Cloud setup and shell startup, including the explicit native backend, are described in
[the shared setup guide](https://github.com/Sinity/polylogue-sinex-common/blob/master/dev/cloud/README.md).
