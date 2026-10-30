# Generated reference

The designed `cargo xtask docs` generator will render the pages below from
the declarations served at run time. They will be committed and compared
with their rendered declarations by the ordinary test suite. That generator
is not implemented yet. For shipped commands and options, use
`sinexctl --help`, `sinexd run --help` and the MCP tool schemas; Nix option
descriptions live in the [module declarations](../../nix/module.nix).

The current generated source and derivation catalog is
[`nix/catalog.json`](../../nix/catalog.json), emitted by `sinexd admin catalog`
and checked by a golden test. The Nix module and discovery code read it;
change the declarations and regenerate it rather than editing it by hand.

## Planned pages

These filenames are planned destinations, not existing pages.

| Page | Rendered from |
|---|---|
| `commands.md` | the operation table: CLI form, MCP tool and its schema, HTTP route, latency class, failure kinds |
| `settings.md` | the typed configuration, the changeable settings catalog (key, type, default, scope), environment names, and the NixOS and Home Manager module options |
| `kinds.md` | every kind Sinex declares: fields, field hints, search participation |
| `sources.md` | every source declaration: family, inputs, change model, kinds produced |
| `query-tables.md` | the query catalog: tables, relations, and each field's meaning and unknown rule |
| `derivations.md` | every derivation's declaration |

Until a page's generator exists, the hand-written table it replaces stays in
the area page that owns the subject; the change that adds the generator
deletes that table. Kinds that other products register (Polylogue's
`polylogue.*` kinds) will be listed in that product's own planned reference.

[Default non-model derivations](default-derivations.md) lists the implemented
portfolio, its stored kinds and the explicit evidence limitations.
