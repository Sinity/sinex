# Generated reference

The pages in this directory are generated, not written by hand. `cargo xtask
docs` renders them from the declarations the code already serves at run
time, and they are committed so they read on GitHub and in a checkout without
building. The ordinary test suite renders them again and compares, so a
declaration change and its page change land together.

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
`polylogue.*` kinds) are listed in that product's own reference.
