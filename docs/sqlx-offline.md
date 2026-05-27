# SQLx offline workflow

Sinex uses SQLx's compile-time query checking (`sqlx::query!`,
`sqlx::query_as!`) in `crate/lib/sinex-db` and a few callers. By default
those macros need a `DATABASE_URL` pointing at a live Postgres with the
sinex schema applied. That is fine on sinnix-prime but breaks anywhere
without a database: CI runners, cloud-agent sandboxes (Claude Code Web,
Codex Cloud), and contributor machines that have not yet bootstrapped
infra.

The fix is to commit SQLx's prepared-query cache to the repo and build
with `SQLX_OFFLINE=true`. The cache lives at `.sqlx/` (one JSON file per
unique query, content-hashed). When `SQLX_OFFLINE=true` is set, the
macros read from that cache instead of phoning Postgres.

## When to refresh `.sqlx/`

Refresh after any change to:

- A `query!`/`query_as!`/`query_file!` invocation (new query, edited
  SQL, changed bound-parameter types).
- A migration that alters a column type touched by an existing query.

If you change neither, the existing cache is still valid and no action
is required.

## Workflow

Run on a host with a live Postgres that has the sinex schema applied
(typically sinnix-prime, after `xtask infra start` and migrations):

```bash
# 1. Install the matching sqlx-cli (once per machine).
cargo install sqlx-cli --no-default-features --features postgres

# 2. Point at the live DB.
export DATABASE_URL="postgres://sinex:dev@localhost:5432/sinex_dev"

# 3. Regenerate the offline cache for every workspace member.
cargo sqlx prepare --workspace

# 4. Review and commit the diff.
git add .sqlx
git status .sqlx
git commit -m "chore(sqlx): refresh offline cache"
```

`cargo sqlx prepare --workspace` writes/updates `.sqlx/*.json` files at
the repo root. The files are deterministic given the same queries +
schema, so the diff is reviewable.

## Verifying the cache works

From any environment, with no live DB available:

```bash
SQLX_OFFLINE=true cargo check -p sinex-db
```

This must compile. If it errors with "no cached data for this query",
`.sqlx/` is stale relative to source — refresh per the workflow above.

## Cloud lane interaction

The cloud-agent setup (`.claude/settings.json`) sets `SQLX_OFFLINE=true`
unconditionally. Cloud sandboxes therefore always read from `.sqlx/` and
never attempt a DB connection at build time. If `.sqlx/` is missing or
stale on master, every cloud sandbox build of `sinex-db` (and downstream
crates that pull it in via macros) breaks until the operator refreshes
the cache from a host with the live DB.

There is intentionally no automation to refresh `.sqlx/` from inside a
cloud sandbox — the sandboxes have no path to the production schema
and a stale-or-missing cache is the right failure mode (loud, fixed in
one operator action).
