# Explicit journal and history imports

These formats read explicitly configured paths.

`hledger=PATH` reads a journal or archive through the shared export reader. Supported syntax is ISO civil dates, optional `=YYYY-MM-DD` effective dates, `*`/`!` status, description or payee `|` narration, semicolon comments, and ordered postings. A tab or at least two spaces separates account and amount. Decimal quantities keep their written precision and may have a leading or trailing commodity, with or without separating whitespace. A quantity without a commodity is allowed; an implicit amount stays absent. Balance assertions, prices, virtual/status-marked postings, periodic/automatic transactions and directives fail visibly. Includes are not traversed; `resolve_includes=true` is refused. Every block has its real byte range and unknown original time. Identity includes host, configured journal, archive member path and its duplicate number, complete block digest and duplicate number within the member. Reordering distinct members preserves their transaction keys. The counter is carried across parts and resets at a new member or export revision.

`text-history=PATH` reads complete appended lines as `command.imported`. It removes only LF or CRLF; spaces, backslashes and timestamp-looking text stay command content. Empty lines emit no event; whitespace-only lines are commands. Starts and durations stay unknown. Equal commands have distinct source locations. The shared append reader handles appends, rotation and restart within its remembered tail; it does not promise complete identity alignment across arbitrary ancient rewrites.

These sources declare guarded defaults. The shared reader currently treats a source's declared class as a minimum; configuring a lower class does not lower these imports.

- `finance.hledger`: `hledger`, `export-archive`, `transaction.posted`, purpose `journal transactions and their postings`.
- `terminal.text-history`: `text-history`, `append-file`, `command.imported`, purpose `explicit untimed command history`.
