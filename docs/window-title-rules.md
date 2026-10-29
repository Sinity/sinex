# Window-title classification

The opt-in `tag.applier` derivation classifies `focus.fused` records by the
rules in `services.sinex.tags`, also read from `tags` in the sources JSON
configuration. It applies the same rules to other registered event types.

| Field | Meaning |
| --- | --- |
| `tag` | The label to assign |
| `event_type` | The registered event type to read |
| `source` | Optional exact producer name |
| `enabled` | Whether this declaration participates; defaults to true |
| `when` | Conditions that must all match, or absent to match the whole type |
| `when[].field` | A JSON pointer into the payload, such as `/app` or `/title` |
| `is` | Exact JSON value equality |
| `contains`, `starts_with`, `ends_with` | Literal text comparisons, with both sides lowercased |
| `exists` | Present and not null, or absent or null |

For example, these rules label a recorded editor window:

```nix
services.sinex.derivations."tag.applier".enable = true;
services.sinex.tags = [{
  tag = "editor";
  event_type = "focus.fused";
  when = [
    { field = "/app"; is = "kitty"; }
    { field = "/title"; contains = "nvim"; }
  ];
}];
```

A missing title cannot match a text comparison. A non-string value does not
match a text comparison. Multiple rules assigning one label produce one
assignment listing every matching declaration in `rules`; other labels stay
separate. Unmatched text has no classification and remains queryable as the
original focus. There are no inferred projects, regular expressions or model
calls. The default rule list is empty.

Each `tag.applied` event names the input record, label, matching rules and
original time, with the input reading as parent and the same class and time
quality. Unknown time stays unknown. Rule order has no effect. A changed
rule list changes the producer version. New and dirty inputs use it; existing
history stays selected until `recompute tag.applier --compare` is followed by
`recompute tag.applier --switch COMPARISON_ID`. The switch archives the old
assignments and their chains, then reprocesses inputs through the ordinary
per-event runner, including when no rule matches any more.
Hiding or replacing an input removes its per-event outputs; unhiding it
computes them again. Assignments use the ordinary query language.

The input fields are the recorded `focus.fused` `record`, `started_at`,
`ended_at`, `app`, `title` and `observed_by`; rules inspect the payload fields
they name. Output fields are `tag`, `record` (the typed source, kind and input
record identity), `event_type`, `rules` and `at`. The event envelope supplies
the exact parent reading, producer version, class and time quality.

For these worked examples, A, B and C name declarations for explanation; they
are not configuration fields:

| Declaration | Conditions | Tag |
| --- | --- | --- |
| A | `app is "kitty"` and `title contains "nvim"` | `editor` |
| B | `title ends_with ".rs"` | `rust` |
| C | `title contains "notes"` | `editor` |

| Recorded input | Expected result |
| --- | --- |
| `app="kitty", title="nvim main.rs"` | Two assignments: `editor` citing A, and `rust` citing B |
| `app="kitty", title="nvim notes.rs"` | Two assignments: one `editor` citing both A and C, and `rust` citing B |
| `app="browser", title="Documentation"` | Unmatched: no `tag.applied` output; the original focus remains queryable |
| `app="kitty", title=null` | Unmatched: no text condition succeeds |

All matching declarations contribute; there is no first-match precedence.
Reversing A, B and C changes neither assignments nor the producer version.
Removing C changes that version. After a full comparison and switch,
`app="browser", title="notes"` loses its old `editor` assignment while its
original focus and archived assignment remain citable.
