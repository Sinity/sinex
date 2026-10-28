# Window-title classification

The opt-in `tag.applier` derivation classifies `focus.fused` records by the
rules in `services.sinex.tags`, also read from `tags` in the sources JSON
configuration. It applies the same rules to other registered event types.

| Field | Meaning |
| --- | --- |
| `tag` | The label to assign |
| `event_type` | The registered event type to read |
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
rule list reprocesses inputs, replacing each input's previous assignments
and saving progress together, including when no rule matches any more.
Hiding or replacing an input removes its per-event outputs; unhiding it
computes them again. Assignments use the ordinary query language.
