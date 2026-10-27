# Window-title classification

Classification is a versioned per-event derivation over `focus.fused`.
It reads the observed application and title and applies a declared rule
list. It emits a derived event for every matching label. Unmatched text has
no classification; the original focus remains queryable.

The rule table has these columns:

| Column | Meaning |
| --- | --- |
| name | Unique rule name, recorded with each result |
| application | Exact application name, or absent for any application |
| field | `application` or `title` |
| comparison | `equals`, `starts with` or `ends with` |
| text | Literal text to compare, with case preserved |
| label | The label the rule assigns |

A missing title cannot match a title rule. Empty comparison text and empty
labels are errors. Multiple rules assigning one label produce one result
listing all matching rules in name order. Conflicting labels remain separate
assignments with their rules visible; the derivation does not choose between
them. There are no inferred projects, keywords, regular expressions or model
calls.

The table is part of the derivation version. Changing a rule increments that
version and reprocesses every focus reading. Admission replaces each input's
previous output set and advances progress in the same transaction, including
when a rule change leaves no outputs. The output names the focus record,
label, matching rules and original start time, and cites the focus reading as
its parent. Its class is at least its parent's class. These records are read
through the existing query language.

The initial rule list must come from the operator's chosen labels and literal
comparisons. No classification rules or labels are enabled by this design.
The tag rule table in `sinex-derivations/src/tags.rs` demonstrates the same
literal comparisons and attribution for source names and file extensions.
