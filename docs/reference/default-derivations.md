# Default non-model derivations

This inventory names the outputs implemented in the current source tree. A
source-tree implementation is separate from its selected producer version and
from activation on a capture host. Model providers remain disabled.

| Named portfolio | Stored kinds | Implementation |
|---|---|---|
| Command fusion | `command.fused` | Preferred observations join by the declared command identity and timing rule. |
| Shell and work sessions | `shell.session`, `activity.window`, `activity.session`, `work.act`, `work.episode` | Shell runtime, operator presence and delegated work remain distinct. |
| State and attention intervals | `state.interval`, `activity.span` | Exact captured scopes, native unit invocations, suspend transitions and boot fences; unknown ends retain no duration. |
| Hourly and daily figures | `metric.figure`, `health.figure` | Daily products consume hourly products. |
| Rhythm deviations | `rhythm.deviation` | Prior-only median over complete input-capture days; default 28 calendar days and at least 14 observed days. Missing coverage and insufficient samples have explicit reasons. |
| Rest and health fusion | `rest.fused`, `health.figure` | Provider sleep observations fuse through the rest rule. Other health series retain their provider and measurement identity. |
| Provider-local communication | `conversation.day` | Recorded thread/account identity; unthreaded mail stays separate. |
| Input bouts and workload | `input.bout`, `input.workload` | Original non-virtual evdev instants, 30 s bouts, typing gaps below 2 s, measured coverage and missing seconds. Fewer than 200 presses leaves typing duration not estimable. |
| Document text | `document.text` | Stored document bytes with their copy event as parent. |
| Mark links | `reference.about` | Native command/focus record keys, or containing shell/activity sessions on the recorded host. Command text alone never establishes a link. |

The pure cases are in `sinex-derivations` tests. The daemon's
`state_closer_after_more_than_sixty_four_refreshes_is_not_lost` test exercises the
structural state reader and a late closer through admission and the runner.
`screen_text` tests exercise captured accessibility text without starting OCR.

AT-SPI capture and compositor-to-terminal hook capture are source adapters.
This portfolio preserves absent adapter evidence as unknown; adding a text
field or a terminal identity does not establish that a host captured either.
