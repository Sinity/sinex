# Personal evidence

What Sinex computes about a life beyond "what was happening": figures over rest, body, intake, mood, money, places and work, each declaring the construct it stands for; usual ranges computed only from the past; studies of whether two things go together; randomized self-experiments; forecasts with a visible track record; and the rule by which Lynchpin's purposes are switched off one by one. The derivation model these products follow is in [derivations](derivations.md); presence, agency, attribution, episodes and the other descriptive products are in [the higher layer](higher-layer.md); notes and instruments are in [recall](recall.md#notes); coverage is in [sources](sources.md#completeness).

Decided target (2026-11-01). Sinex already has the method core (`sinex-analysis`: cell states, findings with typed caveats, correlation with effective sample size, change points, trend, robust baseline, false-discovery control, binary Brier scoring) and the forecast kinds; the construct catalog, studies, experiments, method forecasters, places, money and the retirement ledger below are decided and not built. Personal evidence needs staged exports for its inputs ([staging](materials.md#staging)).

## Purpose

Personal evidence answers questions about one person's life with numbers that say what they count, comparisons that say what they compare with, and tests that say how many other things were tested. It never produces "insights" as objects, never advises, and never turns a missing day into a zero. Its unit of trust is the **track record**: a baseline computed only from the past, a forecast written before its outcome, an experiment whose assignment was fixed before its first unit.

Five jobs:

1. **Know the usual.** Every series carries a usual range computed from prior days only, so a day reads against it without anyone defining "normal".
2. **Ask whether two things go together**, with the honest answer: how many units were usable, the smallest effect they could detect, what other reasonable choices would have given, and how many associations were already tried on the same data.
3. **Find out whether something works**: a randomized self-experiment with washout and, where possible, blinding, reported as an effect with its interval, a blinding check and adherence.
4. **Say what will happen and keep score**: the operator, agents and Sinex's own forecasters write forecasts that resolve themselves from the record, each shown beside its issuer's track record.
5. **Retire Lynchpin row by row**, each purpose switched off only when its replacement passes its named check.

## An example, on the synthetic world

Iris's Today shows last night on the rest lane: `5 h 58 min asleep (band) · usual 6 h 30 – 7 h 40 (prior 28 nights, 25 observed)`, with a thinner bar for the phone app's session of the same night and the standing agreement between the two instruments (`bias −5 min, 95% of nights within −28 to +20 min, 180 paired nights`). Nothing calls the night poor.

She asks whether late screen time goes with shorter sleep:

```
sinexctl study new late-screen --x 'activity.operator_seconds{hours 22-02}' --y rest.duration --lag 0
```

```
late-screen · exploratory · lagged association, Spearman (rest.duration is skewed)
  units: logical nights 2026-02-01 … 2026-09-30 (242)
  used 168 · excluded 74: band not worn 40, activity partial 20, no opportunity 14
  ρ = −0.21   95% interval −0.35 … −0.06   effective n 126 (autocorrelation)
  smallest detectable |ρ| at 80% power: 0.25
  across 12 reasonable specifications: −0.29 … −0.10                 [specification band]
  reverse lag (sleep before → screen after): ρ = −0.04 (−0.19 … 0.11)   [placebo]
  both series trend over the period: no
  test 31 of 380 associations on these nights; q across this study's family 0.03
  association, not cause.                       [result e:91c2 · readset · 168 nights]
  ▸ confirm on future nights
```

She confirms it: the study gains a registered twin whose outcome period starts tonight, labelled confirmatory, while the first stays exploratory forever. Later she runs a 40-night experiment comparing a supplement with a placebo in coded containers; Today shows `night 12 of 40 · tonight: container K7`, her morning guesses are recorded, and at the end the result reports the difference with its interval, the randomization p-value, units analysed, adherence and whether the blind held.

## Model

```
construct ◄── operationalizes ── metric declaration ──► figure cells (hourly → daily)
                                        │                      │ read by
note (self-report: instrument value) ───┘                      ▼
rest, body, money, place, intake, activity events ──► derivations ──► study ──► study.result (findings)
                                                                   │ design = experiment
                                                                   ▼
                              experiment.assignment (from a committed seed), unblinding act
forecast (authored, or issued by a method forecaster) ──► forecast.resolved ──► track record
```

### Constructs and the metric catalog

A **construct** is what a number stands for: sleep duration, sleep regularity, resting heart rate, mood valence, operator time at the machine, spending, time away from home, intake. Constructs are declared data in Rust beside the kinds: name, domain (rest, body, intake, mood, money, place, work, environment, machine), unit and a short definition.

A **metric declaration** operationalizes exactly one construct. Registration refuses a metric without construct, role, validity, opportunity and law:

| Field | Meaning |
|---|---|
| `construct` | which construct it operationalizes |
| `operationalization` | from which fields, and how |
| `role` | who produced the values: `measured`, `self_report` (contemporaneous or retrospective), `proxy`, `modeled`, `classification` |
| `validity` | how the number relates to the construct: `exact`, `parameterized`, `extracted`, `judged`, `vendor` (a provider's proprietary computation, whose validity is the vendor's and unknown; never shown as measured) |
| `law`, `allocation`, `grain` | the aggregation law (count, union duration, ratio, mean as sum and count, time-weighted, last as of, counter delta, quantile, distinct) and how an interval is assigned to a unit (a night belongs to the logical day it ends in) |
| `opportunity` | which sources' coverage makes a unit observed (for a band's sleep: heart-rate samples present across the night) |
| `strata` | fields that separate instruments (device, firmware, parser version); values of different strata are never pooled without a declared calibration |
| `sensors` | physical sensors the value depends on; two metrics sharing one carry a `shared_sensor` caveat when studied together |
| `parameters` | declared constants and their alternates, for the specification band |
| `agreement` | the standing agreement figures with other metrics of the construct, or "no agreement evidence" |
| `threats` | known threats as typed caveats the metric always carries |

Several metrics per construct is normal and is how construct validity becomes visible: every pair of metrics of one construct in the same unit has a standing **agreement study** (bias and limits of agreement after Bland and Altman 1986, not a correlation), shown as the metric's validity line.

### Figure cells

A **figure** is one stored `metric.figure` (or `health.figure`) event per metric version, day setting, grain, unit and dimension values; hourly first, and daily figures read hourly figures only ([figures](higher-layer.md#figures)). Each cell has a **state** and no default: `observed(v)`, `partial(v, fraction)`, `lower_bound(v)` (recorded, coverage unknown), `missing(reason)` with a reason from the coverage vocabulary, `not_applicable`, `not_estimable(reason)`. It cites its read ranges. A rendering never shows a number that is not a cell or a slot bound to one.

- **Usual range.** For a metric declaring `deviation = true`, the daily cell carries the prior-only robust baseline (median and MAD × 1.4826 over the prior 28 calendar days, at least 14 observed) as `usual_low`, `usual_high`, `usual_n`. `rhythm.deviation` remains the stored event for days outside it. A later day never changes an earlier day's range.
- **Affirmed zeros.** A self-report metric's opportunity is the union of **affirmations**: an instrument item such as "that's everything for today", or a declared logging period. A day with an affirmation and no answer is `observed(0)`; a day without one is `missing(unknown)`. Zero is never inferred from silence.

### Self-reports and intake

Self-reports are [notes](recall.md#notes) with an instrument value: instrument and version, the exact response with its scale and unit, answer time, and the prompt and its latency when there was one. The instruments are data the operator edits; those personal evidence uses are `mood` (valence and energy, ordinal), `rested` (ordinal), `intake` (substance from the operator's vocabulary, amount, unit, time as exact, approximate or unknown, route), `intake.day` (an affirmation), `guess` and `adherence` (experiment items), `doing` (free text) and performance tasks imported from a phone app's native records.

- Ordinal scales are analysed with rank methods; a mean over an ordinal scale is refused unless the request asks for it, and then carries a caveat.
- `intake.dose` normalizes each intake answer through a versioned substance table (configuration); an answer with unknown time keeps an unknown time.
- `exposure.level` (role `modeled`) is a one-compartment model over doses with the table's absorption and half-life; doses with unknown time are excluded and counted, never placed at a default time, and every exposure figure carries the specification band across the table's half-life range.
- Prompts for these instruments are signal rules, and none is sent until the operator decides which may be (open operator question). An experiment's own prompts are enabled by its start act and cover only its schedule (default; open operator question).

### Studies

A **study** is an [authored record](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/authored/README.md) (kind `study`, key its name) that states a question in computable form:

| Field | Meaning |
|---|---|
| `question` | the operator's or agent's words |
| `design` | `association` (lagged, over a panel), `contrast` (within-unit before and after), `change` (level shifts and change points), `agreement`, `forecast-evaluation`, or `experiment` |
| `family` | the complete list of (exposure, outcome, lag, stratum) cells, fixed in this revision |
| `population` | eligible units: grain, period, a predicate, a minimum observed fraction |
| `method` | method and version with parameters and the declared alternates |
| `outcome_period` | for confirmatory studies, the period whose outcomes are judged |
| `template` | the shipped template it instantiates, if any |

**Registration is an ordering fact.** A study revision is **confirmatory** when its commit position and its `at` both precede the start of its outcome period; every other study is **exploratory**. The flag is computed, never written, so it cannot be claimed. "Confirm on future units" writes a study of the same cells whose outcome period starts after the write.

A study's computed product is a **`study.result`**: a keyed derived event per study revision and evidence identity, recomputed when its read ranges are dirtied, holding:

- per family cell: the estimate with its interval, n, effective n, excluded units by reason, and diagnostics;
- the **detectable effect** at 80% power for the n used, so a null result reads "could not detect below X";
- the **specification band**: the estimate under every declared alternate;
- **placebo checks**: the reverse lag, a trend check in each series, and for a contrast the same clock hours on control units;
- multiplicity: Benjamini–Hochberg q across the whole family, empty cells included, and the **forking-paths count** (how many exploratory association cells the study ledger holds over overlapping units of the same outcome);
- **findings**: typed statements with template, slots, role, support status, caveats and evidence links. A finding's identity is `(study root, template, subject)`, so a judgment on it survives recomputation and shows stale when its grounds change.

A one-off `compute` is an unnamed study and is listed in the ledger like any other; there is no second analysis path. Every study ever run, null ones included, is a row of the `studies` unit. **Templates** (lifestyle and productivity, weather and outcomes, mood and day, intake and body, evening activity and the night, a generic exposures-versus-outcome scan) are data that fix a family's shape.

**Methods** are pure functions in `sinex-analysis`, each with an estimand, applicability checks and refusals, and each tested against an independent reference and a planted faulty implementation: lagged and rank correlation, trend (Mann–Kendall with Sen's slope), change points (binary segmentation with a BIC penalty; boundaries never moved toward annotations), robust baseline, anomaly, interval regularity (the Sleep Regularity Index), within-unit contrast, the exposure model, composite index (always labelled an index, with prior-only standardization and per-weight sensitivity), agreement, linear forecast, randomization test, bootstrap and permutation deltas, and recurrence.

### Experiments

An **experiment** is a study with `design = experiment` and a protocol. It is the only design whose findings may use the causal `effect` template, and only for units whose assignment was fixed before their exposure.

| Protocol field | Meaning |
|---|---|
| `conditions` | two or more named conditions with descriptions |
| `unit` | night, logical day, session, week |
| `assignment` | `randomized` per unit, `blocked`, `crossover`, or `external` (assignment recorded by another system, such as a benchmark manifest) |
| `washout` | units after each switch excluded from the primary analysis |
| `length` | planned units and the stopping rule: fixed, or fixed with one declared interim look and an alpha split |
| `primary`, `secondary` | one primary outcome with direction and smallest effect of interest; a family of secondary outcomes |
| `exposure` | how the actual condition is recorded |
| `blinding` | `open`; `coded` (identical containers labelled with codes, the key withheld); `self-shuffled` (the key is revealed at the end and recorded) |
| `analysis` | the estimand by assignment (intention to treat) and the randomization test; per protocol as secondary |
| `prompts` | the instrument prompts the experiment owns |

Its records, in order: draft revisions with a **plan check** (the detectable effect for the planned length, from the primary outcome's own prior history, by simulation); `experiment.started` (an act that fixes the protocol revision and commits the hashes of the seed and schedule; the seed is a withheld field); `experiment.assignment` per unit, derived from the seed, with the condition withheld while blinded; exposure, adherence and guess notes with the assignment as parent; `experiment.unblinded` (reading a withheld condition before the planned end needs this act, and units with outcome evidence after it are flagged); `experiment.stopped` for an early stop or amendment; and the `study.result`, **locked** until the planned end or the declared interim unless an `experiment.peeked` act exists, which the result reports.

Analysis is **randomization inference** (Edgington and Onghena): the p-value is the share of re-assignments drawn from the same declared mechanism whose statistic is at least as extreme, which is valid for one person without assuming independent days. Intervals invert the test over shifts. The result reports adherence by condition, blinding (guess accuracy against chance), carryover and the detectable effect achieved. Only the operator's own act starts an experiment, because it commits him to a schedule; agents may draft and plan.

### Forecasts

`forecast.issued` and `forecast.resolved` stay, with four changes:

1. **Two target shapes**: `binary`, a check (`{query, expect: rows | no rows | count op N}`) with a horizon; and `numeric`, a figure query naming one cell, with quantiles.
2. **Self-resolution** at the first evaluation after the horizon at which the check's or cell's coverage is complete. A cell that stays missing resolves `unknown(reason)` and is never scored. Manual resolution is an authored act labelled manual.
3. **Issuers**: the operator, an agent session, or a **method forecaster** (a declaration of method, version, features, target family and origin times) that issues forecasts as derived events. Each target family ships **climatology**, **persistence** and **linear** forecasters.
4. **Information by position**: `information_position`, the commit position at issue, defines what the issuer could have known; the list of event IDs the kind carries today goes.

A **track record** is a standing `study.result` per issuer and target family with `design = forecast-evaluation`, scored by the common [scoring functions](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/ranking/README.md): Brier and log scores, pinball loss, interval coverage, CRPS, calibration bins, and skill against climatology and persistence on the same resolved set. A forecast counts only if it was issued before its target period and before any resolving evidence was committed. **Backtests** of method forecasters read the store as of the commit position Sinex had at each origin, so late data never improves a past record, and they are labelled and never mixed with prospective records.

### Places and money

- **Places.** Location samples stay as recorded. `place.visit` is a stay of at least the declared dwell (default 10 min) within the declared radius (default 75 m), after stay-point detection (Li et al. 2008), with samples whose accuracy is worse than the radius excluded and counted; a gap in samples is a gap, never a stay. **Places** are authored records (name, centre, radius or polygon); a visit belongs to the authored place containing its centre, otherwise to an unnamed geohash cell that can be named later. Weather is acquired for observed locations; reanalysis data has role `modeled`.
- **Money.** Ledger postings and bank statement lines stay separate records. The deployment declares a **book of record** per account and period, and a money metric reads only that book for each account and says which. Categories are ledger accounts (exact) or a versioned payee rule table (`extracted`). Amounts are exact decimals per commodity. `finance.recurring` finds charges at a stable cadence and amount band; **last use** joins a recurring charge to usage only through a declared service rule, and without one it is `unknown`, never "never".

### How it reaches people

No ritual and no notification class of its own: Today and Day (the rest lane with its usual range, the experiment line, forecasts due, "Worth a look" rules such as *registered study concluded*, *measured metric outside its usual range*, *agreement between instruments broke*); recipes (rest-and-body and money sections in the `day`, `period` and `compare` recipes, and a `study` recipe for agents); signals (experiment prompts and passive notices); and agents through the eight Sinex tools.

Agent rules are enforced in the answer: a study result rendered for an agent carries its role and confirmatory flag in its first line; an association finding cannot be rendered through a causal template; an agent writing a study gets the forking-paths count in its answer.

## Retiring Lynchpin

Lynchpin's purposes are judged one row at a time. The ledger of rows lives in the task tracker as one task per purpose, each with its replacement, acceptance and Lynchpin unit, moving `designed → implemented → verified → retired` (or retired deliberately, or set aside with its contract designed). A row closes as verified only with its evidence:

- **reachable**: the replacement answers through `sinex_query`, `sinex_recall` or `sinex_explain` and the CLI, and the web view where the row names one;
- **original formats**: a test runs it from original-format materials, never from Lynchpin's outputs;
- **known defect**: the row's named Lynchpin defect, replayed as a case, fails the native test;
- **parity**: a stored parity study over at least 28 shared logical days, read at Lynchpin's day boundary by re-bucketing hourly cells, with every difference above the row's tolerance attributed to a named cause;
- **consumer**: the Lynchpin action that served the row has a named replacement call, and the Lynchpin module and its step are deleted in the change that retires the row.

Rows whose purpose needs model calls are set aside while model features are off (default; open operator question), with Lynchpin's last outputs kept as files. Lynchpin's timers and its MCP server go when their last row is retired, by the operator's authorization.

## Surfaces

```
sinexctl explain metric NAME | construct NAME | method NAME | instrument NAME | missing METRIC UNIT
sinexctl 'figures where metric in (rest.duration, activity.operator_seconds) since:90d | fill'
sinexctl study new NAME --x M --y M [--lag 0,1] [--grain night|day|week] [--method ID] [--template T]
sinexctl study NAME [--findings | --band | --placebo | --readset] | confirm [--for 60d] | revise …
sinexctl compute lagged_correlation(x = M, y = M, lags = [0, 1]) over ('figures …')
sinexctl experiment plan NAME --conditions 'A: …' 'B: …' --unit night --assignment blocked:2 --washout 1 --length 40 --primary M:+15m --blinding coded
sinexctl experiment NAME [start | unblind | peek | stop --reason TEXT] | experiment today
sinexctl forecast "TEXT" --p 0.35 --resolves-by 'QUERY' --expect '>= N' | forecast 'figures where …' --quantiles 10%=…,50%=…,90%=…
sinexctl forecast record [--issuer me|method:NAME|agent] [--family F] [--backtest]
sinexctl note --value mood.valence=3 | note --value intake.day=complete
sinexctl place name NAME --at here|LAT,LON [--radius 75m]
```

Every printed number ends with a short citation (`[f:3a1c]` for a cell, `[e:91c2#2]` for a finding). `not_estimable` is a successful answer, not an error.

| MCP tool | Rows |
|---|---|
| `sinex_query` | the units below; `compute` |
| `sinex_read` | a study result, a finding, a forecast with its issuer's track record, an experiment's status (withheld fields stay withheld) |
| `sinex_recall` | the `study` and `experiment` recipes; rest, body and money sections in `day`, `period` and `compare` |
| `sinex_write` | studies, experiment plans, forecasts, places, instruments, notes |
| `sinex_act` | experiment start, unblind, peek and stop; manual forecast resolution |
| `sinex_explain` | metric, construct, method, instrument, missing |

The deployed `sinex_analysis-catalog`, `-panel`, `-run`, `-why`, `sinex_forecast-issue`, `-report`, `-resolve` and `sinex_comparisons` fold into these rows.

| Unit | Rows | Notable fields |
|---|---|---|
| `figures` | figure cells | `metric`, `construct`, `grain`, `unit`, dimensions, `state`, `value`, `num`, `den`, `usual_low`, `usual_high`, `deviation`, `role`, `validity`, `provisional` |
| `metrics`, `constructs` | catalog declarations | the declaration fields; `agreement` resolves to the latest agreement result |
| `studies` | studies with computed state | `design`, `confirmatory`, `template`, `family_size`, `forking_paths`, `status` |
| `findings` | findings of current results | `template`, `role`, `support`, `caveats`, `estimate`, `q`, `judged` |
| `forecasts` | issued forecasts | `issuer`, `family`, `shape`, `horizon`, `resolved`, `score` |
| `places`, `visits` | authored places and `place.visit` | `place`, `cell`, `dwell`, `accuracy_excluded` |
| `intakes` | `intake.dose` | `substance`, `amount`, `unit`, `time_quality` |

`fill` over `figures` gives every unit of a finite range its stored state; a unit without a cell is missing with its coverage reason, never zero.

Web views (common web shell, URLs from references): **Evidence** (`/figures`, metrics by domain as rows and units as columns, each cell drawn by its state with the usual range and agreement line), **Study** (question, registration, data quality, intervals with the detectable effect, band, placebo, multiplicity, findings), **Experiment** (protocol, a calendar of units with conditions hidden while blinded, today's instruction, plan check, the locked result), **Forecasts** (open and resolved forecasts, track records with calibration and skill, backtests in their own tab), **Money** and **Places**.

## Performance

Budgets are p95, warm, at ten years of daily and two years of hourly data per metric, measured with paired benchmark runs.

| Operation | Budget | How |
|---|---|---|
| panel, 10 metrics × 365 days | ≤ 40 ms | `figure_cell` read model keyed by metric, version, day setting, grain, unit start and dimensions; one range scan per metric |
| panel, 100 metrics × 3,650 days | ≤ 400 ms, streamed | the same scan |
| study, ≤ 100 cells, ≤ 2,000 units, 12 alternates, placebo | ≤ 300 ms | the panel read once; columnar vectors with state masks |
| randomization test, 10,000 re-assignments × 200 units | ≤ 50 ms | assignments regenerated from the seed in place |
| plan check, 2,000 simulations | ≤ 1 s | resampled from the outcome's prior residuals |
| track record, 10,000 resolved | ≤ 30 ms | a forecasts read model with score columns written at resolution |
| backtest, daily forecaster over 2 years | ≤ 2 s | one sort-merge sweep over a figure-history index against the origins' positions |
| `place.visit` over a day of 1 Hz location | ≤ 200 ms | one pass with a running centroid |
