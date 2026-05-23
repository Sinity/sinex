# Task Domain

Tasks are event-native workflow objects. The canonical task is not a knowledge
graph node, a markdown backlink, or a Taskwarrior row. It is current state
projected from admitted task-domain lifecycle events.

This record defines the v1 task model, Taskwarrior boundaries, provenance rules,
and first implementation slice.

## Layering

| Layer | Role |
| --- | --- |
| Source material | Taskwarrior exports, manual CLI/UI declarations, project notes, agent/session logs. |
| Direct interpretation | Authoritative material can emit task lifecycle events. |
| Proposal layer | Inferred tasks and edits are candidate events reviewed through proposal/judgment/finalizer. |
| Finalizer | Accepted candidate emits admitted task-domain events. |
| Reducer | Shared domain reducer projects current task state. |
| KG | Optional semantic links to projects/entities/files; not lifecycle owner. |

Task state uses the reducer contract in `domain-reducers.md`. Task-specific code
owns lifecycle semantics and typed validation.

## Event Families

V1 task lifecycle events:

| Event | Meaning |
| --- | --- |
| `task.created` | Creates a task object. |
| `task.updated` | Updates title, body, project, tags, priority, due/scheduled metadata. |
| `task.started` | Marks active work start or intent to work now. |
| `task.blocked` | Records a blocking reason and optional blocker reference. |
| `task.deferred` | Hides or postpones until a future activation window. |
| `task.completed` | Terminal successful completion. |
| `task.cancelled` | Terminal cancellation. |
| `task.split` | Replaces one task with multiple successor tasks. |
| `task.merged` | Replaces multiple tasks with one successor task. |
| `task.linked` | Adds relationship to project, note, file, event, or entity. |

Avoid `task.proposed` as a lifecycle event. What is proposed is a candidate
`task.created`, `task.updated`, or `task.completed` event.

## Payload Sketch

```rust
pub struct TaskCreatedPayload {
    pub task_id: TaskId,
    pub title: String,
    pub body: Option<String>,
    pub source_system: TaskSourceSystem,
    pub external_id: Option<String>,
    pub project_id: Option<String>,
    pub tags: Vec<String>,
    pub due_at: Option<Timestamp>,
    pub scheduled_for: Option<TimeRange>,
    pub priority: Option<TaskPriority>,
}

pub struct TaskTransitionPayload {
    pub task_id: TaskId,
    pub transition: TaskTransition,
    pub actor: ActorRef,
    pub reason: Option<String>,
    pub external_version: Option<String>,
}
```

`TaskId` is a Sinex domain object id. External ids, such as Taskwarrior UUIDs,
are authority-scoped aliases, not the primary id unless an import policy
explicitly chooses that mapping.

## Projection State

Task state is a reducer output:

```sql
create table domain.task_state (
  task_id uuid primary key,
  status text not null check (status in ('open', 'started', 'blocked', 'deferred', 'completed', 'cancelled')),
  title text not null,
  body text,
  project_id uuid,
  tags text[] not null default '{}',
  due_at timestamptz,
  scheduled_for tstzrange,
  priority text,
  external_refs jsonb not null default '[]'::jsonb,
  last_event_id uuid not null references core.events(id),
  state_hash text not null,
  updated_at timestamptz not null default now()
);
```

This table is not canonical truth. It is rebuildable from task events and the
task reducer semantics version.

## Provenance

| Origin | Canonical path |
| --- | --- |
| Manual CLI/UI declaration | Register interaction/form input as source material, then emit material-provenance task event. |
| Taskwarrior export | Register export as source material; parser emits material-provenance task events or source observations depending on authority mode. |
| Project note | Register note as source material; extraction creates a proposal unless note policy says direct declarations are authoritative. |
| LLM/task extractor | Record model effect if applicable; emit proposal, not task lifecycle event. |
| Accepted proposal | Finalizer emits canonical task event with synthesis provenance to proposal and judgment, or material provenance if the finalizer records a new user-authored edit as material. |

Accepted synthetic proposals do not create a third provenance class. They become
ordinary material or synthesis events through the existing provenance model.

## Taskwarrior Boundary

Classify Taskwarrior as `BidirectionalAdapter` only after conflict policy
exists. Until then:

| Mode | Policy |
| --- | --- |
| Import source material | Supported first: Taskwarrior exports are staged material. |
| External id mirror | Preserve Taskwarrior UUID/status/version as `external_refs`. |
| Projection export | Safe when Sinex is canonical for the exported subset. |
| Peer canonical mirror | Requires explicit authority and conflict policy. |
| Bidirectional sync | Out of scope for v1. |

Taskwarrior must not become the hidden task ontology. It is an adapter over the
task domain.

## Relations

Task lifecycle fields stay in the task domain. Cross-domain relationships can
also appear as KG or relation events, but ownership must be clear:

| Relationship | Owner |
| --- | --- |
| status/title/due/priority | Task reducer. |
| project membership | Task field plus optional project relation. |
| note/file/event evidence | Relation/link event referenced in trace. |
| people/entities | KG relation or tag, not task lifecycle state. |
| duplicate/split/merge | Task lifecycle event with supersession trace. |

## First Slice

First implementation issue should cover manual task declarations plus reducer
projection, not Taskwarrior sync.

Fixtures:

1. `sinexctl declare task --title "Pay tax" --due 2026-04-30` creates source
   material for the declaration, emits `task.created`, and projects open state.
2. `task.completed` transitions the same task to completed.
3. A note-derived candidate task creates a proposal; rejecting it creates no
   task state, accepting it finalizes into `task.created`.

This proves the reducer path, provenance discipline, and proposal boundary
without external sync complexity.

## Boundaries

- Do not build a full task UI in v1.
- Do not implement Taskwarrior bidirectional sync before conflict policy.
- Do not let LLM-inferred tasks become canonical without judgment/finalizer.
- Do not make KG own task lifecycle.
- Do not encode every personal workflow before the core lifecycle is tested.
