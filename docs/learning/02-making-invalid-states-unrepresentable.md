# Session 1.2: Making Invalid States Unrepresentable

**File**: `crate/lib/sinex-core/src/types/non_empty.rs`
**C++ Anchor**: Runtime assertions, `std::optional`, defensive programming
**Time**: ~25 minutes

---

## The Problem (Every C++ Dev Knows This Pain)

```cpp
std::vector<Event> events = get_events();
Event first = events[0];  // 💥 Undefined behavior if empty
```

Or with "defensive" code:

```cpp
if (!events.empty()) {
    Event first = events[0];  // Safe... if you remembered to check
}
```

The bug: you can **forget** the check. The compiler doesn't help. You find out at runtime (if you're lucky) or in production (if you're not).

---

## Rust's Philosophy: Make It Impossible

Look at lines 56-58 of your code:

```rust
/// Get the first element (always exists)
pub fn first(&self) -> &T {
    &self.inner[0]
}
```

This looks dangerous - direct indexing! But look at the return type: `&T`, not `Option<&T>`.

**Why is this safe?** Because `NonEmptyVec<T>` **cannot be constructed empty**.

---

## The Constructor Wall

Look at the constructors (lines 35-53):

```rust
/// Create a new NonEmptyVec with a single element
pub fn single(value: T) -> Self {
    NonEmptyVec { inner: vec![value] }
}

/// Create a new NonEmptyVec from a vector, returning None if empty
pub fn from_vec(vec: Vec<T>) -> Option<Self> {
    if vec.is_empty() {
        None
    } else {
        Some(NonEmptyVec { inner: vec })
    }
}

/// Create a new NonEmptyVec from a head element and tail vector
pub fn from_head_tail(head: T, tail: Vec<T>) -> Self {
    let mut inner = vec![head];
    inner.extend(tail);
    NonEmptyVec { inner }
}
```

**Every path to creating a `NonEmptyVec` guarantees at least one element:**

| Constructor | Guarantee |
|-------------|-----------|
| `single(value)` | Starts with one element |
| `from_vec(vec)` | Returns `None` if empty |
| `from_head_tail(head, tail)` | `head` is always present |

There is **no way** to create an empty `NonEmptyVec`. The `inner` field is private, so outside code can't construct it directly.

---

## Option Forces You to Handle Emptiness

The key is `from_vec` returning `Option<Self>`:

```rust
let events: Vec<Event> = get_events();

// This doesn't compile - you can't ignore the Option
let first = NonEmptyVec::from_vec(events).first();  // ERROR

// You MUST handle the None case
match NonEmptyVec::from_vec(events) {
    Some(non_empty) => {
        let first = non_empty.first();  // Safe! Type guarantees non-empty
        process(first);
    }
    None => {
        // Handle empty case - compiler forced you to think about this
    }
}
```

**The check moves from runtime to the point of construction.** After that point, the type system guarantees non-emptiness.

---

## C++ Comparison: Where the Check Lives

**C++ (runtime, scattered):**
```cpp
void process_events(std::vector<Event> events) {
    if (events.empty()) return;  // Check 1
    auto first = events[0];
    // ... 100 lines later ...
    auto last = events.back();  // Did you remember events could be empty here?
}
```

**Rust (construction time, centralized):**
```rust
fn process_events(events: NonEmptyVec<Event>) {
    let first = events.first();  // Guaranteed safe
    // ... 100 lines later ...
    let last = events.last();    // Still guaranteed safe
}
```

The function signature `events: NonEmptyVec<Event>` **documents and enforces** the requirement. Callers must prove non-emptiness to call the function.

---

## The `is_empty()` Method is a Lie (On Purpose)

Line 71-73:

```rust
/// NonEmptyVec can never be empty by construction
pub fn is_empty(&self) -> bool {
    false
}
```

This method always returns `false`. It exists for API compatibility (some generic code calls `.is_empty()`), but it's a **type-level tautology**. The compiler could inline this to `false` everywhere.

---

## Deserialization: The Boundary Guard

Lines 16-31 show how to enforce invariants when data comes from outside:

```rust
impl<'de, T> Deserialize<'de> for NonEmptyVec<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let vec = Vec::<T>::deserialize(deserializer)?;
        if vec.is_empty() {
            Err(serde::de::Error::custom("NonEmptyVec cannot be empty"))
        } else {
            Ok(NonEmptyVec { inner: vec })
        }
    }
}
```

**Key insight**: External data (JSON, network, files) is untrusted. The deserializer validates at the boundary and converts to the invariant-preserving type. After deserialization succeeds, the type guarantee holds.

---

## Deref: Transparent Access to Vec Methods

Lines 106-112:

```rust
impl<T> Deref for NonEmptyVec<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
```

This lets you call any `&Vec<T>` method on `NonEmptyVec<T>`:

```rust
let events: NonEmptyVec<Event> = ...;
events.len();           // Works via Deref
events.iter();          // Works via Deref
events.get(5);          // Works via Deref, returns Option<&Event>
```

**C++ equivalent**: Implicit conversion operator to `const std::vector<T>&`.

**Note**: `Deref` only gives `&Vec<T>`, not `&mut Vec<T>`. You can't call `.clear()` or `.pop()` because those could violate the non-empty invariant.

---

## The `map` Function: Preserving Invariants Through Transformations

Lines 96-103:

```rust
pub fn map<U, F>(self, f: F) -> NonEmptyVec<U>
where
    F: FnMut(T) -> U,
{
    NonEmptyVec {
        inner: self.inner.into_iter().map(f).collect(),
    }
}
```

This transforms each element but preserves the non-empty property. If you start with `NonEmptyVec<Event>`, you get `NonEmptyVec<ProcessedEvent>` - still guaranteed non-empty.

**C++ equivalent**: A `std::transform` that guarantees output size equals input size.

---

## Where You Use This in Sinex

Look at `Provenance::Synthesis` in `event_builder.rs`:

```rust
pub enum Provenance {
    Material { ... },
    Synthesis {
        source_event_ids: NonEmptyVec<EventId>,  // <- Here!
        operation_id: Option<Id<Operation>>,
    },
}
```

A synthesized event **must** come from at least one source event. The type system enforces this. You cannot create a `Synthesis` provenance with zero source events.

---

## Key Insight: Types as Documentation That Compiles

| Approach | Documentation | Enforcement | Maintenance |
|----------|---------------|-------------|-------------|
| Comment: "must not be empty" | Human-readable | None | Can drift |
| Runtime assert | Crashes in dev | Runtime only | Can be removed |
| `NonEmptyVec<T>` | In type signature | Compile-time | Cannot drift |

The type signature `fn process(events: NonEmptyVec<Event>)` is:
- **Documentation**: "This function requires non-empty input"
- **Enforcement**: Compiler rejects empty vectors
- **Future-proof**: Can't accidentally break the invariant

---

## Exercise: Feel the Safety

Try this:

```rust
use sinex_core::types::NonEmptyVec;

// This compiles
let good: NonEmptyVec<i32> = NonEmptyVec::single(42);
let first = good.first();  // Returns &i32, not Option<&i32>

// This returns Option - forces you to handle emptiness
let maybe: Option<NonEmptyVec<i32>> = NonEmptyVec::from_vec(vec![]);
// maybe is None

// What happens if you try to unwrap None?
let bad = NonEmptyVec::from_vec(vec![]).unwrap();  // Panic here, not in .first()
```

The panic (if any) happens at **construction**, not at **use**. The error points to where the invariant was violated, not 100 lines later.

---

## Pattern: Parse, Don't Validate

This technique has a name: **"Parse, Don't Validate"** (coined by Alexis King).

- **Validate**: Check a condition, continue with the same type, hope you remember the check later
- **Parse**: Convert to a type that encodes the validated property, compiler remembers for you

`NonEmptyVec::from_vec()` is a parser: it either gives you a proof of non-emptiness (`Some(NonEmptyVec)`) or tells you validation failed (`None`).

---

## What You Built Without Knowing

You implemented **refinement types** - types that carry proofs of properties. Rust doesn't have first-class refinement types (like Liquid Haskell), but you can encode many invariants with newtypes:

- `NonEmptyVec<T>` - at least one element
- `Id<T>` - type-safe identifier (Session 1.1)
- `SanitizedPath` - validated filesystem path (elsewhere in your codebase)
- `EventSource` - validated event source string

Each one moves a runtime check to compile time.

---

## Next: `error.rs`

We'll see how Rust replaces exceptions entirely - making error handling explicit, composable, and impossible to ignore.
