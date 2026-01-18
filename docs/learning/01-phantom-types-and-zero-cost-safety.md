# Session 1.1: Phantom Types and Zero-Cost Type Safety

**File**: `crate/lib/sinex-core/src/types/ids.rs`
**C++ Anchor**: Template tags, `std::enable_if`, strong typedefs
**Time**: ~20 minutes

---

## The Problem (You Know This)

In C++, you've probably written something like:

```cpp
using UserId = uint64_t;
using EventId = uint64_t;

void process_event(EventId event_id);

UserId user = 42;
process_event(user);  // Compiles! But it's a bug.
```

The `using` alias provides documentation but **zero type safety**. The compiler happily accepts a `UserId` where an `EventId` is expected because they're both `uint64_t`.

---

## The Rust Solution: Phantom Types

Look at line 24-28 of your code:

```rust
pub struct Id<T> {
    ulid: Ulid,
    #[serde(skip)]
    _phantom: PhantomData<T>,
}
```

**What's happening here:**

1. `Id<T>` is generic over `T` - any type
2. `ulid: Ulid` is the actual data (128-bit unique ID)
3. `_phantom: PhantomData<T>` is the magic

---

## PhantomData: The Zero-Cost Marker

`PhantomData<T>` is a **zero-sized type**. It takes up no memory at runtime. Its only purpose is to tell the compiler "pretend this struct contains a `T`."

**Memory layout:**
```
Id<Event>  = [ 128 bits of Ulid ] + [ 0 bits of PhantomData ]
Id<User>   = [ 128 bits of Ulid ] + [ 0 bits of PhantomData ]
```

Same size. Same runtime representation. But to the compiler, `Id<Event>` and `Id<User>` are **completely different types**.

---

## The Compile-Time Guarantee

If you wrote:

```rust
fn process_event(event_id: Id<Event>) { ... }

let user_id: Id<User> = Id::new();
process_event(user_id);  // COMPILE ERROR
```

The error would be:
```
error[E0308]: mismatched types
  expected `Id<Event>`, found `Id<User>`
```

No runtime check. No possibility of mixing IDs. The bug is impossible.

---

## C++ Comparison: How You'd Do This

In C++, you'd need either:

**Option A: Separate classes (boilerplate explosion)**
```cpp
class UserId { uint64_t id; /* copy ctor, operator==, operator<, hash... */ };
class EventId { uint64_t id; /* same boilerplate again */ };
```

**Option B: Template tags (getting closer)**
```cpp
template<typename Tag>
struct Id { uint64_t value; };

struct UserTag {};
struct EventTag {};

using UserId = Id<UserTag>;
using EventId = Id<EventId>;
```

This works! But you still need to manually implement comparison operators, hash, etc. for each instantiation, or use CRTP tricks.

---

## Rust's `#[derive]` Eliminates Boilerplate

Look at line 22:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
```

This single line generates:
- `Debug` - print formatting
- `Clone`, `Copy` - value semantics (like C++ trivially copyable)
- `PartialEq`, `Eq` - `==` and `!=` operators
- `PartialOrd`, `Ord` - `<`, `>`, `<=`, `>=` operators
- `Hash` - for use in hash maps
- `Serialize`, `Deserialize` - JSON/binary encoding

**All of these work correctly for ANY `T`**. `Id<Event>` and `Id<User>` both get these implementations automatically, and they're type-safe.

---

## The `impl<T>` Block Pattern

Lines 30-66 show a single implementation that works for all `Id<T>`:

```rust
impl<T> Id<T> {
    pub fn new() -> Self {
        Self {
            ulid: Ulid::new(),
            _phantom: PhantomData,
        }
    }

    pub fn as_ulid(&self) -> &Ulid {
        &self.ulid
    }

    // ... more methods
}
```

**C++ equivalent** would be a class template. The key difference: Rust's trait bounds (coming later) let you constrain what `T` can be. Here, `T` is unconstrained - any type works.

---

## Serde: Transparent Serialization

Line 23:
```rust
#[serde(transparent)]
```

This tells the serializer "serialize this struct as if it were just the inner `ulid` field." The `PhantomData` is skipped (line 26: `#[serde(skip)]`).

**Result**: `Id<Event>` serializes to `"01ARZ3NDEKTSV4RRFFQ69G5FAV"` - just the ULID string. No wrapper object. No type tag in JSON.

The type safety exists only at compile time. At runtime and in serialized form, it's just a ULID.

---

## Key Insight: Zero-Cost Abstraction

This is what Rust means by "zero-cost abstractions":

| Aspect | Cost |
|--------|------|
| Runtime memory | Zero (PhantomData is 0 bytes) |
| Runtime performance | Zero (no type checks at runtime) |
| Compile-time safety | Maximum (impossible to mix ID types) |
| Code duplication | Zero (one impl block serves all) |

You get the safety of separate classes with the performance of raw integers.

---

## Exercise: Try to Break It

In a test file or scratch code, try:

```rust
use sinex_core::types::Id;

struct Event;
struct User;

let event_id: Id<Event> = Id::new();
let user_id: Id<User> = event_id;  // What happens?
```

Read the compiler error carefully. It's not "runtime type mismatch" - the assignment is statically rejected.

---

## What You Built Without Knowing

You implemented the **phantom type pattern** - a well-known Rust idiom for encoding type-level information without runtime overhead. This is used throughout the Rust ecosystem:

- `std::marker::PhantomData` is in the standard library
- Database ORMs use it for table/column type safety
- Web frameworks use it for request/response type tracking
- Your codebase uses it to prevent ID mixups

**You already wrote production-quality Rust. Now you know what to call it.**

---

## Next: `non_empty.rs`

We'll see how Rust encodes **invariants** (not just types) at compile time - making "empty vector access" impossible.
