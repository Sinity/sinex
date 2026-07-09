//! `CommitFrontier<C>`: a typed progress-frontier primitive (sinex-4as3).
//!
//! Atoms enter IN ORDER — [`CommitFrontier::submit`] returns an [`AtomTicket`]
//! that is the ordering witness — and complete OUT OF ORDER: receipts settle
//! whenever [`CommitFrontier::complete`] is called for them. The committable
//! frontier only advances over a CONTIGUOUS run of terminal atoms from the
//! front — a still-pending atom (a "hole") blocks every atom submitted after
//! it from joining the frontier, regardless of when those later atoms
//! themselves complete.
//!
//! This is the mechanism half of Batch A ([`crate::events`] provenance and the
//! sinex-r6d.11 durable-emission-receipt design are the durability half).
//! Today frontier logic across sinexd is hand-rolled and scattered per caller
//! (source cursors, automaton checkpoints, invalidation acks each reinvent
//! ordering and get it differently wrong) — this primitive is meant to be the
//! one shared implementation all of them route through.
//!
//! Deliberately scoped: this module is the pure, sync, dependency-free core
//! only (no tokio, no NATS, no clock) — exactly what the ratified design
//! calls for so it can be shared as the reference model for property tests
//! elsewhere in the workspace (sinex-87hu). Wiring this into source cursor
//! commits, automaton input-batch marks, or invalidation scopes — and any
//! wall-clock "how long has this hole been blocking" observability — is
//! caller-side integration work explicitly NOT done here; see sinex-r6d.4,
//! sinex-vxu, sinex-r6d.7, sinex-w4i.
//!
//! Property invariants (exercised by the tests in this module):
//! - monotonicity: the frontier position never decreases
//! - out-of-order completion never reorders commits: the frontier only ever
//!   advances over a contiguous prefix from the start
//! - a hole blocks every atom submitted after it, regardless of completion
//!   order
//! - checkpoint round-trip: restoring from an observed checkpoint reproduces
//!   the same frontier position and payload
//! - a duplicate `complete()` for an already-completed ticket is a no-op

use std::collections::VecDeque;

/// Opaque ordering witness returned by [`CommitFrontier::submit`].
///
/// Comparable and orderable by submission sequence; treat the wrapped value
/// as opaque outside of logging/debugging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AtomTicket(u64);

impl AtomTicket {
    /// The raw monotonic submission sequence number.
    #[must_use]
    pub fn sequence(self) -> u64 {
        self.0
    }
}

/// Terminal outcome for a submitted atom — the progress-unlocking receipt
/// states from the sinex-r6d.11 durable-emission-receipt design.
///
/// Every variant here IS progress-unlocking by construction: there is no
/// "pending" or "failed, will retry" variant. An atom that has not (yet)
/// reached one of these states has simply not completed yet, and stays a
/// hole until [`CommitFrontier::complete`] is called for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalOutcome {
    /// The atom's effect was durably persisted and confirmed.
    PersistedConfirmed,
    /// The atom was suppressed, deduplicated, or superseded, with durable
    /// evidence backing that decision — not a bare unaccounted skip.
    SuppressedWithEvidence,
    /// The atom's effect could not be durably settled, but the failure
    /// itself was durably recorded as debt for later reconciliation.
    DurableDebt,
    /// The atom legitimately produced no output (e.g. a filtered record)
    /// and there is nothing further to settle.
    NoOutputSettled,
    /// The atom's raw input was durably accepted into a lossless local
    /// spool. Only a valid terminal state once the spool itself is lossless
    /// (sinex-r6d.5) — a capped/discarding spool must never report this.
    SpoolAcceptedLossless,
}

#[derive(Debug, Clone)]
struct Entry<C> {
    ticket: AtomTicket,
    outcome: Option<(TerminalOutcome, C)>,
}

/// A non-terminal atom currently blocking the frontier — the debt/
/// observability surface named `holes()` in the ratified design.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hole {
    pub ticket: AtomTicket,
    /// This atom's position in the frontier's global submission order
    /// (0-based, counting every atom ever submitted — not just pending
    /// ones), so an operator can say which submission is stuck in absolute
    /// terms, not just "Nth pending".
    pub position: u64,
}

/// A typed progress-frontier over a caller-defined checkpoint payload `C`.
///
/// `C` is whatever the caller needs to durably resume from a given point (a
/// source cursor fragment, an automaton input-batch identity, ...). Only the
/// checkpoint payload of the LAST atom in the contiguous-terminal run is
/// retained as the frontier's current checkpoint: earlier payloads are
/// superseded once folded in, since a resume only ever needs the newest
/// committable point, per the ratified design's "checkpoint = serialized
/// frontier" rule.
#[derive(Debug, Clone)]
pub struct CommitFrontier<C> {
    /// Atoms not yet folded into the committed frontier, oldest first. Once
    /// `advance()` runs, either this is empty or its front entry is a hole
    /// (an entry with `outcome: None`) — a completed front entry is always
    /// immediately drained.
    pending: VecDeque<Entry<C>>,
    next_ticket: u64,
    /// Checkpoint payload of the highest contiguous terminal atom folded in
    /// so far, if any.
    committed: Option<C>,
    /// How many atoms have ever been folded into the frontier. Monotonically
    /// non-decreasing — this IS the frontier position.
    committed_count: u64,
}

impl<C> Default for CommitFrontier<C> {
    fn default() -> Self {
        Self {
            pending: VecDeque::new(),
            next_ticket: 0,
            committed: None,
            committed_count: 0,
        }
    }
}

impl<C> CommitFrontier<C> {
    /// A fresh frontier with nothing submitted and nothing committed.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Restore a frontier from a previously observed checkpoint and its
    /// frontier position. The restored frontier starts with no pending
    /// atoms — atoms submitted before the checkpoint are, by definition,
    /// already folded in; anything after it re-enters via `submit` as the
    /// caller replays or redelivers it (the r6d.11 request_id ledger is what
    /// lets a restarted producer reconcile in-flight receipts rather than
    /// blindly re-emitting).
    #[must_use]
    pub fn restore(checkpoint: C, committed_count: u64) -> Self {
        Self {
            pending: VecDeque::new(),
            next_ticket: 0,
            committed: Some(checkpoint),
            committed_count,
        }
    }

    /// Admit one atom. Admission is strictly in submission order: the
    /// returned ticket is the ordering witness `complete()` uses to fold
    /// this atom into the frontier once every atom submitted before it has
    /// also completed.
    pub fn submit(&mut self) -> AtomTicket {
        let ticket = AtomTicket(self.next_ticket);
        self.next_ticket += 1;
        self.pending.push_back(Entry {
            ticket,
            outcome: None,
        });
        ticket
    }

    /// Record the terminal outcome and checkpoint payload for a previously
    /// submitted atom. Completion may arrive in any order relative to other
    /// pending atoms — the frontier only actually advances once every atom
    /// ahead of this one has also completed.
    ///
    /// A duplicate `complete()` for a ticket that already recorded an
    /// outcome is a no-op: idempotent by ticket, per the r6d.11 doctrine
    /// that a restarted producer must be able to reconcile in-flight
    /// receipts without corrupting the frontier. Completing an unknown
    /// ticket (never submitted, or already folded past the frontier) is
    /// also a no-op — this primitive does not police caller bugs at the
    /// ticket level, only the frontier's own invariants.
    pub fn complete(&mut self, ticket: AtomTicket, outcome: TerminalOutcome, checkpoint: C) {
        for entry in &mut self.pending {
            if entry.ticket == ticket {
                if entry.outcome.is_none() {
                    entry.outcome = Some((outcome, checkpoint));
                }
                break;
            }
        }
        self.advance();
    }

    /// Fold every contiguous-from-the-front terminal atom into the committed
    /// frontier, stopping at the first hole (or an empty queue).
    fn advance(&mut self) {
        while let Some(front) = self.pending.front() {
            if front.outcome.is_none() {
                break;
            }
            let Entry { outcome, .. } = self
                .pending
                .pop_front()
                .expect("front just observed Some above");
            let (_, checkpoint) = outcome.expect("checked is_some above");
            self.committed = Some(checkpoint);
            self.committed_count += 1;
        }
    }

    /// The current committable frontier: how many atoms have ever been
    /// folded in (the frontier position — monotonically non-decreasing) and
    /// the checkpoint payload to resume from, if any atom has ever
    /// committed.
    #[must_use]
    pub fn frontier(&self) -> (u64, Option<&C>) {
        (self.committed_count, self.committed.as_ref())
    }

    /// Non-terminal atoms currently blocking the frontier, oldest (i.e. most
    /// blocking) first.
    #[must_use]
    pub fn holes(&self) -> Vec<Hole> {
        self.pending
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.outcome.is_none())
            .map(|(offset, entry)| Hole {
                ticket: entry.ticket,
                position: self.committed_count + offset as u64,
            })
            .collect()
    }

    /// Whether every atom submitted so far has been folded into the
    /// frontier (no holes at all right now).
    #[must_use]
    pub fn is_fully_committed(&self) -> bool {
        self.pending.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(payload: u64) -> (TerminalOutcome, u64) {
        (TerminalOutcome::PersistedConfirmed, payload)
    }

    #[test]
    fn empty_frontier_has_no_holes_and_is_fully_committed() {
        let frontier: CommitFrontier<u64> = CommitFrontier::new();
        assert_eq!(frontier.frontier(), (0, None));
        assert!(frontier.holes().is_empty());
        assert!(frontier.is_fully_committed());
    }

    #[test]
    fn in_order_completion_advances_monotonically() {
        let mut frontier = CommitFrontier::new();
        let t0 = frontier.submit();
        let t1 = frontier.submit();
        let t2 = frontier.submit();

        assert_eq!(frontier.frontier().0, 0, "nothing completed yet");
        let (outcome0, payload0) = outcome(100);
        frontier.complete(t0, outcome0, payload0);
        assert_eq!(frontier.frontier(), (1, Some(&100)));
        let (outcome1, payload1) = outcome(101);
        frontier.complete(t1, outcome1, payload1);
        assert_eq!(frontier.frontier(), (2, Some(&101)));
        let (outcome2, payload2) = outcome(102);
        frontier.complete(t2, outcome2, payload2);
        assert_eq!(frontier.frontier(), (3, Some(&102)));
        assert!(frontier.is_fully_committed());
    }

    #[test]
    fn out_of_order_completion_never_reorders_commits() {
        let mut frontier = CommitFrontier::new();
        let t0 = frontier.submit();
        let t1 = frontier.submit();
        let t2 = frontier.submit();

        // Complete the LAST atom first: the frontier must not advance past
        // t0 just because a later atom happened to settle sooner.
        let (o2, p2) = outcome(2);
        frontier.complete(t2, o2, p2);
        assert_eq!(
            frontier.frontier(),
            (0, None),
            "out-of-order completion must not advance the frontier past a hole"
        );
        assert_eq!(frontier.holes().len(), 2, "t0 and t1 are still holes");

        let (o1, p1) = outcome(1);
        frontier.complete(t1, o1, p1);
        assert_eq!(
            frontier.frontier(),
            (0, None),
            "t1 completing does not help while t0 (earlier) is still a hole"
        );

        // Now complete t0: everything folds in at once, in submission order,
        // ending on t2's payload (the last atom in the now-contiguous run).
        let (o0, p0) = outcome(0);
        frontier.complete(t0, o0, p0);
        assert_eq!(frontier.frontier(), (3, Some(&2)));
        assert!(frontier.is_fully_committed());
    }

    #[test]
    fn a_hole_blocks_every_atom_submitted_after_it() {
        let mut frontier = CommitFrontier::new();
        let t0 = frontier.submit();
        let _t1 = frontier.submit();
        let t2 = frontier.submit();
        let t3 = frontier.submit();

        // t1 never completes (simulating a still-in-flight or genuinely
        // stuck atom). t0, t2, t3 all complete.
        let (o0, p0) = outcome(0);
        frontier.complete(t0, o0, p0);
        let (o2, p2) = outcome(2);
        frontier.complete(t2, o2, p2);
        let (o3, p3) = outcome(3);
        frontier.complete(t3, o3, p3);

        assert_eq!(
            frontier.frontier(),
            (1, Some(&0)),
            "only t0 is contiguous from the front; t1's hole blocks t2 and t3 \
             even though they are individually complete"
        );
        let holes = frontier.holes();
        assert_eq!(holes.len(), 1, "only t1 is a genuine hole");
        assert_eq!(holes[0].position, 1, "t1 was the 2nd atom submitted (0-based index 1)");
        assert!(!frontier.is_fully_committed());
    }

    #[test]
    fn duplicate_complete_on_the_same_ticket_is_a_no_op() {
        let mut frontier = CommitFrontier::new();
        let t0 = frontier.submit();
        let (o0, p0) = outcome(0);
        frontier.complete(t0, o0, p0);
        assert_eq!(frontier.frontier(), (1, Some(&0)));

        // Completing the same (already-folded) ticket again must not double
        // count, corrupt the payload, or panic.
        let (o0b, p0b) = outcome(999);
        frontier.complete(t0, o0b, p0b);
        assert_eq!(
            frontier.frontier(),
            (1, Some(&0)),
            "duplicate complete() on an already-folded ticket must be a no-op"
        );
    }

    #[test]
    fn duplicate_complete_before_the_hole_clears_keeps_the_first_outcome() {
        let mut frontier = CommitFrontier::new();
        let t0 = frontier.submit();
        let t1 = frontier.submit();

        // t1 completes first (out of order) while t0 is still a hole.
        let (o1, p1) = outcome(1);
        frontier.complete(t1, o1, p1);
        // A duplicate completion for t1 before it has folded in must still
        // be a no-op — the first recorded outcome wins.
        let (o1b, p1b) = outcome(999);
        frontier.complete(t1, o1b, p1b);

        let (o0, p0) = outcome(0);
        frontier.complete(t0, o0, p0);
        assert_eq!(
            frontier.frontier(),
            (2, Some(&1)),
            "t1's payload must be the FIRST recorded one (1), not the duplicate (999)"
        );
    }

    #[test]
    fn checkpoint_round_trip_reproduces_the_same_frontier() {
        let mut frontier = CommitFrontier::new();
        let t0 = frontier.submit();
        let t1 = frontier.submit();
        let (o0, p0) = outcome(10);
        frontier.complete(t0, o0, p0);
        let (o1, p1) = outcome(11);
        frontier.complete(t1, o1, p1);

        let (position, checkpoint) = frontier.frontier();
        let checkpoint = checkpoint.copied().expect("frontier committed at least once");

        let restored: CommitFrontier<u64> = CommitFrontier::restore(checkpoint, position);
        assert_eq!(
            restored.frontier(),
            (position, Some(&checkpoint)),
            "restoring from an observed checkpoint must reproduce the same frontier"
        );
        assert!(restored.is_fully_committed(), "a freshly restored frontier has no holes");

        // Post-checkpoint atoms re-enter via submit(), continuing from the
        // restored position rather than resetting to zero.
        let mut restored = restored;
        let t2 = restored.submit();
        let (o2, p2) = outcome(12);
        restored.complete(t2, o2, p2);
        assert_eq!(restored.frontier(), (position + 1, Some(&12)));
    }

    proptest::proptest! {
        /// General-shape invariant: no matter what order completions arrive
        /// in, the frontier position always equals the length of the
        /// longest CONTIGUOUS run of completed atoms starting from index 0
        /// — never more (no reordering past a hole) and never less (every
        /// contiguous completed atom is eventually folded in).
        #[test]
        fn frontier_position_always_equals_longest_completed_prefix(
            completion_order in proptest::collection::vec(0usize..20, 0..20)
        ) {
            let n = 20usize;
            let mut frontier: CommitFrontier<usize> = CommitFrontier::new();
            let tickets: Vec<AtomTicket> = (0..n).map(|_| frontier.submit()).collect();
            let mut completed = vec![false; n];

            for &idx in &completion_order {
                if completed[idx] {
                    continue;
                }
                completed[idx] = true;
                frontier.complete(tickets[idx], TerminalOutcome::PersistedConfirmed, idx);

                let expected_prefix = completed.iter().take_while(|&&done| done).count() as u64;
                proptest::prop_assert_eq!(frontier.frontier().0, expected_prefix);
            }
        }
    }
}
