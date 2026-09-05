//! The single-flight, time-bounded cache behind the browser credentials provider's
//! `GET /api/ai/keys` (b11 §3.17), as a value type with no locking and no I/O so the state
//! machine is unit-tested natively. `zed_web/src/ai.rs` wraps it in a mutex and supplies the
//! shared request future as the handle `F`.
//!
//! Rules it enforces:
//! - every provider that asks while a request is in flight joins that request instead of
//!   starting its own (about ten providers ask at boot, against a 30/min route limit);
//! - only a read inventory ([`KeysOutcome::Known`]) is cached, for `ttl`; a disabled proxy
//!   or a failed request leaves the cache empty so the next lookup asks again;
//! - an answer that arrives after [`KeysCache::invalidate`] (a key was just written or
//!   deleted) is discarded, because it describes the state before that write.

use std::{collections::HashSet, time::Duration};

use web_time::Instant;

use super::KeysOutcome;

/// Identifies one request started through [`KeysCache::start`] or joined through
/// [`Lookup::Join`]. [`KeysCache::settle`] applies an outcome only while that request is
/// still the one the cache waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ticket(u64);

/// What a caller does next after [`KeysCache::lookup`].
#[derive(Debug)]
pub enum Lookup<F> {
    /// The inventory is fresh: these providers are configured.
    Fresh(HashSet<String>),
    /// A request is in flight: await this clone of its handle, then
    /// [`KeysCache::settle`] with the ticket.
    Join(F, Ticket),
    /// Nothing usable is cached: send a request, register it with [`KeysCache::start`],
    /// await it, then settle with the ticket `start` returned.
    Start,
}

enum State<F> {
    Empty,
    InFlight { ticket: Ticket, handle: F },
    Fresh { at: Instant, ids: HashSet<String> },
}

/// See the module documentation.
pub struct KeysCache<F> {
    state: State<F>,
    ttl: Duration,
    next_ticket: u64,
}

impl<F: Clone> KeysCache<F> {
    /// An empty cache whose inventories stay fresh for `ttl`.
    pub fn new(ttl: Duration) -> Self {
        Self {
            state: State::Empty,
            ttl,
            next_ticket: 0,
        }
    }

    /// The cached inventory, the request to join, or the instruction to start one.
    pub fn lookup(&self, now: Instant) -> Lookup<F> {
        match &self.state {
            State::Fresh { at, ids } if now.saturating_duration_since(*at) < self.ttl => {
                Lookup::Fresh(ids.clone())
            }
            State::Fresh { .. } | State::Empty => Lookup::Start,
            State::InFlight { ticket, handle } => Lookup::Join(handle.clone(), *ticket),
        }
    }

    /// Registers the request a [`Lookup::Start`] caller sent; later lookups join it. The
    /// returned ticket settles it.
    pub fn start(&mut self, handle: F) -> Ticket {
        let ticket = Ticket(self.next_ticket);
        self.next_ticket += 1;
        self.state = State::InFlight { ticket, handle };
        ticket
    }

    /// Records the answer of the request `ticket` names. Returns `false`, changing nothing,
    /// when that request is no longer the one in flight: it was settled already by another
    /// awaiter, or [`invalidate`](Self::invalidate) ran (and possibly a newer request
    /// started) while it was pending.
    pub fn settle(&mut self, ticket: Ticket, outcome: &KeysOutcome, now: Instant) -> bool {
        match &self.state {
            State::InFlight {
                ticket: current, ..
            } if *current == ticket => {}
            _ => return false,
        }
        self.state = match outcome {
            KeysOutcome::Known(ids) => State::Fresh {
                at: now,
                ids: ids.clone(),
            },
            KeysOutcome::Disabled | KeysOutcome::Unknown { .. } => State::Empty,
        };
        true
    }

    /// Forgets the inventory and disowns any request in flight; the next lookup starts a
    /// new request.
    pub fn invalidate(&mut self) {
        self.state = State::Empty;
    }

    /// Whether a fresh inventory is cached at `now` (diagnostics and tests).
    pub fn is_fresh(&self, now: Instant) -> bool {
        matches!(self.lookup(now), Lookup::Fresh(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(60);

    fn known(ids: &[&str]) -> KeysOutcome {
        KeysOutcome::Known(ids.iter().map(|id| (*id).to_owned()).collect())
    }

    fn unknown() -> KeysOutcome {
        KeysOutcome::Unknown {
            status: 503,
            detail: "down".into(),
        }
    }

    #[track_caller]
    fn expect_fresh(lookup: Lookup<&'static str>) -> HashSet<String> {
        match lookup {
            Lookup::Fresh(ids) => ids,
            Lookup::Join(handle, ticket) => {
                panic!("expected Fresh, got Join({handle}, {ticket:?})")
            }
            Lookup::Start => panic!("expected Fresh, got Start"),
        }
    }

    #[track_caller]
    fn expect_join(lookup: Lookup<&'static str>) -> (&'static str, Ticket) {
        match lookup {
            Lookup::Join(handle, ticket) => (handle, ticket),
            Lookup::Fresh(ids) => panic!("expected Join, got Fresh({ids:?})"),
            Lookup::Start => panic!("expected Join, got Start"),
        }
    }

    #[test]
    fn empty_cache_starts_one_request_and_everyone_else_joins_it() {
        let now = Instant::now();
        let mut cache: KeysCache<&'static str> = KeysCache::new(TTL);
        assert!(matches!(cache.lookup(now), Lookup::Start));
        assert!(!cache.is_fresh(now));

        let ticket = cache.start("request-a");
        // The nine other providers authenticating at boot all join the same request.
        for _ in 0..9 {
            let (handle, joined) = expect_join(cache.lookup(now));
            assert_eq!(handle, "request-a");
            assert_eq!(joined, ticket);
        }

        assert!(cache.settle(ticket, &known(&["anthropic"]), now));
        let ids = expect_fresh(cache.lookup(now));
        assert_eq!(ids, ["anthropic".to_owned()].into_iter().collect());
        assert!(cache.is_fresh(now));
        // The joiners settle too, after the starter: no-ops.
        assert!(!cache.settle(ticket, &known(&["openai"]), now));
        assert_eq!(expect_fresh(cache.lookup(now)), ids);
    }

    #[test]
    fn a_fresh_inventory_expires_after_the_ttl() {
        // Offset from the process's monotonic epoch: `Instant - Duration` panics on
        // underflow, and on a host whose `CLOCK_MONOTONIC` is seconds-since-boot the
        // backwards-clock case below would crash instead of asserting.
        let now = Instant::now() + Duration::from_secs(60);
        let mut cache: KeysCache<&'static str> = KeysCache::new(TTL);
        let ticket = cache.start("request-a");
        assert!(cache.settle(ticket, &known(&["anthropic", "codestral"]), now));

        let just_before = now + TTL - Duration::from_millis(1);
        assert!(cache.is_fresh(just_before));
        assert_eq!(expect_fresh(cache.lookup(just_before)).len(), 2);

        let at_ttl = now + TTL;
        assert!(!cache.is_fresh(at_ttl));
        assert!(matches!(cache.lookup(at_ttl), Lookup::Start));
        // A clock that went backwards reads as "still fresh", never as an overflow.
        assert!(cache.is_fresh(now - Duration::from_secs(5)));
    }

    #[test]
    fn a_disabled_proxy_or_a_failed_request_is_not_cached() {
        let now = Instant::now();
        let mut cache: KeysCache<&'static str> = KeysCache::new(TTL);

        let ticket = cache.start("request-a");
        assert!(cache.settle(ticket, &KeysOutcome::Disabled, now));
        assert!(matches!(cache.lookup(now), Lookup::Start));

        let ticket = cache.start("request-b");
        assert!(cache.settle(ticket, &unknown(), now));
        assert!(matches!(cache.lookup(now), Lookup::Start));
        assert!(!cache.is_fresh(now));
    }

    #[test]
    fn invalidate_disowns_the_request_in_flight() {
        let now = Instant::now();
        let mut cache: KeysCache<&'static str> = KeysCache::new(TTL);

        // Boot: request A is in flight when the user saves a key, which invalidates.
        let a = cache.start("request-a");
        cache.invalidate();
        assert!(matches!(cache.lookup(now), Lookup::Start));

        // The write's own re-read starts request B; A's answer arrives first and must not
        // be cached over B (it predates the write).
        let b = cache.start("request-b");
        assert!(!cache.settle(a, &known(&[]), now));
        let (handle, ticket) = expect_join(cache.lookup(now));
        assert_eq!((handle, ticket), ("request-b", b));

        assert!(cache.settle(b, &known(&["anthropic"]), now));
        assert_eq!(
            expect_fresh(cache.lookup(now)),
            ["anthropic".to_owned()].into_iter().collect()
        );
        // A's other awaiters settling late are still no-ops.
        assert!(!cache.settle(a, &known(&[]), now));
        assert!(cache.is_fresh(now));
    }

    #[test]
    fn invalidate_forgets_a_fresh_inventory() {
        let now = Instant::now();
        let mut cache: KeysCache<&'static str> = KeysCache::new(TTL);
        let ticket = cache.start("request-a");
        assert!(cache.settle(ticket, &known(&["anthropic"]), now));
        assert!(cache.is_fresh(now));

        cache.invalidate();
        assert!(!cache.is_fresh(now));
        assert!(matches!(cache.lookup(now), Lookup::Start));
        // A late settle of the forgotten request changes nothing either.
        assert!(!cache.settle(ticket, &known(&["anthropic"]), now));
        assert!(matches!(cache.lookup(now), Lookup::Start));
    }

    #[test]
    fn tickets_are_unique_across_restarts() {
        let now = Instant::now();
        let mut cache: KeysCache<&'static str> = KeysCache::new(TTL);
        let a = cache.start("request-a");
        // A second `start` without an invalidate (a caller racing the lock) supersedes A.
        let b = cache.start("request-b");
        assert_ne!(a, b);
        assert!(!cache.settle(a, &known(&["x"]), now));
        let (handle, _) = expect_join(cache.lookup(now));
        assert_eq!(handle, "request-b");
        assert!(cache.settle(b, &known(&["y"]), now));
        assert_eq!(
            expect_fresh(cache.lookup(now)),
            ["y".to_owned()].into_iter().collect()
        );
    }
}
