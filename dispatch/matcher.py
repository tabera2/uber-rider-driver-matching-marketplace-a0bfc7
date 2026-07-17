"""The matcher: turn a REQUESTED trip into a MATCHED trip, or leave it alone.

This is the heart of the marketplace and the only place where two processes
genuinely fight over the same resource. Read the RACE section below before the
code; the code will not make sense as a design until the race does.

THE RACE
--------
Two dispatch pods, A and B, are draining the pending queue. Rider 1 goes to A,
rider 2 goes to B. Both riders are on the same street corner. Both pods call
nearby(), and both get the same answer: driver d-77 is closest.

Now both pods are about to assign d-77. If the check is "is d-77 free?" followed
by "mark d-77 taken", then:

    pod A: is d-77 free?  -> yes
    pod B: is d-77 free?  -> yes        (A has not written yet)
    pod A: mark d-77 taken
    pod B: mark d-77 taken

Both pods believe they won. Two riders are told a car is coming. One driver
receives two dispatches. This is not a rare interleaving you can test away — at
2000 requests/sec against a dense downtown cell, it is a Tuesday.

You cannot fix this by checking harder, checking twice, or checking faster. The
window between the check and the act is where the bug lives, and the only way to
kill it is to have NO window: make the check and the act a SINGLE atomic
operation and let the database decide who won.

That is `store.claim_driver`. It is one constrained INSERT. Exactly one pod
commits; the other gets DriverAlreadyClaimed and moves to the next driver.
"""
from __future__ import annotations

import logging
import time

from . import metrics
from .domain import Trip, TripState
from .geo import GeoIndex
from .pricing_client import PricingClient
from .queue import PendingQueue
from .store import ConcurrentUpdate, DriverAlreadyClaimed, TripStore

log = logging.getLogger("matcher")

MAX_CLAIM_ATTEMPTS = 5


class Matcher:
    def __init__(
        self,
        store: TripStore,
        geo: GeoIndex,
        queue: PendingQueue,
        pricing: PricingClient,
    ) -> None:
        self._store = store
        self._geo = geo
        self._queue = queue
        self._pricing = pricing

    def match_once(self, trip_id: str) -> Trip | None:
        """Attempt to match one trip. Returns the matched trip, or None."""
        trip = self._store.get(trip_id)
        if trip is None:
            log.warning("trip %s vanished before matching", trip_id)
            metrics.matches_total.labels(outcome="error").inc()
            return None

        # THE IDEMPOTENCE GUARD. The queue is at-least-once: this id can be
        # delivered twice (a pod died after popping but before matching, and the
        # reaper re-queued it). If the trip already left REQUESTED, someone
        # already did this work. Do nothing.
        if trip.state is not TripState.REQUESTED:
            log.info("trip %s already %s; nothing to do", trip_id, trip.state.value)
            metrics.matches_total.labels(outcome="already_done").inc()
            return trip

        candidates = self._geo.nearby(trip.pickup)
        if not candidates:
            metrics.matches_total.labels(outcome="no_driver").inc()
            return None

        for driver, distance_m in candidates[:MAX_CLAIM_ATTEMPTS]:
            # 1) ATOMIC CLAIM. One INSERT. Exactly one racer commits.
            try:
                self._store.claim_driver(driver.driver_id, trip.trip_id)
            except DriverAlreadyClaimed:
                # We LOST the race. Normal, expected, cheap. Count it: a SPIKE
                # in contention is a supply signal, not an error.
                metrics.claim_contention_total.inc()
                log.debug("lost claim on %s, trying next", driver.driver_id)
                continue

            metrics.claims_held.inc()
            # From here we HOLD the claim. Every path below must either commit
            # the match or RELEASE the driver.
            try:
                with metrics.Timer(metrics.quote_latency):
                    fare_cents = self._pricing.quote(
                        trip.pickup, driver.position, distance_m
                    )
                matched = self._store.save_match(trip, driver.driver_id, fare_cents)
            except ConcurrentUpdate:
                self._store.release_driver(driver.driver_id)
                metrics.claims_held.dec()
                metrics.matches_total.labels(outcome="already_done").inc()
                log.info("trip %s changed under us; released %s",
                         trip_id, driver.driver_id)
                return self._store.get(trip_id)
            except Exception:
                self._store.release_driver(driver.driver_id)
                metrics.claims_held.dec()
                metrics.matches_total.labels(outcome="error").inc()
                log.exception("pricing failed for trip %s", trip_id)
                raise

            metrics.matches_total.labels(outcome="matched").inc()
            log.info(
                "matched trip=%s driver=%s distance_m=%.0f fare=%d",
                matched.trip_id, driver.driver_id, distance_m, fare_cents,
            )
            return matched

        metrics.matches_total.labels(outcome="no_driver").inc()
        return None

    def run_forever(self, stop) -> None:
        """Drain the queue until `stop()` says otherwise.

        The stop check is BETWEEN trips, never inside one: a trip is matched to
        completion or not started. That is what makes SIGTERM safe (step 10).
        """
        last_gauge = 0.0
        while not stop():
            # Refresh the depth gauge about once a second. Cheap, and it is the
            # single most informative number in the entire system.
            now = time.monotonic()
            if now - last_gauge > 1.0:
                metrics.queue_depth.set(self._queue.depth())
                last_gauge = now

            trip_id = self._queue.pop(timeout_s=1)
            if trip_id is None:
                continue
            try:
                with metrics.Timer(metrics.match_latency):
                    self.match_once(trip_id)
            except Exception:
                # A trip that blew up goes back on the queue rather than being
                # dropped. The idempotence guard makes redelivery harmless.
                log.exception("match failed for %s; re-queueing", trip_id)
                self._queue.push(trip_id)
