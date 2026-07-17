"""The reaper: the recovery mechanism that makes a SIGKILL survivable.

Graceful shutdown handles the polite kill. This handles the impolite one.

WHAT A SIGKILL MID-MATCH ACTUALLY LEAVES BEHIND
-----------------------------------------------
The matcher was here, between two committed writes:

    store.claim_driver(d77, t9)   <-- COMMITTED. Driver d77 is claimed.
    <<< SIGKILL >>>                    No handler runs. No finally. Nothing.
    store.save_match(t9, d77, ...)     <-- NEVER RAN.

The process is gone. The trip id is gone (it was popped off Redis into RAM).
What remains, durably, in MySQL:

    trips:         t9 is REQUESTED. No driver. No fare. Rider still waiting.
    driver_claims: d77 -> t9. Driver d77 is CLAIMED for a trip that will
                   never be matched. He is invisible to every other matcher,
                   forever, and NOTHING will ever release him.

Two leaks, one event. This is what "partial failure against a state machine"
means concretely: the machine is not in an illegal state — REQUESTED is a
perfectly legal state — it is in a legal state that no longer has anyone
working toward the next transition. There is no exception to catch, no error
rate to spike, no log line. THE ONLY THING THAT FINDS THIS IS A SWEEP.

Both leaks are found by the same rule: reconcile the DURABLE state against
what SHOULD be true, on a timer, forever. That is the reaper.
"""
from __future__ import annotations

import logging
import time

from . import metrics
from .queue import PendingQueue
from .store import TripStore

log = logging.getLogger("reaper")

# A trip REQUESTED for longer than this is presumed abandoned by its matcher.
# It MUST exceed the worst-case honest match time, or we re-queue trips that
# are being actively worked — which is harmless (the idempotence guard eats
# the duplicate) but wasteful, so we still pick a number with margin.
STRANDED_TRIP_MS = 15_000

# A claim held longer than this without its trip advancing is orphaned.
ORPHANED_CLAIM_MS = 30_000

SWEEP_INTERVAL_S = 5.0


class Reaper:
    def __init__(self, store: TripStore, queue: PendingQueue) -> None:
        self._store = store
        self._queue = queue

    def sweep_stranded_trips(self) -> int:
        """Re-queue REQUESTED trips that nobody is working on.

        SAFE TO RUN AGAINST A LIVE TRIP. If a matcher IS still working the
        trip, we push a duplicate id — and the idempotence guard in match_once
        turns that duplicate into a no-op. The reaper is only correct because
        the matcher is idempotent. Retry and idempotency are one mechanism.
        """
        stranded = self._store.stale_matched_trips(STRANDED_TRIP_MS)
        for trip in stranded:
            log.warning("re-queueing stranded trip %s (age)", trip.trip_id)
            self._queue.push(trip.trip_id)
        return len(stranded)

    def sweep_orphaned_claims(self) -> int:
        """Release drivers claimed for trips that are dead or never advanced.

        This is the leak with no error. A driver here is online, heartbeating,
        sitting in the geo index, being offered to every matcher — and every
        single claim attempt fails, forever, because a process that died an
        hour ago still owns him. He earns nothing. No alarm fires.
        """
        cutoff = int(time.time() * 1000) - ORPHANED_CLAIM_MS
        released = 0
        with self._store._conn.cursor() as cur:  # noqa: SLF001 - reaper is internal
            cur.execute(
                """SELECT c.driver_id, c.trip_id, t.state
                     FROM driver_claims c
                     LEFT JOIN trips t ON t.trip_id = c.trip_id
                    WHERE c.claimed_ms < %s
                      AND (t.trip_id IS NULL
                           OR t.state IN ('requested','complete','cancelled'))
                    LIMIT 500""",
                (cutoff,),
            )
            orphans = cur.fetchall()
        self._store._conn.commit()  # noqa: SLF001

        for row in orphans:
            # 'requested' + an old claim is the SIGKILL signature exactly: the
            # claim committed, the match never did. Release the driver and let
            # the stranded-trip sweep re-queue the trip. Two independent
            # sweeps, one crash, and neither needs to know about the other.
            log.warning(
                "releasing orphaned claim driver=%s trip=%s trip_state=%s",
                row["driver_id"], row["trip_id"], row["state"],
            )
            self._store.release_driver(row["driver_id"])
            metrics.claims_held.dec()
            released += 1
        return released

    def run_forever(self, stop) -> None:
        while not stop():
            try:
                requeued = self.sweep_stranded_trips()
                released = self.sweep_orphaned_claims()
                if requeued or released:
                    log.info("sweep: requeued=%d released=%d", requeued, released)
            except Exception:
                # The reaper must NEVER die. It is the last line of defence, and
                # a reaper that crashed silently is worse than no reaper, because
                # you believe you have one.
                log.exception("sweep failed; continuing")
            time.sleep(SWEEP_INTERVAL_S)
