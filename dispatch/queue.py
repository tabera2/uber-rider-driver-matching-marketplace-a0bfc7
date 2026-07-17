"""The pending-trip queue: the seam between accepting a ride and matching it.

Redis LIST as a work queue, drained with BLPOP. Deliberately simple, and
deliberately *not* the source of truth — the trip already exists in MySQL by
the time it lands here. If this queue is lost entirely, the reaper in step 10
rebuilds it by scanning MySQL for REQUESTED trips older than a few seconds.

That is the property to hold onto: the queue is a HINT about work to do, and
the database is the FACT of work to do. Losing a hint is survivable. That is
why we can use an unreplicated Redis list here without losing a rider.
"""
from __future__ import annotations

import redis


class PendingQueue:
    KEY = "dispatch:pending"

    def __init__(self, client: redis.Redis) -> None:
        self._r = client

    def push(self, trip_id: str) -> None:
        self._r.rpush(self.KEY, trip_id)

    def pop(self, timeout_s: int = 1) -> str | None:
        """Block for up to timeout_s waiting for a trip id. None on timeout.

        BLPOP blocks server-side, so an idle matcher costs one open connection
        and zero CPU — no polling loop burning a core to discover there is
        nothing to do.
        """
        item = self._r.blpop(self.KEY, timeout=timeout_s)
        if item is None:
            return None
        _key, value = item
        return value.decode() if isinstance(value, bytes) else value

    def depth(self) -> int:
        """How much work is waiting. This is the single best load signal we have."""
        return int(self._r.llen(self.KEY))
