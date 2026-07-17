"""A live index of available drivers, bucketed by geo-cell.

The question a dispatcher asks a thousand times a second is: "who is near this
pickup?" The naive answer is to iterate every online driver and compute a
distance. That is O(drivers) per request, and with 100k online drivers and 2k
requests/sec it is 200 million distance computations per second. It does not
work, and no amount of faster hardware makes it work.

So we bucket. Every driver lives in exactly one CELL, and a pickup only ever
reads its own cell and the ring of cells around it. That turns an O(drivers)
scan into an O(drivers-in-a-few-cells) scan — a couple of dozen candidates,
regardless of how many drivers exist worldwide.
"""
from __future__ import annotations

import math
import time
from collections import defaultdict

import redis

from .domain import Driver, LatLng

# Cell size in degrees. 0.01 deg of latitude is ~1.11 km; longitude shrinks
# with latitude but at city scale this is a fine, cheap approximation and it
# keeps a cell to roughly a square kilometre — a few hundred metres of walking.
CELL_DEG = 0.01

# A driver who has not heartbeat in this long is not dispatchable. Their phone
# died, they hit a tunnel, or the app was killed. Matching to a ghost means a
# rider waits at a corner for a car that is never coming.
HEARTBEAT_TTL_MS = 30_000

EARTH_RADIUS_M = 6_371_000.0


def cell_of(p: LatLng) -> str:
    """The cell id a point falls into. Pure, total, and cheap."""
    ci = math.floor(p.lat / CELL_DEG)
    cj = math.floor(p.lng / CELL_DEG)
    return f"{ci}:{cj}"


def neighbours(p: LatLng, ring: int = 1) -> list[str]:
    """All cell ids within `ring` cells of p, centre first.

    ring=0 -> 1 cell, ring=1 -> 9 cells, ring=2 -> 25 cells. We start at ring 1
    and only widen when a ring comes back empty, because a wider ring costs
    strictly more Redis reads for candidates that are strictly further away.
    """
    ci = math.floor(p.lat / CELL_DEG)
    cj = math.floor(p.lng / CELL_DEG)
    out: list[str] = []
    for di in range(-ring, ring + 1):
        for dj in range(-ring, ring + 1):
            out.append(f"{ci + di}:{cj + dj}")
    return out


def haversine_m(a: LatLng, b: LatLng) -> float:
    """Great-circle distance in metres. Used only to RANK the few candidates
    the cell index already narrowed us down to — never to find them."""
    p1, p2 = math.radians(a.lat), math.radians(b.lat)
    dp = p2 - p1
    dl = math.radians(b.lng - a.lng)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(h))


class GeoIndex:
    """Redis-backed driver index. Two structures, deliberately:

      geo:cell:<cell>   a SET of driver ids currently in that cell
      geo:driver:<id>   a HASH of {lat, lng, cell, hb} for that driver

    The set answers "who is here"; the hash answers "where exactly, and how
    fresh". Keeping the driver's current cell IN the hash is what lets a move
    remove them from the old cell — without it we would have to search every
    cell to evict a driver, which is the very scan we are avoiding.
    """

    def __init__(self, client: redis.Redis) -> None:
        self._r = client

    def heartbeat(self, driver_id: str, pos: LatLng, now_ms: int | None = None) -> None:
        """Record a driver's position. Called every few seconds, per driver.

        This is the hottest write path in the system, so it is one pipelined
        round trip: read the old cell, and if the driver crossed a boundary,
        move them. Most heartbeats do not cross a boundary and cost two writes.
        """
        now = now_ms if now_ms is not None else int(time.time() * 1000)
        new_cell = cell_of(pos)
        dkey = f"geo:driver:{driver_id}"
        old_cell = self._r.hget(dkey, "cell")

        pipe = self._r.pipeline()
        if old_cell is not None and old_cell != new_cell:
            pipe.srem(f"geo:cell:{old_cell}", driver_id)
        pipe.sadd(f"geo:cell:{new_cell}", driver_id)
        pipe.hset(
            dkey,
            mapping={"lat": pos.lat, "lng": pos.lng, "cell": new_cell, "hb": now},
        )
        # Let Redis expire the driver hash on its own if heartbeats stop. The
        # TTL is a backstop, not the primary staleness check — we still filter
        # on `hb` at read time, because an expiring key is eventually consistent
        # and a match must not be.
        pipe.expire(dkey, HEARTBEAT_TTL_MS // 1000 * 4)
        pipe.execute()

    def go_offline(self, driver_id: str) -> None:
        """Driver ended their shift. Remove them from the index entirely."""
        dkey = f"geo:driver:{driver_id}"
        cell = self._r.hget(dkey, "cell")
        pipe = self._r.pipeline()
        if cell is not None:
            pipe.srem(f"geo:cell:{cell}", driver_id)
        pipe.delete(dkey)
        pipe.execute()

    def nearby(
        self,
        pickup: LatLng,
        now_ms: int | None = None,
        max_ring: int = 3,
    ) -> list[tuple[Driver, float]]:
        """Candidate drivers near a pickup, nearest first, freshest only.

        Widens the ring only when the current ring yields nothing. Returns
        (driver, distance_metres) so the caller can rank without recomputing.
        """
        now = now_ms if now_ms is not None else int(time.time() * 1000)
        for ring in range(1, max_ring + 1):
            cells = neighbours(pickup, ring)
            ids = self._members(cells)
            if not ids:
                continue
            drivers = self._load(ids, now)
            if not drivers:
                continue
            ranked = [(d, haversine_m(pickup, d.position)) for d in drivers]
            ranked.sort(key=lambda pair: pair[1])
            return ranked
        return []

    # ---- internals -------------------------------------------------------

    def _members(self, cells: list[str]) -> list[str]:
        pipe = self._r.pipeline()
        for c in cells:
            pipe.smembers(f"geo:cell:{c}")
        out: list[str] = []
        for members in pipe.execute():
            out.extend(m.decode() if isinstance(m, bytes) else m for m in members)
        return out

    def _load(self, driver_ids: list[str], now_ms: int) -> list[Driver]:
        """Hydrate driver hashes, dropping anyone whose heartbeat went stale."""
        pipe = self._r.pipeline()
        for did in driver_ids:
            pipe.hgetall(f"geo:driver:{did}")
        fresh: list[Driver] = []
        for did, h in zip(driver_ids, pipe.execute()):
            if not h:
                continue  # the hash expired out from under the cell set
            hb = int(h[b"hb"] if b"hb" in h else h["hb"])
            if now_ms - hb > HEARTBEAT_TTL_MS:
                continue  # stale: this driver is a ghost, do not dispatch to them
            lat = float(h[b"lat"] if b"lat" in h else h["lat"])
            lng = float(h[b"lng"] if b"lng" in h else h["lng"])
            fresh.append(Driver(driver_id=did, position=LatLng(lat, lng),
                                last_heartbeat_ms=hb))
        return fresh


class InMemoryGeoIndex(GeoIndex):
    """The same index without Redis — used by the tests and the local runner."""

    def __init__(self) -> None:  # noqa: D107 - deliberately does not call super
        self._cells: dict[str, set[str]] = defaultdict(set)
        self._drivers: dict[str, Driver] = {}

    def heartbeat(self, driver_id: str, pos: LatLng, now_ms: int | None = None) -> None:
        now = now_ms if now_ms is not None else int(time.time() * 1000)
        prev = self._drivers.get(driver_id)
        if prev is not None:
            self._cells[cell_of(prev.position)].discard(driver_id)
        self._cells[cell_of(pos)].add(driver_id)
        self._drivers[driver_id] = Driver(driver_id, pos, now)

    def go_offline(self, driver_id: str) -> None:
        prev = self._drivers.pop(driver_id, None)
        if prev is not None:
            self._cells[cell_of(prev.position)].discard(driver_id)

    def _members(self, cells: list[str]) -> list[str]:
        out: list[str] = []
        for c in cells:
            out.extend(self._cells.get(c, ()))
        return out

    def _load(self, driver_ids: list[str], now_ms: int) -> list[Driver]:
        return [
            d
            for did in driver_ids
            if (d := self._drivers.get(did)) is not None
            and not d.is_stale(now_ms, HEARTBEAT_TTL_MS)
        ]
