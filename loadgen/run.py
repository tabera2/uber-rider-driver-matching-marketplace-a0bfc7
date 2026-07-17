"""Hammer the marketplace with a realistic, geo-distributed load.

Two properties make this load REAL rather than a synthetic benchmark:

  1. HOTSPOTS. Riders are not uniformly distributed. 70% of requests land in a
     handful of dense cells (downtown, the airport) and 30% are scattered. A
     uniform load would never produce claim contention, and claim contention is
     the exact behaviour we built this system to survive. A load test that
     cannot reproduce your hardest case is a load test that lies to you.

  2. DRIVERS THAT MOVE. Drivers heartbeat continuously and drift between cells,
     so the geo index is being written to while it is being read — which is the
     state it will always be in during production.
"""
from __future__ import annotations

import argparse
import asyncio
import random
import time
import uuid

import httpx

API = "http://localhost:8000"

# Dense hotspots: downtown SF, SFO, the Mission. 70% of demand lands here.
HOTSPOTS = [(37.7897, -122.4000), (37.6213, -122.3790), (37.7599, -122.4148)]
# The sprawl: everything else, uniformly over the city bounding box.
CITY = ((37.70, 37.83), (-122.52, -122.36))


def sample_pickup() -> tuple[float, float]:
    if random.random() < 0.70:
        lat, lng = random.choice(HOTSPOTS)
        # ~200m of jitter: same cell or its immediate neighbours. This is what
        # produces two riders wanting the SAME driver — the whole point.
        return lat + random.gauss(0, 0.002), lng + random.gauss(0, 0.002)
    (lo_a, hi_a), (lo_o, hi_o) = CITY
    return random.uniform(lo_a, hi_a), random.uniform(lo_o, hi_o)


async def driver_loop(client: httpx.AsyncClient, driver_id: str, stop: float) -> None:
    """One driver: heartbeat every 3s, drifting slowly across the map."""
    lat, lng = sample_pickup()
    while time.monotonic() < stop:
        lat += random.gauss(0, 0.0004)   # ~40m of drift per beat
        lng += random.gauss(0, 0.0004)
        try:
            await client.post(
                f"{API}/drivers/heartbeat",
                json={"driver_id": driver_id, "lat": lat, "lng": lng},
                timeout=2.0,
            )
        except httpx.HTTPError:
            pass  # a dropped heartbeat is survivable by design (staleness TTL)
        await asyncio.sleep(3.0)


async def rider_loop(client: httpx.AsyncClient, rate: float, stop: float,
                     stats: dict[str, int]) -> None:
    """Fire ride requests at `rate` per second until `stop`."""
    interval = 1.0 / rate
    while time.monotonic() < stop:
        lat, lng = sample_pickup()
        try:
            r = await client.post(
                f"{API}/rides",
                json={"rider_id": f"r-{uuid.uuid4().hex[:8]}",
                      "pickup_lat": lat, "pickup_lng": lng},
                # A fresh key per INTENT. A retry of this same request would
                # reuse this key; a new tap generates a new one.
                headers={"Idempotency-Key": uuid.uuid4().hex},
                timeout=5.0,
            )
            stats[str(r.status_code)] = stats.get(str(r.status_code), 0) + 1
        except httpx.HTTPError as exc:
            stats[type(exc).__name__] = stats.get(type(exc).__name__, 0) + 1
        await asyncio.sleep(interval)


async def main_async(rate: int, duration: int, drivers: int) -> None:
    stop = time.monotonic() + duration
    stats: dict[str, int] = {}
    limits = httpx.Limits(max_connections=500, max_keepalive_connections=200)

    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [
            asyncio.create_task(driver_loop(client, f"d-{i}", stop))
            for i in range(drivers)
        ]
        # Fan the request rate across 20 concurrent riders so one slow response
        # cannot throttle the whole offered load — otherwise your load generator
        # silently becomes the bottleneck and you "prove" the server is fine.
        per_task = rate / 20
        tasks += [
            asyncio.create_task(rider_loop(client, per_task, stop, stats))
            for _ in range(20)
        ]
        await asyncio.gather(*tasks)

    print("offered rate:", rate, "req/s for", duration, "s")
    for k, v in sorted(stats.items()):
        print(f"  {k:>24}: {v}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=int, default=200)
    p.add_argument("--duration", type=int, default=60)
    p.add_argument("--drivers", type=int, default=1000)
    args = p.parse_args()
    asyncio.run(main_async(args.rate, args.duration, args.drivers))


if __name__ == "__main__":
    main()
