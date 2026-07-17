"""The pricing service. Separate process, separate deploy, separate scaling.

It owns exactly one decision — what does this trip cost — and it owns the data
that decision needs (surge by cell). Dispatch does not know how a fare is
computed, and pricing does not know what a trip or a claim is. That is a real
boundary, not a folder.
"""
from __future__ import annotations

import logging
import os
import time
from concurrent import futures

import grpc
import redis

from proto import pricing_pb2, pricing_pb2_grpc

log = logging.getLogger("pricing")

BASE_FARE_CENTS = 250
PER_KM_CENTS = 140
PICKUP_PER_KM_CENTS = 60   # the driver's drive to the rider is not free
MIN_FARE_CENTS = 500


class PricingService(pricing_pb2_grpc.PricingServicer):
    def __init__(self, r: redis.Redis) -> None:
        self._r = r

    def _surge_bps(self, cell: str) -> int:
        """Surge for a cell, in basis points. 10000 == 1.0x (no surge).

        Written by a separate demand job that watches queue depth per cell. If
        the key is missing — because that job is down, or the cell is new — we
        return 1.0x rather than guessing. A missing surge signal must mean
        'no surge', never 'unknown surge', because there is no third fare.
        """
        raw = self._r.get(f"surge:{cell}")
        if raw is None:
            return 10_000
        try:
            return max(10_000, min(int(raw), 30_000))  # clamp to [1.0x, 3.0x]
        except (TypeError, ValueError):
            log.warning("garbage surge value for cell %s: %r", cell, raw)
            return 10_000

    def Quote(  # noqa: N802 - gRPC generated name
        self,
        request: pricing_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> pricing_pb2.QuoteResponse:
        from dispatch.geo import LatLng, cell_of, haversine_m

        pickup = LatLng(request.pickup.lat, request.pickup.lng)
        driver = LatLng(request.driver.lat, request.driver.lng)

        # Trip distance stands in for the ride itself; pickup distance is the
        # driver's deadhead. Both are metres; all money is integer cents.
        trip_m = haversine_m(pickup, driver) + request.pickup_distance_m
        surge = self._surge_bps(cell_of(pickup))

        base = (
            BASE_FARE_CENTS
            + int(trip_m / 1000.0 * PER_KM_CENTS)
            + int(request.pickup_distance_m / 1000.0 * PICKUP_PER_KM_CENTS)
        )
        # Integer arithmetic end to end: (base * surge) // 10000 is exactly
        # reproducible on every machine. base * 1.35 is not.
        fare = max(MIN_FARE_CENTS, (base * surge) // 10_000)

        return pricing_pb2.QuoteResponse(fare_cents=int(fare), surge_bps=surge)


def serve() -> None:
    port = os.getenv("PRICING_PORT", "50051")
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    pricing_pb2_grpc.add_PricingServicer_to_server(PricingService(r), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    log.info("pricing listening on :%s", port)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        # Drain in-flight RPCs before dying. 10s is longer than any Quote takes.
        server.stop(grace=10).wait()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    serve()
