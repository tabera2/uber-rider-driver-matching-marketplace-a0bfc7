"""The dispatch side of the pricing boundary — and its defences.

A network call is not a function call, and pretending otherwise is how a
healthy service gets taken down by a sick one. Three defences, in order of
importance:

  1. DEADLINE. Every RPC has one. A call with no deadline can hang forever,
     and a hung call holds a driver claim, a thread, and a rider's patience.
  2. CIRCUIT BREAKER. When pricing is failing, stop calling it. Failing FAST
     is strictly better than failing SLOW: a fast failure lets us fall back
     and match the rider; a slow failure ties up every matcher thread waiting
     on a service that is already down.
  3. FALLBACK. A quote we can always produce without the network, so a pricing
     outage degrades the FARE rather than taking down MATCHING.
"""
from __future__ import annotations

import logging
import os
import threading
import time

import grpc

from proto import pricing_pb2, pricing_pb2_grpc

from .domain import LatLng
from .geo import haversine_m

log = logging.getLogger("pricing_client")

QUOTE_DEADLINE_S = 0.25      # a quote that takes >250ms is a quote we do not want
BREAKER_THRESHOLD = 5        # consecutive failures before the breaker opens
BREAKER_COOLDOWN_S = 10.0    # how long it stays open before we probe again

FALLBACK_BASE_CENTS = 250
FALLBACK_PER_KM_CENTS = 140
FALLBACK_MIN_CENTS = 500


class BreakerOpen(Exception):
    """The circuit is open: we are deliberately not calling pricing right now."""


class CircuitBreaker:
    """A three-state breaker: CLOSED -> OPEN -> HALF_OPEN -> CLOSED.

    CLOSED    calls flow; count consecutive failures.
    OPEN      calls are refused instantly, without touching the network.
    HALF_OPEN after the cooldown, let exactly ONE call through as a probe.
              It succeeds -> CLOSED. It fails -> OPEN again for another cooldown.

    The half-open probe is what makes the breaker self-healing without a
    thundering herd: when pricing comes back, ONE request discovers it, not
    every matcher thread at once slamming a service that just got up.
    """

    def __init__(self, threshold: int, cooldown_s: float) -> None:
        self._threshold = threshold
        self._cooldown = cooldown_s
        self._lock = threading.Lock()
        self._failures = 0
        self._opened_at = 0.0
        self._probing = False

    def allow(self) -> bool:
        with self._lock:
            if self._failures < self._threshold:
                return True  # CLOSED
            elapsed = time.monotonic() - self._opened_at
            if elapsed < self._cooldown:
                return False  # OPEN: refuse instantly, no network call
            if self._probing:
                return False  # someone else is already the probe
            self._probing = True
            return True       # HALF_OPEN: this one call is the probe

    def on_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._probing = False

    def on_failure(self) -> None:
        with self._lock:
            self._failures += 1
            self._probing = False
            if self._failures == self._threshold:
                self._opened_at = time.monotonic()
                log.error("pricing circuit OPEN after %d failures", self._failures)

    @property
    def is_open(self) -> bool:
        with self._lock:
            return (
                self._failures >= self._threshold
                and time.monotonic() - self._opened_at < self._cooldown
            )


class PricingClient:
    def __init__(self, target: str | None = None) -> None:
        self._target = target or os.getenv("PRICING_ADDR", "localhost:50051")
        self._channel = grpc.insecure_channel(self._target)
        self._stub = pricing_pb2_grpc.PricingStub(self._channel)
        self._breaker = CircuitBreaker(BREAKER_THRESHOLD, BREAKER_COOLDOWN_S)

    def quote(self, pickup: LatLng, driver: LatLng, pickup_distance_m: float) -> int:
        """Fare in cents. NEVER raises for a pricing outage — always returns a fare.

        The caller holds a driver claim while this runs. That is precisely why
        this method must be bounded in time and must not propagate a pricing
        failure into a matching failure: a rider should get a car at a fallback
        price, not no car because a fare service is unwell.
        """
        if not self._breaker.allow():
            return self._fallback(pickup, driver, pickup_distance_m)

        req = pricing_pb2.QuoteRequest(
            pickup=pricing_pb2.LatLng(lat=pickup.lat, lng=pickup.lng),
            driver=pricing_pb2.LatLng(lat=driver.lat, lng=driver.lng),
            pickup_distance_m=pickup_distance_m,
        )
        try:
            # THE DEADLINE. Not a suggestion: gRPC cancels the RPC server-side
            # too, so a timed-out call stops consuming capacity at BOTH ends.
            resp = self._stub.Quote(req, timeout=QUOTE_DEADLINE_S)
        except grpc.RpcError as exc:
            code = exc.code()  # type: ignore[attr-defined]
            self._breaker.on_failure()
            log.warning("pricing rpc failed (%s); using fallback fare", code)
            return self._fallback(pickup, driver, pickup_distance_m)

        self._breaker.on_success()
        return int(resp.fare_cents)

    def _fallback(self, pickup: LatLng, driver: LatLng, pickup_m: float) -> int:
        """A fare computed locally, with no network and no surge.

        Deliberately CONSERVATIVE: no surge multiplier. If we cannot reach the
        service that knows about demand, we must not guess that demand is high
        and overcharge — we charge base rate and eat the margin. When you are
        degraded, err in the direction that does not harm the customer.
        """
        metres = haversine_m(pickup, driver) + pickup_m
        fare = FALLBACK_BASE_CENTS + int(metres / 1000.0 * FALLBACK_PER_KM_CENTS)
        return max(FALLBACK_MIN_CENTS, fare)
