"""Core domain types for the marketplace.

Everything downstream — the store, the geo index, the matcher, the API — is
built from these three value types plus one state machine. Nothing here talks
to a database, a socket, or a clock: it is pure, so it is trivially testable.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum


class TripState(str, Enum):
    """The only states a trip may ever be in.

    Inheriting from ``str`` means the value serializes to JSON and stores into
    a MySQL ENUM column as-is, while still being a closed set at the type level.
    """

    REQUESTED = "requested"   # rider asked; nobody assigned yet
    MATCHED = "matched"       # a driver has been claimed for this trip
    EN_ROUTE = "en_route"     # driver picked the rider up
    COMPLETE = "complete"     # trip finished
    CANCELLED = "cancelled"   # terminal: rider or system gave up


# The transition table IS the state machine. A transition that is not listed
# here does not exist. There is no `if state == ...` ladder anywhere else in
# this codebase, because there is exactly one place that knows the rules.
_ALLOWED: dict[TripState, frozenset[TripState]] = {
    TripState.REQUESTED: frozenset({TripState.MATCHED, TripState.CANCELLED}),
    TripState.MATCHED: frozenset({TripState.EN_ROUTE, TripState.CANCELLED}),
    TripState.EN_ROUTE: frozenset({TripState.COMPLETE, TripState.CANCELLED}),
    TripState.COMPLETE: frozenset(),   # terminal
    TripState.CANCELLED: frozenset(),  # terminal
}


class IllegalTransition(Exception):
    """Raised when code attempts a transition the state machine forbids."""

    def __init__(self, src: TripState, dst: TripState) -> None:
        super().__init__(f"illegal trip transition {src.value} -> {dst.value}")
        self.src = src
        self.dst = dst


def can_transition(src: TripState, dst: TripState) -> bool:
    return dst in _ALLOWED[src]


def assert_transition(src: TripState, dst: TripState) -> None:
    if not can_transition(src, dst):
        raise IllegalTransition(src, dst)


def is_terminal(state: TripState) -> bool:
    return not _ALLOWED[state]


@dataclass(frozen=True, slots=True)
class LatLng:
    """A point on the earth. Frozen: a location value never mutates in place."""

    lat: float
    lng: float


@dataclass(frozen=True, slots=True)
class Rider:
    rider_id: str
    pickup: LatLng


@dataclass(slots=True)
class Driver:
    """A driver as the dispatcher sees them: an id, a position, a heartbeat."""

    driver_id: str
    position: LatLng
    last_heartbeat_ms: int

    def is_stale(self, now_ms: int, ttl_ms: int) -> bool:
        """A driver that stopped heartbeating is not a driver we can dispatch."""
        return now_ms - self.last_heartbeat_ms > ttl_ms


@dataclass(slots=True)
class Trip:
    """The unit of work. Its state field only ever moves through `advance`."""

    trip_id: str
    rider_id: str
    pickup: LatLng
    state: TripState = TripState.REQUESTED
    driver_id: str | None = None
    fare_cents: int | None = None
    created_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    version: int = 0  # bumped on every persisted mutation; drives optimistic locking

    def advance(self, dst: TripState) -> None:
        """The ONLY way a trip changes state. Rejects any illegal transition."""
        assert_transition(self.state, dst)
        self.state = dst

    def assign(self, driver_id: str, fare_cents: int) -> None:
        """Matched = a driver AND a fare. The two facts land together or not at all."""
        self.advance(TripState.MATCHED)
        self.driver_id = driver_id
        self.fare_cents = fare_cents


def new_trip_id() -> str:
    return uuid.uuid4().hex
