"""The durable store. Every mutation is idempotent and version-checked.

The rules this module exists to enforce:
  1. A retried create with the same idempotency key returns the SAME trip.
  2. A state change only lands if nobody else changed the row since we read it.
  3. A driver can hold at most one live claim, enforced by the database.

CONNECTION POOLING (added after the load test — see step 11)
------------------------------------------------------------
The first version of this file opened ONE pymysql connection per TripStore and
shared it. Under load that connection became the bottleneck: pymysql
connections are NOT thread-safe, so every query in the process serialized
behind one socket, and p99 match latency went to 4 seconds while CPU sat at 12%.
A queue with no CPU behind it is always a lock or an I/O serialization point.

The fix is a bounded pool. Bounded, not unlimited: MySQL's max_connections is
finite, and 6 matchers x unlimited connections is how you take down the database
you were trying to scale against. The pool is BACKPRESSURE — when it is empty,
callers wait, and the wait is the system telling you it is full.
"""
from __future__ import annotations

import queue
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

import pymysql
from pymysql.err import IntegrityError

from .domain import LatLng, Trip, TripState, assert_transition


def _now_ms() -> int:
    return int(time.time() * 1000)


class ConcurrentUpdate(Exception):
    """Someone else wrote this trip between our read and our write."""


class DriverAlreadyClaimed(Exception):
    """The driver is on another live trip. This is the double-booking guard."""


class PoolExhausted(Exception):
    """No connection available within the wait budget. This is backpressure."""


@dataclass(slots=True)
class MySQLConfig:
    host: str
    user: str
    password: str
    database: str
    port: int = 3306
    pool_size: int = 10
    pool_wait_s: float = 2.0


class ConnectionPool:
    """A fixed-size pool of pymysql connections, checked out per operation."""

    def __init__(self, cfg: MySQLConfig) -> None:
        self._cfg = cfg
        self._pool: queue.LifoQueue = queue.LifoQueue(maxsize=cfg.pool_size)
        for _ in range(cfg.pool_size):
            self._pool.put(self._connect())

    def _connect(self) -> pymysql.connections.Connection:
        return pymysql.connect(
            host=self._cfg.host,
            port=self._cfg.port,
            user=self._cfg.user,
            password=self._cfg.password,
            database=self._cfg.database,
            autocommit=False,
            cursorclass=pymysql.cursors.DictCursor,
        )

    @contextmanager
    def acquire(self) -> Iterator[pymysql.connections.Connection]:
        try:
            conn = self._pool.get(timeout=self._cfg.pool_wait_s)
        except queue.Empty as exc:
            # We waited and got nothing. Do NOT open a new connection here —
            # that turns a bounded pool into an unbounded one at exactly the
            # moment the database is least able to take it.
            raise PoolExhausted() from exc
        try:
            conn.ping(reconnect=True)  # heal a connection MySQL timed out
            yield conn
        finally:
            self._pool.put(conn)


class TripStore:
    def __init__(self, cfg: MySQLConfig) -> None:
        self._cfg = cfg
        self._pool = ConnectionPool(cfg)

    # ---- reads -----------------------------------------------------------

    def get(self, trip_id: str) -> Trip | None:
        with self._pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM trips WHERE trip_id = %s", (trip_id,))
                row = cur.fetchone()
            conn.commit()
        return _row_to_trip(row) if row else None

    def get_by_idempotency_key(self, key: str) -> Trip | None:
        with self._pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM trips WHERE idempotency_key = %s", (key,))
                row = cur.fetchone()
            conn.commit()
        return _row_to_trip(row) if row else None

    # ---- writes ----------------------------------------------------------

    def create_trip(self, trip: Trip, idempotency_key: str) -> Trip:
        """Insert a new trip, or return the existing one for this key."""
        now = _now_ms()
        with self._pool.acquire() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO trips
                           (trip_id, rider_id, pickup_lat, pickup_lng, state,
                            idempotency_key, version, created_ms, updated_ms)
                           VALUES (%s, %s, %s, %s, %s, %s, 0, %s, %s)""",
                        (trip.trip_id, trip.rider_id, trip.pickup.lat,
                         trip.pickup.lng, TripState.REQUESTED.value,
                         idempotency_key, now, now),
                    )
                conn.commit()
                return trip
            except IntegrityError:
                conn.rollback()
        existing = self.get_by_idempotency_key(idempotency_key)
        if existing is None:  # pragma: no cover - only on a torn write
            raise RuntimeError("idempotency key collided but no row found")
        return existing

    def claim_driver(self, driver_id: str, trip_id: str) -> None:
        """Atomically take exclusive hold of a driver, or refuse."""
        with self._pool.acquire() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO driver_claims (driver_id, trip_id, claimed_ms) "
                        "VALUES (%s, %s, %s)",
                        (driver_id, trip_id, _now_ms()),
                    )
                conn.commit()
            except IntegrityError as exc:
                conn.rollback()
                raise DriverAlreadyClaimed(driver_id) from exc

    def release_driver(self, driver_id: str) -> None:
        """Give the driver back to the pool. Safe to call twice."""
        with self._pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM driver_claims WHERE driver_id = %s", (driver_id,)
                )
            conn.commit()

    def save_match(self, trip: Trip, driver_id: str, fare_cents: int) -> Trip:
        """Persist REQUESTED -> MATCHED under an optimistic-lock check."""
        assert_transition(trip.state, TripState.MATCHED)
        with self._pool.acquire() as conn:
            with conn.cursor() as cur:
                affected = cur.execute(
                    """UPDATE trips
                          SET state = %s, driver_id = %s, fare_cents = %s,
                              version = version + 1, updated_ms = %s
                        WHERE trip_id = %s AND version = %s""",
                    (TripState.MATCHED.value, driver_id, fare_cents,
                     _now_ms(), trip.trip_id, trip.version),
                )
            if affected == 0:
                conn.rollback()
                raise ConcurrentUpdate(trip.trip_id)
            conn.commit()
        trip.assign(driver_id, fare_cents)
        trip.version += 1
        return trip

    def advance(self, trip: Trip, dst: TripState) -> Trip:
        """Persist any legal transition, releasing the driver on terminal states."""
        assert_transition(trip.state, dst)
        with self._pool.acquire() as conn:
            with conn.cursor() as cur:
                affected = cur.execute(
                    """UPDATE trips
                          SET state = %s, version = version + 1, updated_ms = %s
                        WHERE trip_id = %s AND version = %s""",
                    (dst.value, _now_ms(), trip.trip_id, trip.version),
                )
                if affected == 0:
                    conn.rollback()
                    raise ConcurrentUpdate(trip.trip_id)
                if dst in (TripState.COMPLETE, TripState.CANCELLED) and trip.driver_id:
                    cur.execute(
                        "DELETE FROM driver_claims "
                        "WHERE driver_id = %s AND trip_id = %s",
                        (trip.driver_id, trip.trip_id),
                    )
            conn.commit()
        trip.advance(dst)
        trip.version += 1
        return trip

    def stale_matched_trips(self, older_than_ms: int) -> list[Trip]:
        """Trips a crashed dispatcher may have abandoned. The reaper reads this."""
        cutoff = _now_ms() - older_than_ms
        with self._pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM trips WHERE state = 'requested' "
                    "AND created_ms < %s ORDER BY created_ms LIMIT 200",
                    (cutoff,),
                )
                rows = cur.fetchall()
            conn.commit()
        return [_row_to_trip(r) for r in rows]


def _row_to_trip(row: dict) -> Trip:
    return Trip(
        trip_id=row["trip_id"],
        rider_id=row["rider_id"],
        pickup=LatLng(row["pickup_lat"], row["pickup_lng"]),
        state=TripState(row["state"]),
        driver_id=row["driver_id"],
        fare_cents=row["fare_cents"],
        created_ms=row["created_ms"],
        version=row["version"],
    )
