"""The rider-facing HTTP edge.

Design rule: this process does the LEAST possible work per request. It
validates, it writes one row, it pushes one queue entry, it returns. Matching
happens elsewhere, on its own schedule. A ride request must not wait for a
driver search, a pricing RPC, and a claim — that would put a whole distributed
transaction on the critical path of a button tap.
"""
from __future__ import annotations

import os

import redis
from fastapi import FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field

from .domain import LatLng, Trip, TripState, new_trip_id
from .geo import GeoIndex
from .queue import PendingQueue
from .store import MySQLConfig, TripStore

app = FastAPI(title="dispatch")

_redis = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
_store = TripStore(
    MySQLConfig(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DB", "marketplace"),
    )
)
_geo = GeoIndex(_redis)
_queue = PendingQueue(_redis)


class RideRequest(BaseModel):
    rider_id: str = Field(min_length=1, max_length=64)
    # Validation at the EDGE. A latitude of 200 is not a coordinate; it is a
    # bug or an attack, and it must be rejected here, not discovered three
    # services deep when a haversine returns NaN.
    pickup_lat: float = Field(ge=-90.0, le=90.0)
    pickup_lng: float = Field(ge=-180.0, le=180.0)


class TripView(BaseModel):
    trip_id: str
    state: str
    driver_id: str | None = None
    fare_cents: int | None = None


class Heartbeat(BaseModel):
    driver_id: str = Field(min_length=1, max_length=64)
    lat: float = Field(ge=-90.0, le=90.0)
    lng: float = Field(ge=-180.0, le=180.0)


@app.post("/rides", response_model=TripView, status_code=202)
def request_ride(
    req: RideRequest,
    response: Response,
    idempotency_key: str = Header(..., alias="Idempotency-Key", max_length=80),
) -> TripView:
    """Accept a ride request. 202 = "recorded, not yet matched".

    The order of operations is the contract, and it is not negotiable:
      1. persist the trip (durable — survives this pod dying on the next line)
      2. push the id onto the pending queue (a hint that work exists)
      3. return 202 with the trip id

    Do 2 before 1 and a crash between them leaves a queue entry for a trip that
    does not exist. Do 1 and crash before 2 and the trip is stranded in
    REQUESTED — which is exactly what the reaper is for, and is recoverable.
    ALWAYS make the durable write first and the hint second.
    """
    trip = Trip(
        trip_id=new_trip_id(),
        rider_id=req.rider_id,
        pickup=LatLng(req.pickup_lat, req.pickup_lng),
    )
    saved = _store.create_trip(trip, idempotency_key)

    if saved.trip_id == trip.trip_id:
        # Genuinely new: enqueue it. A replay returns the existing trip and does
        # NOT enqueue again — that is the idempotency key earning its keep.
        _queue.push(saved.trip_id)
    else:
        response.status_code = 200  # replay of a request we already accepted

    return TripView(
        trip_id=saved.trip_id,
        state=saved.state.value,
        driver_id=saved.driver_id,
        fare_cents=saved.fare_cents,
    )


@app.get("/rides/{trip_id}", response_model=TripView)
def get_ride(trip_id: str) -> TripView:
    """The rider's app polls this until state leaves REQUESTED."""
    trip = _store.get(trip_id)
    if trip is None:
        raise HTTPException(status_code=404, detail="no such trip")
    return TripView(
        trip_id=trip.trip_id,
        state=trip.state.value,
        driver_id=trip.driver_id,
        fare_cents=trip.fare_cents,
    )


@app.post("/drivers/heartbeat", status_code=204)
def heartbeat(hb: Heartbeat) -> Response:
    """Every online driver posts here every few seconds. Highest-volume route."""
    _geo.heartbeat(hb.driver_id, LatLng(hb.lat, hb.lng))
    return Response(status_code=204)


@app.post("/drivers/{driver_id}/offline", status_code=204)
def offline(driver_id: str) -> Response:
    _geo.go_offline(driver_id)
    return Response(status_code=204)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Liveness: am I, the process, alive? Nothing else. See step 8."""
    return {"status": "ok"}


@app.get("/readyz")
def readyz(response: Response) -> dict[str, object]:
    """Readiness: can I actually SERVE? Checks my dependencies, unlike healthz."""
    checks: dict[str, bool] = {}
    try:
        _redis.ping()
        checks["redis"] = True
    except Exception:
        checks["redis"] = False
    try:
        _store.get("0" * 32)  # a read that exercises the connection, not the data
        checks["mysql"] = True
    except Exception:
        checks["mysql"] = False

    ok = all(checks.values())
    if not ok:
        response.status_code = 503
    return {"ready": ok, "checks": checks}
