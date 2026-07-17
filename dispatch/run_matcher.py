"""Entrypoint for the matcher process.

Note what this file is for beyond starting a loop: it owns the SIGTERM
contract. Kubernetes sends SIGTERM and then waits `terminationGracePeriod`
before sending SIGKILL. Everything we do in that window determines whether a
pod dying mid-match loses a rider or not. Step 10 is where we prove it.
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import threading

import redis

from .geo import GeoIndex
from .matcher import Matcher
from .pricing_client import PricingClient
from .queue import PendingQueue
from .store import MySQLConfig, TripStore

log = logging.getLogger("run_matcher")

_stopping = threading.Event()


def _on_sigterm(signum: int, _frame) -> None:
    """Ask the loop to stop. Do NOT kill it mid-trip.

    The loop checks `_stopping` between trips, so a trip that has already begun
    runs to completion — claim, price, save — before we exit. Half-finished
    work is the thing we are avoiding, and the way to avoid it is to never
    start work we cannot finish, then finish what we started.
    """
    log.info("received signal %s; draining", signum)
    _stopping.set()


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    signal.signal(signal.SIGTERM, _on_sigterm)
    signal.signal(signal.SIGINT, _on_sigterm)

    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    store = TripStore(
        MySQLConfig(
            host=os.getenv("MYSQL_HOST", "127.0.0.1"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", ""),
            database=os.getenv("MYSQL_DB", "marketplace"),
        )
    )
    matcher = Matcher(store, GeoIndex(r), PendingQueue(r), PricingClient())

    log.info("matcher up")
    matcher.run_forever(stop=_stopping.is_set)
    log.info("matcher drained cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
