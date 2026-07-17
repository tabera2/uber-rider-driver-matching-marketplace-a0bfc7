"""The invariants that MUST hold after a pod was killed mid-match.

A chaos drill that only proves "the pods came back" proves nothing — Kubernetes
restarting a container is table stakes. What we assert here is that the DATA is
still correct and that no rider was silently abandoned.
"""
from __future__ import annotations

import os
import sys

import pymysql


def main() -> int:
    conn = pymysql.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "dev"),
        database=os.getenv("MYSQL_DB", "marketplace"),
        cursorclass=pymysql.cursors.DictCursor,
    )
    failures: list[str] = []

    with conn.cursor() as cur:
        # INVARIANT 1: no driver is on two live trips. This is THE invariant.
        # If the atomic claim works, this query returns nothing, always, no
        # matter how many pods you kill or when you kill them.
        cur.execute(
            """SELECT driver_id, COUNT(*) AS n
                 FROM trips
                WHERE state IN ('matched','en_route')
                  AND driver_id IS NOT NULL
                GROUP BY driver_id
               HAVING n > 1"""
        )
        for row in cur.fetchall():
            failures.append(
                f"DOUBLE-BOOKED: driver {row['driver_id']} on {row['n']} live trips"
            )

        # INVARIANT 2: every matched trip has a fare. `assign` bundles them, so
        # a null fare here would mean someone wrote state outside the domain.
        cur.execute(
            "SELECT COUNT(*) AS n FROM trips "
            "WHERE state = 'matched' AND (driver_id IS NULL OR fare_cents IS NULL)"
        )
        n = cur.fetchone()["n"]
        if n:
            failures.append(f"PARTIAL MATCH: {n} matched trips missing driver or fare")

        # INVARIANT 3: no ORPHANED CLAIM. A claim row whose trip is terminal (or
        # gone) means a driver is locked out of the marketplace forever. This is
        # the one the SIGKILL actually produces, and the reaper is the answer.
        cur.execute(
            """SELECT c.driver_id, c.trip_id
                 FROM driver_claims c
                 LEFT JOIN trips t ON t.trip_id = c.trip_id
                WHERE t.trip_id IS NULL
                   OR t.state IN ('complete','cancelled')"""
        )
        for row in cur.fetchall():
            failures.append(
                f"ORPHANED CLAIM: driver {row['driver_id']} held for dead trip "
                f"{row['trip_id']}"
            )

        # INVARIANT 4: no trip stranded in REQUESTED long past the reaper window.
        # A stranded trip is a rider who is still waiting and nobody is coming.
        cur.execute(
            "SELECT COUNT(*) AS n FROM trips "
            "WHERE state = 'requested' "
            "  AND created_ms < (UNIX_TIMESTAMP() * 1000 - 60000)"
        )
        n = cur.fetchone()["n"]
        if n:
            failures.append(f"STRANDED: {n} trips REQUESTED for over 60s")

    for f in failures:
        print(f"FAIL  {f}")
    if failures:
        print(f"\n{len(failures)} invariant violation(s)")
        return 1
    print("PASS  no double-booking, no partial matches, no orphaned claims, "
          "no stranded riders")
    return 0


if __name__ == "__main__":
    sys.exit(main())
