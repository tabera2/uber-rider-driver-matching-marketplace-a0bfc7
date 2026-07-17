"""The metrics that would let you debug this system at 3am.

Choosing WHAT to measure is the hard part; the library is trivial. The rule:
instrument the things a human would ask about during an incident.

  "Are riders getting cars?"        -> match rate (counter)
  "How long are they waiting?"      -> match latency (histogram)
  "Is the backlog growing?"         -> queue depth (gauge)
  "Is pricing hurting us?"          -> quote latency + fallback rate
  "Are we double-booking?"          -> claim contention (counter)

A metric nobody would look at during an outage is a metric you should delete.
"""
from __future__ import annotations

import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

# COUNTER: monotonically increasing. You never read its value — you read its
# RATE. Labelled by outcome so one metric answers both "how many matched" and
# "how many found no driver".
matches_total = Counter(
    "dispatch_matches_total",
    "Trips a matcher finished, by outcome.",
    ["outcome"],  # matched | no_driver | already_done | error
)

# HISTOGRAM, not a gauge or an average. An AVERAGE latency is a lie: 99 requests
# at 10ms and 1 request at 10s averages to 110ms, which looks fine and describes
# nobody. Buckets let us ask for the p99 — the experience of the unhappiest 1%,
# which is the number that actually generates support tickets.
match_latency = Histogram(
    "dispatch_match_seconds",
    "Wall time of one match_once call.",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

quote_latency = Histogram(
    "dispatch_quote_seconds",
    "Wall time of the pricing RPC, from the caller's side.",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

# COUNTER: how often we lost a claim race. This is not an error rate — losing is
# correct behaviour. But a SPIKE means supply is scarce relative to demand in
# some cell, which is a real marketplace signal, not an engineering one.
claim_contention_total = Counter(
    "dispatch_claim_contention_total",
    "Claim attempts that lost the race to another matcher.",
)

pricing_fallback_total = Counter(
    "dispatch_pricing_fallback_total",
    "Quotes served from the local fallback because pricing was unavailable.",
)

# GAUGE: a value that goes up AND down. Queue depth is the single best load
# signal in the system: it is the integral of (arrival rate - service rate), so
# a rising depth means you are losing, full stop, no matter how good latency
# looks right now.
queue_depth = Gauge(
    "dispatch_queue_depth",
    "Trips waiting to be matched.",
)

# GAUGE: drivers currently holding a claim. If this only ever goes UP, you are
# leaking claims — the failure from step 5 that has no error to log.
claims_held = Gauge(
    "dispatch_claims_held",
    "Drivers currently claimed for a live trip.",
)


class _MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - http.server API
        if self.path == "/healthz":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPE_LATEST)
        self.end_headers()
        self.wfile.write(generate_latest())

    def log_message(self, *_args) -> None:
        pass  # do not log every scrape; Prometheus polls every 15s forever


def serve_metrics(port: int = 9100) -> None:
    """Expose /metrics and /healthz on a daemon thread. Never blocks the loop."""
    server = HTTPServer(("0.0.0.0", port), _MetricsHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


class Timer:
    """`with Timer(hist):` — observes elapsed seconds even if the block raises."""

    def __init__(self, hist: Histogram) -> None:
        self._hist = hist
        self._t0 = 0.0

    def __enter__(self) -> "Timer":
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *_exc) -> None:
        self._hist.observe(time.perf_counter() - self._t0)
