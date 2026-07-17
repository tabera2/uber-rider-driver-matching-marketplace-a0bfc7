# Marketplace Runbook

You were paged. Work top to bottom. Every alert in `prometheus/rules.yml` has
an entry here — **an alert without a runbook entry is a bug in the alert**,
because it wakes a human and gives them nowhere to start.

## First 60 seconds, every alert

    kubectl -n marketplace get pods
    open http://grafana/d/marketplace

Read three numbers, in this order, and do not skip the order:

1. `dispatch_queue_depth` — is the backlog growing? (the load question)
2. `dispatch:match_success_ratio` — are riders getting cars? (the rider question)
3. `dispatch:pricing_fallback_ratio` — is pricing degraded? (the dependency question)

Those three tell you WHICH subsystem is unhappy before you touch anything.

---

## riders-not-getting-cars

`dispatch:match_success_ratio < 0.90`

Fewer than 90% of match attempts are producing a car. Riders are feeling this
right now.

**Find out which outcome is eating the attempts:**

    sum by (outcome) (rate(dispatch_matches_total[5m]))

| dominant outcome | meaning | action |
|---|---|---|
| `no_driver` | supply, not software | check driver count by cell; is a city event on? this may be *correct* |
| `error` | we are broken | check matcher logs; is pricing raising past the breaker? |
| `already_done` | the reaper is over-firing | trips are being re-queued while still live — raise `STRANDED_TRIP_MS` |

**If `no_driver` dominates, the system is working.** It is telling you there
are no cars. That is a marketplace problem, not an engineering one, and the fix
is supply incentives — do not "fix" it by widening `max_ring`, which only
matches riders to drivers 8km away and makes the experience worse.

## backlog-growing

`deriv(dispatch_queue_depth[10m]) > 0.5`

Arrivals exceed service. This does **not** self-heal — the queue is an
integral, so a positive slope runs away.

    # Are the matchers busy, or blocked?
    rate(process_cpu_seconds_total{app="matcher"}[5m])

- **CPU high** → they are genuinely saturated. Scale out:
  `kubectl -n marketplace scale deploy/matcher --replicas=12`
  (this is SAFE at any replica count — the atomic claim is what makes it safe)
- **CPU low** → they are BLOCKED on something. This is the connection-pool
  signature from step 11. Check `PoolExhausted` in the logs, then MySQL:

      SHOW STATUS LIKE 'Threads_connected';
      SHOW STATUS LIKE 'Threads_running';

  `Threads_running` pinned at max means MySQL is the bottleneck; scaling
  matchers will make it **worse**, not better. Scale the database or shed load.

**Never scale matchers to fix a database bottleneck.** You will convert a slow
system into a dead one.

## claims-leaking

`deriv(dispatch_claims_held[30m]) > 0 and dispatch_claims_held > 100`

Drivers are being claimed and never released. Every one is a driver who is
online, earning nothing, invisible to matching.

**Confirm it:**

    SELECT c.driver_id, c.trip_id, t.state,
           (UNIX_TIMESTAMP()*1000 - c.claimed_ms)/1000 AS age_s
      FROM driver_claims c
      LEFT JOIN trips t ON t.trip_id = c.trip_id
     WHERE t.trip_id IS NULL OR t.state IN ('requested','complete','cancelled')
     ORDER BY age_s DESC LIMIT 20;

**Then check the reaper is actually alive** — this alert usually means it is
not, because the reaper exists precisely to keep this number flat:

    kubectl -n marketplace logs deploy/reaper --tail=50 | grep sweep

- No `sweep:` lines → the reaper is dead or wedged. Restart it. The backlog of
  orphans will clear within a few sweep intervals.
- Sweeps running, claims still climbing → the leak is faster than the sweep, OR
  there is a code path releasing nothing. Look for a matcher exception path
  added since the last deploy that returns without `release_driver`.

## pricing-degraded

`dispatch:pricing_fallback_ratio > 0.10`

More than 10% of fares are coming from the local fallback. **Riders are still
getting cars** — this is a revenue problem, not an availability problem, and it
does not warrant a page at 3am. That is the circuit breaker working exactly as
designed.

    kubectl -n marketplace get pods -l app=pricing
    kubectl -n marketplace logs deploy/pricing --tail=100
    histogram_quantile(0.99, sum by (le) (rate(dispatch_quote_seconds_bucket[5m])))

- p99 quote latency near `QUOTE_DEADLINE_S` (0.25s) → pricing is **slow**, not
  down, and every call is burning the full deadline. This is the "slow is worse
  than down" case. Consider dropping the deadline so the breaker opens sooner.
- Pricing pods healthy but fallbacks high → check the breaker is not stuck open.
  It re-probes every `BREAKER_COOLDOWN_S`; a restart of the matchers resets it.

---

## Rolling back

One image, three commands (step 7), so a rollback is one operation per
deployment and they are all the same SHA:

    kubectl -n marketplace rollout undo deploy/dispatch-api
    kubectl -n marketplace rollout undo deploy/matcher
    kubectl -n marketplace rollout undo deploy/pricing

Rolling back the matcher mid-match is **safe** — that is precisely what the
chaos drill in step 10 proves. Graceful shutdown drains the in-flight trip; the
reaper catches anything the drain misses.

## Things that are NOT incidents

- **Claim contention spiking.** `dispatch_claim_contention_total` climbing means
  many riders want the same drivers. That is the marketplace working. It is a
  *demand* signal, not an error.
- **`already_done` matches.** The reaper re-queued a trip that a matcher had
  already handled. The idempotence guard absorbed it. Working as designed.
- **A matcher pod restarting.** Six replicas exist so any of them can die.
