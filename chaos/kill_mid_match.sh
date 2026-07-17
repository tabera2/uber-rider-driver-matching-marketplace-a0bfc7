#!/usr/bin/env bash
# THE DRILL. Not a simulation — a real pod, killed mid-match, under real load.
#
# Everything before this step was a claim. This is the measurement.
set -euo pipefail

NS="${NS:-marketplace}"
DURATION="${DURATION:-90}"

echo "==> baseline: 6 matchers, load running for ${DURATION}s"
kubectl -n "$NS" get pods -l app=matcher --no-headers | wc -l

# 1) Put the system under real, sustained load (step 11 builds this).
python -m loadgen.run --rate 400 --duration "$DURATION" --drivers 2000 &
LOAD_PID=$!
sleep 15   # let the queue reach a steady state before we break anything

# 2) SIGTERM one matcher: the GRACEFUL path. It should drain and exit clean.
VICTIM=$(kubectl -n "$NS" get pods -l app=matcher -o name | head -1)
echo "==> [graceful] deleting $VICTIM (SIGTERM, 30s grace)"
kubectl -n "$NS" delete "$VICTIM" --wait=false
sleep 20

# 3) SIGKILL a matcher: the UNGRACEFUL path. No handler runs. No drain. This is
#    the OOM-kill, the node loss, the kernel panic. THIS is the real test — the
#    graceful path was never in doubt.
VICTIM=$(kubectl -n "$NS" get pods -l app=matcher -o name | head -1)
echo "==> [ungraceful] SIGKILL on $VICTIM (grace=0, no handler runs)"
kubectl -n "$NS" delete "$VICTIM" --grace-period=0 --force

wait $LOAD_PID

# 4) THE ASSERTIONS. A drill that does not assert is a demo.
echo "==> verifying invariants"
python -m chaos.verify
