# Uber | Rider-Driver Matching Marketplace

An advanced distributed-systems capstone in Python. Model the trip state machine so illegal states can't exist, persist it in MySQL with idempotency keys, index available drivers by geo-cell from live heartbeats, accept ride requests over an API, and write the matching algorithm that assigns each rider a driver without ever double-booking one. Split pricing into its own gRPC service behind a timeout and a circuit breaker, containerize with Docker, deploy on Kubernetes with real probes, emit latency and match-rate metrics to Prometheus, then run the chaos drill — kill the dispatch pod mid-match under load and prove requests still get served. Finish with a load generator and a runbook.

Built step-by-step with [KhwajaLabs Build](https://khwajalabs.com).

## Stack
- Python
- MySQL
- Redis
- gRPC
- Docker
- Kubernetes
- Prometheus
- Grafana
