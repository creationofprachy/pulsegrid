 PulseGrid
A real-time microservice health monitoring, anomaly-detection, and analytics platform — built on Redis Streams, FastAPI, and SQLite.
Overview
PulseGrid simulates a fleet of microservices (API gateway, auth, payments, orders, search), continuously ingests their health telemetry through a Redis Streams message broker, detects statistical anomalies in real time, and serves live analytics through a REST + WebSocket API and a browser dashboard.
It is a complete, runnable streaming data pipeline — not a mockup — designed to demonstrate distributed-systems and data-engineering fundamentals on a single laptop, with no paid APIs or cloud infrastructure required.
Problem Statement
Production services emit a constant stream of health signals (CPU, memory, latency, error rate, throughput). Left unmonitored, gradual degradation or sudden spikes go unnoticed until users are impacted. Engineering teams need a system that can ingest telemetry at scale, tell the difference between normal noise and a genuine problem, and surface that information within seconds — without every consumer of that data needing to reprocess the raw firehose itself.
Motivation
This project was built to demonstrate, end-to-end, the core concerns of real-time data engineering: a decoupled producer/consumer pipeline, at-least-once delivery handled idempotently, statistical anomaly detection over sliding windows, tumbling-window aggregation for cheap historical queries, and graceful degradation when components fail — all while staying honest about what runs locally versus what a production deployment would need.
Architecture
```
┌──────────────┐     ┌───────────────┐     ┌────────────────────┐     ┌──────────┐
│   Producer   │────▶│ Redis Stream  │────▶│  Stream Processor   │────▶│  SQLite  │
│ (12 sim'd    │ XADD│ "pulsegrid:   │XREAD│  - validate          │     │  events  │
│  service     │     │  events"      │GROUP│  - dedupe (idempotent│     │anomalies │
│  instances)  │     │ (consumer     │     │  - z-score + EWMA    │     │aggregates│
└──────────────┘     │  group)       │     │  - tumbling windows  │     └────┬─────┘
                      └───────┬───────┘     │  - heartbeat watchdog│          │
                              │             └──────────┬──────────┘          │
                       malformed events                │ publish                    │
                       ────▶ DLQ stream          Redis pub/sub                │
                                                  "pulsegrid:updates"         │
                                                         │                    │
                                                         ▼                    ▼
                                              ┌────────────────────────────────────┐
                                              │           FastAPI (api/main.py)     │
                                              │  REST: /api/events, /api/anomalies, │
                                              │  /api/sources, /api/statistics, ... │
                                              │  WebSocket: /ws  (live fan-out)     │
                                              └───────────────┬──────────────────────┘
                                                              ▼
                                              ┌────────────────────────────────────┐
                                              │   Browser Dashboard (HTML/JS/CSS)   │
                                              │  live table · charts · anomalies ·  │
                                              │  historical analytics               │
                                              └────────────────────────────────────┘
```
System Components
Component	Location	Responsibility
Event Generator	`producer/event_generator.py`	Simulates 12 service instances with drifting baselines, noise, injected anomalies, and dropped heartbeats
Schema/Validation	`producer/schemas.py`	Pydantic schema + strict validation for every event
Stream Processor	`processor/stream_processor.py`	Consumer-group based consumption, validation, dedup, persistence, pub/sub fan-out
Anomaly Detector	`processor/anomaly_detector.py`	Rolling z-score + EWMA detection with sustained-anomaly escalation
Aggregator	`processor/aggregator.py`	Tumbling-window per-source rollups
Storage	`storage/database.py`, `storage/models.py`	SQLite schema, idempotent writes, indexed queries
API	`api/main.py`, `api/routes.py`, `api/websocket_manager.py`	REST endpoints, health checks, WebSocket live feed
Dashboard	`dashboard/`	Static HTML/CSS/JS UI served by FastAPI
Event Flow
`EventGenerator` produces a `ServiceHealthEvent` per simulated instance on a jittered schedule (~missing heartbeats and injected anomalies included).
`RedisPublisher` XADDs it onto the `pulsegrid:events` stream (capped with `MAXLEN ~`).
`StreamProcessor` reads batches via `XREADGROUP` under a consumer group (`pulsegrid-processors`).
Each event is schema-validated; failures go to `pulsegrid:events:dlq` and are ACKed off the main stream so one bad message never blocks the group.
Valid events are deduplicated via `INSERT OR IGNORE` on `event_id` (protects against Redis's at-least-once delivery).
The anomaly detector updates rolling per-(instance, metric) statistics and flags deviations.
The event, any anomalies, and the per-source health record are persisted to SQLite.
A compact JSON update is published on `pulsegrid:updates` (Redis pub/sub) for any number of API processes to relay over WebSocket.
The tumbling aggregator flushes 10-second window rollups to the `aggregates` table.
Streaming Pipeline
Broker: Redis Streams, not Kafka. Kafka's cluster/ZooKeeper (or KRaft) footprint is unjustified for a single-laptop, tens-of-events/sec workload; Redis Streams gives consumer groups, at-least-once delivery, `XPENDING`/`XAUTOCLAIM` reclaim, and a dead-letter pattern with a single lightweight binary. This is a deliberate trade-off, documented rather than hidden — see Design Trade-offs.
Processing model: a single async consumer per process (`consumer_name` is configurable, so multiple processor instances could join the same `consumer_group` to shard load — the code path already supports it via `XREADGROUP`/`XACK`).
Anomaly Detection
Two techniques run per `(instance_id, metric)` pair:
Rolling z-score over the last `ROLLING_WINDOW_SIZE` (default 50) samples. A reading is flagged once at least `MIN_SAMPLES_FOR_DETECTION` (default 15) samples exist and `|z| ≥ ZSCORE_THRESHOLD` (default 3.0). The rolling window is not updated by anomalous points — this keeps a sustained spike from getting silently absorbed into "normal," so genuinely sustained abnormal behaviour keeps triggering.
EWMA (α=0.3) tracks the live smoothed trend for each series (exposed via the detector's `series_snapshot`, useful for future dashboards/alerts).
Sustained-behaviour escalation: three or more consecutive anomalous readings on the same series escalate `medium` → `high` and mark `detection_method` as `rolling_zscore+sustained`.
Missing/irregular events: a background watchdog (`_detect_silent_sources`) compares `source_health.last_seen` against `HEARTBEAT_TIMEOUT_SEC` and raises a `heartbeat`-metric anomaly for any instance that's gone quiet.
Every anomaly record includes: timestamp, source, instance, metric, observed value, baseline mean/std, expected range, severity (`low`/`medium`/`high`/`critical`), a human-readable reason, and the detection method — never a bare "anomaly=true" flag.
Storage
SQLite (WAL mode) with five tables: `events`, `anomalies`, `aggregates` (UNIQUE on `window_start, source` for idempotent upserts), `source_health` (one row per instance, incrementally updated), and `processing_stats` (periodic throughput/latency snapshots). Indexes on `timestamp`, `source`, `instance_id`, and `severity` back the query patterns the API actually uses. No raw+processed duplication: an event is stored once.
API
Base path `/api` (plus root-level `/health`, `/metrics`, `/ws`):
Endpoint	Description
`GET /health`	Liveness/readiness — checks Redis connectivity and DB connectivity
`GET /metrics`	Quick snapshot (totals + uptime + websocket client count)
`GET /api/events?limit=&source=`	Most recent processed events
`GET /api/anomalies?limit=&severity=&source=`	Most recent detected anomalies
`GET /api/sources`	Known instances and their live status
`GET /api/statistics`	Aggregate system-wide counters
`GET /api/aggregates?source=&limit=`	Tumbling-window history for charts
Interactive docs auto-generated by FastAPI at `/docs` and `/redoc`.
WebSocket
`ws://<host>:8000/ws` — every connected client receives the same JSON messages the processor publishes: `{"type": "event", "event": {...}, "anomalies": [...]}`, `{"type": "aggregate", "windows": [...]}`, and `{"type": "silence", ...}` for heartbeat timeouts. The dashboard reconnects automatically with a 2s backoff on drop.
Dashboard
Plain HTML/CSS/JS (Chart.js from CDN, no build step) served directly by FastAPI at `/`. Tabs: Live Stream (raw event table), Metrics (4 live charts: event rate, latency, CPU/memory, error rate), Anomalies (live-updating table with severity coloring), Historical Analytics (pulls `/api/aggregates` on demand).
Fault Tolerance
Malformed events never crash the processor — routed to `pulsegrid:events:dlq` with the failure reason attached, then ACKed off the main stream.
Duplicate events (Redis at-least-once redelivery) are silently ignored via `INSERT OR IGNORE` keyed on `event_id`.
Consumer crash recovery: `XAUTOCLAIM` reclaims messages left pending longer than `PENDING_CLAIM_IDLE_MS` (30s default); after `MAX_RETRIES` (3) failed attempts a message is moved to the DLQ instead of looping forever.
Redis connection loss: caught explicitly in the read loop; the processor backs off 2s and retries rather than crashing.
Graceful shutdown: `processor.stop()` breaks the loop cleanly and flushes any partial aggregation window before exiting.
Missing/silent sources: detected by the heartbeat watchdog independently of stream health, since a silent instance produces no events to react to.
Health endpoint: `/health` independently checks Redis and SQLite so a load balancer (or you) can tell degraded from dead.
Performance
Measured with `scripts/benchmark.py` on this development machine (single vCPU sandbox, not representative of a dedicated server). All numbers below are from actual runs, not estimates.
Run	Sources	Target rate	Measured throughput	Avg processing latency	Avg detect lag	Anomalies found	Peak RSS
Moderate	12	50 events/s	44.4 events/s	0.75 ms/event	0.49 ms	74	42.1 MB
Stress	15	300 events/s	173.5 events/s	0.66 ms/event	0.45 ms	777	42.2 MB
Reproduce with:
```powershell
python scripts/benchmark.py --duration 20 --rate 300 --sources 15
```
Benchmark Results (raw output, stress run)
```
Events produced:            3523
Events processed:           3523
Duplicates skipped:         0
Validation errors:          0
Wall clock duration:        20.30s
Throughput (produced):      173.5 events/sec
Throughput (processed):     173.5 events/sec
Avg processing latency:     0.663 ms/event (in-process compute time)
Max processing latency:     0.663 ms/event
Avg end-to-end detect lag:  0.45 ms (publish -> stored)
Anomalies detected:         777
Peak RSS memory:            42.2 MB
```
Honest caveat: the throughput ceiling here is the single-process async event generator (each simulated source sleeps between ticks), not the processor — the processor's own per-event compute cost (~0.7ms) implies it could sustain well over 1,000 events/sec if fed faster. SQLite in WAL mode with a single writer is the next likely bottleneck at higher sustained throughput; a production deployment would move to PostgreSQL and/or shard consumers across the same consumer group.
Tech Stack
Python 3.12 · FastAPI · Uvicorn · Redis Streams (`redis-py` async client) · SQLite (WAL) · Pydantic v2 · vanilla HTML/CSS/JS · Chart.js · pytest / pytest-asyncio · httpx
Installation
Prerequisites
Python 3.10+
Redis server (7.x used in development)
Windows PowerShell Setup
```powershell
# 1. Clone and enter the project
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd pulsegrid

# 2. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1

# 3. Install dependencies
pip install -r requirements.txt

# 4. Install & start Redis for Windows (e.g. via https://github.com/microsoftarchive/redis/releases
#    or `wsl` + `sudo apt install redis-server`, or Memurai as a native alternative)
redis-server

# 5. Copy the environment template
Copy-Item .env.example .env

# 6. In separate terminals (all inside the activated venv):
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000     # API + dashboard
python scripts\run_processor.py                                # stream processor
python scripts\run_producer.py                                 # event generator (Ctrl+C to stop)

# 7. Open the dashboard
start http://localhost:8000
```
Linux / macOS
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
redis-server --daemonize yes
cp .env.example .env
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 &
python scripts/run_processor.py &
python scripts/run_producer.py
```
Docker Setup
Not included. Redis is the only external service required, and it installs as a single package on every major OS (`apt install redis-server`, `brew install redis`, or a native Windows build) — adding Docker/Compose here would add a moving part without adding real value for a single-service dependency. If you already use Docker, `docker run -p 6379:6379 redis:7` works as a drop-in for the Redis step above.
Usage
Once all three processes (API, processor, producer) are running:
Dashboard: `http://localhost:8000`
API docs: `http://localhost:8000/docs`
Health check: `http://localhost:8000/health`
Stop the producer at any time (Ctrl+C) — the processor keeps running and will flag every instance as silent once `HEARTBEAT_TIMEOUT_SEC` elapses, which is a convenient way to see the missing-heartbeat detection fire live.
API Documentation
Full interactive OpenAPI docs are generated automatically by FastAPI and served at `/docs` (Swagger UI) and `/redoc` while the API is running.
Testing
30 automated tests across 5 files, covering event generation, schema validation, anomaly detection (including sustained-anomaly escalation and zero-variance edge cases), SQLite idempotency, full stream-processor integration against a live local Redis (valid events, malformed→DLQ, duplicate dedup, aggregate flushing), and every REST endpoint plus the health check.
```powershell
pip install -r requirements.txt
pytest tests/ -v
```
Actual result from this repository:
```
======================== 30 passed, 2 warnings in 0.51s ========================
```
Manual failure-mode verification performed during development (not part of the automated suite, but exercised live): a hand-crafted malformed event and a garbage payload were pushed directly onto the Redis stream — both were routed to the dead-letter stream with a recorded reason, and the processor continued consuming subsequent valid events without interruption. A source was also allowed to go silent, and the heartbeat watchdog correctly raised a `heartbeat` anomaly once the configured timeout elapsed.
Project Structure
```
pulsegrid/
│
├── README.md
├── requirements.txt
├── .gitignore
├── LICENSE
├── .env.example
├── pytest.ini
├── config.py                  # centralized settings (env-driven)
│
├── producer/
│   ├── event_generator.py     # synthetic fleet simulator + Redis publisher
│   └── schemas.py             # Pydantic event schema + validation
│
├── processor/
│   ├── stream_processor.py    # consumer group, DLQ, reclaim, pub/sub fan-out
│   ├── anomaly_detector.py    # rolling z-score + EWMA + sustained escalation
│   └── aggregator.py          # tumbling-window rollups
│
├── storage/
│   ├── database.py            # SQLite access layer
│   └── models.py              # schema DDL
│
├── api/
│   ├── main.py                 # FastAPI app, health check, dashboard hosting
│   ├── routes.py                # REST endpoints
│   └── websocket_manager.py     # Redis pub/sub -> WebSocket bridge
│
├── dashboard/
│   ├── index.html
│   ├── style.css
│   └── app.js
│
├── tests/
│   ├── conftest.py
│   ├── test_event_generator.py
│   ├── test_anomaly_detector.py
│   ├── test_storage.py
│   ├── test_stream_processor.py
│   └── test_api.py
│
└── scripts/
    ├── run_producer.py
    ├── run_processor.py
    └── benchmark.py
```
Design Trade-offs
Redis Streams over Kafka: chosen for local-laptop practicality (see Streaming Pipeline). The consumer-group/XACK/XAUTOCLAIM model maps directly onto Kafka's consumer-group/offset-commit/rebalance model, so the concepts transfer if this were ever migrated.
SQLite over PostgreSQL: zero-setup, WAL mode handles concurrent reader (API) + writer (processor) safely at this scale. A real multi-node deployment would need PostgreSQL for concurrent writers across multiple processor instances.
Single processor process in the reference run, but the code already reads via a named consumer group (`XREADGROUP`), so running a second `python scripts/run_processor.py` with a different `CONSUMER_NAME` would let it share the same stream and consumer group today — shown but not benchmarked, since a single process comfortably outpaces the demo producer.
No authentication: this is a local demo/portfolio project with no real user data; adding fake JWT auth would add surface area without adding a genuine guarantee.
Limitations
All telemetry is synthetic — there is no real infrastructure behind these "services."
Anomaly detection is unsupervised and per-instance; it does not correlate anomalies across instances/services (e.g. a shared downstream dependency causing simultaneous latency spikes everywhere) — a natural next step.
SQLite's single-writer model means throughput is bounded by disk-write serialization once you push well past the numbers benchmarked here; it was not stress-tested beyond ~175 events/sec.
The dashboard polls `/api/statistics` and `/health` every 3s for the overview cards (WebSocket only carries the live feed) — fine at this scale, would move to push-only at larger scale.
Future Improvements
Cross-instance/cross-service correlation for anomalies (e.g. flag a shared root cause instead of N independent alerts).
Pluggable detection backend (Isolation Forest / seasonal decomposition) behind the same `AnomalyDetector` interface.
Horizontal scaling validation: multiple `StreamProcessor` instances in the same consumer group under real load.
Alerting integration (webhook/Slack) triggered directly off the `pulsegrid:updates` pub/sub channel.
Swap SQLite for PostgreSQL behind the same `Database` interface for multi-writer deployments.
License
MIT — see LICENSE.
Author
Aninda Nath
