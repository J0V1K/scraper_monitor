# Court Scraper Monitor

A lightweight, stdlib-only dashboard for any number of court-docket scrapers
that share the convention of `<root>/YYYY-MM-DD/<CASE>/register_of_actions.json`
(or the `YYYY-MM/YYYY-MM-DD/<CASE>/...` variant).

## Setup

```bash
# 1. Copy the example config and edit it for your data roots.
cp scrapers.json.example scrapers.json

# 2. Launch.
python server.py
# default: http://127.0.0.1:8791
```

Optional flags:

| Flag | Default | Purpose |
|---|---|---|
| `--host` | `127.0.0.1` | Bind address. |
| `--port` | `8791` | TCP port. |
| `--config` | `scrapers.json` | List of scrapers to surface. |

## What it shows

* **Aggregate strip** at the top (active runs, total cases, doc/day throughput).
* **Live runs panel** — one row per heartbeat file found, classified as
  `ACTIVE` / `HUNG` / `CRASHED` / `EXITED`. Surfaces the run's intent
  (county / case types / date range / filter flags) and current state
  (current day, current case, session counters, current IP).
* **Per-scraper section** — day calendar, rolling counts (rate/min,
  cases/hr, cases/24h, docs/24h), totals, and — when nothing is running
  for that scraper — a copy-paste **resume command** built from the
  scraper's configured scope.

The HTTP cache TTL is 20 seconds; the page polls every 15 seconds. Effective
freshness is ~20 seconds in the worst case.

## Configuration (`scrapers.json`)

`scrapers.json` is git-ignored; copy `scrapers.json.example` and edit it
for your environment.

```json
{
  "scrapers": [
    {
      "name": "OK Tulsa 2025",
      "scraper_kind": "ok",
      "root": "/Volumes/Seagate/Oklahoma/2025",
      "layout": "day",
      "intended_scope": {
        "county": "tulsa",
        "types": "CJ,CV,CF,CM",
        "start_date": "2025-01-01",
        "end_date": "2025-12-31",
        "filters": ["no-filter", "no-cap", "refresh-on-gate"]
      }
    },
    {
      "name": "SF",
      "scraper_kind": "sf",
      "root": "../sf_scraper_fork/data",
      "layout": "month-day"
    }
  ]
}
```

Per scraper:

| Field | Required | Purpose |
|---|---|---|
| `name` | yes | Display name in the UI. |
| `root` | yes | Path to the data root. Relative paths resolve against `monitor/`'s parent directory. |
| `scraper_kind` | recommended | One of `ok` / `sc` / `sf`. Drives the resume-command template. |
| `layout` | no (default `day`) | `day` for `<root>/YYYY-MM-DD/<CASE>/...`, `month-day` for `<root>/YYYY-MM/YYYY-MM-DD/<CASE>/...`. |
| `intended_scope` | no | Static run intent surfaced as a "configured scope" chip and used to render the resume command. See the example. |

## Heartbeat protocol

Scrapers write `_heartbeat.json` (or `_heartbeat_worker_<N>.json` when running
under `--workers N`) at the top of their data root every ~10 seconds while
running. The monitor reads these files to render the live-runs panel.

Wire `monitor.heartbeat.Heartbeat` into scraper code so the dashboard can
classify runs as `ACTIVE` / `HUNG` / `CRASHED` / `EXITED`:

```python
from monitor.heartbeat import Heartbeat, probe_public_ip, rotation_managed

hb = Heartbeat(data_root, scraper="ok", args=sys.argv[1:])
hb.update(county="tulsa", types="CJ,CV,CF,CM",
          start_date="2025-01-01", end_date="2025-12-31",
          rotation_managed=rotation_managed(),
          current_ip=probe_public_ip())
hb.start()
try:
    for day in days:
        hb.update(current_day=day.isoformat())
        for case in cases:
            hb.update(current_case=case.number)
            # ... per-download:
            hb.increment("session_docs_collected")
        hb.increment("session_cases_scraped")
    hb.close(status="exited")
except Exception as exc:
    hb.close(status="crashed", finished_reason=str(exc)[:200])
    raise
```

`update()` flushes immediately; `increment()` mutates in-memory state and
lets the periodic beat flush, so high-frequency counter bumps don't
serialize on disk writes.

Liveness classification (`monitor.heartbeat.classify_liveness`):

| State | Condition |
|---|---|
| `ACTIVE` | heartbeat fresh (≤ 30 s old) and pid still running on the same host |
| `HUNG` | heartbeat stale, pid still running |
| `CRASHED` | heartbeat stale, pid gone (or hostname differs) |
| `EXITED` | heartbeat's `status` field is terminal (`exited` / `crashed`) |
| `NONE` | no heartbeat file at all |
