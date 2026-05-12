#!/usr/bin/env python3
"""Cross-scraper monitor.

Aggregates the OK / SC / SF docket scraper trees into a single dashboard
keyed by configured roots (see ``scrapers.json``). Reports:

* per-scraper day calendar with status (complete / in-progress / failures / etc.)
* per-scraper rolling case + doc throughput
* live-run panel built from each root's ``_heartbeat.json`` file
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import sys
import threading
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
STATIC_ROOT = ROOT / "static"
REPO_ROOT = ROOT.parent
DEFAULT_CONFIG = ROOT / "scrapers.json"

# Local imports — keep relative-safe so the script runs from any cwd.
sys.path.insert(0, str(ROOT.parent))
from monitor.heartbeat import (  # noqa: E402
    classify_liveness, epoch_to_iso, read_heartbeat,
)
from monitor.walker import LAYOUTS, iter_case_contents, list_day_dirs  # noqa: E402


def _utc_now_ts() -> float:
    return datetime.now(tz=timezone.utc).timestamp()


def load_scrapers(config_path: Path) -> list[dict]:
    """Load and resolve the configured scrapers."""
    if not config_path.exists():
        example = config_path.with_suffix(config_path.suffix + ".example")
        hint = f"  cp {example} {config_path}" if example.exists() else ""
        raise SystemExit(
            f"Missing config: {config_path}\n"
            f"Create one (typically by copying the example):\n{hint}"
        )
    data = json.loads(config_path.read_text(encoding="utf-8"))
    out = []
    for i, entry in enumerate(data.get("scrapers", [])):
        if "name" not in entry:
            raise ValueError(f"scraper #{i} in {config_path} is missing 'name'")
        if "root" not in entry:
            raise ValueError(f"scraper {entry['name']!r} is missing 'root'")
        root = Path(entry["root"])
        if not root.is_absolute():
            root = (REPO_ROOT / root).resolve()
        layout_name = entry.get("layout", "day")
        if layout_name not in LAYOUTS:
            raise ValueError(f"unknown layout {layout_name!r} for {entry['name']}")
        out.append({
            "name": entry["name"],
            "scraper_kind": entry.get("scraper_kind", "?"),
            "root": root,
            "layout": LAYOUTS[layout_name],
            # Optional static intent — what this scraper is configured
            # to do, regardless of whether anything is running. Surfaced
            # in the dashboard so the operator can see scope without
            # needing a live run.
            "intended_scope": entry.get("intended_scope") or {},
        })
    return out


def classify_day(day_dir: Path) -> dict:
    """Reduce one day folder's day_summary.json + failed_cases.json into a
    single status string for the calendar UI."""
    summary_path = day_dir / "day_summary.json"
    failed_path = day_dir / "failed_cases.json"

    total = scraped = failed_count = 0
    run_error = None

    if summary_path.exists():
        try:
            data = json.loads(summary_path.read_text())
            total = int(data.get("total_cases") or 0)
            scraped = int(data.get("scraped_cases") or 0)
            failed_count = int(data.get("failed_cases") or 0)
            run_error = data.get("run_error")
        except Exception:
            pass

    if failed_path.exists():
        try:
            payload = json.loads(failed_path.read_text())
            if isinstance(payload, list):
                failed_count = max(failed_count, len(payload))
        except Exception:
            pass

    if run_error:
        status = "run_error"
    elif total == 0 and scraped == 0:
        status = "no_cases"
    elif failed_count > 0:
        status = "has_failures"
    elif scraped >= total and total > 0:
        status = "complete"
    elif scraped > 0:
        status = "in_progress"
    else:
        status = "pending"

    return {
        "date": day_dir.name,
        "total": total,
        "scraped": scraped,
        "failed": failed_count,
        "status": status,
        "run_error": run_error,
    }


def gather_days(scraper: dict) -> list[dict]:
    return [classify_day(d) for d in list_day_dirs(scraper["root"], scraper["layout"])]


EMPTY_RATE = {
    "cases_last_5min": 0, "cases_last_hour": 0,
    "cases_last_24h": 0, "cases_last_7d": 0,
    "docs_last_24h": 0, "last_activity_at": None,
    "recent_rate_per_min": 0.0,
}


def gather_rate(scraper: dict, now_ts: float) -> dict:
    """Walk each case dir once, collecting register mtimes (cases) and
    doc mtimes (.pdf + .txt) in a single pass per dir."""
    five_min_ago = now_ts - 300
    hour_ago = now_ts - 3600
    day_ago = now_ts - 86400
    week_ago = now_ts - 7 * 86400

    in_5min = in_hour = in_day = in_week = 0
    docs_24h = 0
    most_recent_ts = 0.0

    root = scraper["root"]
    if not root.exists():
        return dict(EMPTY_RATE)

    for register, docs in iter_case_contents(root, scraper["layout"]):
        if register is not None:
            try:
                mtime = register.stat().st_mtime
            except OSError:
                mtime = None
            if mtime is not None:
                most_recent_ts = max(most_recent_ts, mtime)
                if mtime >= five_min_ago: in_5min += 1
                if mtime >= hour_ago: in_hour += 1
                if mtime >= day_ago: in_day += 1
                if mtime >= week_ago: in_week += 1
        for doc in docs:
            try:
                mtime = doc.stat().st_mtime
            except OSError:
                continue
            if mtime >= day_ago:
                docs_24h += 1

    # Prefer the recent 5-minute window for "right-now" rate. Fall back
    # to the hourly window if the recent one is empty.
    recent_rate = (in_5min / 5.0) if in_5min else (in_hour / 60.0)

    return {
        "cases_last_5min": in_5min,
        "cases_last_hour": in_hour,
        "cases_last_24h": in_day,
        "cases_last_7d": in_week,
        "docs_last_24h": docs_24h,
        "last_activity_at": epoch_to_iso(most_recent_ts) if most_recent_ts > 0 else None,
        "recent_rate_per_min": round(recent_rate, 2),
    }


def gather_heartbeats(scraper: dict, now_ts: float) -> list[dict]:
    """Collect all heartbeat files at a root (handles multi-worker)."""
    root = scraper["root"]
    if not root.exists():
        return []
    out = []
    for hb_path in sorted(root.glob("_heartbeat*.json")):
        # Skip any in-flight tmp writes (e.g., _heartbeat.json.tmp) —
        # defensive even though the .json suffix anchor usually excludes
        # them. Also skip if the path resolved to something non-regular.
        if hb_path.name.endswith(".tmp") or not hb_path.is_file():
            continue
        hb = read_heartbeat(root, filename=hb_path.name)
        if hb is None:
            continue
        try:
            mtime = hb_path.stat().st_mtime
        except OSError:
            continue
        out.append({
            "filename": hb_path.name,
            "liveness": classify_liveness(hb, now=now_ts),
            "data": hb,
            "mtime": epoch_to_iso(mtime),
        })
    return out


def build_scraper_status(scraper: dict, now_ts: float) -> dict:
    days = gather_days(scraper)
    rate = gather_rate(scraper, now_ts)
    heartbeats = gather_heartbeats(scraper, now_ts)
    totals = {
        "days_tracked": len(days),
        "days_complete": sum(1 for d in days if d["status"] == "complete"),
        "days_in_progress": sum(1 for d in days if d["status"] == "in_progress"),
        "days_with_failures": sum(1 for d in days if d["status"] == "has_failures"),
        "days_with_run_error": sum(1 for d in days if d["status"] == "run_error"),
        "cases_total": sum(d["total"] for d in days),
        "cases_scraped": sum(d["scraped"] for d in days),
    }
    return {
        "name": scraper["name"],
        "scraper_kind": scraper["scraper_kind"],
        "root": str(scraper["root"]),
        "layout": scraper["layout"].name,
        "root_exists": scraper["root"].exists(),
        "intended_scope": scraper.get("intended_scope") or {},
        "suggested_next_command": suggest_next_command(scraper),
        "totals": totals,
        "rate": rate,
        "heartbeats": heartbeats,
        "days": days,
    }


def suggest_next_command(scraper: dict) -> str | None:
    """Build a shell command the operator can copy-paste to resume scraping.

    Uses the static `intended_scope` from scrapers.json — auto-resume in
    each scraper handles skipping days already on disk, so the same
    command works whether you're starting fresh or picking up later.
    Returns None when there isn't enough scope to build a useful line.
    """
    scope = scraper.get("intended_scope") or {}
    if not scope:
        return None
    kind = scraper["scraper_kind"]
    root = str(scraper["root"])
    venv_py = "detection_pilot/.venv/bin/python"
    filters = scope.get("filters") or []
    flag_to_cli = {
        "no-filter": "--no-filter",
        "no-cap": "--no-cap",
        "refresh-on-gate": "--refresh-on-gate",
        "popup-fallback": "--enable-popup-fallback",
    }
    flag_args = [flag_to_cli[f] for f in filters if f in flag_to_cli]

    if kind == "ok":
        # If the scope requests rotation discipline (refresh-on-gate is
        # the signal we've been using), suggest the rotate.py wrapper;
        # otherwise the bare scraper.
        wrapper = "ok_scraper/rotate.py" if "refresh-on-gate" in filters else "ok_scraper/scraper.py"
        parts = [
            venv_py, "-u", wrapper,
            f"--start-date {scope['start_date']}" if scope.get("start_date") else None,
            f"--end-date {scope['end_date']}" if scope.get("end_date") else None,
            f"--county {scope['county']}" if scope.get("county") else None,
            f"--type {scope['types']}" if scope.get("types") else None,
            *flag_args,
            f"--data-root {root}",
        ]
    elif kind == "sc":
        parts = [
            venv_py, "-u", "santa_clara_scraper/scraper.py",
            f"--start-date {scope['start_date']}" if scope.get("start_date") else None,
            f"--end-date {scope['end_date']}" if scope.get("end_date") else None,
            f"--case-type {scope['case_type']}" if scope.get("case_type") else None,
            *[f"--case-prefix {p}" for p in scope.get("case_prefixes") or []],
            *flag_args,
            f"--data-root {root}",
        ]
    elif kind == "sf":
        parts = [
            venv_py, "-u", "sf_scraper_fork/launcher.py",
            f"--start-date {scope['start_date']}" if scope.get("start_date") else None,
            f"--end-date {scope['end_date']}" if scope.get("end_date") else None,
            f"--data-root {root}",
        ]
    else:
        return None
    return " ".join(p for p in parts if p)


_STATUS_CACHE: dict = {"data": None, "timestamp": 0.0}
# TTL is intentionally a touch longer than the dashboard's 15s poll
# interval so back-to-back polls reuse the same scan instead of just
# missing it. Bump together with REFRESH_MS in static/app.js.
CACHE_TTL_SECONDS = 20.0
_CACHE_LOCK = threading.Lock()


def build_status(scrapers: list[dict]) -> dict:
    """Aggregate per-scraper status with a short TTL cache.

    Locked so concurrent HTTP requests during a miss don't all rebuild
    in parallel; the second waiter sees the first one's fresh result.
    """
    now = _utc_now_ts()
    cached = _STATUS_CACHE["data"]
    if cached is not None and (now - _STATUS_CACHE["timestamp"]) < CACHE_TTL_SECONDS:
        return cached
    with _CACHE_LOCK:
        # Double-check under the lock — another thread may have just
        # populated the cache while we were waiting.
        cached = _STATUS_CACHE["data"]
        if cached is not None and (now - _STATUS_CACHE["timestamp"]) < CACHE_TTL_SECONDS:
            return cached
        per = [build_scraper_status(s, now) for s in scrapers]
        aggregate = {
            "days_tracked": sum(s["totals"]["days_tracked"] for s in per),
            "days_complete": sum(s["totals"]["days_complete"] for s in per),
            "cases_total": sum(s["totals"]["cases_total"] for s in per),
            "cases_scraped": sum(s["totals"]["cases_scraped"] for s in per),
            "docs_last_24h": sum(s["rate"]["docs_last_24h"] for s in per),
            "cases_last_24h": sum(s["rate"]["cases_last_24h"] for s in per),
            "active_runs": sum(1 for s in per for hb in s["heartbeats"] if hb["liveness"] == "ACTIVE"),
        }
        status = {
            "generated_at": epoch_to_iso(now),
            "aggregate": aggregate,
            "scrapers": per,
        }
        _STATUS_CACHE["data"] = status
        _STATUS_CACHE["timestamp"] = now
        return status


class MonitorHandler(BaseHTTPRequestHandler):
    scrapers: list[dict] = []

    def log_message(self, *args, **kwargs):
        return

    def _send_json(self, payload: dict, status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_static(self, rel: str) -> None:
        target = (STATIC_ROOT / rel).resolve()
        if not str(target).startswith(str(STATIC_ROOT.resolve())) or not target.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        ctype, _ = mimetypes.guess_type(target.name)
        body = target.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", ctype or "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/api/status":
            self._send_json(build_status(self.scrapers))
            return
        if path in {"", "/"}:
            self._send_static("index.html")
            return
        self._send_static(path.lstrip("/"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-scraper docket monitor")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8791)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG,
                        help=f"Scraper config JSON. Default: {DEFAULT_CONFIG}")
    args = parser.parse_args()

    MonitorHandler.scrapers = load_scrapers(args.config)
    summary = ", ".join(f"{s['name']} -> {s['root']}" for s in MonitorHandler.scrapers)
    server = ThreadingHTTPServer((args.host, args.port), MonitorHandler)
    print(f"Monitor serving http://{args.host}:{args.port}")
    print(f"  scrapers: {summary}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
