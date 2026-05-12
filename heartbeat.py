"""Heartbeat file primitive for live-run visibility in the monitor.

Each scraper, while running, writes ``<data_root>/_heartbeat.json`` every
~``period_s`` seconds with its current state. The monitor reads these
files to render a "what's running right now" panel; comparing
``last_heartbeat_at`` and ``pid`` lets the monitor distinguish ACTIVE /
HUNG / DEAD / EXITED states without scraping the filesystem.

Design constraints:

* No external dependencies; works in both async and sync scrapers
  (uses :mod:`threading` for the background beat).
* Atomic writes via ``tmp + os.replace`` so the monitor never reads a
  half-written JSON.
* The scraper passes incremental state via :meth:`update`, which writes
  immediately AND refreshes the next periodic-beat baseline.
* On clean exit, :meth:`close` writes a terminal record with
  ``status: "exited"``. The file is left in place so the monitor can
  show "ran from X to Y" until the next run overwrites it.

Usage:

    hb = Heartbeat(data_root, scraper="ok", args=sys.argv[1:])
    hb.start()
    try:
        for day in days:
            hb.update(current_day=day.isoformat())
            for case in cases:
                hb.update(current_case=case.number)
                ...
        hb.close(status="exited", finished_reason="completed")
    except Exception as exc:
        hb.close(status="crashed", finished_reason=str(exc)[:200])
        raise
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HEARTBEAT_FILENAME = "_heartbeat.json"


def utc_now_iso() -> str:
    """Aware UTC timestamp as ISO 8601 with a `Z` suffix."""
    return (
        datetime.now(tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def epoch_to_iso(ts: float) -> str:
    """Epoch seconds -> UTC ISO 8601 with `Z` suffix."""
    return (
        datetime.fromtimestamp(ts, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def probe_public_ip(timeout_s: float = 5.0) -> str:
    """Best-effort fetch of the current public IPv4. Returns "" on failure.

    Used by scrapers at run start to surface which VPN exit they're on;
    the monitor renders this in the live-runs panel.
    """
    try:
        out = subprocess.run(
            ["curl", "-s", "--max-time", str(int(timeout_s)),
             "https://ipv4.icanhazip.com"],
            capture_output=True, text=True, check=False,
            timeout=timeout_s + 3,
        )
        return out.stdout.strip()
    except Exception:
        return ""


def rotation_managed() -> bool:
    """True when the process is running under `rotate.py`. Set via env var."""
    return os.environ.get("ROTATE_MANAGED") == "1"


class Heartbeat:
    """Background-thread heartbeat writer keyed at a data root.

    Thread-safe; safe to call :meth:`update` from any task or thread.
    """

    def __init__(
        self,
        data_root: Path,
        scraper: str,
        *,
        period_s: float = 10.0,
        args: list[str] | None = None,
        worker_id: int | None = None,
    ) -> None:
        self.data_root = Path(data_root).resolve()
        self.scraper = scraper
        self.period_s = float(period_s)
        # Allow multi-worker setups (OK's --workers) to write distinct files
        # without stomping on each other.
        if worker_id is None:
            self.filename = HEARTBEAT_FILENAME
        else:
            self.filename = f"_heartbeat_worker_{worker_id}.json"
        self._lock = threading.Lock()
        self._state: dict[str, Any] = {
            "scraper": scraper,
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "worker_id": worker_id,
            "started_at": utc_now_iso(),
            "last_heartbeat_at": utc_now_iso(),
            "data_root": str(self.data_root),
            "args": list(args or []),
            "status": "starting",
        }
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # -- lifecycle --

    def start(self) -> None:
        """Begin periodic heartbeat writes in a background thread."""
        if self._thread is not None:
            return
        self.data_root.mkdir(parents=True, exist_ok=True)
        self._state["status"] = "running"
        with self._lock:
            self._state["last_heartbeat_at"] = utc_now_iso()
            self._write_locked()
        self._thread = threading.Thread(
            target=self._beat_loop, name="heartbeat", daemon=True
        )
        self._thread.start()

    def update(self, **fields: Any) -> None:
        """Merge new fields into the heartbeat and flush immediately.

        Use for run-intent fields and state transitions the monitor
        should reflect right away. High-frequency counter bumps should
        use :meth:`increment` instead — it doesn't write to disk per
        call, relying on the periodic beat to flush.
        """
        with self._lock:
            self._state.update(fields)
            self._state["last_heartbeat_at"] = utc_now_iso()
            self._write_locked()

    def increment(self, field: str, amount: int = 1) -> int:
        """Atomically increment an integer field. Returns the new value.

        Does NOT flush to disk — would serialize the scraper on every
        download under a per-write lock, especially painful on USB or
        network-mounted data roots. The periodic beat picks up the new
        value at the next ``period_s`` boundary.
        """
        with self._lock:
            current = int(self._state.get(field, 0) or 0)
            new = current + amount
            self._state[field] = new
            return new

    def close(self, *, status: str = "exited", **fields: Any) -> None:
        """Write a final heartbeat then stop the background thread."""
        with self._lock:
            self._state.update(fields)
            self._state["status"] = status
            self._state["finished_at"] = utc_now_iso()
            self._state["last_heartbeat_at"] = self._state["finished_at"]
            self._write_locked()
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    # -- internals --

    def _beat_loop(self) -> None:
        # Event.wait returns True only when the stop event is set; sleep
        # first so we don't double-write right after start().
        while not self._stop.wait(self.period_s):
            with self._lock:
                self._state["last_heartbeat_at"] = utc_now_iso()
                self._write_locked()

    def _write_locked(self) -> None:
        """Atomic write via tmp + os.replace. Caller holds self._lock.

        os.replace is atomic only on the same filesystem; tmp lives next
        to target, so this is always satisfied unless the data_root is
        on a stacked overlay/network mount with unusual semantics.
        """
        target = self.data_root / self.filename
        tmp = target.with_suffix(target.suffix + ".tmp")
        try:
            tmp.write_text(json.dumps(self._state, indent=2), encoding="utf-8")
            os.replace(tmp, target)
        except OSError:
            # External-drive unmount / permission flap: don't crash the
            # scraper. The next beat retries; the monitor's staleness
            # detection will surface the gap.
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass


def read_heartbeat(data_root: Path, *, filename: str = HEARTBEAT_FILENAME) -> dict | None:
    """Read a heartbeat file. Returns None if missing or malformed."""
    path = Path(data_root) / filename
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def classify_liveness(
    heartbeat: dict | None,
    *,
    stale_after_s: float = 30.0,
    now: float | None = None,
) -> str:
    """Classify a heartbeat as ACTIVE / HUNG / EXITED / CRASHED / NONE.

    * NONE     — no heartbeat file.
    * EXITED   — heartbeat has terminal status (``exited`` / ``crashed``).
    * ACTIVE   — heartbeat fresh AND pid responds to signal 0.
    * HUNG     — heartbeat stale, pid still exists (process alive, not writing).
    * CRASHED  — heartbeat stale, pid gone (or terminal status with no
                 finished_at, conservatively).
    """
    if heartbeat is None:
        return "NONE"

    status = heartbeat.get("status")
    if status in {"exited", "crashed"}:
        return "EXITED"

    last = heartbeat.get("last_heartbeat_at")
    pid = heartbeat.get("pid")
    # Only honor the pid check when the heartbeat is from this host;
    # otherwise the pid would refer to an unrelated process here.
    same_host = heartbeat.get("hostname") in (None, socket.gethostname())
    pid_alive = same_host and isinstance(pid, int) and _pid_alive(pid)

    last_ts = _iso_to_epoch(last) if isinstance(last, str) else None
    age = None
    if last_ts is not None:
        if now is None:
            now = datetime.now(tz=timezone.utc).timestamp()
        age = now - last_ts

    fresh = (age is not None and age <= stale_after_s)
    if fresh and pid_alive:
        return "ACTIVE"
    if pid_alive:
        return "HUNG"
    return "CRASHED"


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _iso_to_epoch(iso: str) -> float | None:
    try:
        # ``fromisoformat`` accepts the ``Z`` suffix in Python 3.11+; we
        # paper over older runtimes by swapping it for ``+00:00``.
        normalized = iso.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).timestamp()
    except (ValueError, TypeError):
        return None
