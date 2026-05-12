"""Layout-aware filesystem walker for the multi-scraper monitor.

The three scrapers in this repo use slightly different on-disk shapes:

* ``ok`` and ``sc``: ``<root>/<YYYY-MM-DD>/<CASE>/register_of_actions.json``
* ``sf``:           ``<root>/<YYYY-MM>/<YYYY-MM-DD>/<CASE>/register_of_actions.json``

This module hides that with a single ``walk_*`` API the monitor calls,
plus a tiny ``LAYOUTS`` registry the scrapers config references by name.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

DAY_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MONTH_DIR_RE = re.compile(r"^\d{4}-\d{2}$")
REGISTER_FILENAME = "register_of_actions.json"
DOC_SUFFIXES = (".pdf", ".txt")


@dataclass(frozen=True)
class Layout:
    """How to discover day folders under a data root."""

    name: str
    day_dirs_glob: str   # passed to Path.glob to find <YYYY-MM-DD> dirs
    day_dir_re: re.Pattern[str]


LAYOUTS: dict[str, Layout] = {
    "day": Layout(
        name="day",
        day_dirs_glob="*",
        day_dir_re=DAY_DIR_RE,
    ),
    "month-day": Layout(
        name="month-day",
        day_dirs_glob="*/*",
        day_dir_re=DAY_DIR_RE,  # match on the inner basename
    ),
}


def list_day_dirs(root: Path, layout: Layout) -> list[Path]:
    """Return all matching <YYYY-MM-DD> directories under root."""
    if not root.exists():
        return []
    out: list[Path] = []
    for candidate in root.glob(layout.day_dirs_glob):
        if not candidate.is_dir():
            continue
        if not layout.day_dir_re.match(candidate.name):
            continue
        if layout.name == "month-day" and not MONTH_DIR_RE.match(candidate.parent.name):
            continue
        out.append(candidate)
    out.sort(key=lambda p: p.name)
    return out


def iter_case_dirs(day_dir: Path) -> Iterable[Path]:
    """Yield case directories under a day folder."""
    if not day_dir.exists():
        return
    for case in day_dir.iterdir():
        if case.is_dir() and not case.name.startswith("_"):
            yield case


def iter_case_contents(
    root: Path, layout: Layout
) -> Iterator[tuple[Path | None, list[os.DirEntry]]]:
    """Yield ``(register_dir_entry, doc_dir_entries)`` per case directory.

    Single pass per case dir using ``os.scandir`` — avoids the prior
    double-glob pattern (registers + docs separately) and yields the
    raw DirEntry objects so callers can stat() without an extra syscall.
    Either tuple element may be empty: a case dir without a register
    still yields its docs, and vice versa.
    """
    for day_dir in list_day_dirs(root, layout):
        for case in iter_case_dirs(day_dir):
            register: Path | None = None
            docs: list[os.DirEntry] = []
            try:
                entries = list(os.scandir(case))
            except OSError:
                continue
            for entry in entries:
                name = entry.name
                if name == REGISTER_FILENAME:
                    register = Path(entry.path)
                elif name.endswith(DOC_SUFFIXES):
                    docs.append(entry)
            yield register, docs
