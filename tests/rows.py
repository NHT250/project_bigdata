"""Dựng record bronze/silver giả cho test Spark."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

T0 = datetime(2026, 11, 4, 10, 0, 0, tzinfo=timezone.utc)

_counter = [1000]


def bronze(**over) -> dict:
    _counter[0] += 1
    rev = over.pop("rev_id", _counter[0])
    event_time = over.pop("event_time", T0)
    rec = {
        "event_id": f"ev-{rev}",
        "wiki": "enwiki",
        "page_id": 42,
        "page_title": "Example page",
        "namespace": 0,
        "rev_id": rev,
        "rev_parent_id": rev - 1,
        "user_hash": "u-default",
        "is_anon": False,
        "is_minor": False,
        "is_bot": False,
        "comment": "copyedit",
        "byte_old": 1000,
        "byte_new": 1010,
        "tags": [],
        "server_name": "en.wikipedia.org",
        "event_time": event_time,
        "ingest_time": event_time + timedelta(seconds=2),
        "clock_skew": False,
        "raw_json": "{}",
    }
    rec.update(over)
    return rec


def with_diff(rec: dict, added: str = "", removed: str = "", status: str = "ok") -> dict:
    out = dict(rec)
    out.update(diff_added=added, diff_removed=removed, diff_status=status)
    return out


def at(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)
