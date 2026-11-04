"""Hợp lệ hoá sự kiện EventStreams và lược đồ tầng bronze.

Spark schema (``bronze_struct``) được xây trễ, bên trong hàm, để module này
import được mà không cần pyspark — tầng ingest và test không dùng Spark.
"""

from __future__ import annotations

import ipaddress
import json
from datetime import datetime, timedelta, timezone
from typing import Any, MutableMapping

from src.common.hashing import hash_user

BRONZE_FIELDS: tuple[str, ...] = (
    "event_id",
    "wiki",
    "page_id",
    "page_title",
    "namespace",
    "rev_id",
    "rev_parent_id",
    "user_hash",
    "is_anon",
    "is_minor",
    "is_bot",
    "comment",
    "byte_old",
    "byte_new",
    "tags",
    "server_name",
    "event_time",
    "ingest_time",
    "clock_skew",
    "raw_json",
)

SILVER_EXTRA_FIELDS: tuple[str, ...] = (
    "diff_added",
    "diff_removed",
    "diff_status",
    "byte_delta",
    "abs_byte_delta",
    "comment_len",
    "comment_empty",
    "has_url_added",
    "upper_ratio",
    "repeat_char",
    "words_added",
    "words_removed",
    "hour_of_day",
    "day_of_week",
    "page_edits_prev_1h",
    "user_edits_prev_1h",
    "risk_score",
    "model_version",
    "summary",
)

REJECT_NOT_EDIT = "not_edit"
REJECT_MISSING_REVISION = "missing_revision"
REJECT_MISSING_LENGTH = "missing_length"

CLOCK_SKEW_TOLERANCE = timedelta(minutes=1)
"""Event_time vượt ingest_time quá mức này bị kẹp lại.

Vì sao điều này quan trọng: Spark lấy watermark từ event_time lớn nhất đã thấy.
Một event có event_time ở tương lai sẽ đẩy watermark lên và làm Spark BỎ mọi
record thật đến sau đó. Xem Review Focus #2.
"""

_PII_KEYS = ("user", "user_text")


def is_anonymous(ev: MutableMapping[str, Any]) -> bool:
    """Edit vô danh dùng địa chỉ IP làm tên tác giả."""
    if ev.get("user_is_anon"):
        return True
    user = ev.get("user")
    if not user:
        return False
    try:
        ipaddress.ip_address(str(user))
    except ValueError:
        return False
    return True


def _strip_pii(ev: MutableMapping[str, Any]) -> dict[str, Any]:
    cleaned = {k: v for k, v in ev.items() if k not in _PII_KEYS}
    return cleaned


def _parse_event_time(ev: MutableMapping[str, Any]) -> datetime | None:
    ts = ev.get("timestamp")
    if ts is not None:
        try:
            return datetime.fromtimestamp(float(ts), tz=timezone.utc)
        except (TypeError, ValueError, OSError, OverflowError):
            return None
    dt = (ev.get("meta") or {}).get("dt")
    if dt:
        try:
            return datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def to_record(
    ev: MutableMapping[str, Any],
    salt: str,
    now: datetime,
    rejects: MutableMapping[str, int] | None = None,
) -> dict[str, Any] | None:
    """Chuyển một sự kiện EventStreams thành record bronze.

    Trả ``None`` khi sự kiện không dùng được, và tăng bộ đếm ``rejects`` theo
    lý do. Một event bẩn không được làm chết cả batch (Review Focus #5).
    """

    def reject(reason: str) -> None:
        if rejects is not None:
            rejects[reason] = rejects.get(reason, 0) + 1

    if ev.get("type") != "edit":
        reject(REJECT_NOT_EDIT)
        return None

    revision = ev.get("revision")
    if not isinstance(revision, dict) or revision.get("new") is None:
        reject(REJECT_MISSING_REVISION)
        return None

    length = ev.get("length")
    if not isinstance(length, dict) or length.get("new") is None:
        reject(REJECT_MISSING_LENGTH)
        return None

    event_time = _parse_event_time(ev)
    if event_time is None:
        reject(REJECT_MISSING_REVISION)
        return None

    clock_skew = False
    if event_time > now + CLOCK_SKEW_TOLERANCE:
        event_time = now
        clock_skew = True

    return {
        "event_id": (ev.get("meta") or {}).get("id"),
        "wiki": ev.get("wiki"),
        "page_id": ev.get("id"),
        "page_title": ev.get("title"),
        "namespace": ev.get("namespace"),
        "rev_id": revision.get("new"),
        "rev_parent_id": revision.get("old"),
        "user_hash": hash_user(ev.get("user"), salt),
        "is_anon": is_anonymous(ev),
        "is_minor": bool(ev.get("minor", False)),
        "is_bot": bool(ev.get("bot", False)),
        "comment": ev.get("comment"),
        "byte_old": length.get("old"),
        "byte_new": length.get("new"),
        "tags": list(ev.get("tags") or []),
        "server_name": ev.get("server_name"),
        "event_time": event_time,
        "ingest_time": now,
        "clock_skew": clock_skew,
        "raw_json": json.dumps(_strip_pii(ev), ensure_ascii=False, sort_keys=True),
    }


def bronze_struct():  # pragma: no cover - cần pyspark, chạy trên VM
    """Trả ``StructType`` cho tầng bronze. Import pyspark trễ."""
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("event_id", StringType()),
            StructField("wiki", StringType()),
            StructField("page_id", LongType()),
            StructField("page_title", StringType()),
            StructField("namespace", IntegerType()),
            StructField("rev_id", LongType()),
            StructField("rev_parent_id", LongType()),
            StructField("user_hash", StringType()),
            StructField("is_anon", BooleanType()),
            StructField("is_minor", BooleanType()),
            StructField("is_bot", BooleanType()),
            StructField("comment", StringType()),
            StructField("byte_old", IntegerType()),
            StructField("byte_new", IntegerType()),
            StructField("tags", ArrayType(StringType())),
            StructField("server_name", StringType()),
            StructField("event_time", TimestampType()),
            StructField("ingest_time", TimestampType()),
            StructField("clock_skew", BooleanType()),
            StructField("raw_json", StringType()),
        ]
    )
