"""Tầng ingest: EventStreams -> Kafka.

Trách nhiệm duy nhất: giữ kết nối, lọc, băm danh tính, publish. Không parse
diff, không tính toán, không ghi Delta (spec §3.2).

``run()`` nhận producer và luồng sự kiện qua tham số nên test được mà không cần
Kafka; ``main()`` mới dựng KafkaProducer thật.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from src.common.config import Settings
from src.common.hashing import load_salt
from src.common.schema import to_record
from src.ingest.sse_client import stream

MAX_KEY_BYTES = 255
FALLBACK_KEY = b"__unknown__"
STATE_FILENAME = "last_event_id.txt"
FLUSH_EVERY = 200


@dataclass
class IngestStats:
    published: int = 0
    skipped: int = 0
    rejected: int = 0
    reject_reasons: dict[str, int] = field(default_factory=dict)


def kafka_key(page_title: str | None) -> bytes:
    """Key Kafka là tên trang.

    Vì sao tên trang chứ không phải wiki: burst detection cần mọi edit của cùng
    một trang vào cùng partition. Key theo wiki sẽ dồn toàn bộ dữ liệu vào một
    partition — data skew kinh điển (spec §3.3).

    Cắt ở 255 byte theo ranh giới ký tự, không cắt giữa một ký tự multibyte.
    """
    if not page_title:
        return FALLBACK_KEY
    encoded = page_title.encode("utf-8")
    if len(encoded) <= MAX_KEY_BYTES:
        return encoded
    truncated = encoded[:MAX_KEY_BYTES]
    while truncated:
        try:
            truncated.decode("utf-8")
            return truncated
        except UnicodeDecodeError:
            truncated = truncated[:-1]
    return FALLBACK_KEY


def _state_file(state_path: str) -> str:
    return os.path.join(state_path, STATE_FILENAME)


def save_last_event_id(state_path: str, event_id: str) -> None:
    os.makedirs(state_path, exist_ok=True)
    path = _state_file(state_path)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(event_id)
    os.replace(tmp, path)  # ghi nguyên tử: không bao giờ để lại file rỗng


def load_last_event_id(state_path: str) -> str | None:
    path = _state_file(state_path)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read().strip() or None


def _encode_value(record: dict[str, Any]) -> bytes:
    payload = dict(record)
    for key in ("event_time", "ingest_time"):
        value = payload.get(key)
        if isinstance(value, datetime):
            payload[key] = value.isoformat()
    return json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")


def run(
    settings: Settings,
    producer,
    events: Iterable[tuple[str | None, dict[str, Any]]],
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> IngestStats:
    """Lọc và publish từng sự kiện. Trả thống kê để caller ghi log."""
    salt = load_salt(settings.salt_path)
    stats = IngestStats()
    last_id: str | None = None

    for event_id, event in events:
        if event_id:
            last_id = str(event_id)

        if (
            event.get("wiki") not in settings.wikis
            or event.get("type") != "edit"
            or event.get("bot")
        ):
            stats.skipped += 1
            continue

        record = to_record(event, salt, now(), rejects=stats.reject_reasons)
        if record is None:
            stats.rejected += 1
            continue

        producer.send(
            settings.kafka_topic,
            key=kafka_key(record["page_title"]),
            value=_encode_value(record),
        )
        stats.published += 1

        if stats.published % FLUSH_EVERY == 0:
            producer.flush()
            if last_id:
                save_last_event_id(settings.state_path, last_id)

    producer.flush()
    if last_id:
        save_last_event_id(settings.state_path, last_id)
    return stats


def main() -> int:  # pragma: no cover - cần Kafka thật, chạy trên VM
    from kafka import KafkaProducer

    settings = Settings.load()
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap.split(","),
        linger_ms=200,
        acks="all",
    )
    events = stream(
        settings.stream_url,
        settings.user_agent,
        load_last_event_id(settings.state_path),
    )
    stats = run(settings, producer, events)
    print(f"published={stats.published} skipped={stats.skipped} rejected={stats.rejected}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
