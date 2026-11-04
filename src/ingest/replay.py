"""Phát lại các file ``data/*.jsonl.gz`` của collector.py vào Kafka.

Vì sao cần: collector.py chạy được ngay, không cần Kafka hay Spark, nên việc
thu dữ liệu bắt đầu trước khi hạ tầng dựng xong. Module này nạp dữ liệu đã thu
vào pipeline sau đó.

Phụ thêm, phát lại chính là nguồn "dữ liệu đến muộn" thật để thử watermark:
mọi record có event_time trong quá khứ nhưng process_time là hiện tại
(bằng chứng #2, spec §8).

Record của collector KHÔNG được băm lại — nó đã băm ở tầng ingest rồi. Nhưng
collector không có cột ``clock_skew`` và không kẹp event_time ở tương lai, nên
việc kẹp xảy ra ở đây (ruling Task 2 trong ledger).
"""

from __future__ import annotations

import glob
import gzip
import json
import os
from datetime import datetime, timezone
from typing import Any

from src.common.schema import BRONZE_FIELDS, CLOCK_SKEW_TOLERANCE
from src.ingest.main import kafka_key

REQUIRED_KEYS = ("wiki", "rev_id", "page_title", "event_time")


def replay_record(record: dict[str, Any], now: datetime) -> dict[str, Any]:
    """Đưa một record của collector về đúng lược đồ bronze.

    Đổi ``raw`` thành ``raw_json`` và thêm ``clock_skew``.
    """
    out = dict(record)

    raw = out.pop("raw", None)
    if "raw_json" not in out:
        out["raw_json"] = json.dumps(raw or {}, ensure_ascii=False, sort_keys=True)

    skew = False
    event_time_raw = out.get("event_time")
    if isinstance(event_time_raw, str):
        try:
            event_time = datetime.fromisoformat(event_time_raw)
        except ValueError:
            event_time = None
        if event_time is not None:
            if event_time.tzinfo is None:
                event_time = event_time.replace(tzinfo=timezone.utc)
            if event_time > now + CLOCK_SKEW_TOLERANCE:
                out["event_time"] = now.isoformat()
                skew = True
    out["clock_skew"] = skew

    for field in BRONZE_FIELDS:
        out.setdefault(field, None)
    return {k: out[k] for k in BRONZE_FIELDS}


def replay_file(
    path: str,
    producer,
    topic: str,
    now: datetime | None = None,
) -> int:
    """Phát lại một file ``.jsonl.gz``. Trả số message đã gửi.

    Dòng cuối bị cắt dở (máy sập khi collector đang ghi) hoặc record thiếu
    trường bắt buộc bị bỏ qua, không làm dừng cả lần phát lại.
    """
    moment = now or datetime.now(timezone.utc)
    sent = 0
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
        while True:
            try:
                line = fh.readline()
            except (EOFError, OSError, gzip.BadGzipFile):
                break  # member cuối chưa đóng trọn vẹn
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            if any(record.get(key) is None for key in REQUIRED_KEYS):
                continue
            bronze = replay_record(record, moment)
            producer.send(
                topic,
                key=kafka_key(bronze["page_title"]),
                value=json.dumps(bronze, ensure_ascii=False, sort_keys=True).encode(
                    "utf-8"
                ),
            )
            sent += 1
    producer.flush()
    return sent


def replay_dir(data_dir: str, producer, topic: str) -> int:  # pragma: no cover
    total = 0
    for path in sorted(glob.glob(os.path.join(data_dir, "*.jsonl.gz"))):
        count = replay_file(path, producer, topic)
        print(f"{path}: {count} messages")
        total += count
    return total


def main() -> int:  # pragma: no cover - cần Kafka thật
    from kafka import KafkaProducer

    from src.common.config import Settings

    settings = Settings.load()
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap.split(","),
        linger_ms=200,
        acks="all",
    )
    total = replay_dir(settings.data_dir, producer, settings.kafka_topic)
    print(f"total={total}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
