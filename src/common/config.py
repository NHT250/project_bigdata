"""Cấu hình toàn hệ thống, đọc từ biến môi trường ở một chỗ duy nhất."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

UA_TEMPLATE = (
    "WikiPulse/1.0 "
    "(Van Lang University course project; {contact})"
)
"""Wikimedia yêu cầu User-Agent khai báo thật kèm liên hệ.

Giả dạng UA trình duyệt là vi phạm chính sách và bị chặn — xem spec §2 mục 1.
"""

SUMMARY_MODES = ("template", "llm")

_FORBIDDEN_UA_TOKENS = ("Mozilla", "Chrome", "Safari", "AppleWebKit", "Edge/")


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(
            f"{name} chưa được đặt. Sao chép .env.example thành .env và điền vào."
        )
    return value


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} phải là số nguyên, nhận được {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    kafka_topic: str
    pg_dsn: str
    delta_root: str
    wiki_contact: str
    salt_path: str = "state/salt.txt"
    state_path: str = "state"
    data_dir: str = "data"
    checkpoint_root: str = "state/checkpoints"
    stream_url: str = "https://stream.wikimedia.org/v2/stream/recentchange"
    api_url: str = "https://en.wikipedia.org/w/api.php"
    wikis: frozenset[str] = field(default_factory=lambda: frozenset({"enwiki"}))
    kafka_partitions: int = 3
    diff_max_rps: int = 10
    diff_timeout_seconds: float = 3.0
    diff_max_chars: int = 10_000
    watermark_minutes: int = 10
    label_cutoff_minutes: int = 60
    late_recheck_hours: int = 24
    burst_z_threshold: float = 3.0
    burst_min_users: int = 3
    risk_queue_size: int = 50
    summary_mode: str = "template"

    @property
    def user_agent(self) -> str:
        ua = UA_TEMPLATE.format(contact=self.wiki_contact)
        for token in _FORBIDDEN_UA_TOKENS:
            if token in ua:
                raise ValueError(
                    "User-Agent không được giả dạng trình duyệt (spec §2 mục 1)"
                )
        return ua

    @classmethod
    def load(cls) -> "Settings":
        summary_mode = os.environ.get("SUMMARY_MODE", "template").strip() or "template"
        if summary_mode not in SUMMARY_MODES:
            raise ValueError(
                f"SUMMARY_MODE phải là một trong {SUMMARY_MODES}, "
                f"nhận được {summary_mode!r}"
            )
        wikis = os.environ.get("WIKIS", "enwiki").strip()
        return cls(
            kafka_bootstrap=_require("KAFKA_BOOTSTRAP"),
            kafka_topic=_require("KAFKA_TOPIC"),
            pg_dsn=_require("PG_DSN"),
            delta_root=_require("DELTA_ROOT"),
            wiki_contact=_require("WIKI_CONTACT"),
            salt_path=os.environ.get("SALT_PATH", "state/salt.txt"),
            state_path=os.environ.get("STATE_PATH", "state"),
            data_dir=os.environ.get("DATA_DIR", "data"),
            checkpoint_root=os.environ.get("CHECKPOINT_ROOT", "state/checkpoints"),
            wikis=frozenset(w.strip() for w in wikis.split(",") if w.strip()),
            kafka_partitions=_int_env("KAFKA_PARTITIONS", 3),
            diff_max_rps=_int_env("DIFF_MAX_RPS", 10),
            diff_max_chars=_int_env("DIFF_MAX_CHARS", 10_000),
            watermark_minutes=_int_env("WATERMARK_MINUTES", 10),
            label_cutoff_minutes=_int_env("LABEL_CUTOFF_MINUTES", 60),
            late_recheck_hours=_int_env("LATE_RECHECK_HOURS", 24),
            risk_queue_size=_int_env("RISK_QUEUE_SIZE", 50),
            summary_mode=summary_mode,
        )
