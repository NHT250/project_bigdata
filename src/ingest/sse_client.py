"""Đọc Server-Sent Events từ Wikimedia EventStreams.

Chỉ dùng thư viện chuẩn, để tầng ingest chạy được ở bất kỳ đâu.

Về khả năng khôi phục: EventStreams gửi một ``id:`` theo mỗi sự kiện. Lưu id
cuối và gửi lại qua header ``Last-Event-ID`` khi kết nối lại thì Wikimedia phát
lại từ chỗ cũ, nên mất mạng không làm mất sự kiện (spec §2 mục 7).
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any, Iterable, Iterator

MAX_BACKOFF_SECONDS = 30
READ_TIMEOUT_SECONDS = 60


def parse_sse(
    raw_lines: Iterable[bytes],
) -> Iterator[tuple[str | None, dict[str, Any]]]:
    """Sinh ``(event_id, payload)`` cho mỗi sự kiện SSE hợp lệ.

    Payload không phải JSON object, hoặc JSON hỏng, bị bỏ qua chứ không raise:
    một sự kiện bẩn không được làm chết cả luồng.
    """
    event_id: str | None = None
    data_parts: list[str] = []

    def flush() -> Iterator[tuple[str | None, dict[str, Any]]]:
        nonlocal data_parts
        if not data_parts:
            return
        payload = "".join(data_parts)
        data_parts = []
        try:
            parsed = json.loads(payload)
        except (json.JSONDecodeError, ValueError):
            return
        if isinstance(parsed, dict):
            yield event_id, parsed

    for raw in raw_lines:
        line = raw.decode("utf-8", errors="replace").rstrip("\r\n")

        if line == "":
            yield from flush()
            continue
        if line.startswith(":"):
            continue

        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value

        if field == "id":
            event_id = value.strip() or None
        elif field == "data":
            data_parts.append(value)
        # 'event' và 'retry' không cần cho luồng này

    yield from flush()


def stream(
    url: str,
    user_agent: str,
    last_event_id: str | None = None,
    opener=urllib.request.urlopen,
    sleep=time.sleep,
    max_reconnects: int | None = None,
) -> Iterator[tuple[str | None, dict[str, Any]]]:
    """Giữ kết nối tới ``url``, tự kết nối lại kèm ``Last-Event-ID``.

    ``max_reconnects=None`` nghĩa là chạy mãi; test truyền số hữu hạn.
    """
    backoff = 1
    attempts = 0
    current_id = last_event_id

    while max_reconnects is None or attempts <= max_reconnects:
        attempts += 1
        request = urllib.request.Request(url)
        request.add_header("User-Agent", user_agent)
        request.add_header("Accept", "text/event-stream")
        if current_id:
            request.add_header("Last-Event-ID", current_id)
        try:
            response = opener(request, timeout=READ_TIMEOUT_SECONDS)
            backoff = 1
            try:
                for event_id, payload in parse_sse(response):
                    if event_id:
                        current_id = event_id
                    yield event_id, payload
            finally:
                close = getattr(response, "close", None)
                if close:
                    close()
        except (urllib.error.URLError, TimeoutError, OSError):
            sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
