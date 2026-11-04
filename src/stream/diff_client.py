"""Lấy nội dung thay đổi (diff) từ MediaWiki API.

Đây là tầng "cào nội dung" thật của hệ thống: EventStreams chỉ cho metadata,
muốn biết đã sửa cái gì thì phải gọi ``action=compare``.

Ràng buộc bắt buộc (spec §2):
- User-Agent khai báo thật, không giả dạng trình duyệt
- Tối đa 10 req/s, timeout 3 giây
- Không proxy xoay vòng
- Dùng API chính thức, không parse HTML của trang bài viết

Mọi lỗi trả về dưới dạng ``status``, không raise: một timeout của Wikimedia
không được làm nghẽn cả batch streaming.
"""

from __future__ import annotations

import html
import json
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

STATUS_OK = "ok"
STATUS_NEW_PAGE = "new_page"
STATUS_TRUNCATED = "truncated"
STATUS_TIMEOUT = "timeout"
STATUS_ERROR = "error"
STATUS_SKIPPED = "skipped"

DEFAULT_API = "https://en.wikipedia.org/w/api.php"
DEFAULT_TIMEOUT = 3.0
DEFAULT_MAX_CHARS = 10_000

_ADDED_RE = re.compile(
    r'<td[^>]*class="[^"]*diff-addedline[^"]*"[^>]*>(.*?)</td>', re.S
)
_REMOVED_RE = re.compile(
    r'<td[^>]*class="[^"]*diff-deletedline[^"]*"[^>]*>(.*?)</td>', re.S
)
_TAG_RE = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class DiffResult:
    added: str
    removed: str
    status: str


class RateLimiter:
    """Giới hạn số lần gọi mỗi giây, an toàn khi dùng nhiều luồng.

    Giữ dưới ngưỡng của Wikimedia là một yêu cầu đạo đức của đồ án, không chỉ
    là tối ưu hoá — xem spec §2 mục 3.
    """

    def __init__(self, max_rps: int, monotonic=time.monotonic, sleep=time.sleep):
        if max_rps <= 0:
            raise ValueError("max_rps phải lớn hơn 0")
        self._interval = 1.0 / max_rps
        self._monotonic = monotonic
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next_at = None

    def acquire(self) -> None:
        with self._lock:
            now = self._monotonic()
            if self._next_at is None or now >= self._next_at:
                self._next_at = now + self._interval
                return
            wait = self._next_at - now
            self._next_at += self._interval
        self._sleep(wait)


def _strip(fragment: str) -> str:
    return html.unescape(_TAG_RE.sub("", fragment)).strip()


def parse_compare_html(markup: str) -> tuple[str, str]:
    """Rút phần được thêm và phần bị xoá khỏi HTML diff của MediaWiki."""
    added = "\n".join(
        s for s in (_strip(m) for m in _ADDED_RE.findall(markup)) if s
    )
    removed = "\n".join(
        s for s in (_strip(m) for m in _REMOVED_RE.findall(markup)) if s
    )
    return added, removed


def _default_http(url: str, headers: dict[str, str], timeout: float) -> str:
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def _truncate(text: str, max_chars: int) -> tuple[str, bool]:
    if len(text) <= max_chars:
        return text, False
    return text[:max_chars], True


def fetch_diff(
    rev_id: int,
    parent_id: int | None,
    user_agent: str,
    http=_default_http,
    api_url: str = DEFAULT_API,
    timeout: float = DEFAULT_TIMEOUT,
    max_chars: int = DEFAULT_MAX_CHARS,
    page_title: str | None = None,
) -> DiffResult:
    """Lấy diff của một revision.

    ``parent_id is None`` nghĩa là trang mới tạo: không có gì để so sánh, nên
    lấy nội dung của chính revision đó (Review Focus #1).
    """
    new_page = parent_id is None

    if new_page:
        params = {
            "action": "query",
            "prop": "revisions",
            "revids": str(rev_id),
            "rvprop": "content",
            "rvslots": "main",
            "formatversion": "2",
            "format": "json",
        }
        if page_title:
            params["titles"] = page_title
    else:
        # action=compare nhận fromtitle/totitle, KHÔNG nhận titles. Gửi titles
        # vào đây làm MediaWiki trả lỗi và mọi diff thất bại.
        params = {
            "action": "compare",
            "fromrev": str(parent_id),
            "torev": str(rev_id),
            "format": "json",
        }

    url = f"{api_url}?{urllib.parse.urlencode(params, quote_via=urllib.parse.quote)}"
    headers = {"User-Agent": user_agent, "Accept": "application/json"}

    try:
        body = http(url, headers, timeout)
    except TimeoutError:
        return DiffResult("", "", STATUS_TIMEOUT)
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", None)
        if isinstance(reason, TimeoutError):
            return DiffResult("", "", STATUS_TIMEOUT)
        return DiffResult("", "", STATUS_ERROR)
    except (OSError, ValueError):
        return DiffResult("", "", STATUS_ERROR)

    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, TypeError, ValueError):
        return DiffResult("", "", STATUS_ERROR)

    if not isinstance(payload, dict) or "error" in payload:
        return DiffResult("", "", STATUS_ERROR)

    if new_page:
        try:
            pages = payload["query"]["pages"]
            content = pages[0]["revisions"][0]["slots"]["main"]["content"]
        except (KeyError, IndexError, TypeError):
            return DiffResult("", "", STATUS_ERROR)
        added, cut = _truncate(str(content), max_chars)
        return DiffResult(added, "", STATUS_TRUNCATED if cut else STATUS_NEW_PAGE)

    markup = (payload.get("compare") or {}).get("*")
    if markup is None:
        return DiffResult("", "", STATUS_ERROR)

    added, removed = parse_compare_html(str(markup))
    added, cut_a = _truncate(added, max_chars)
    removed, cut_r = _truncate(removed, max_chars)
    status = STATUS_TRUNCATED if (cut_a or cut_r) else STATUS_OK
    return DiffResult(added, removed, status)
