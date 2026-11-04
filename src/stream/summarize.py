"""Tóm tắt nội dung thay đổi.

Tính năng AI thứ ba của đồ án — và nhóm **SỬ DỤNG**, không huấn luyện
(spec §6.3). Khai báo đúng điều này là một tiêu chí đánh giá, không phải lời
thú nhận: huấn luyện một mô hình tóm tắt từ đầu trong 8 ngày là không thể, và
máy Oracle ARM không có GPU.

Mặc định là khuôn mẫu. Chế độ LLM bật được qua ``SUMMARY_MODE=llm`` nhưng luôn
quay về khuôn mẫu khi lỗi: bản demo không bao giờ được phụ thuộc vào quota.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, MutableMapping

MAX_SUMMARY_ROWS = 50
MAX_SUMMARY_CHARS = 180

MODE_TEMPLATE = "template"
MODE_LLM = "llm"


@dataclass(frozen=True)
class Row:
    rev_id: int
    added: str
    removed: str
    byte_delta: int


def _count_words(text: str) -> int:
    return len(text.split()) if text else 0


def _format_bytes(byte_delta: int) -> str:
    magnitude = abs(byte_delta)
    sign = "-" if byte_delta < 0 else "+"
    if magnitude >= 1024:
        return f"{sign}{magnitude / 1024:.1f} KB"
    return f"{sign}{magnitude} byte"


def template_summary(added: str, removed: str, byte_delta: int) -> str:
    """Một câu tiếng Việt mô tả thay đổi, luôn trả về chuỗi không rỗng."""
    added_words = _count_words(added)
    removed_words = _count_words(removed)

    parts: list[str] = []
    if added_words:
        parts.append(f"Thêm {added_words} từ")
    if removed_words:
        parts.append(f"{'xoá' if parts else 'Xoá'} {removed_words} từ")

    if not parts:
        if byte_delta:
            return f"Thay đổi nhỏ không có nội dung văn bản ({_format_bytes(byte_delta)})"
        return "Thay đổi không có nội dung văn bản"

    summary = ", ".join(parts) + f" ({_format_bytes(byte_delta)})"
    summary = " ".join(summary.split())
    return summary[:MAX_SUMMARY_CHARS]


def summarize_hot(
    rows: Iterable[Row],
    mode: str = MODE_TEMPLATE,
    llm: Callable[[str, str], str] | None = None,
    cache: MutableMapping[int, str] | None = None,
    max_rows: int = MAX_SUMMARY_ROWS,
) -> dict[int, str]:
    """Tóm tắt các dòng thuộc trang đang nóng, trả ``{rev_id: summary}``.

    Chỉ tóm tắt tối đa ``max_rows`` dòng: chi phí được kiểm soát bằng cách chỉ
    chạy trên trang đang nóng, không phải mọi edit.
    """
    store = cache if cache is not None else {}
    out: dict[int, str] = {}

    for row in rows:
        if len(out) >= max_rows:
            break
        if row.rev_id in out:
            continue
        if row.rev_id in store:
            out[row.rev_id] = store[row.rev_id]
            continue

        summary = ""
        if mode == MODE_LLM and llm is not None:
            try:
                candidate = llm(row.added, row.removed)
                if candidate and str(candidate).strip():
                    summary = str(candidate).strip()[:MAX_SUMMARY_CHARS]
            except Exception:
                summary = ""  # hết quota, lỗi mạng, phản hồi hỏng -> khuôn mẫu

        if not summary:
            summary = template_summary(row.added, row.removed, row.byte_delta)

        store[row.rev_id] = summary
        out[row.rev_id] = summary

    return out
