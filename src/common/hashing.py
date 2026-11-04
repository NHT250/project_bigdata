"""Băm danh tính người sửa.

Edit vô danh trên Wikipedia dùng ĐỊA CHỈ IP làm tên tác giả, và IP là dữ liệu
cá nhân. Mọi danh tính bị băm ngay tại tầng ingest, trước khi ghi xuống bất kỳ
đâu. Xem spec §2 mục 4 và NIST Privacy Framework.
"""

from __future__ import annotations

import hashlib
import os
import secrets

HASH_LEN = 16
"""Số ký tự hex giữ lại. 16 hex = 64 bit, đủ chống trùng ở quy mô đồ án."""


def load_salt(path: str) -> str:
    """Nạp salt từ ``path``, sinh mới và ghi ra nếu chưa có.

    Salt PHẢI cố định giữa các lần chạy: nếu đổi, cùng một người sẽ cho hash
    khác nhau và các feature đếm hành vi (``user_edits_prev_1h``) sẽ sai.
    Không commit file này lên git.
    """
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            existing = fh.read().strip()
        if existing:
            return existing
    salt = secrets.token_hex(16)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(salt)
    os.replace(tmp, path)
    return salt


def hash_user(user: str | None, salt: str) -> str | None:
    """Băm tên người dùng hoặc IP. Trả ``None`` khi không có danh tính."""
    if user is None:
        return None
    digest = hashlib.sha256(f"{salt}|{user}".encode("utf-8")).hexdigest()
    return digest[:HASH_LEN]
