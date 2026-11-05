"""Chấm ``risk_score`` bằng model nhóm tự huấn luyện (spec §6.1).

``FEATURE_COLS`` là nguồn sự thật duy nhất cho cả lúc train và lúc chấm: lệch
danh sách giữa hai nơi là lỗi im lặng kinh điển (training/serving skew).
Chỉ gồm thông tin biết được TẠI thời điểm edit.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FEATURE_COLS: tuple[str, ...] = (
    "byte_delta",
    "abs_byte_delta",
    "is_anon",
    "is_minor",
    "comment_len",
    "comment_empty",
    "hour_of_day",
    "day_of_week",
    "upper_ratio",
    "repeat_char",
    "has_url_added",
    "words_added",
    "words_removed",
    "page_edits_prev_1h",
    "user_edits_prev_1h",
)

MODEL_DIR = "models"
CURRENT_VERSION_FILE = "CURRENT_VERSION"


def prepare_features(df: DataFrame, cols=FEATURE_COLS) -> DataFrame:
    """Ép mọi feature về DOUBLE, null → 0 (GBT không nhận NaN)."""
    for c in cols:
        df = df.withColumn(f"f_{c}", F.coalesce(F.col(c).cast("double"), F.lit(0.0)))
    return df


def feature_input_cols(cols=FEATURE_COLS) -> list[str]:
    return [f"f_{c}" for c in cols]


def score(df: DataFrame, model, model_version: str | None) -> DataFrame:
    """Thêm ``risk_score`` (xác suất lớp 1) và ``model_version``.

    Không có model (lần chạy đầu, trước khi job batch train xong) thì
    ``risk_score = null`` — website hiển thị "chưa có model", không bịa số.
    """
    if model is None:
        return df.withColumn("risk_score", F.lit(None).cast("double")).withColumn(
            "model_version", F.lit(None).cast("string")
        )
    from pyspark.ml.functions import vector_to_array

    original = df.columns
    prepared = prepare_features(df)
    scored = model.transform(prepared)
    scored = scored.withColumn(
        "risk_score", vector_to_array(F.col("probability"))[1].cast("double")
    ).withColumn("model_version", F.lit(model_version).cast("string"))
    keep = [c for c in original if c not in ("risk_score", "model_version")]
    return scored.select(*keep, "risk_score", "model_version")


def load_current(delta_root: str):
    """Nạp model mới nhất do job batch ghi. Trả ``(model, version)``.

    Chưa có model (hoặc nạp lỗi) thì trả ``(None, None)``: job streaming vẫn
    chạy, chỉ là ``risk_score = null``.
    """
    from pyspark.ml import PipelineModel

    pointer = os.path.join(delta_root, MODEL_DIR, CURRENT_VERSION_FILE)
    if not os.path.exists(pointer):
        return None, None
    with open(pointer, encoding="utf-8") as fh:
        version = fh.read().strip()
    try:
        return PipelineModel.load(os.path.join(delta_root, MODEL_DIR, version)), version
    except Exception:
        return None, None


load_latest_model = load_current
