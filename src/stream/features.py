"""Feature cho model phát hiện phá hoại (spec §6.1).

Chỉ dùng thông tin biết được TẠI thời điểm edit. Mọi biểu thức là Spark SQL
thuần, không UDF Python: UDF buộc Spark serialize từng dòng sang tiến trình
Python và chậm hơn hàng chục lần.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

ONE_HOUR_SECONDS = 3600

_REPEAT_RE = r"(.)\1{6,}"
_URL_RE = r"(?i)(https?://|www\.)"


def _text(col: str):
    return F.coalesce(F.col(col), F.lit(""))


def _word_count(col: str):
    trimmed = F.trim(_text(col))
    return (
        F.when(F.length(trimmed) == 0, F.lit(0))
        .otherwise(F.size(F.split(trimmed, r"\s+")))
        .cast("int")
    )


def add_cheap_features(df: DataFrame) -> DataFrame:
    """Thêm các feature tính được từ chính dòng đó, không cần dòng khác.

    Trang mới tạo có ``byte_old = null``: coi như 0, không sinh ``None - int``
    (Review Focus #1). Diff lỗi/timeout có ``diff_added = null``: coi như rỗng.
    """
    added = _text("diff_added")
    letters = F.length(F.regexp_replace(added, r"[^\p{L}]", ""))
    uppers = F.length(F.regexp_replace(added, r"[^\p{Lu}]", ""))
    comment = F.trim(F.coalesce(F.col("comment"), F.lit("")))
    delta = (
        F.coalesce(F.col("byte_new"), F.lit(0)) - F.coalesce(F.col("byte_old"), F.lit(0))
    ).cast("int")

    return (
        df.withColumn("byte_delta", delta)
        .withColumn("abs_byte_delta", F.abs(F.col("byte_delta")).cast("int"))
        .withColumn("comment_len", F.length(comment).cast("int"))
        .withColumn("comment_empty", F.length(comment) == 0)
        .withColumn(
            "upper_ratio",
            F.when(letters == 0, F.lit(0.0)).otherwise(uppers / letters).cast("double"),
        )
        .withColumn("repeat_char", added.rlike(_REPEAT_RE))
        .withColumn("has_url_added", added.rlike(_URL_RE))
        .withColumn("words_added", _word_count("diff_added"))
        .withColumn("words_removed", _word_count("diff_removed"))
        .withColumn("hour_of_day", F.hour("event_time").cast("int"))
        .withColumn("day_of_week", F.dayofweek("event_time").cast("int"))
    )
