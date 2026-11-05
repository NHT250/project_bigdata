"""Phát hiện sự kiện nóng (spec §6.2). Thống kê cửa sổ trượt, KHÔNG phải AI.

Cửa sổ 5 phút, bước 1 phút, theo ``(wiki, page_title)``:

    z = (edit_count − ewma) / std,   nóng khi z > 3 VÀ distinct_users ≥ 3

``ewma`` và ``std`` là trung bình/độ lệch chuẩn có trọng số mũ của số edit mỗi
cửa sổ trong 24 giờ QUÁ KHỨ của chính trang đó. Phút trang không có edit nào
được tính là 0 (qua mẫu số dạng cấp số nhân), nên trang yên ắng có ewma thấp
thật sự. Chỉ xét cửa sổ kết thúc trước ít nhất 5 phút: cửa sổ chồng lấn với
cửa sổ hiện tại chứa chính các edit đang bùng nổ và sẽ kéo đường nền lên.

Khởi động lạnh: trang có dưới 30 phút lịch sử dùng đường nền toàn cục (trung bình số
edit của mọi cửa sổ trong dữ liệu đầu vào).

``distinct_users ≥ 3`` là bắt buộc: thiếu nó, hai người sửa qua sửa lại
(edit war) bị báo là sự kiện nóng toàn cầu.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

WINDOW = "5 minutes"
SLIDE = "1 minute"
SLIDE_SECONDS = 60
HISTORY_SECONDS = 24 * 3600
MIN_LAG_SLOTS = 5  # bỏ các cửa sổ chồng lấn với cửa sổ hiện tại
MIN_HISTORY_SLOTS = 30
"""Cần ít nhất 30 phút lịch sử mới tin đường nền của riêng trang.

Vài cửa sổ đầu tiên của dữ liệu là cửa sổ "dở dang" (chỉ phủ một phần 5 phút)
nên số edit thấp giả tạo; đường nền dựng từ 2–3 cửa sổ như vậy báo nhầm mọi
trang đông đúc là đang nóng. Trước mốc này dùng đường nền toàn cục.
"""
EWMA_TAU_SECONDS = 4 * 3600
STD_FLOOR = 1.0

_R = float(__import__("math").exp(-SLIDE_SECONDS / EWMA_TAU_SECONDS))


def _windows(df: DataFrame) -> DataFrame:
    has_bytes = "abs_byte_delta" in df.columns
    byte_expr = (
        F.sum("abs_byte_delta") if has_bytes else F.lit(None).cast("long")
    ).alias("byte_delta_sum")
    return (
        df.groupBy("wiki", "page_title", F.window("event_time", WINDOW, SLIDE).alias("w"))
        .agg(
            F.count(F.lit(1)).cast("int").alias("edit_count"),
            F.countDistinct("user_hash").cast("int").alias("distinct_users"),
            byte_expr,
        )
        .withColumn("window_end", F.col("w.end"))
        .drop("w")
        .withColumn("t", F.unix_timestamp("window_end"))
    )


def detect(df: DataFrame, z_threshold: float = 3.0, min_users: int = 3) -> DataFrame:
    """Trả mọi cửa sổ kèm ``ewma``, ``z_score``, ``is_hot``.

    ``df`` cần ``wiki``, ``page_title``, ``user_hash``, ``event_time``; nên chứa
    24 giờ gần nhất để đường nền có ý nghĩa.
    """
    w = _windows(df)

    everything = Window.partitionBy()
    w = (
        w.withColumn("data_start", F.min("t").over(everything))
        .withColumn("global_mean", F.avg("edit_count").over(everything))
        .withColumn("global_std", F.stddev_pop("edit_count").over(everything))
    )

    past = (
        Window.partitionBy("wiki", "page_title")
        .orderBy("t")
        .rangeBetween(-HISTORY_SECONDS, -MIN_LAG_SLOTS * SLIDE_SECONDS)
    )
    w = w.withColumn("hist", F.collect_list(F.struct("t", "edit_count")).over(past))

    # Số slot 1 phút trong lịch sử: từ MIN_LAG_SLOTS tới k_max (tính cả phút = 0).
    k_max = F.least(
        F.lit(HISTORY_SECONDS // SLIDE_SECONDS),
        F.floor((F.col("t") - F.col("data_start")) / SLIDE_SECONDS),
    )
    r = F.lit(_R)
    a = MIN_LAG_SLOTS
    # Σ_{k=a..k_max} r^k = r^a (1 − r^(k_max − a + 1)) / (1 − r)
    denom = F.when(
        k_max >= a,
        F.pow(r, a) * (1 - F.pow(r, k_max - a + 1)) / (1 - r),
    )

    weighted = F.expr(
        f"""aggregate(hist, named_struct('s1', 0D, 's2', 0D),
              (acc, x) -> named_struct(
                 's1', acc.s1 + x.edit_count * power({_R}D, (t - x.t) / {SLIDE_SECONDS}),
                 's2', acc.s2 + x.edit_count * x.edit_count
                              * power({_R}D, (t - x.t) / {SLIDE_SECONDS})))"""
    )
    w = w.withColumn("acc", weighted).withColumn("denom", denom)

    has_history = (
        (F.size("hist") > 0)
        & F.col("denom").isNotNull()
        & (k_max >= MIN_HISTORY_SLOTS)
    )
    page_mean = F.col("acc.s1") / F.col("denom")
    page_var = F.col("acc.s2") / F.col("denom") - page_mean * page_mean
    ewma = F.when(has_history, page_mean).otherwise(F.col("global_mean"))
    std = F.when(
        has_history, F.sqrt(F.greatest(page_var, F.lit(0.0)))
    ).otherwise(F.coalesce(F.col("global_std"), F.lit(0.0)))

    out = (
        w.withColumn("ewma", ewma.cast("double"))
        .withColumn("std", F.greatest(std, F.lit(STD_FLOOR)).cast("double"))
        .withColumn("z_score", ((F.col("edit_count") - F.col("ewma")) / F.col("std")).cast("double"))
        .withColumn(
            "is_hot",
            (F.col("z_score") > z_threshold) & (F.col("distinct_users") >= min_users),
        )
    )
    return out.select(
        "wiki",
        "page_title",
        "window_end",
        "edit_count",
        "distinct_users",
        "byte_delta_sum",
        "ewma",
        "std",
        "z_score",
        "is_hot",
    )
