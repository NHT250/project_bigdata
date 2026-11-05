"""Sinh nhãn từ dấu hiệu revert — không gán tay (spec §6.1).

Một edit được gắn ``label = 1`` nếu trong ``cutoff_minutes`` sau nó, trên CÙNG
trang, có một edit khác gỡ nó đi:

| label_source   | dấu hiệu                                                      |
|----------------|---------------------------------------------------------------|
| tag_rollback   | edit sau có tag ``mw-rollback``                               |
| tag_undo       | edit sau có tag ``mw-undo`` và tóm tắt "Undid revision <ta>"  |
| comment_match  | tóm tắt edit sau khớp "Reverted edits by [[Special:Contributions/" |
| survived       | không có gì sau ``cutoff_minutes`` → ``label = 0``            |

Rollback và "Reverted edits by" gỡ cả CHUỖI edit liên tiếp của cùng một người
cuối cùng; vì vậy edit của ta bị coi là bị gỡ khi nó thuộc cùng chuỗi
(``run_id``) với edit cha của edit gỡ.

EventStreams không cho biết undo trỏ về revision nào; MediaWiki ghi điều đó
vào tóm tắt mặc định "Undid revision 123 by ...", nên ta đọc từ comment.

Giới hạn khai báo chủ động: "bị revert" ≠ "phá hoại". Model học "edit có khả
năng bị cộng đồng gỡ bỏ".
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

ROLLBACK_TAG = "mw-rollback"
UNDO_TAG = "mw-undo"
UNDO_RE = r"(?i)Undid revision (\d+)"
REVERTED_RE = r"(?i)Reverted (\d+ )?edits? by \[\[Special:Contributions/"

SOURCE_ROLLBACK = "tag_rollback"
SOURCE_UNDO = "tag_undo"
SOURCE_COMMENT = "comment_match"
SOURCE_SURVIVED = "survived"


def _with_run_id(df: DataFrame) -> DataFrame:
    """Đánh số chuỗi edit liên tiếp của cùng một người trên mỗi trang."""
    order = Window.partitionBy("wiki", "page_title").orderBy("event_time", "rev_id")
    changed = (
        F.lag("user_hash").over(order).isNull()
        | (F.lag("user_hash").over(order) != F.col("user_hash"))
        | F.col("user_hash").isNull()
    ).cast("int")
    cumulative = order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return df.withColumn("_changed", changed).withColumn(
        "run_id", F.sum("_changed").over(cumulative)
    ).drop("_changed")


def label_edits(
    spark: SparkSession, silver_df: DataFrame, cutoff_minutes: int, now: datetime
) -> DataFrame:
    """Gắn nhãn cho mọi edit đã đủ ``cutoff_minutes`` tuổi tính tới ``now``.

    Edit trẻ hơn mốc bị loại hẳn — chưa đủ thời gian để biết nó có bị gỡ không.
    """
    del spark
    cutoff_s = cutoff_minutes * 60
    s = _with_run_id(silver_df)

    parents = s.select(
        F.col("wiki").alias("p_wiki"),
        F.col("rev_id").alias("p_rev"),
        F.col("run_id").alias("p_run"),
    )
    tags = F.coalesce(F.col("tags"), F.array().cast("array<string>"))
    comment = F.coalesce(F.col("comment"), F.lit(""))
    reverts = (
        s.select(
            "wiki",
            "page_title",
            F.col("event_time").alias("l_time"),
            F.col("rev_parent_id").alias("l_parent"),
            F.array_contains(tags, ROLLBACK_TAG).alias("is_rb"),
            F.array_contains(tags, UNDO_TAG).alias("is_undo"),
            comment.rlike(REVERTED_RE).alias("is_cm"),
            F.when(comment.rlike(UNDO_RE), F.regexp_extract(comment, UNDO_RE, 1).cast("long"))
            .alias("undo_of"),
        )
        .filter("is_rb OR is_undo OR is_cm")
        .join(
            parents,
            (F.col("wiki") == F.col("p_wiki")) & (F.col("l_parent") == F.col("p_rev")),
            "left",
        )
        .drop("p_wiki", "p_rev")
    )

    age_ok = F.unix_timestamp(F.lit(now).cast("timestamp")) - F.unix_timestamp("event_time")
    targets = s.filter(age_ok >= cutoff_s)

    t = targets.select(
        F.col("wiki").alias("t_wiki"),
        F.col("page_title").alias("t_page"),
        F.col("rev_id").alias("t_rev"),
        F.col("run_id").alias("t_run"),
        F.col("event_time").alias("t_time"),
    )
    pairs = t.join(
        reverts,
        (F.col("t_wiki") == F.col("wiki"))
        & (F.col("t_page") == F.col("page_title"))
        & (F.col("l_time") > F.col("t_time"))
        & (F.unix_timestamp("l_time") - F.unix_timestamp("t_time") <= cutoff_s),
    )
    hits_chain = (F.col("l_parent") == F.col("t_rev")) | (F.col("p_run") == F.col("t_run"))
    undo_hit = F.col("is_undo") & F.coalesce(
        F.col("undo_of") == F.col("t_rev"),
        F.col("undo_of").isNull() & (F.col("l_parent") == F.col("t_rev")),
    )
    flags = pairs.groupBy("t_wiki", "t_rev").agg(
        F.max((F.col("is_rb") & hits_chain).cast("int")).alias("rb"),
        F.max(undo_hit.cast("int")).alias("undo"),
        F.max((F.col("is_cm") & hits_chain).cast("int")).alias("cm"),
    )

    out = targets.drop("run_id").join(
        flags,
        (F.col("wiki") == F.col("t_wiki")) & (F.col("rev_id") == F.col("t_rev")),
        "left",
    )
    source = (
        F.when(F.col("rb") == 1, F.lit(SOURCE_ROLLBACK))
        .when(F.col("undo") == 1, F.lit(SOURCE_UNDO))
        .when(F.col("cm") == 1, F.lit(SOURCE_COMMENT))
        .otherwise(F.lit(SOURCE_SURVIVED))
    )
    return (
        out.withColumn("label_source", source)
        .withColumn("label", (F.col("label_source") != SOURCE_SURVIVED).cast("int"))
        .withColumn("labeled_at", F.lit(now).cast("timestamp"))
        .drop("t_wiki", "t_rev", "rb", "undo", "cm")
    )
