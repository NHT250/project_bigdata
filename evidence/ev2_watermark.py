"""Bằng chứng #2 — thực nghiệm watermark: 10 phút vs 2 giờ.

Xếp record theo thứ tự HỆ THỐNG NHẬN (``ingest_time``). Với mỗi record, đường
watermark là ``max(event_time đã thấy trước đó) − độ trễ``. Record có
``event_time`` nhỏ hơn watermark là record Spark sẽ BỎ trong phép tổng hợp
có trạng thái (cửa sổ phát hiện sự kiện nóng).

Xấp xỉ: Spark cập nhật watermark theo micro-batch, không theo từng dòng; mô
phỏng theo dòng là cận trên của số record bị bỏ.

    python -m evidence.ev2_watermark --delta-root /data/delta [--day 2026-11-09]
"""

from __future__ import annotations

import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from evidence import common

NAME = "ev2_watermark"
DELAYS = {"10min": 10 * 60, "2h": 2 * 3600}


def run(spark, df) -> dict:
    del spark
    order = Window.orderBy("ingest_time", "event_time", "rev_id").rowsBetween(
        Window.unboundedPreceding, -1
    )
    seen = df.withColumn("prev_max", F.max("event_time").over(order))
    lateness = F.unix_timestamp("prev_max") - F.unix_timestamp("event_time")
    seen = seen.withColumn("lateness_s", lateness)
    agg = seen.agg(
        F.count(F.lit(1)).alias("total"),
        *[F.sum((F.col("lateness_s") > s).cast("int")).alias(f"dropped_{k}") for k, s in DELAYS.items()],
        F.max("lateness_s").alias("max_lateness_s"),
    ).head()
    out = {"total": int(agg["total"]), "max_lateness_min": (agg["max_lateness_s"] or 0) / 60}
    for k in DELAYS:
        out[f"dropped_{k}"] = int(agg[f"dropped_{k}"] or 0)
    return out


def _markdown(r: dict) -> str:
    rows = [
        {"watermark": "10 phút", "record bị bỏ": r["dropped_10min"],
         "tỉ lệ": r["dropped_10min"] / r["total"] if r["total"] else 0.0},
        {"watermark": "2 giờ", "record bị bỏ": r["dropped_2h"],
         "tỉ lệ": r["dropped_2h"] / r["total"] if r["total"] else 0.0},
    ]
    return (
        f"Tổng {r['total']} record; record đến muộn nhất trễ {r['max_lateness_min']:.1f} phút.\n\n"
        + common.md_table(rows)
        + "\nĐánh đổi: watermark 2 giờ giữ được dữ liệu bù muộn nhưng Spark phải giữ "
        "state cửa sổ lâu gấp 12 lần (bộ nhớ) và cảnh báo sự kiện nóng chậm hơn.\n"
    )


def main(out_dir=common.DEFAULT_OUT, pg_dsn=None, delta_root=None, day=None):
    del pg_dsn
    spark = common.get_spark()
    if delta_root is None:
        df = common.late_events_fixture(spark)
    else:
        df = spark.read.format("delta").load(f"{delta_root.rstrip('/')}/bronze/edits")
        if day:
            df = df.filter(F.col("dt") == day)
    r = run(spark, df.select("rev_id", "event_time", "ingest_time"))
    return common.write_md(out_dir, NAME, "Bằng chứng #2 — watermark 10 phút vs 2 giờ",
                           _markdown(r), delta_root is None, delta_root)


if __name__ == "__main__":  # pragma: no cover
    p = argparse.ArgumentParser()
    p.add_argument("--delta-root")
    p.add_argument("--day")
    p.add_argument("--out-dir", default=str(common.DEFAULT_OUT))
    a = p.parse_args()
    print(main(a.out_dir, delta_root=a.delta_root, day=a.day))
