"""Bằng chứng #3 — ``DESCRIBE HISTORY`` và số file trước/sau ``OPTIMIZE``.

    python -m evidence.ev3_optimize --delta-root /data/delta --day 2026-11-09
"""

from __future__ import annotations

import argparse
import tempfile
import time
from datetime import date

from pyspark.sql import functions as F

from evidence import common
from src.batch.maintenance import active_file_count, optimize_tables

NAME = "ev3_optimize"
TABLE = "bronze/edits"


def _history(spark, path: str) -> list[dict]:
    rows = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select(
        "version", "timestamp", "operation"
    ).orderBy(F.col("version").desc()).limit(5).collect()
    return [r.asDict() for r in rows]


def _query_seconds(spark, path: str, day: date, repeats: int = 3) -> float:
    best = float("inf")
    for _ in range(repeats):
        t = time.perf_counter()
        spark.read.format("delta").load(path).filter(F.col("dt") == day).count()
        best = min(best, time.perf_counter() - t)
    return best


def run(spark, delta_root: str, day: date) -> dict:
    path = f"{delta_root.rstrip('/')}/{TABLE}"
    before = {
        "files": active_file_count(spark, path),
        "seconds": _query_seconds(spark, path, day),
        "history": _history(spark, path),
    }
    optimize_tables(spark, delta_root, day)
    return {
        "day": day.isoformat(),
        "files_before": before["files"],
        "files_after": active_file_count(spark, path),
        "query_seconds_before": before["seconds"],
        "query_seconds_after": _query_seconds(spark, path, day),
        "history_before": before["history"],
        "history_after": _history(spark, path),
    }


def _markdown(r: dict) -> str:
    summary = [
        {"": "số file đang dùng", "trước": r["files_before"], "sau": r["files_after"]},
        {"": "đếm 1 ngày (giây, tốt nhất 3 lần)", "trước": r["query_seconds_before"],
         "sau": r["query_seconds_after"]},
    ]
    return (
        f"Bảng `{TABLE}`, partition `dt = {r['day']}`.\n\n" + common.md_table(summary)
        + "\n## DESCRIBE HISTORY — trước\n\n" + common.md_table(r["history_before"])
        + "\n## DESCRIBE HISTORY — sau\n\n" + common.md_table(r["history_after"])
    )


def main(out_dir=common.DEFAULT_OUT, pg_dsn=None, delta_root=None, day=None):
    del pg_dsn
    spark = common.get_spark()
    demo = delta_root is None
    if demo:
        root = tempfile.mkdtemp(prefix="ev3-")
        the_day = common.many_small_bronze_files(spark, root)
    else:
        root, the_day = delta_root, date.fromisoformat(day)
    r = run(spark, root, the_day)
    return common.write_md(out_dir, NAME, "Bằng chứng #3 — OPTIMIZE gộp file nhỏ",
                           _markdown(r), demo, delta_root)


if __name__ == "__main__":  # pragma: no cover
    p = argparse.ArgumentParser()
    p.add_argument("--delta-root")
    p.add_argument("--day", help="YYYY-MM-DD, thường là hôm qua")
    p.add_argument("--out-dir", default=str(common.DEFAULT_OUT))
    a = p.parse_args()
    print(main(a.out_dir, delta_root=a.delta_root, day=a.day))
