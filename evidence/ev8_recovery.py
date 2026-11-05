"""Bằng chứng #8 — phục hồi sau sự cố (spec §8, §9).

Script vận hành, chạy trên VM, theo đúng trình tự:

    1. dừng ingest để số liệu đứng yên:      docker compose stop ingest
    2. đợi job-stream xử lý hết, chụp số:     python -m evidence.ev8_recovery --delta-root /data/delta --phase before
    3. khởi động lại ingest, đợi vài phút
    4. GIẾT job-stream giữa chừng:            docker kill <container job-stream>
    5. chạy lại job-stream (đọc checkpoint + Kafka offset)
    6. dừng ingest, đợi xử lý hết, chụp số:   python -m evidence.ev8_recovery --delta-root /data/delta --phase after

Kỳ vọng: silver có số dòng == số ``rev_id`` khác nhau (không trùng nhờ MERGE),
và mọi ``rev_id`` có ở bronze đều có ở silver (không mất).
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from pyspark.sql import functions as F

from evidence import common

NAME = "ev8_recovery"


def run(spark, delta_root: str) -> dict:
    root = delta_root.rstrip("/")
    bronze = spark.read.format("delta").load(f"{root}/bronze/edits")
    silver = spark.read.format("delta").load(f"{root}/silver/edits_enriched")
    missing = (
        bronze.select("wiki", "rev_id").distinct()
        .join(silver.select("wiki", "rev_id"), ["wiki", "rev_id"], "left_anti")
        .count()
    )
    return {
        "bronze_rows": bronze.count(),
        "bronze_rev_ids": bronze.select("wiki", "rev_id").distinct().count(),
        "silver_rows": silver.count(),
        "silver_rev_ids": silver.select("wiki", "rev_id").distinct().count(),
        "rev_ids_in_bronze_missing_from_silver": missing,
        "silver_max_event_time": str(silver.agg(F.max("event_time")).head()[0]),
    }


def main(out_dir=common.DEFAULT_OUT, pg_dsn=None, delta_root=None, phase="snapshot"):
    spark = common.get_spark()
    demo = delta_root is None
    if demo:
        root = tempfile.mkdtemp(prefix="ev8-")
        with common.scratch_schema(common.resolve_pg_dsn(pg_dsn)) as conn:
            common.build_pipeline_sample(spark, root, conn)
    else:
        root = delta_root
    r = run(spark, root)

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{NAME}_{phase}.json").write_text(json.dumps(r, indent=2), encoding="utf-8")
    snapshots = []
    for ph in ("before", "after", "snapshot"):
        f = out / f"{NAME}_{ph}.json"
        if f.exists():
            snapshots.append({"lần chụp": ph, **json.loads(f.read_text(encoding="utf-8"))})
    body = (
        (__doc__ or "").split("\n\n", 1)[1].replace("    ", "", 0)
        + "\n## Số liệu\n\n" + common.md_table(snapshots)
        + "\nBronze được phép trùng (sự thật thô, replay sinh trùng). Silver phải có "
        "`silver_rows == silver_rev_ids` và `rev_ids_in_bronze_missing_from_silver == 0`.\n"
    )
    return common.write_md(out_dir, NAME, "Bằng chứng #8 — phục hồi sau khi giết job-stream",
                           body, demo, delta_root)


if __name__ == "__main__":  # pragma: no cover
    p = argparse.ArgumentParser()
    p.add_argument("--delta-root")
    p.add_argument("--phase", choices=("before", "after", "snapshot"), default="snapshot")
    p.add_argument("--out-dir", default=str(common.DEFAULT_OUT))
    a = p.parse_args()
    print(main(a.out_dir, delta_root=a.delta_root, phase=a.phase))
