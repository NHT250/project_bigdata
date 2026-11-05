"""Bằng chứng #1 — vết một event đi hết hệ thống.

Chọn một ``rev_id``, in mốc thời gian nó xuất hiện ở từng tầng:
Wikimedia (event_time) → ingest/Kafka (ingest_time) → bronze → silver →
PostgreSQL → API.

    python -m evidence.ev1_trace --delta-root /data/delta --rev-id 1234 \
        --api http://localhost:8000
"""

from __future__ import annotations

import argparse
import tempfile
from datetime import datetime, timezone

from pyspark.sql import functions as F

from evidence import common

NAME = "ev1_trace"


def _file_time(spark, path: str, rev_id: int):
    """Thời điểm file Delta chứa dòng đó được ghi (``_metadata``)."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        return None
    df = spark.read.format("delta").load(path).filter(F.col("rev_id") == rev_id)
    row = df.select(
        "event_time", "ingest_time", F.col("_metadata.file_modification_time").alias("written")
    ).orderBy("written").first()
    return row


def run(spark, delta_root: str, conn, rev_id: int, api_get=None) -> dict:
    root = delta_root.rstrip("/")
    stages: dict[str, dict] = {}

    bronze = _file_time(spark, f"{root}/bronze/edits", rev_id)
    silver = _file_time(spark, f"{root}/silver/edits_enriched", rev_id)
    event_time = bronze.event_time if bronze else (silver.event_time if silver else None)
    ingest_time = bronze.ingest_time if bronze else None

    stages["kafka"] = {"seen_at": ingest_time, "note": "ingest_time: lúc ingest nhận SSE và publish Kafka"}
    stages["bronze"] = {"seen_at": bronze.written if bronze else None, "note": "file Delta bronze được ghi"}
    stages["silver"] = {"seen_at": silver.written if silver else None, "note": "file Delta silver (sau MERGE) được ghi"}

    pg = conn.execute(
        "SELECT event_time, risk_score, now() FROM edits_live WHERE rev_id = %s", (rev_id,)
    ).fetchone()
    stages["postgres"] = {
        "seen_at": pg[2] if pg else None,
        "note": "dòng có trong edits_live (thời điểm truy vấn thấy nó)",
    }

    api_seen = None
    if api_get is not None:
        items = api_get("/api/live?limit=200").get("items", [])
        if any(i.get("rev_id") == rev_id for i in items):
            api_seen = datetime.now(timezone.utc)
    stages["api"] = {"seen_at": api_seen, "note": "GET /api/live trả về revision này"}

    return {"rev_id": rev_id, "event_time": event_time, "stages": stages}


def _markdown(r: dict) -> str:
    rows = [{"tầng": "wikimedia", "thời điểm": r["event_time"], "ghi chú": "event_time do Wikimedia sinh"}]
    for name in ("kafka", "bronze", "silver", "postgres", "api"):
        s = r["stages"][name]
        rows.append({"tầng": name, "thời điểm": s["seen_at"], "ghi chú": s["note"]})
    return f"Revision `{r['rev_id']}`.\n\n" + common.md_table(rows)


def main(out_dir=common.DEFAULT_OUT, pg_dsn=None, delta_root=None, rev_id=None, api=None):
    spark = common.get_spark()
    dsn = common.resolve_pg_dsn(pg_dsn)
    demo = delta_root is None
    if demo:
        with common.scratch_schema(dsn) as conn:
            root = tempfile.mkdtemp(prefix="ev1-")
            rev = common.build_pipeline_sample(spark, root, conn)
            r = run(spark, root, conn, rev, api_get=common.local_api_get(conn))
    else:
        import psycopg

        with psycopg.connect(dsn, autocommit=True) as conn:
            r = run(spark, delta_root, conn, int(rev_id),
                    api_get=common.http_api_get(api) if api else common.local_api_get(conn))
    return common.write_md(out_dir, NAME, "Bằng chứng #1 — vết một event qua mọi tầng",
                           _markdown(r), demo, delta_root)


if __name__ == "__main__":  # pragma: no cover
    p = argparse.ArgumentParser()
    p.add_argument("--delta-root")
    p.add_argument("--rev-id")
    p.add_argument("--api", help="ví dụ http://localhost:8000")
    p.add_argument("--out-dir", default=str(common.DEFAULT_OUT))
    a = p.parse_args()
    print(main(a.out_dir, delta_root=a.delta_root, rev_id=a.rev_id, api=a.api))
