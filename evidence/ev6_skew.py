"""Bằng chứng #6 — data skew: key Kafka theo ``wiki`` vs theo ``page_title``.

Tính partition bằng ĐÚNG công thức của default partitioner Kafka
(murmur2 của key, & 0x7fffffff, mod số partition) trên dữ liệu thật.

    python -m evidence.ev6_skew --delta-root /data/delta [--partitions 3]
"""

from __future__ import annotations

import argparse
from collections import Counter

from evidence import common
from src.ingest.main import kafka_key

NAME = "ev6_skew"


def murmur2(data: bytes) -> int:
    """Bản Python của murmur2 trong Kafka (``Utils.murmur2``), seed 0x9747b28c."""
    length = len(data)
    seed, m, r = 0x9747B28C, 0x5BD1E995, 24
    h = (seed ^ length) & 0xFFFFFFFF
    for i in range(length // 4):
        k = int.from_bytes(data[i * 4:i * 4 + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    tail = length & ~3
    extra = length % 4
    if extra >= 3:
        h ^= data[tail + 2] << 16
    if extra >= 2:
        h ^= data[tail + 1] << 8
    if extra >= 1:
        h ^= data[tail]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h - (1 << 32) if h & 0x80000000 else h


def kafka_partition(key: bytes, partitions: int) -> int:
    return (murmur2(key) & 0x7FFFFFFF) % partitions


def _shares(counts: Counter, partitions: int, total: int) -> list[float]:
    return [counts.get(p, 0) / total if total else 0.0 for p in range(partitions)]


def run(spark, df, partitions: int = 3) -> dict:
    del spark
    per_key = df.groupBy("wiki", "page_title").count().collect()
    by_wiki, by_title = Counter(), Counter()
    total = 0
    for row in per_key:
        n = row["count"]
        total += n
        by_wiki[kafka_partition(kafka_key(row["wiki"]), partitions)] += n
        by_title[kafka_partition(kafka_key(row["page_title"]), partitions)] += n
    wiki_shares = _shares(by_wiki, partitions, total)
    title_shares = _shares(by_title, partitions, total)
    return {
        "partitions": partitions,
        "total": total,
        "shares_by_wiki": wiki_shares,
        "shares_by_title": title_shares,
        "max_partition_share_by_wiki": max(wiki_shares),
        "max_partition_share_by_title": max(title_shares),
    }


def _markdown(r: dict) -> str:
    rows = [
        {"partition": p, "key = wiki": r["shares_by_wiki"][p], "key = page_title": r["shares_by_title"][p]}
        for p in range(r["partitions"])
    ]
    return (
        f"{r['total']} edit, {r['partitions']} partition. Ô là tỉ lệ edit rơi vào partition đó.\n\n"
        + common.md_table(rows)
        + f"\nKey theo `wiki`: một partition gánh {r['max_partition_share_by_wiki']:.0%} tải — "
        "chỉ một consumer làm việc. Key theo `page_title`: lớn nhất "
        f"{r['max_partition_share_by_title']:.0%}, và mọi edit của cùng trang vẫn vào cùng "
        "partition nên phát hiện sự kiện nóng giữ đúng thứ tự.\n"
    )


def main(out_dir=common.DEFAULT_OUT, pg_dsn=None, delta_root=None, partitions: int = 3):
    del pg_dsn
    spark = common.get_spark()
    if delta_root is None:
        df = common.skew_fixture(spark)
    else:
        df = spark.read.format("delta").load(f"{delta_root.rstrip('/')}/bronze/edits")
    return common.write_md(out_dir, NAME, "Bằng chứng #6 — data skew theo key Kafka",
                           _markdown(run(spark, df, partitions)), delta_root is None, delta_root)


if __name__ == "__main__":  # pragma: no cover
    p = argparse.ArgumentParser()
    p.add_argument("--delta-root")
    p.add_argument("--partitions", type=int, default=3)
    p.add_argument("--out-dir", default=str(common.DEFAULT_OUT))
    a = p.parse_args()
    print(main(a.out_dir, delta_root=a.delta_root, partitions=a.partitions))
