"""Script sinh bằng chứng thực nghiệm (spec §8)."""


import pytest

pytest.importorskip("pyspark")

from evidence import (  # noqa: E402
    ev1_trace,
    ev2_watermark,
    ev3_optimize,
    ev5_leakage,
    ev6_skew,
    ev8_recovery,
)
from evidence import common  # noqa: E402

ALL_EVIDENCE = (ev1_trace, ev2_watermark, ev3_optimize, ev5_leakage, ev6_skew, ev8_recovery)


def test_ev1_trace_prints_all_five_stages_for_one_rev_id(spark, tmp_path, pg_conn):
    rev = common.build_pipeline_sample(spark, str(tmp_path), pg_conn)
    r = ev1_trace.run(spark, str(tmp_path), pg_conn, rev_id=rev,
                      api_get=common.local_api_get(pg_conn))
    assert set(r["stages"]) == {"kafka", "bronze", "silver", "postgres", "api"}
    assert all(r["stages"][s]["seen_at"] for s in r["stages"])


def test_ev1_trace_reports_missing_stage_for_unknown_rev(spark, tmp_path, pg_conn):
    common.build_pipeline_sample(spark, str(tmp_path), pg_conn)
    r = ev1_trace.run(spark, str(tmp_path), pg_conn, rev_id=1,
                      api_get=common.local_api_get(pg_conn))
    assert r["stages"]["silver"]["seen_at"] is None


def test_ev5_leakage_shows_inflated_then_honest_metric(spark):
    r = ev5_leakage.run(spark, common.leakage_fixture(spark))
    assert r["pr_auc_with_leak"] > r["pr_auc_clean"] + 0.2


def test_ev6_skew_shows_wiki_key_more_imbalanced_than_title_key(spark):
    r = ev6_skew.run(spark, common.skew_fixture(spark))
    assert r["max_partition_share_by_wiki"] > r["max_partition_share_by_title"]
    assert r["max_partition_share_by_wiki"] == pytest.approx(1.0)


def test_ev6_uses_kafka_default_partitioner():
    pytest.importorskip("kafka")
    from kafka.partitioner.default import murmur2

    assert ev6_skew.kafka_partition(b"enwiki", 3) == (murmur2(b"enwiki") & 0x7FFFFFFF) % 3


def test_ev2_watermark_drops_more_under_short_watermark(spark):
    r = ev2_watermark.run(spark, common.late_events_fixture(spark))
    assert r["dropped_10min"] > r["dropped_2h"]
    assert r["dropped_2h"] == 0


def test_ev3_optimize_reports_fewer_files(spark, tmp_path):
    day = common.many_small_bronze_files(spark, str(tmp_path))
    r = ev3_optimize.run(spark, str(tmp_path), day)
    assert r["files_after"] < r["files_before"]
    assert "OPTIMIZE" in {h["operation"] for h in r["history_after"]}


def test_ev8_counts_rev_ids(spark, tmp_path, pg_conn):
    common.build_pipeline_sample(spark, str(tmp_path), pg_conn)
    r = ev8_recovery.run(spark, str(tmp_path))
    assert r["silver_rev_ids"] == r["silver_rows"] > 0


def test_every_script_writes_a_markdown_file(tmp_path, spark, pg_dsn):
    for mod in ALL_EVIDENCE:
        mod.main(out_dir=str(tmp_path / "out"), pg_dsn=pg_dsn)
    files = list((tmp_path / "out").glob("*.md"))
    assert len(files) == 6
    for f in files:
        assert f.read_text(encoding="utf-8").startswith("# ")


