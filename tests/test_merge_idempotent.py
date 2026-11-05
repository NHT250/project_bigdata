"""Silver là idempotent theo (wiki, rev_id) — spec §5.2, §8.1."""

import pytest

from tests.rows import bronze

pytest.importorskip("pyspark")

from src.stream.main import merge_silver, silver_frame  # noqa: E402


def make_batch(spark, recs):
    return silver_frame(spark, recs)


def silver_count(spark, root):
    return spark.read.format("delta").load(f"{root}/silver/edits_enriched").count()


def silver_row(spark, root, rev_id):
    df = spark.read.format("delta").load(f"{root}/silver/edits_enriched")
    return df.filter(f"rev_id = {rev_id}").collect()[0]


def rec(rev_id=1, **over):
    r = bronze(rev_id=rev_id)
    r.update(diff_added="", diff_removed="", diff_status="ok")
    r.update(over)
    return r


def test_silver_merge_idempotent(spark, tmp_path):
    batch = make_batch(spark, [rec(i) for i in range(1, 11)])
    merge_silver(spark, batch, str(tmp_path))
    n1 = silver_count(spark, tmp_path)
    merge_silver(spark, batch, str(tmp_path))
    assert n1 == 10 and silver_count(spark, tmp_path) == n1


def test_rerun_updates_diff_that_had_timed_out(spark, tmp_path):
    merge_silver(spark, make_batch(spark, [rec(1, diff_status="timeout")]), str(tmp_path))
    merge_silver(
        spark, make_batch(spark, [rec(1, diff_status="ok", diff_added="xyz")]), str(tmp_path)
    )
    row = silver_row(spark, tmp_path, rev_id=1)
    assert row.diff_status == "ok" and row.diff_added == "xyz"


def test_duplicate_rev_ids_inside_one_batch_collapse_to_one(spark, tmp_path):
    r = rec(5)
    merge_silver(spark, make_batch(spark, [r, r]), str(tmp_path))
    assert silver_count(spark, tmp_path) == 1


def test_same_rev_id_on_different_wiki_is_a_different_row(spark, tmp_path):
    merge_silver(
        spark, make_batch(spark, [rec(7), rec(7, wiki="dewiki")]), str(tmp_path)
    )
    assert silver_count(spark, tmp_path) == 2
