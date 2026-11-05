"""Bằng chứng #5 — demo rò rỉ dữ liệu CÓ CHỦ ĐÍCH (spec §8, phản ví dụ).

Thêm feature ``has_later_edit`` = "trong 1 giờ SAU edit này có edit khác trên
cùng trang không". Feature này nhìn vào tương lai: edit bị revert thì luôn có
edit sau (chính lượt revert). Model học được "đáp án" và PR-AUC tăng vọt —
nhưng lúc chạy thật, tại thời điểm chấm điểm, edit sau CHƯA tồn tại.

    python -m evidence.ev5_leakage --delta-root /data/delta
"""

from __future__ import annotations

import argparse
from datetime import timedelta

from pyspark.sql import Window
from pyspark.sql import functions as F

from evidence import common
from src.batch import train
from src.batch.labeling import label_edits
from src.stream.scoring import FEATURE_COLS

NAME = "ev5_leakage"
LEAK_COL = "has_later_edit"


def add_leaky_feature(df):
    future = (
        Window.partitionBy("wiki", "page_title")
        .orderBy(F.unix_timestamp("event_time"))
        .rangeBetween(1, 3600)  # TƯƠNG LAI — đây chính là lỗi
    )
    return df.withColumn(LEAK_COL, (F.count(F.lit(1)).over(future) > 0).cast("int"))


def run(spark, silver_df, now=None, test_days: int = 2, max_iter: int = 20) -> dict:
    moment = now or (silver_df.agg(F.max("event_time")).head()[0] + timedelta(hours=2))
    labeled = label_edits(spark, add_leaky_feature(silver_df), 60, moment).cache()
    tr, te = train.temporal_split(labeled, test_days=test_days)

    leak_cols = (*FEATURE_COLS, LEAK_COL)
    clean = train.evaluate(train.train_gbt(tr, max_iter=max_iter), te)
    leaky = train.evaluate(train.train_gbt(tr, max_iter=max_iter, feature_cols=leak_cols), te,
                           feature_cols=leak_cols)
    baseline = train.hand_rule_baseline(te)
    labeled.unpersist()
    return {
        "n_test": clean["n_samples"],
        "n_positive_test": clean["n_positive"],
        "pr_auc_with_leak": leaky["pr_auc"],
        "pr_auc_clean": clean["pr_auc"],
        "pr_auc_hand_rule": baseline["pr_auc"],
        "precision_at_50_with_leak": leaky["precision_at_50"],
        "precision_at_50_clean": clean["precision_at_50"],
        "precision_at_50_hand_rule": baseline["precision_at_50"],
    }


def _markdown(r: dict) -> str:
    rows = [
        {"model": f"GBT + `{LEAK_COL}` (rò rỉ)", "PR-AUC": r["pr_auc_with_leak"],
         "precision@50": r["precision_at_50_with_leak"]},
        {"model": "GBT sạch (chỉ feature quá khứ)", "PR-AUC": r["pr_auc_clean"],
         "precision@50": r["precision_at_50_clean"]},
        {"model": "luật thủ công: vô danh và |Δbyte| > 500", "PR-AUC": r["pr_auc_hand_rule"],
         "precision@50": r["precision_at_50_hand_rule"]},
    ]
    return (
        f"Tập test chia theo thời gian: {r['n_test']} edit, {r['n_positive_test']} bị revert.\n\n"
        + common.md_table(rows)
        + f"\n`{LEAK_COL}` dùng thông tin chỉ có SAU thời điểm edit. Số đẹp ở dòng đầu là "
        "giả: khi chấm điểm thật, edit sau chưa xảy ra. Test `test_features_no_future_leak` "
        "chặn lỗi này trong pipeline thật.\n"
    )


def main(out_dir=common.DEFAULT_OUT, pg_dsn=None, delta_root=None):
    del pg_dsn
    spark = common.get_spark()
    if delta_root is None:
        df = common.leakage_fixture(spark)
    else:
        df = spark.read.format("delta").load(f"{delta_root.rstrip('/')}/silver/edits_enriched")
    return common.write_md(out_dir, NAME, "Bằng chứng #5 — rò rỉ dữ liệu có chủ đích",
                           _markdown(run(spark, df)), delta_root is None, delta_root)


if __name__ == "__main__":  # pragma: no cover
    p = argparse.ArgumentParser()
    p.add_argument("--delta-root")
    p.add_argument("--out-dir", default=str(common.DEFAULT_OUT))
    a = p.parse_args()
    print(main(a.out_dir, delta_root=a.delta_root))
