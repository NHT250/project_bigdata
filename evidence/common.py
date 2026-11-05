"""Hàm dùng chung cho các script bằng chứng.

Mỗi script có hai chế độ:
- **thật**: ``--delta-root /data/delta`` (và ``PG_DSN``) đọc dữ liệu hệ thống
  đã thu — đây là số đưa vào báo cáo;
- **demo**: không truyền gì, chạy trên dữ liệu tổng hợp để kiểm tra script và
  để ai cũng tái lập được. File markdown ghi rõ đang ở chế độ nào.
"""

from __future__ import annotations

import gzip
import json
import os
import random
import tempfile
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "evidence" / "out"
DEMO_NOTE = (
    "> **Chế độ demo** — số liệu dưới đây sinh từ dữ liệu TỔNG HỢP để kiểm tra "
    "script. Chạy lại với `--delta-root` trên VM để có số thật đưa vào báo cáo."
)
REAL_NOTE = "> Chế độ thật — đọc dữ liệu hệ thống tại `{root}`, chạy lúc {at}."


# --- Spark / Postgres ---------------------------------------------------------


def get_spark():
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    from src.stream.main import build_spark

    return build_spark("realtime-wiki-evidence")


def resolve_pg_dsn(pg_dsn: str | None = None) -> str:
    dsn = pg_dsn or os.environ.get("PG_DSN", "").strip()
    if dsn:
        return dsn
    import pgserver  # pip install pgserver — Postgres nhúng cho chế độ demo

    server = pgserver.get_server(tempfile.mkdtemp(prefix="rw-ev-"), cleanup_mode="stop")
    resolve_pg_dsn.server = server  # giữ tham chiếu
    return server.get_uri()


@contextmanager
def scratch_schema(pg_dsn: str):
    """Kết nối tới một lược đồ tạm đã nạp schema.sql; xoá khi xong."""
    import psycopg

    schema = f"ev_{uuid.uuid4().hex[:10]}"
    conn = psycopg.connect(pg_dsn, autocommit=True)
    conn.execute(f"CREATE SCHEMA {schema}")
    conn.execute(f"SET search_path TO {schema}")
    conn.execute((ROOT / "sql" / "schema.sql").read_text(encoding="utf-8"))
    try:
        yield conn
    finally:
        conn.execute(f"DROP SCHEMA {schema} CASCADE")
        conn.close()


def local_api_get(conn):
    """Gọi API thật (FastAPI app) trong tiến trình, trên đúng kết nối ``conn``."""
    from fastapi.testclient import TestClient

    from src.api.main import create_app

    @contextmanager
    def factory():
        yield conn

    client = TestClient(create_app(factory))
    return lambda path: client.get(path).json()


def http_api_get(base: str):
    import urllib.request

    def get(path: str):
        with urllib.request.urlopen(base.rstrip("/") + path, timeout=10) as r:
            return json.loads(r.read().decode("utf-8"))

    return get


# --- markdown -----------------------------------------------------------------


def md_table(rows: list[dict]) -> str:
    if not rows:
        return "_(không có dòng nào)_\n"
    cols = list(rows[0].keys())
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in rows:
        out.append("| " + " | ".join(_cell(r.get(c)) for c in cols) + " |")
    return "\n".join(out) + "\n"


def _cell(v) -> str:
    if v is None:
        return "–"
    if isinstance(v, float):
        return f"{v:.4f}"
    return str(v).replace("|", "\\|").replace("\n", " ")


def write_md(out_dir: str | os.PathLike, name: str, title: str, body: str, demo: bool,
             root: str | None = None) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    note = DEMO_NOTE if demo else REAL_NOTE.format(
        root=root, at=datetime.now(timezone.utc).isoformat(timespec="seconds"))
    path = out / f"{name}.md"
    path.write_text(f"# {title}\n\n{note}\n\n{body}", encoding="utf-8")
    return path


# --- dữ liệu tổng hợp -----------------------------------------------------------

T0 = datetime(2026, 11, 5, 0, 0, tzinfo=timezone.utc)
_rev = [5_000_000]


def record(**over) -> dict:
    _rev[0] += 1
    rev = over.pop("rev_id", _rev[0])
    t = over.pop("event_time", T0)
    rec = {
        "event_id": f"ev-{rev}", "wiki": "enwiki", "page_id": 1, "page_title": "Sample",
        "namespace": 0, "rev_id": rev, "rev_parent_id": rev - 1, "user_hash": "u0",
        "is_anon": False, "is_minor": False, "is_bot": False, "comment": "copyedit",
        "byte_old": 1000, "byte_new": 1010, "tags": [], "server_name": "en.wikipedia.org",
        "event_time": t, "ingest_time": t + timedelta(seconds=2), "clock_skew": False,
        "raw_json": "{}",
    }
    rec.update(over)
    return rec


class FakeCompareHttp:
    """Trả một diff cố định — chế độ demo không gọi Wikimedia."""

    def __call__(self, url, headers, timeout):
        return json.dumps({"compare": {"*": '<td class="diff-addedline"><div>demo text</div></td>'}})


def build_pipeline_sample(spark, delta_root: str, conn, n: int = 5) -> int:
    """Đẩy ``n`` record qua đường đi thật: Kafka value → bronze → process_batch."""
    from src.common.config import Settings
    from src.ingest.replay import replay_file
    from src.stream.main import process_batch, write_bronze_from_values

    class Producer:
        values: list = []

        def send(self, topic, key, value):
            self.values.append(value)

        def flush(self):
            pass

    now = datetime.now(timezone.utc).replace(microsecond=0)
    recs = [record(page_title=f"Trace page {i}", event_time=now - timedelta(seconds=30 - i))
            for i in range(n)]
    gz = Path(tempfile.mkdtemp()) / "sample.jsonl.gz"
    with gzip.open(gz, "wt", encoding="utf-8") as fh:
        for r in recs:
            row = dict(r, event_time=r["event_time"].isoformat(),
                       ingest_time=r["ingest_time"].isoformat())
            fh.write(json.dumps(row) + "\n")
    producer = Producer()
    producer.values = []
    replay_file(str(gz), producer, "rc.raw", now=now)
    batch = spark.createDataFrame([(v,) for v in producer.values], "value binary")
    settings = Settings(kafka_bootstrap="-", kafka_topic="rc.raw", pg_dsn="-",
                        delta_root=delta_root, wiki_contact="demo@example.com",
                        diff_max_rps=100)
    write_bronze_from_values(spark, batch, delta_root)
    process_batch(spark, batch, settings, FakeCompareHttp(), conn)
    return recs[-1]["rev_id"]


def leakage_fixture(spark, days: int = 8, per_day: int = 150, pages: int = 400, seed: int = 11):
    """Silver tổng hợp: edit phá hoại luôn bị rollback vài phút sau.

    Tín hiệu thật (``is_anon``, ``byte_delta``) cố tình yếu, để thấy rõ feature
    rò rỉ ``has_later_edit`` thổi phồng PR-AUC đến mức nào.
    """
    from src.stream.features import add_cheap_features, add_window_features
    from src.stream.main import silver_frame

    rnd = random.Random(seed)
    recs = []
    for d in range(days):
        for _ in range(per_day):
            t = T0 + timedelta(days=d, seconds=rnd.randint(0, 86_399))
            page = f"Page {rnd.randrange(pages)}"
            vandal = rnd.random() < 0.1
            delta = rnd.randint(-400, 400) + (rnd.choice([-1, 1]) * 150 if vandal else 0)
            r = record(page_title=page, event_time=t, user_hash=f"u{rnd.randrange(500)}",
                       is_anon=rnd.random() < (0.45 if vandal else 0.3),
                       byte_old=5000, byte_new=5000 + delta)
            r.update(diff_added="text", diff_removed="", diff_status="ok")
            recs.append(r)
            if vandal:
                rb = record(page_title=page, rev_parent_id=r["rev_id"], user_hash="patrol",
                            tags=["mw-rollback"],
                            event_time=t + timedelta(minutes=rnd.randint(1, 8)))
                rb.update(diff_added="", diff_removed="text", diff_status="ok")
                recs.append(rb)
    df = add_cheap_features(silver_frame(spark, recs).drop(
        "byte_delta", "abs_byte_delta", "comment_len", "comment_empty", "upper_ratio",
        "repeat_char", "has_url_added", "words_added", "words_removed", "hour_of_day",
        "day_of_week"))
    return add_window_features(df.drop("page_edits_prev_1h", "user_edits_prev_1h"))


def skew_fixture(spark, n: int = 3000, pages: int = 800, seed: int = 5):
    # Phần lớn edit rải trên nhiều trang; 10% dồn vào 5 trang đang nóng.
    rnd = random.Random(seed)
    rows = [
        ("enwiki", f"Hot {rnd.randrange(5)}" if rnd.random() < 0.1 else f"Page {rnd.randrange(pages)}")
        for _ in range(n)
    ]
    return spark.createDataFrame(rows, "wiki string, page_title string")


def late_events_fixture(spark):
    """3 giờ edit đều đặn; ingest mất mạng 30 phút (phút 60–90).

    Khi nối lại, luồng trực tiếp chạy tiếp từ hiện tại; 30 phút bị lỡ được bù
    bằng ``replay`` từ file của collector, đến muộn ~40 phút.
    """
    rows = []
    for s in range(0, 3 * 3600, 30):
        et = T0 + timedelta(seconds=s)
        in_gap = 3600 <= s < 5400
        it = et + timedelta(minutes=40) if in_gap else et + timedelta(seconds=2)
        rows.append((s, et, it, in_gap))
    return spark.createDataFrame(
        rows, "rev_id long, event_time timestamp, ingest_time timestamp, from_backfill boolean")


def many_small_bronze_files(spark, delta_root: str, appends: int = 12) -> date:
    from src.stream.main import write_bronze_batch

    for i in range(appends):
        write_bronze_batch(
            spark, [record(event_time=T0 + timedelta(minutes=i, seconds=k)) for k in range(3)],
            delta_root)
    return T0.date()
