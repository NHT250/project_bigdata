# Realtime Wikipedia Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thu mọi lượt chỉnh sửa `enwiki` theo thời gian thực, huấn luyện model phát hiện phá hoại trên nhãn tự sinh từ revert, và phục vụ dashboard + tra cứu + hỏi đáp qua website.

**Architecture:** SSE → Kafka → một Spark Structured Streaming job (bronze + enrich + scoring + serving) → Delta Lake (bronze/silver/gold) + PostgreSQL (tầng phục vụ) → FastAPI → HTML/JS thuần. Một job batch theo cron gắn nhãn, huấn luyện và bảo trì bảng. Spark không nằm trên đường request của website.

**Tech Stack:** Python 3.11 · Kafka (KRaft) · PySpark 3.5 + delta-spark 3.2 · PostgreSQL 16 · FastAPI + uvicorn · psycopg 3 · Chart.js (CDN) · pytest · Docker Compose (arm64)

**Spec:** `docs/superpowers/specs/2026-11-04-realtime-wikipedia-design.md`

## Global Constraints

- User-Agent mọi request ra ngoài: `VLU-BigData-RealtimeWiki/1.0 (Van Lang University course project; <CONTACT>)` — đọc từ env `WIKI_CONTACT`; **không bao giờ giả dạng UA trình duyệt**
- Giới hạn tối đa **10 req/s** tới MediaWiki API, timeout 3 giây, backoff luỹ thừa khi lỗi
- **Không proxy xoay vòng**
- Chỉ `wiki == "enwiki"`, `type == "edit"`, `bot == false`
- IP/username bị băm `sha256(salt + "|" + user)[:16]` **tại tầng ingest**; trường `user` và `user_text` bị loại khỏi payload thô trước khi ghi xuống bất kỳ đâu
- Salt đọc từ `state/salt.txt`, cố định giữa các lần chạy, không commit
- Watermark event-time: **10 phút**
- Mốc gắn nhãn revert: **60 phút**; job đêm kiểm lại ở mốc **24 giờ** để đo % bỏ sót
- Delta partition theo `dt DATE` — **không** theo `page_title`
- Kafka topic `rc.raw`, **3 partition, key = `page_title`**, retention 24h
- Mọi image Docker: `platform: linux/arm64`
- Báo cáo **PR-AUC** và **precision@50**, không báo cáo accuracy
- Chia tập train/test **theo thời gian**, không random
- Mọi diff hiển thị trên web kèm ghi nguồn CC BY-SA 4.0 + link bài gốc

## Review Focus

1. **Trang mới tạo có `revision.old = null` và `length.old = null`** — `byte_delta` và lệnh gọi diff phải xử lý được, không được crash hay sinh `None - int`. → test ở Task 2 và Task 7.
2. **`event_time` ở tương lai** (đồng hồ lệch, dữ liệu bẩn) đẩy watermark lên và làm Spark bỏ mọi record thật sau đó. Phải kẹp `event_time` không vượt `ingest_time` quá 1 phút. → test ở Task 2.
3. **Diff rất lớn** (edit thêm hàng trăm KB) làm phình bộ nhớ và vượt giới hạn dòng Postgres. Phải cắt `diff_added`/`diff_removed` ở 10.000 ký tự và đánh dấu `diff_status = "truncated"`. → test ở Task 7.
4. **`page_title` chứa unicode, dấu gạch chéo, dấu nháy, hoặc rất dài** — phải an toàn khi làm Kafka key, khi encode vào URL API, và khi truyền vào SQL tham số hoá. → test ở Task 5 và Task 18.
5. **Event `type=edit` nhưng thiếu `revision` hoặc `length`** — phải bỏ qua record đó và tăng một bộ đếm, không được làm chết cả batch. → test ở Task 2.

---

## File Structure

| File | Trách nhiệm |
|---|---|
| `src/common/config.py` | Đọc toàn bộ cấu hình từ env, một chỗ duy nhất |
| `src/common/hashing.py` | Nạp salt, băm user/IP |
| `src/common/schema.py` | Schema Spark bronze/silver, `to_record()`, hợp lệ hoá event |
| `src/ingest/sse_client.py` | Phân tích luồng SSE thành dict, giữ `Last-Event-ID` |
| `src/ingest/main.py` | SSE → Kafka |
| `src/ingest/replay.py` | `.jsonl.gz` → Kafka |
| `src/stream/diff_client.py` | Gọi MediaWiki `action=compare`, giới hạn tần suất, cắt ngắn |
| `src/stream/features.py` | Feature rẻ + feature cửa sổ quá khứ |
| `src/stream/burst.py` | Phát hiện bùng nổ bằng cửa sổ trượt |
| `src/stream/serving.py` | Upsert 4 bảng Postgres |
| `src/stream/main.py` | Job streaming: bronze → enrich → silver → serving |
| `src/batch/labeling.py` | Sinh nhãn từ dấu hiệu revert |
| `src/batch/train.py` | Huấn luyện GBT, đánh giá, ghi `model_metrics` |
| `src/batch/aggregate.py` | `page_hourly` → Postgres |
| `src/batch/maintenance.py` | TTL, OPTIMIZE/VACUUM, đo lại nhãn ở 24h |
| `src/batch/main.py` | Điều phối job batch |
| `src/api/db.py`, `queries.py`, `main.py` | Pool, SQL, 6 endpoint |
| `src/web/*` | 3 trang HTML + `app.js` |
| `sql/schema.sql` | 5 bảng Postgres + index + ràng buộc |
| `evidence/*.py` | 5 script sinh bằng chứng #2 #3 #5 #6 #8 |
| `tests/*` | pytest; `conftest.py` cấp SparkSession local + Delta |

---

## Track 0 · Nền tảng (làm trước, cả 3 người cùng cần)

### Task 1: Khung repo, cấu hình, băm user

**Files:**
- Create: `src/common/config.py`, `src/common/hashing.py`, `requirements.txt`, `.env.example`, `pytest.ini`
- Test: `tests/test_hashing.py`

**Interfaces:**
- Produces: `config.Settings` (dataclass, `Settings.load() -> Settings` với các trường `kafka_bootstrap: str`, `kafka_topic: str`, `pg_dsn: str`, `delta_root: str`, `wiki_contact: str`, `salt_path: str`, `state_path: str`, `wikis: set[str]`, `diff_max_rps: int`, `watermark_minutes: int`, `label_cutoff_minutes: int`, `summary_mode: str` — `"template"` hoặc `"llm"`); `hashing.load_salt(path: str) -> str`; `hashing.hash_user(user: str | None, salt: str) -> str | None`

- [ ] **Step 1: Write the failing test**

```python
def test_hash_is_16_hex_chars_and_stable():
    assert hash_user("203.113.45.7", "s") == hash_user("203.113.45.7", "s")
    assert len(hash_user("203.113.45.7", "s")) == 16

def test_different_salt_gives_different_hash():
    assert hash_user("203.113.45.7", "s1") != hash_user("203.113.45.7", "s2")

def test_none_user_returns_none():
    assert hash_user(None, "s") is None

def test_load_salt_creates_then_reuses(tmp_path):
    p = tmp_path / "salt.txt"
    first = load_salt(str(p))
    assert len(first) >= 32 and load_salt(str(p)) == first

def test_settings_requires_wiki_contact(monkeypatch):
    monkeypatch.delenv("WIKI_CONTACT", raising=False)
    with pytest.raises(ValueError, match="WIKI_CONTACT"):
        Settings.load()
```

- [ ] **Step 2: Run test to verify it fails** — `pytest tests/test_hashing.py -v`, kỳ vọng FAIL (module không tồn tại)
- [ ] **Step 3: Implement `load_salt`, `hash_user` trong `src/common/hashing.py` và `Settings` trong `src/common/config.py`.** `load_salt` sinh `secrets.token_hex(16)` và ghi file nếu chưa có. `Settings.load()` raise `ValueError` nêu tên biến env còn thiếu.
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit** — `git commit -m "feat: config and user hashing"`

---

### Task 2: Hợp lệ hoá event và schema bronze

**Files:**
- Create: `src/common/schema.py`
- Test: `tests/test_schema.py`

**Interfaces:**
- Consumes: `hashing.hash_user`
- Produces: `schema.BRONZE_SCHEMA: StructType`; `schema.SILVER_SCHEMA: StructType`; `schema.to_record(ev: dict, salt: str, now: datetime) -> dict | None` (trả `None` nếu event không hợp lệ); `schema.REJECT_REASONS: Counter`

- [ ] **Step 1: Write the failing test**

```python
def test_new_page_with_null_parent_is_kept():
    ev = make_event(revision={"new": 5, "old": None}, length={"new": 100, "old": None})
    r = to_record(ev, "s", NOW)
    assert r["rev_parent_id"] is None and r["byte_old"] is None

def test_missing_revision_is_rejected_not_crashed():
    ev = make_event(); del ev["revision"]
    assert to_record(ev, "s", NOW) is None

def test_missing_length_is_rejected_not_crashed():
    ev = make_event(); del ev["length"]
    assert to_record(ev, "s", NOW) is None

def test_event_time_far_in_future_is_clamped_to_ingest_time():
    ev = make_event(timestamp=int(NOW.timestamp()) + 86400)
    r = to_record(ev, "s", NOW)
    assert r["event_time"] <= NOW + timedelta(minutes=1)
    assert r["clock_skew"] is True

def test_raw_json_contains_no_user_field():
    ev = make_event(user="203.113.45.7")
    r = to_record(ev, "s", NOW)
    assert "203.113" not in r["raw_json"] and '"user"' not in r["raw_json"]

def test_anon_detected_from_ipv4_and_ipv6():
    assert to_record(make_event(user="203.113.45.7"), "s", NOW)["is_anon"] is True
    assert to_record(make_event(user="2001:db8::1"), "s", NOW)["is_anon"] is True
    assert to_record(make_event(user="Jimbo_Wales"), "s", NOW)["is_anon"] is False

def test_bronze_schema_field_names_match_record_keys():
    r = to_record(make_event(), "s", NOW)
    assert set(r) == {f.name for f in BRONZE_SCHEMA.fields}
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `to_record`, `BRONZE_SCHEMA`, `SILVER_SCHEMA`.** Cột bronze theo spec §5.1 cộng `clock_skew BOOLEAN`. Kẹp `event_time` ở `min(event_time, now + 1 phút)` và đặt `clock_skew = True` khi bị kẹp. Trả `None` và tăng `REJECT_REASONS` khi thiếu `revision` hoặc `length`.
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 3: Docker Compose và lược đồ Postgres

**Files:**
- Create: `docker-compose.yml`, `sql/schema.sql`, `Makefile`
- Test: `tests/test_schema_sql.py`

**Interfaces:**
- Produces: 5 bảng `edits_live`, `hot_pages`, `risk_queue`, `metrics_minute`, `page_hourly` theo spec §5.4

- [ ] **Step 1: Write the failing test**

```python
def test_all_five_tables_exist(pg_conn):
    names = {r[0] for r in pg_conn.execute(
        "select table_name from information_schema.tables where table_schema='public'")}
    assert {"edits_live","hot_pages","risk_queue","metrics_minute","page_hourly"} <= names

def test_edits_live_primary_key_is_wiki_rev_id(pg_conn):
    # upsert cùng rev_id hai lần -> vẫn một dòng
    ...
    assert count == 1

def test_page_title_accepts_unicode_and_long_values(pg_conn):
    pg_conn.execute("insert into edits_live(wiki,rev_id,page_title) values(%s,%s,%s)",
                    ("enwiki", 1, "Bão/Cơn_bão–2026 " + "x"*400))
```

- [ ] **Step 2: Run test to verify it fails** — `docker compose up -d postgres && pytest tests/test_schema_sql.py -v`
- [ ] **Step 3: Viết `sql/schema.sql`** — khoá chính `(wiki, rev_id)` cho `edits_live` và `risk_queue`; `page_title TEXT`; index trên `event_time DESC`, `risk_score DESC`, `(page_title, hour_bucket)`. Viết `docker-compose.yml` với 5 service theo spec §3.4, `platform: linux/arm64`, Kafka KRaft, volume cho Delta và checkpoint. `Makefile` có `up`, `down`, `logs`, `psql`, `test`.
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

## Track A · Ingest (Người A)

### Task 4: Phân tích luồng SSE

**Files:**
- Create: `src/ingest/sse_client.py`
- Test: `tests/test_sse_client.py`

**Interfaces:**
- Produces: `sse_client.parse_sse(lines: Iterable[bytes]) -> Iterator[tuple[str | None, dict]]` sinh `(event_id, payload)`; `sse_client.stream(url: str, ua: str, last_event_id: str | None) -> Iterator[tuple[str | None, dict]]`

- [ ] **Step 1: Write the failing test**

```python
def test_parses_two_events_with_ids():
    raw = b'id: a\ndata: {"x":1}\n\nid: b\ndata: {"x":2}\n\n'
    out = list(parse_sse(raw.splitlines(keepends=True)))
    assert out == [("a", {"x": 1}), ("b", {"x": 2})]

def test_multiline_data_is_concatenated():
    raw = b'id: a\ndata: {"x":\ndata: 1}\n\n'
    assert list(parse_sse(raw.splitlines(keepends=True))) == [("a", {"x": 1})]

def test_malformed_json_is_skipped_not_raised():
    raw = b'id: a\ndata: not json\n\nid: b\ndata: {"x":2}\n\n'
    assert list(parse_sse(raw.splitlines(keepends=True))) == [("b", {"x": 2})]

def test_comment_and_event_lines_ignored():
    raw = b':ping\nevent: message\nid: a\ndata: {"x":1}\n\n'
    assert list(parse_sse(raw.splitlines(keepends=True))) == [("a", {"x": 1})]
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `parse_sse` và `stream`.** `stream` dùng `urllib.request` với header `User-Agent` và `Last-Event-ID`, backoff luỹ thừa tối đa 30 giây.
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 5: Ingest → Kafka

**Files:**
- Create: `src/ingest/main.py`
- Test: `tests/test_ingest_main.py`

**Interfaces:**
- Consumes: `sse_client.stream`, `schema.to_record`, `hashing.load_salt`, `config.Settings`
- Produces: `main.run(settings, producer, events) -> IngestStats`; `main.kafka_key(page_title: str) -> bytes`

- [ ] **Step 1: Write the failing test**

```python
def test_only_enwiki_non_bot_edits_are_published(fake_producer):
    events = [("1", make_event()),
              ("2", make_event(wiki="viwiki")),
              ("3", make_event(bot=True)),
              ("4", make_event(type="log"))]
    stats = run(SETTINGS, fake_producer, events)
    assert stats.published == 1 and stats.skipped == 3

def test_kafka_key_is_page_title_utf8_encoded(fake_producer):
    run(SETTINGS, fake_producer, [("1", make_event(title="Bão_Yagi"))])
    assert fake_producer.sent[0].key == "Bão_Yagi".encode("utf-8")

def test_very_long_page_title_key_is_truncated_to_255_bytes(fake_producer):
    run(SETTINGS, fake_producer, [("1", make_event(title="ă"*500))])
    assert len(fake_producer.sent[0].key) <= 255

def test_last_event_id_persisted_after_each_batch(fake_producer, tmp_path):
    run(replace(SETTINGS, state_path=str(tmp_path)), fake_producer, [("abc", make_event())])
    assert (tmp_path / "last_event_id.txt").read_text() == "abc"
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `run` và `kafka_key`.** `kafka_key` encode UTF-8 rồi cắt an toàn theo ranh giới ký tự ở 255 byte. Dùng `kafka-python` `KafkaProducer` ở `main()`, nhưng `run()` nhận producer qua tham số để test được.
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 6: Phát lại file .jsonl.gz vào Kafka

**Files:**
- Create: `src/ingest/replay.py`
- Test: `tests/test_replay.py`

**Interfaces:**
- Produces: `replay.replay_file(path: str, producer, topic: str) -> int` trả số message đã gửi

- [ ] **Step 1: Write the failing test**

```python
def test_replays_every_line_of_multimember_gzip(tmp_path, fake_producer):
    p = write_multimember_gzip(tmp_path, n_members=3, rows_per_member=10)
    assert replay_file(str(p), fake_producer, "rc.raw") == 30

def test_record_from_collector_is_republished_unchanged(tmp_path, fake_producer):
    # collector.py đã băm IP sẵn -> replay không băm lại
    ...
    assert json.loads(fake_producer.sent[0].value)["user_hash"] == "abc123"
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `replay_file`.** Đọc `gzip.open(path, "rt")`; các record từ `collector.py` đã ở dạng bronze nên gửi nguyên, không gọi `to_record` lần hai.
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

## Track B · Streaming job (Người A sau Task 6, hoặc Người B)

### Task 7: Lấy diff từ MediaWiki API

**Files:**
- Create: `src/stream/diff_client.py`
- Test: `tests/test_diff_client.py`

**Interfaces:**
- Produces: `diff_client.DiffResult` (dataclass: `added: str`, `removed: str`, `status: str`); `diff_client.fetch_diff(rev_id: int, parent_id: int | None, ua: str, http=...) -> DiffResult`; `diff_client.RateLimiter(max_rps: int)` với `.acquire()`

- [ ] **Step 1: Write the failing test**

```python
def test_parses_added_and_removed_from_compare_html(fake_http):
    fake_http.respond(COMPARE_FIXTURE)
    r = fetch_diff(2, 1, UA, fake_http)
    assert r.status == "ok" and "casualty" in r.added and "old text" in r.removed

def test_null_parent_id_uses_new_page_mode(fake_http):
    r = fetch_diff(2, None, UA, fake_http)
    assert "torelative=prev" not in fake_http.last_url
    assert r.status in {"ok", "new_page"}

def test_diff_longer_than_10000_chars_is_truncated(fake_http):
    fake_http.respond(compare_with_added_text("x" * 50000))
    r = fetch_diff(2, 1, UA, fake_http)
    assert len(r.added) == 10_000 and r.status == "truncated"

def test_timeout_returns_status_timeout_not_raise(fake_http):
    fake_http.raise_(TimeoutError)
    assert fetch_diff(2, 1, UA, fake_http).status == "timeout"

def test_user_agent_is_sent_and_is_not_a_browser(fake_http):
    fetch_diff(2, 1, UA, fake_http)
    assert fake_http.last_headers["User-Agent"] == UA
    assert "Mozilla" not in fake_http.last_headers["User-Agent"]

def test_rate_limiter_caps_at_max_rps(monkeypatch):
    rl = RateLimiter(10)
    # 20 lần acquire phải mất >= 1 giây theo đồng hồ giả
    assert elapsed >= 1.0
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement.** Dùng endpoint `action=compare&fromrev=<parent>&torev=<new>&format=json`; khi `parent_id is None` thì lấy nội dung revision mới làm `added`. Cắt ở 10.000 ký tự. Mọi lỗi mạng/HTTP → `status` tương ứng, không raise.
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 8: Feature rẻ

**Files:**
- Create: `src/stream/features.py`
- Test: `tests/test_features.py`

**Interfaces:**
- Produces: `features.add_cheap_features(df: DataFrame) -> DataFrame` thêm `byte_delta`, `abs_byte_delta`, `comment_len`, `comment_empty`, `upper_ratio`, `repeat_char`, `has_url_added`, `words_added`, `words_removed`, `hour_of_day`, `day_of_week`

- [ ] **Step 1: Write the failing test**

```python
def test_byte_delta_is_null_safe_for_new_pages(spark):
    out = add_cheap_features(df_with(byte_old=None, byte_new=100)).collect()[0]
    assert out.byte_delta == 100 and out.abs_byte_delta == 100

def test_upper_ratio_counts_only_letters(spark):
    out = add_cheap_features(df_with(diff_added="HELLO world 123!!!")).collect()[0]
    assert out.upper_ratio == pytest.approx(5/10)

def test_repeat_char_detects_seven_or_more_repeats(spark):
    assert row_for(diff_added="aaaaaaa").repeat_char is True
    assert row_for(diff_added="aaa").repeat_char is False

def test_empty_diff_added_gives_zero_not_null(spark):
    out = row_for(diff_added="")
    assert out.upper_ratio == 0.0 and out.words_added == 0
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `add_cheap_features` bằng biểu thức Spark SQL thuần (không UDF Python, để tránh chi phí serialize).**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 9: Feature cửa sổ quá khứ — chặn rò rỉ dữ liệu

**Files:**
- Modify: `src/stream/features.py`
- Test: `tests/test_features_no_future_leak.py`

**Interfaces:**
- Produces: `features.add_window_features(df: DataFrame) -> DataFrame` thêm `page_edits_prev_1h`, `user_edits_prev_1h`

Đây là test quan trọng nhất của toàn bộ dự án: nó là thứ chặn không cho nhóm nộp một model có số liệu giả.

- [ ] **Step 1: Write the failing test**

```python
def test_window_counts_only_strictly_earlier_events(spark):
    # 3 edit cùng trang lúc 10:00, 10:30, 11:00
    rows = add_window_features(three_edits_one_page(spark)).orderBy("event_time").collect()
    assert [r.page_edits_prev_1h for r in rows] == [0, 1, 2]

def test_event_itself_is_never_counted(spark):
    rows = add_window_features(single_edit(spark)).collect()
    assert rows[0].page_edits_prev_1h == 0

def test_events_outside_one_hour_are_excluded(spark):
    # edit lúc 08:00 và 10:00 cùng trang -> edit sau thấy 0
    assert rows[1].page_edits_prev_1h == 0

def test_other_pages_do_not_leak_into_count(spark):
    # 5 edit trang khác cùng thời điểm -> không ảnh hưởng
    assert rows[-1].page_edits_prev_1h == 0

def test_adding_a_later_event_does_not_change_earlier_rows(spark):
    before = counts_for(edits[:2])
    after  = counts_for(edits[:3])
    assert after[:2] == before   # chèn event tương lai không đổi quá khứ
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `add_window_features` bằng `Window.partitionBy("wiki","page_title").orderBy(unix_timestamp("event_time")).rangeBetween(-3600, -1)` — biên trên là `-1`, không phải `0`, để không đếm chính nó.**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 10: Ghi Delta bronze từ Kafka

**Files:**
- Create: `src/stream/main.py`, `tests/conftest.py`
- Test: `tests/test_bronze_write.py`

**Interfaces:**
- Consumes: `schema.BRONZE_SCHEMA`
- Produces: `main.build_spark() -> SparkSession`; `main.write_bronze(df: DataFrame, delta_root: str, checkpoint: str) -> StreamingQuery`; `conftest.spark` fixture (local[2] + Delta)

- [ ] **Step 1: Write the failing test**

```python
def test_bronze_write_roundtrips_records(spark, tmp_path):
    write_bronze_batch(spark, sample_records(50), str(tmp_path))
    assert spark.read.format("delta").load(f"{tmp_path}/bronze/edits").count() == 50

def test_bronze_is_partitioned_by_dt(spark, tmp_path):
    write_bronze_batch(spark, records_over_two_days(), str(tmp_path))
    assert len(glob(f"{tmp_path}/bronze/edits/dt=*")) == 2

def test_bronze_keeps_duplicates(spark, tmp_path):
    write_bronze_batch(spark, [r, r], str(tmp_path))
    assert count == 2   # bronze là sự thật thô, không dedup
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement.** `build_spark` cấu hình `delta.sql.DeltaSparkSessionExtension`, `spark.sql.parquet.compression.codec=zstd`. `conftest.spark` dùng `local[2]`, `delta-spark` qua `configure_spark_with_delta_pip`. Trong test dùng batch write; `main()` dùng `readStream` Kafka với `trigger(processingTime="10 seconds")`.
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 11: MERGE vào silver — idempotent

**Files:**
- Modify: `src/stream/main.py`
- Test: `tests/test_merge_idempotent.py`

**Interfaces:**
- Produces: `main.merge_silver(spark, batch_df: DataFrame, delta_root: str) -> None`

- [ ] **Step 1: Write the failing test**

```python
def test_same_batch_merged_twice_gives_same_row_count(spark, tmp_path):
    merge_silver(spark, batch, str(tmp_path))
    n1 = silver_count(spark, tmp_path)
    merge_silver(spark, batch, str(tmp_path))
    assert silver_count(spark, tmp_path) == n1

def test_rerun_updates_diff_that_had_timed_out(spark, tmp_path):
    merge_silver(spark, batch_with(diff_status="timeout"), str(tmp_path))
    merge_silver(spark, batch_with(diff_status="ok", diff_added="xyz"), str(tmp_path))
    row = silver_row(spark, tmp_path, rev_id=1)
    assert row.diff_status == "ok" and row.diff_added == "xyz"

def test_duplicate_rev_ids_inside_one_batch_collapse_to_one(spark, tmp_path):
    merge_silver(spark, spark.createDataFrame([r, r]), str(tmp_path))
    assert silver_count(spark, tmp_path) == 1
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `merge_silver` bằng `DeltaTable.merge(... "t.wiki = s.wiki AND t.rev_id = s.rev_id") .whenMatchedUpdateAll().whenNotMatchedInsertAll()`. Trước khi merge phải `dropDuplicates(["wiki","rev_id"])` trên batch — MERGE của Delta lỗi nếu nguồn có khoá trùng.**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 12: Phát hiện sự kiện nóng

**Files:**
- Create: `src/stream/burst.py`
- Test: `tests/test_burst.py`

**Interfaces:**
- Produces: `burst.detect(df: DataFrame, z_threshold: float = 3.0, min_users: int = 3) -> DataFrame` với các cột `page_title`, `wiki`, `window_end`, `edit_count`, `distinct_users`, `ewma`, `z_score`, `is_hot`

- [ ] **Step 1: Write the failing test**

```python
def test_sudden_spike_from_many_users_is_hot(spark):
    out = detect(baseline_then_spike(n_users=8))
    assert out.filter("is_hot").count() >= 1

def test_edit_war_between_two_users_is_not_hot(spark):
    # 20 edit trong 5 phút nhưng chỉ 2 user -> không phải sự kiện nóng
    assert detect(edit_war(n_users=2)).filter("is_hot").count() == 0

def test_steady_high_traffic_page_is_not_hot(spark):
    # trang luôn nhiều edit -> ewma cao -> z thấp
    assert detect(steady_traffic()).filter("is_hot").count() == 0

def test_brand_new_page_uses_global_baseline(spark):
    out = detect(single_new_page_burst())
    assert out.collect()[0].ewma > 0   # không chia cho 0
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `detect`.** Cửa sổ `window("event_time", "5 minutes", "1 minute")` nhóm theo `(wiki, page_title)`; `ewma` tính bằng window hàm trên 24h quá khứ của chính trang đó, dùng đường nền toàn cục khi chưa có lịch sử; `std` kẹp sàn ở 1.0 để không chia cho số rất nhỏ.
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 13: Upsert Postgres (tầng phục vụ)

**Files:**
- Create: `src/stream/serving.py`
- Test: `tests/test_serving.py`

**Interfaces:**
- Produces: `serving.upsert_edits_live(conn, rows) -> int`; `upsert_hot_pages`, `upsert_risk_queue`, `upsert_metrics_minute` cùng chữ ký; `serving.cleanup_ttl(conn) -> dict[str, int]`

- [ ] **Step 1: Write the failing test**

```python
def test_upsert_twice_keeps_one_row_and_latest_score(pg_conn):
    upsert_edits_live(pg_conn, [row(rev_id=1, risk_score=0.1)])
    upsert_edits_live(pg_conn, [row(rev_id=1, risk_score=0.9)])
    assert fetch(pg_conn, 1).risk_score == 0.9 and count(pg_conn) == 1

def test_cleanup_removes_rows_past_ttl(pg_conn):
    upsert_edits_live(pg_conn, [row(event_time=NOW - timedelta(hours=3))])
    assert cleanup_ttl(pg_conn)["edits_live"] == 1 and count(pg_conn) == 0

def test_risk_queue_keeps_only_top_50(pg_conn):
    upsert_risk_queue(pg_conn, [row(rev_id=i, risk_score=i/100) for i in range(80)])
    assert count(pg_conn, "risk_queue") == 50
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement bằng `psycopg.cursor.executemany` với `INSERT ... ON CONFLICT (wiki, rev_id) DO UPDATE`. `cleanup_ttl` xoá theo TTL ở spec §5.4 và trả số dòng đã xoá mỗi bảng.**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 14: Nối job streaming end-to-end

**Files:**
- Modify: `src/stream/main.py`
- Test: `tests/test_replay_end_to_end.py`

**Interfaces:**
- Produces: `main.process_batch(spark, batch_df, settings, http, conn) -> BatchStats`

- [ ] **Step 1: Write the failing test**

```python
def test_replaying_a_gz_file_fills_bronze_silver_and_postgres(spark, tmp_path, pg_conn, fake_http):
    n = load_gz_into_bronze_and_enrich(GZ_FIXTURE_200_ROWS, ...)
    assert bronze_count == 200
    assert silver_count == 200
    assert pg_count("edits_live") == 200

def test_one_timed_out_diff_does_not_fail_the_batch(spark, ..., fake_http):
    fake_http.fail_nth(7, TimeoutError)
    stats = process_batch(...)
    assert stats.rows == 200 and stats.diff_timeout == 1

def test_rejected_events_are_counted_not_dropped_silently(spark, ...):
    stats = process_batch(spark, batch_with_3_malformed(), ...)
    assert stats.rejected == 3
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `process_batch`: `add_cheap_features` → `fetch_diff` song song có `RateLimiter` → `add_window_features` → scoring (Task 16 nạp model; trước đó dùng `risk_score = null`) → `merge_silver` → `burst.detect` → 4 hàm upsert. Mọi lỗi diff được đếm, không raise.**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

## Track C · Gắn nhãn và huấn luyện (Người B)

### Task 15: Sinh nhãn từ dấu hiệu revert

**Files:**
- Create: `src/batch/labeling.py`
- Test: `tests/test_labeling.py`

**Interfaces:**
- Produces: `labeling.label_edits(spark, silver_df: DataFrame, cutoff_minutes: int, now: datetime) -> DataFrame` thêm `label INT`, `label_source STRING`, `labeled_at TIMESTAMP`

- [ ] **Step 1: Write the failing test**

```python
def test_rollback_tag_on_later_edit_labels_target_as_reverted(spark):
    assert label_for(later_tags=["mw-rollback"]) == (1, "tag_rollback")

def test_undo_tag_pointing_at_our_parent_labels_reverted(spark):
    assert label_for(later_tags=["mw-undo"], undo_of=OUR_REV) == (1, "tag_undo")

def test_reverted_edits_by_comment_pattern_labels_reverted(spark):
    c = "Reverted edits by [[Special:Contributions/1.2.3.4|1.2.3.4]]"
    assert label_for(later_comment=c) == (1, "comment_match")

def test_edit_surviving_past_cutoff_labels_zero(spark):
    assert label_for(age_minutes=61, later=None) == (0, "survived")

def test_edit_younger_than_cutoff_is_not_labeled_yet(spark):
    assert label_edits(spark, df_with_age(30), 60, NOW).count() == 0

def test_undo_tag_pointing_at_a_different_revision_does_not_label_us(spark):
    assert label_for(later_tags=["mw-undo"], undo_of=OTHER_REV) == (0, "survived")
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `label_edits`. Self-join silver theo `(wiki, page_title)` với điều kiện `later.event_time > ours.event_time`; chỉ xét edit đã đủ `cutoff_minutes` tuổi. Thứ tự ưu tiên `label_source`: `tag_rollback` > `tag_undo` > `comment_match`.**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 16: Huấn luyện và đánh giá

**Files:**
- Create: `src/batch/train.py`, `src/stream/scoring.py`
- Test: `tests/test_train.py`, `tests/test_scoring.py`

**Interfaces:**
- Consumes: `labeling.label_edits`
- Produces: `train.FEATURE_COLS: list[str]`; `train.temporal_split(df, test_days: int) -> tuple[DataFrame, DataFrame]`; `train.train_gbt(train_df) -> PipelineModel`; `train.evaluate(model, test_df) -> dict`; `train.hand_rule_baseline(df) -> dict`; `train.add_class_weights(df) -> DataFrame`; `train.write_metrics(spark, metrics: dict, delta_root: str) -> None`; `scoring.load_latest_model(delta_root) -> tuple[PipelineModel | None, str]`; `scoring.score_batch(model, df) -> DataFrame` (thêm cột `risk_score`, `model_version`; trả `risk_score = null` khi `model is None`)

- [ ] **Step 1: Write the failing test**

```python
def test_feature_cols_exclude_every_forbidden_column():
    forbidden = {"label","label_source","labeled_at","risk_score","model_version",
                 "tags","rev_id","rev_parent_id","event_id","raw_json","page_title","user_hash"}
    assert not (set(FEATURE_COLS) & forbidden)

def test_temporal_split_puts_no_test_day_before_any_train_day(spark):
    tr, te = temporal_split(df_over_10_days(spark), test_days=3)
    assert tr.agg(max("event_time")).head()[0] <= te.agg(min("event_time")).head()[0]

def test_evaluate_reports_pr_auc_and_precision_at_50_and_not_accuracy(spark):
    m = evaluate(model, test_df)
    assert {"pr_auc","roc_auc","precision_at_50","recall_at_50"} <= set(m)
    assert "accuracy" not in m

def test_class_weight_column_is_inverse_of_class_frequency(spark):
    w = add_class_weights(df_with_5pct_positive(spark))
    assert w.filter("label=1").head().weight == pytest.approx(19.0, rel=0.1)

def test_hand_rule_baseline_returns_same_metric_keys_as_model(spark):
    assert set(hand_rule_baseline(test_df)) == set(evaluate(model, test_df))

def test_write_metrics_appends_one_row_per_run(spark, tmp_path):
    write_metrics(spark, m1, str(tmp_path)); write_metrics(spark, m2, str(tmp_path))
    assert read_metrics(spark, tmp_path).count() == 2
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement.** `FEATURE_COLS` lấy đúng danh sách "được dùng" ở spec §6.1. Pipeline: `VectorAssembler` → `GBTClassifier(weightCol="weight", maxIter=50)`. `evaluate` dùng `BinaryClassificationEvaluator(metricName="areaUnderPR")` cộng precision/recall@50 tính thủ công trên 50 dòng risk cao nhất. `hand_rule_baseline` là `is_anon AND abs_byte_delta > 500`.
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 17: Tổng hợp page_hourly và bảo trì

**Files:**
- Create: `src/batch/aggregate.py`, `src/batch/maintenance.py`, `src/batch/main.py`
- Test: `tests/test_aggregate.py`, `tests/test_maintenance.py`

**Interfaces:**
- Produces: `aggregate.page_hourly(silver_df) -> DataFrame`; `aggregate.to_postgres(conn, df, min_edits_per_day: int = 5) -> int`; `maintenance.optimize_tables(spark, delta_root, day) -> dict`; `maintenance.measure_late_reverts(spark, delta_root, day) -> dict`

- [ ] **Step 1: Write the failing test**

```python
def test_page_hourly_buckets_by_hour_and_counts_distinct_users(spark):
    row = page_hourly(three_edits_two_users_same_hour(spark)).head()
    assert row.edit_count == 3 and row.distinct_users == 2

def test_pages_below_min_edits_are_not_written_to_postgres(pg_conn, spark):
    assert to_postgres(pg_conn, df_with_one_page_of_2_edits(spark), min_edits_per_day=5) == 0

def test_measure_late_reverts_reports_missed_fraction(spark, tmp_path):
    # 10 edit: 2 bị revert sau 61-1440 phút -> nhãn 60 phút bỏ sót 2/10
    r = measure_late_reverts(spark, str(tmp_path), DAY)
    assert r["missed"] == 2 and r["missed_fraction"] == pytest.approx(0.2)

def test_optimize_reduces_file_count(spark, tmp_path):
    before = count_parquet_files(tmp_path)
    optimize_tables(spark, str(tmp_path), DAY)
    assert count_parquet_files(tmp_path) < before
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement. `batch/main.py` chạy tuần tự: `label_edits` → ghi `gold/training_set` → `train_gbt` → `evaluate` → `write_metrics` → `page_hourly` → `to_postgres` → `cleanup_ttl`; thêm cờ `--nightly` để chạy `optimize_tables` và `measure_late_reverts`.**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

## Track D · API và website (Người C)

### Task 18: API — 6 endpoint

**Files:**
- Create: `src/api/db.py`, `src/api/queries.py`, `src/api/main.py`
- Test: `tests/test_api.py`

**Interfaces:**
- Produces: `queries.CANNED: dict[int, tuple[str, str]]` (qid → (tiêu đề, SQL)); FastAPI app với 6 route ở spec §7.1

- [ ] **Step 1: Write the failing test**

```python
def test_metrics_returns_points_ordered_by_time(client, seeded_pg):
    pts = client.get("/api/metrics?hours=6").json()["points"]
    assert pts == sorted(pts, key=lambda p: p["t"])

def test_risk_endpoint_includes_diff_and_wikipedia_link(client, seeded_pg):
    item = client.get("/api/risk?limit=5").json()["items"][0]
    assert "diff_added" in item and item["url"].startswith("https://en.wikipedia.org/wiki/")

def test_page_endpoint_handles_unicode_and_slash_in_title(client, seeded_pg):
    assert client.get("/api/page", params={"title": "Bão/Yagi", "days": 7}).status_code == 200

def test_page_endpoint_is_parameterised_against_sql_injection(client, seeded_pg):
    r = client.get("/api/page", params={"title": "x'; drop table edits_live; --"})
    assert r.status_code == 200 and table_still_exists(seeded_pg, "edits_live")

def test_ask_rejects_unknown_qid(client):
    assert client.get("/api/ask?qid=99").status_code == 400

def test_all_five_canned_queries_execute(client, seeded_pg):
    for qid in range(1, 6):
        assert client.get(f"/api/ask?qid={qid}").status_code == 200

def test_every_response_carries_cc_by_sa_attribution(client, seeded_pg):
    assert client.get("/api/risk").json()["license"] == "CC BY-SA 4.0"
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement.** `db.py` dùng `psycopg_pool.ConnectionPool`. Mọi truy vấn tham số hoá; không nối chuỗi SQL ở bất kỳ đâu. `queries.CANNED` chứa đúng 5 câu ở spec §7.3.
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 19: Dashboard

**Files:**
- Create: `src/web/index.html`, `src/web/app.js`, `src/web/styles.css`
- Test: `tests/test_web_dashboard.py` (Playwright, chạy với API đã seed)

**Interfaces:**
- Consumes: `/api/metrics`, `/api/hot`, `/api/risk`, `/api/live`

- [ ] **Step 1: Write the failing test**

```python
def test_dashboard_shows_four_stat_cards(page, live_server):
    page.goto(live_server + "/")
    assert page.locator(".stat-card").count() == 4

def test_hot_pages_table_renders_rows_from_api(page, live_server):
    assert page.locator("#hot-table tbody tr").count() > 0

def test_risk_panel_shows_score_and_link_to_wikipedia(page, live_server):
    assert "en.wikipedia.org" in page.locator("#risk-table a").first.get_attribute("href")

def test_page_refreshes_data_every_five_seconds(page, live_server):
    # đếm số request tới /api/live trong 11 giây
    assert 2 <= request_count <= 4

def test_license_footer_is_present(page, live_server):
    assert "CC BY-SA 4.0" in page.locator("footer").inner_text()
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement.** Chart.js qua CDN cho biểu đồ đường; `setInterval(load, 5000)`; không framework, không build step. FastAPI mount `StaticFiles` tại `/`.
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 20: Trang tra cứu và trang hỏi đáp

**Files:**
- Create: `src/web/page.html`, `src/web/ask.html`
- Modify: `src/web/app.js`
- Test: `tests/test_web_pages.py`

- [ ] **Step 1: Write the failing test**

```python
def test_lookup_shows_hourly_chart_for_searched_title(page, live_server):
    page.goto(live_server + "/page.html"); page.fill("#title", "Typhoon_Yagi"); page.click("#go")
    assert page.locator("canvas#hourly").is_visible()

def test_lookup_shows_message_for_unknown_page(page, live_server):
    ...
    assert "không có dữ liệu" in page.inner_text("#empty")

def test_ask_page_renders_five_buttons(page, live_server):
    assert page.locator(".ask-btn").count() == 5

def test_clicking_a_question_renders_a_result_table(page, live_server):
    page.click(".ask-btn >> nth=0")
    assert page.locator("#ask-result tbody tr").count() > 0
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

## Track E · Bằng chứng và bàn giao

### Task 21: Script sinh bằng chứng

**Files:**
- Create: `evidence/ev1_trace.py`, `evidence/ev2_watermark.py`, `evidence/ev3_optimize.py`, `evidence/ev5_leakage.py`, `evidence/ev6_skew.py`, `evidence/ev8_recovery.py`
- Test: `tests/test_evidence.py`

**Interfaces:**
- Mỗi script có `run(spark, ...) -> dict` và `main()` in bảng kết quả dạng markdown vào `evidence/out/<name>.md`

- [ ] **Step 1: Write the failing test**

```python
def test_ev1_trace_prints_all_five_stages_for_one_rev_id(spark, tmp_path, pg_conn):
    r = ev1_trace.run(spark, str(tmp_path), pg_conn, rev_id=REV)
    assert set(r["stages"]) == {"kafka","bronze","silver","postgres","api"}
    assert all(r["stages"][s]["seen_at"] for s in r["stages"])

def test_ev5_leakage_shows_inflated_then_honest_metric(spark):
    r = ev5_leakage.run(spark, fixture_df)
    assert r["pr_auc_with_leak"] > r["pr_auc_clean"] + 0.2

def test_ev6_skew_shows_wiki_key_more_imbalanced_than_title_key(spark):
    r = ev6_skew.run(spark, fixture_df)
    assert r["max_partition_share_by_wiki"] > r["max_partition_share_by_title"]

def test_ev2_watermark_drops_more_under_short_watermark(spark):
    r = ev2_watermark.run(spark, late_events_fixture)
    assert r["dropped_10min"] > r["dropped_2h"]

def test_every_script_writes_a_markdown_file(tmp_path):
    for mod in ALL_EVIDENCE: mod.main(out_dir=str(tmp_path))
    assert len(list(tmp_path.glob("*.md"))) == 6
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement.** `ev5` huấn luyện hai lần: một lần có thêm feature rò rỉ `has_later_edit`, một lần không, rồi so PR-AUC. `ev8` không chạy trong test — nó là script vận hành `docker kill` + đếm `rev_id`; test chỉ kiểm nó in ra markdown.
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 23: Tóm tắt thay đổi theo khuôn mẫu

**Files:**
- Create: `src/stream/summarize.py`
- Modify: `src/stream/main.py` (gọi trong `process_batch`), `sql/schema.sql` (thêm cột `summary TEXT` vào `risk_queue` và `hot_pages`)
- Test: `tests/test_summarize.py`

**Interfaces:**
- Consumes: `diff_client.DiffResult`
- Produces: `summarize.template_summary(added: str, removed: str, byte_delta: int) -> str`; `summarize.summarize_hot(rows, settings, llm=None) -> dict[int, str]` (chỉ gọi LLM khi `settings.summary_mode == "llm"`, có cache theo `rev_id`)

Đây là tính năng AI thứ ba ở spec §6.3. Khuôn mẫu là mặc định, LLM là tuỳ chọn — demo không được phụ thuộc quota.

- [ ] **Step 1: Write the failing test**

```python
def test_template_reports_words_added_and_removed():
    s = template_summary("alpha beta gamma", "delta", 120)
    assert "3 từ" in s and "1 từ" in s

def test_pure_deletion_is_described_as_deletion():
    assert "xoá" in template_summary("", "a b c", -40)

def test_empty_diff_gives_non_empty_fallback_text():
    assert template_summary("", "", 0).strip() != ""

def test_llm_is_not_called_in_template_mode():
    fake = FakeLLM()
    summarize_hot(rows, replace(SETTINGS, summary_mode="template"), llm=fake)
    assert fake.calls == 0

def test_llm_called_once_per_rev_id_then_cached():
    fake = FakeLLM()
    summarize_hot(rows_with_duplicate_rev_ids, replace(SETTINGS, summary_mode="llm"), llm=fake)
    assert fake.calls == len(set(r.rev_id for r in rows_with_duplicate_rev_ids))

def test_llm_failure_falls_back_to_template():
    out = summarize_hot(rows, replace(SETTINGS, summary_mode="llm"), llm=FailingLLM())
    assert all(v.strip() for v in out.values())
```

- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement.** `template_summary` sinh câu tiếng Việt từ số từ thêm/xoá và `byte_delta`, ví dụ `"Thêm 340 từ, xoá 12 từ (+1.8 KB)"`. `summarize_hot` chỉ nhận các dòng thuộc trang đang nóng, tối đa 50 dòng mỗi lần gọi.
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 22: README và tuyên bố sử dụng AI

**Files:**
- Create: `README.md`, `docs/ai-use-declaration.md`
- Modify: `.env.example`

- [ ] **Step 1: Viết `README.md`** — cách chạy (`make up`, `make test`, chạy `collector.py`, phát lại file, chạy batch), biến môi trường, bảng phiên bản thực tế của Spark/Kafka/Delta ghi lại khi demo, cảnh báo giữ `state/salt.txt`, mục pháp lý/đạo đức rút từ spec §2.
- [ ] **Step 2: Viết `docs/ai-use-declaration.md`** theo spec §12.
- [ ] **Step 3: Chạy toàn bộ test** — `pytest -v`, kỳ vọng tất cả PASS
- [ ] **Step 4: Commit**

---

### Task 24: Chat hỏi đáp sinh SQL

**Files:** Create `src/api/nl2sql.py`, `src/web/chat.html`, `sql/chat_role.sql`; Modify `src/api/main.py` (`POST /api/chat`), `src/web/app.js`, `README.md`, `docs/ai-use-declaration.md`; Test `tests/test_nl2sql.py`.

**Interfaces:** `nl2sql.SCHEMA_PROMPT: str`; `nl2sql.generate_sql(question, llm) -> str`; `nl2sql.validate_sql(sql) -> tuple[bool, str]`; `nl2sql.answer(question, conn, llm) -> dict` (`sql`, `rows`, `columns`, `error`).

Đây không phải AI nhóm train. Bảy lớp rào chắn: chỉ SELECT; chặn từ khoá ghi; chặn `;` ở giữa; whitelist 5 bảng; LIMIT 100; `statement_timeout = 5s`; role Postgres chỉ có SELECT. LLM lỗi hoặc SQL bị chặn thì trả lỗi rõ ràng và gợi ý 5 câu bấm sẵn.

---

## Thứ tự và phụ thuộc

```
Task 1 ─┬─ Task 2 ─┬─ Task 4 → Task 5 → Task 6              (Người A)
        │          ├─ Task 7, 8 → Task 9 → Task 10 → 11 → 12 → 13 → 14 → Task 23
        │          └─ Task 15 → Task 16 → Task 17            (Người B, cần Task 2)
        └─ Task 3 ───── Task 18 → Task 19 → Task 20          (Người C, cần Task 3)
                                                       Task 21 → Task 22
```

Task 16 tạo `scoring.py`; Task 14 nối nó vào `process_batch` và tạm dùng `risk_score = null` cho tới khi có model đầu tiên. Task 23 (tóm tắt) cần `diff_client` từ Task 7 và chạy sau Task 14.

Task 1–3 là nền tảng, phải xong trước, nên **một người làm cả ba trong buổi đầu** rồi hai người kia bắt đầu song song. Người C có thể làm Task 18–20 với Postgres seed dữ liệu giả, không cần chờ pipeline chạy thật.
