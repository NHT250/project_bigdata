# Đặc tả thiết kế — Hệ thống phân tích Wikipedia thời gian thực

**Môn học:** Big Data Analytics (72ITDS40303) — Đại học Văn Lang
**Chủ đề:** Số 6 — Wikipedia thời gian thực
**Ngày viết:** 04/11/2026
**Hạn báo cáo cuối kỳ:** Thứ Năm 12/11/2026 (còn 8 ngày)
**Nhóm:** 3 thành viên

---

## 1. Mục tiêu và phạm vi

### 1.1 Yêu cầu của giảng viên

Một hệ thống cào dữ liệu nội dung theo thời gian thực, một website, và AI được huấn luyện để người dùng tìm hiểu / phân tích nội dung.

### 1.2 Phạm vi đã chốt

| Hạng mục | Quyết định |
|---|---|
| Nguồn dữ liệu | Wikimedia EventStreams (`recentchange`) + MediaWiki API (`action=compare`) |
| Wiki theo dõi | **Chỉ `enwiki`** |
| Loại sự kiện | `type = edit`, bỏ bot |
| Cào nội dung diff | **Mọi edit** (≈1–2 req/s, an toàn với rate limit) |
| Hạ tầng | 1 VM Oracle Cloud Always Free (4 core ARM, 24 GB RAM, 200 GB đĩa), Docker Compose |
| Stack | Kafka (KRaft) · Spark Structured Streaming · Delta Lake · PostgreSQL · FastAPI |

### 1.3 Tiêu chí thành công

1. Hệ thống chạy liên tục 24/7, thu và xử lý mọi lượt chỉnh sửa `enwiki`
2. Model phát hiện phá hoại **do nhóm tự huấn luyện** trên nhãn tự sinh, có PR-AUC đo được và so được với đường cơ sở
3. Website có 3 trang: dashboard live, tra cứu theo chủ đề, hỏi đáp
4. Có đủ 8 bằng chứng thực nghiệm ở mục 8 để bảo vệ

### 1.4 Ngoài phạm vi (đã cắt do hạn 8 ngày)

- Đa ngôn ngữ (>1 wiki)
- Đường cơ sở Lift Wing `revertrisk` của Wikimedia — *chỉ làm nếu còn thời gian ngày 11/11*
- Tóm tắt bằng LLM — **dùng khuôn mẫu**, API LLM là tuỳ chọn bật được
- Hỏi đáp ngôn ngữ tự nhiên sinh SQL — **dùng 5 câu hỏi mẫu bấm sẵn**
- Cluster nhiều node, đăng nhập/phân quyền, phân tích ảnh/media

---

## 2. Nguyên tắc pháp lý và đạo đức thu thập dữ liệu

Nguyên tắc bắt buộc, áp dụng cho mọi thành phần gọi ra ngoài:

1. **User-Agent khai báo thật.** Wikimedia yêu cầu UA mô tả kèm liên hệ:
   `VLU-BigData-RealtimeWiki/1.0 (Van Lang University course project; <email nhóm>)`
   **Tuyệt đối không giả dạng User-Agent trình duyệt** — vi phạm chính sách và bị chặn.
2. **Không dùng proxy xoay vòng.** Đổi IP để né rate limit là hành vi lạm dụng; tải của nhóm (1–2 req/s) dưới ngưỡng rất xa nên không cần.
3. **Giới hạn tần suất cứng:** tối đa 10 req/s tới MediaWiki API, có backoff luỹ thừa khi lỗi.
4. **Không lưu PII.** Edit vô danh trên Wikipedia dùng **địa chỉ IP làm tên tác giả**. IP bị **băm SHA-256 kèm salt ngay tại tầng ingest**, trước khi ghi xuống bất kỳ đâu. Trường `user`/`user_text` bị loại khỏi cả payload thô. Tham chiếu: NIST Privacy Framework.
5. **Tôn trọng bản quyền.** Nội dung Wikipedia là CC BY-SA 4.0 → mọi diff hiển thị trên website phải ghi nguồn và link tới bài gốc. Không thương mại hoá, dùng cho mục đích học thuật.
6. **Ưu tiên API chính thức thay vì cào HTML.** Hệ thống không parse HTML ở bất kỳ đâu. `robots.txt` của Wikipedia được kiểm tra và chụp lại làm bằng chứng, dù không áp dụng cho endpoint API.
7. **Có lưu vết và tự khôi phục.** `Last-Event-ID` (SSE) + Kafka offset + Spark checkpoint.

---

## 3. Kiến trúc

### 3.1 Sơ đồ

```
Wikimedia EventStreams (SSE)
        │
        ▼
  ingest (Python)                     collector.py (dự phòng/khởi động sớm)
  - lọc enwiki/edit/non-bot           - cùng logic lọc + băm IP
  - băm IP (salt cố định)             - ghi data/YYYY-MM-DD.jsonl.gz
  - lưu Last-Event-ID                 - dùng để phát lại vào Kafka
        │                                      │
        ▼                                      │
  Kafka topic: rc.raw  ◄───────────────────────┘ (replay)
  3 partition, key = page_title, retention 24h
        │
        ▼
  job-stream (Spark Structured Streaming, 1 job 2 stage)
  ├─ stage 1: ghi Delta bronze nguyên trạng (append, checkpoint)
  └─ stage 2: foreachBatch →
       - tính feature rẻ
       - gọi MediaWiki API lấy diff (≤10 req/s, timeout 3s, lỗi → diff=null)
       - chấm risk_score bằng model đã load
       - burst detection (cửa sổ trượt)
       - MERGE vào Delta silver (idempotent theo rev_id)
       - upsert vào Postgres (tầng phục vụ)
        │
        ▼
  job-batch (cron 15 phút)
  - gắn nhãn revert (mốc 60 phút) → Delta gold/training_set
  - train GBTClassifier → lưu model
  - ghi gold/model_metrics
  - tổng hợp page_hourly → ghi THẲNG vào Postgres
  - (hằng đêm) OPTIMIZE + VACUUM, kiểm lại nhãn ở mốc 24h
        │
        ▼
  PostgreSQL (tầng phục vụ, luôn < 200 MB)
        │
        ▼
  FastAPI → website (HTML + JS thuần + Chart.js, polling 5s)
```

### 3.2 Bảng trách nhiệm

| Thành phần | Trách nhiệm duy nhất | Không làm |
|---|---|---|
| `ingest` | Giữ kết nối SSE, lọc, băm IP, publish Kafka | Không parse diff, không tính toán |
| Kafka | Đệm bền, phân vùng, cho phép replay | Không biến đổi dữ liệu |
| `job-stream` | Bronze + enrich + scoring + serving | Không train model |
| `job-batch` | Gắn nhãn, train, tổng hợp, bảo trì bảng | Không phục vụ web |
| PostgreSQL | Phục vụ truy vấn web độ trễ thấp | Không là nơi lưu trữ gốc |
| `api` | Đọc Postgres, phục vụ website | **Không có Spark trên đường request** |

**Quyết định kiến trúc quan trọng:** API chỉ đọc PostgreSQL. Spark không nằm trên đường xử lý request. Mọi dữ liệu website cần đều được job ghi sẵn vào Postgres. Lý do: giữ Spark session trong tiến trình web làm mỗi query mất 3–8 giây và là nguồn lỗi lớn nhất lúc demo. Delta giữ đúng vai nơi lưu trữ gốc + huấn luyện.

### 3.3 Vì sao key Kafka là `page_title`

Burst detection cần mọi edit của cùng một trang vào cùng partition để giữ thứ tự và không phân tán state. Nếu key theo `wiki` thì toàn bộ dữ liệu dồn vào một partition — data skew kinh điển. Quyết định này được chứng minh bằng bằng chứng #6 (mục 8).

### 3.4 Docker Compose

`kafka` · `postgres` · `spark` (local mode trên VM 24 GB) · `ingest` · `api`
Mọi image dùng `platform: linux/arm64`.

---

## 4. Ba mốc thời gian

| Trường | Nghĩa | Ai sinh ra |
|---|---|---|
| `event_time` | Lúc lượt sửa xảy ra trên Wikipedia | Wikimedia |
| `ingest_time` | Lúc hệ thống nhận được qua SSE | nhóm |
| `process_time` | Lúc Spark xử lý | Spark |

Bình thường cách nhau 1–3 giây. Khi `ingest` mất mạng rồi reconnect bằng `Last-Event-ID`, Wikimedia phát lại event cũ → `event_time` lùi về quá khứ trong khi `process_time` là hiện tại. Đây là **dữ liệu đến muộn thật**, không dàn dựng, và là cơ sở cho bằng chứng #2.

**Watermark:** `withWatermark("event_time", "10 minutes")`.

---

## 5. Lược đồ dữ liệu

### 5.1 Delta bronze — `bronze/edits`

Nguyên trạng, append-only, **không dedup**.

```
event_id STRING, wiki STRING, page_id BIGINT, page_title STRING, namespace INT,
rev_id BIGINT, rev_parent_id BIGINT, user_hash STRING, is_anon BOOLEAN,
is_minor BOOLEAN, is_bot BOOLEAN, comment STRING, byte_old INT, byte_new INT,
tags ARRAY<STRING>, server_name STRING,
event_time TIMESTAMP, ingest_time TIMESTAMP, raw_json STRING
PARTITIONED BY (dt DATE)
```

`raw_json` được giữ (tốn thêm ~15% sau nén ZSTD) để không phải cào lại nếu sau này cần một trường chưa nghĩ tới. Partition theo `dt` — **không** theo `page_title` (sẽ sinh hàng trăm nghìn thư mục nhỏ).

### 5.2 Delta silver — `silver/edits_enriched`

Một dòng duy nhất cho mỗi `(wiki, rev_id)`.

```
[toàn bộ cột bronze]
+ diff_added STRING, diff_removed STRING, diff_status STRING  -- ok|timeout|error
+ byte_delta INT, comment_len INT, comment_empty BOOLEAN
+ has_url_added BOOLEAN, upper_ratio DOUBLE, repeat_char BOOLEAN
+ words_added INT, words_removed INT
+ page_edits_prev_1h INT, user_edits_prev_1h INT
+ risk_score DOUBLE, model_version STRING
PARTITIONED BY (dt DATE)
```

**Bronze cho phép trùng, silver thì không.** Replay bằng `Last-Event-ID` chắc chắn sinh event trùng. Bronze giữ nguyên vì nó là sự thật thô. Silver dùng `MERGE INTO ... ON wiki AND rev_id WHEN MATCHED THEN UPDATE` → idempotent: xử lý một event 5 lần vẫn cho đúng một dòng.

> Đây là cách đạt hiệu quả *effectively-once* end-to-end mà không cần Kafka transaction: **at-least-once từ Kafka + sink ghi idempotent**.

### 5.3 Delta gold

**`gold/training_set`** — chỉ chứa edit đã đủ 60 phút tuổi:
```
rev_id, wiki, dt, [toàn bộ feature từ silver],
label INT,              -- 1 = bị revert, 0 = còn nguyên
label_source STRING,    -- tag_rollback | tag_undo | comment_match | survived
labeled_at TIMESTAMP
```

**`gold/model_metrics`** — một dòng mỗi lần train:
```
model_version, trained_at, n_samples, n_positive,
pr_auc, roc_auc, precision_at_50, recall_at_50,
train_days, test_days, notes
```

### 5.4 PostgreSQL — tầng phục vụ

| Bảng | Nội dung | TTL |
|---|---|---|
| `edits_live` | Edit 2 giờ gần nhất + risk_score | 2 giờ |
| `hot_pages` | Trang đang bùng nổ + z-score | 1 giờ |
| `risk_queue` | Top 50 edit rủi ro cao + diff | 6 giờ |
| `metrics_minute` | edit/phút, anon%, byte_delta/phút | 24 giờ |
| `page_hourly` | Tổng hợp theo giờ, chỉ trang ≥5 edit/ngày | 30 ngày |

Job dọn rác chạy mỗi 10 phút theo TTL. Postgres là cache, không phải nơi lưu trữ; dữ liệu gốc luôn ở Delta.

### 5.5 Nén file và dung lượng

Trigger 10 giây sinh ≈8.600 file nhỏ/ngày/bảng. Chạy hằng đêm:

```sql
OPTIMIZE delta.`bronze/edits` WHERE dt = current_date - 1;
VACUUM  delta.`bronze/edits` RETAIN 168 HOURS;
```

Ước tính: ~172.000 edit/ngày × (metadata + diff ~2 KB) ≈ **350 MB/ngày**, 8 ngày ≈ 3 GB. Đĩa 200 GB dư sức.

---

## 6. Thành phần AI

### 6.1 Model phát hiện phá hoại — nhóm TỰ HUẤN LUYỆN

#### Nguồn nhãn (tự sinh, không gán tay)

| Tín hiệu | Cách nhận | Độ tin |
|---|---|---|
| Edit sau có tag `mw-rollback` | Rollback chỉ dùng cho phá hoại rõ ràng | Cao nhất |
| Edit sau có tag `mw-undo` trỏ về `rev_parent_id` của ta | Undo có chủ đích | Cao |
| Comment edit sau khớp `Reverted edits by [[Special:Contributions/...]]` | Mẫu comment chuẩn | Trung bình |
| Không có gì sau 60 phút | `label = 0` | Tốt |

`label_source` được lưu để đo: nhãn từ `rollback` có cho model tốt hơn nhãn từ `comment_match`?

#### Giới hạn phải khai báo chủ động

> **"Bị revert" không đồng nghĩa "phá hoại".** Edit thiện chí nhưng sai định dạng, sai nguồn, hoặc trùng nội dung cũng bị revert. Model thực chất học *"edit có khả năng bị cộng đồng gỡ bỏ"*, rộng hơn phá hoại. Chính Wikimedia tách thành hai model riêng (`damaging` và `goodfaith`) vì lý do này.

#### Feature — chỉ dùng thông tin biết được TẠI thời điểm edit

**Được dùng:** `byte_delta`, `|byte_delta|`, `is_anon`, `is_minor`, `comment_len`, `comment_empty`, giờ trong ngày, ngày trong tuần, `upper_ratio`, `repeat_char`, `has_url_added`, `words_added`, `words_removed`, `page_edits_prev_1h`, `user_edits_prev_1h`.

**Tuyệt đối không dùng:** tag của edit SAU; sự tồn tại của edit sau; `risk_score` do chính model chấm; số lần trang bị revert về sau.

Hai feature `*_prev_1h` phải tính bằng window `rangeBetween` sắp theo `event_time`, **chỉ trên cửa sổ quá khứ**. Nếu làm sai, model nhìn thấy tương lai → accuracy giả ~97% rồi sập hoàn toàn khi chạy thật. Có test riêng chặn việc này (`test_features_no_future_leak`).

#### Mô hình và đánh giá

```
Đường cơ sở 1: luật thủ công  (is_anon AND |byte_delta| > 500)
Mô hình chính: GBTClassifier (Spark MLlib)
Đường cơ sở 2: Lift Wing revertrisk  [chỉ nếu còn thời gian 11/11]
```

- **Chia tập theo thời gian, không random.** Train trên các ngày đầu, test trên các ngày cuối. Chia random trên dữ liệu chuỗi thời gian là rò rỉ.
- **Mất cân bằng lớp:** chỉ ~3–7% edit bị revert → dùng `weightCol` theo tỉ lệ nghịch.
- **Báo cáo PR-AUC và precision@top-50, KHÔNG báo cáo accuracy.** Một model luôn trả "bình thường" đạt accuracy ~95% và vô dụng hoàn toàn. Điều này được nói rõ trên slide.

### 6.2 Phát hiện sự kiện nóng — THỐNG KÊ, không phải AI

```
Cửa sổ trượt 5 phút, bước 1 phút, theo (wiki, page_title)
  → đếm: số edit, số user duy nhất, tổng |byte_delta|
  → so với đường nền EWMA 24h của CHÍNH trang đó
  → z = (count − ewma) / std
  → "nóng" khi z > 3 VÀ số_user_duy_nhất ≥ 3
```

Điều kiện `≥3 user` là bắt buộc: không có nó, hai người cãi nhau sửa qua sửa lại (edit war) sẽ bị báo là sự kiện nóng toàn cầu — đây là phản ví dụ được đưa lên slide.

**Khởi động lạnh:** trang mới chưa có EWMA → dùng đường nền toàn cục trong 24 giờ đầu.
**State:** watermark 10 phút chặn state phình vô hạn.

### 6.3 Tóm tắt thay đổi — SỬ DỤNG, không train

Mặc định: **sinh theo khuôn mẫu** từ `diff_added`/`diff_removed`, ví dụ
`"Thêm 340 từ ở mục Phản ứng quốc tế; xoá 2 nguồn dẫn"`.

Tuỳ chọn bật được: gọi API LLM, **chỉ cho các trang đang nóng** (~20–50 lượt/giờ), có cache theo `rev_id`. Không train, không chạy model local (VM ARM không có GPU).

Demo không bao giờ phụ thuộc vào quota hay mạng ra ngoài.

### 6.4 Bảng tổng kết (đưa lên slide)

| Tính năng | Bản chất | Nhóm train? | Thước đo |
|---|---|---|---|
| Phát hiện phá hoại | Học có giám sát | **Có** | PR-AUC, precision@50, so luật thủ công |
| Phát hiện sự kiện nóng | Thống kê cửa sổ trượt | Không | Kiểm thủ công 20 cảnh báo: đúng/sai |
| Tóm tắt thay đổi | Khuôn mẫu / LLM có sẵn | Không | Đánh giá người: 30 mẫu, thang 1–5 |

---

## 7. API và website

### 7.1 Endpoint

| Endpoint | Nguồn | Dùng ở |
|---|---|---|
| `GET /api/metrics?hours=6` | `metrics_minute` | Biểu đồ edit/phút |
| `GET /api/hot?limit=20` | `hot_pages` | Bảng sự kiện nóng |
| `GET /api/risk?limit=50` | `risk_queue` | Panel edit rủi ro + diff |
| `GET /api/live?limit=100` | `edits_live` | Dòng edit chảy liên tục |
| `GET /api/page?title=X&days=7` | `page_hourly` | Trang tra cứu chủ đề |
| `GET /api/ask?qid=1..5` | SQL cố định | Hỏi đáp |

### 7.2 Công nghệ frontend

**HTML + JavaScript thuần + Chart.js qua CDN. Không React, không build step.** FastAPI serve file tĩnh. Cập nhật live bằng **polling 5 giây**, không WebSocket.

Lý do: thiết lập toolchain build ăn gần một ngày của 8 ngày và không được điểm nào — đánh giá nhắm vào kiến trúc dữ liệu, không nhắm vào frontend framework.

### 7.3 Ba trang

**Dashboard** (trang demo chính): thẻ số (edit/phút, % vô danh, số trang nóng, số edit rủi ro cao) · biểu đồ đường edit/phút 6 giờ · bảng **Sự kiện nóng** (trang, z-score, số edit, số user) · bảng **Edit rủi ro cao** (trang, risk_score, diff thu gọn, link Wikipedia) · dòng chảy edit mới nhất.

Panel rủi ro là nơi model tự train của nhóm hiển thị ra mặt người dùng.

**Tra cứu chủ đề:** nhập tên trang → biểu đồ edit/giờ 7 ngày + risk_score theo thời gian + danh sách tóm tắt thay đổi + user_hash đã sửa.

**Hỏi đáp:** 5 nút bấm sẵn, mỗi nút là một câu SQL viết trước:
1. Trang nào nóng nhất 6 giờ qua?
2. Edit nào rủi ro cao nhất hôm nay?
3. Tỉ lệ edit vô danh thay đổi thế nào trong 24h?
4. Trang nào bị sửa nhiều nhất tuần này?
5. Giờ nào trong ngày có nhiều phá hoại nhất?

*Tuỳ chọn nếu còn thời gian 11/11:* nối LLM sinh SQL, với rào chắn — chỉ `SELECT`, chỉ 5 bảng đã biết, bắt buộc `LIMIT 100`, chặn mọi từ khoá ghi.

Mỗi diff hiển thị kèm ghi nguồn CC BY-SA 4.0 và link bài gốc.

---

## 8. Bằng chứng thực nghiệm

| # | Bằng chứng | Cách tạo | Tiêu chí của giảng viên |
|---|---|---|---|
| 1 | Vết một event đi hết hệ thống | Chọn 1 `rev_id`, in timestamp tại từng tầng | "cho thấy dữ liệu/state/control di chuyển ở đâu" |
| 2 | Thực nghiệm watermark | Ngắt mạng `ingest` 30 phút → nối lại → so watermark 10 phút vs 2 giờ: record nào phát ra, nào bị bỏ | "bằng chứng tái lập được" |
| 3 | `DESCRIBE HISTORY` trước/sau `OPTIMIZE` | Số file 8.600 → ~20, thời gian query giảm | Nối trực tiếp bài P14 của nhóm |
| 4 | Bảng PR-AUC + ma trận nhầm lẫn | Chia tập theo thời gian, so luật thủ công | "bằng chứng có thể đo" |
| 5 | **Demo rò rỉ dữ liệu có chủ đích** | Cố tình thêm feature "có edit sau hay không" → accuracy giả ~97% → bỏ ra → về số thật | "ít nhất một phản ví dụ" |
| 6 | Demo data skew | Key Kafka theo `wiki` vs `page_title` → bảng lệch partition | Bài P04 |
| 7 | Đo mốc nhãn 60 phút | Job đêm kiểm lại nhãn ngày hôm trước ở mốc 24h → % revert muộn bị bỏ sót | "nêu giới hạn vận hành" |
| 8 | Phục hồi sau sự cố | `docker kill job-stream` → restart → đếm `rev_id` trước/sau bằng nhau | "điều kiện thất bại" |

Bằng chứng #5 là ưu tiên cao: tự phơi ra một lỗi rồi chứng minh biết sửa thuyết phục hơn mọi biểu đồ đẹp, và trả lời trước câu hỏi khó nhất có thể bị đặt.

### 8.1 Test code bắt buộc

| Test | Kiểm tra |
|---|---|
| `test_features_no_future_leak` | Feature cửa sổ quá khứ không nhìn thấy record tương lai |
| `test_silver_merge_idempotent` | Chạy lại cùng một lô 2 lần → số dòng không đổi |
| `test_label_logic` | Fixture rollback / undo / comment / sống sót → nhãn đúng |
| `test_replay_end_to_end` | Phát lại một file `.jsonl.gz` → đếm bronze/silver khớp |

---

## 9. Chế độ thất bại

| Sự cố | Hệ thống làm gì | Mất gì |
|---|---|---|
| Mất mạng tới EventStreams | Reconnect + `Last-Event-ID` | Không mất event |
| `job-stream` crash | Restart từ checkpoint, đọc lại từ Kafka offset | Diff của batch đang chạy phải gọi lại API |
| MediaWiki API timeout | `diff_status = timeout`, model dùng feature metadata | Giảm độ chính xác cho edit đó |
| Kafka đầy đĩa | Retention 24h tự xoá | Mất khả năng replay quá 24h |
| PostgreSQL chết | Dashboard trắng, bronze/silver vẫn ghi | Chỉ mất tầng phục vụ, không mất dữ liệu |
| `collector.py` bị kill -9 | Mỗi flush là một gzip member hoàn chỉnh | Tối đa 200 record cuối (~100 giây) |
| Mất `state/salt.txt` | Hash không còn nhất quán | Feature `user_edits_prev_1h` sai với dữ liệu cũ |

---

## 10. Lịch 8 ngày và phân công

| Ngày | Người A — hạ tầng | Người B — dữ liệu & AI | Người C — web |
|---|---|---|---|
| **04/11 tối** | **Chạy `collector.py`** trên laptop + đăng ký Oracle | — | — |
| 05–06/11 | VM Oracle + Docker Compose (Kafka, Postgres, Spark) · `ingest` → Kafka → bronze | Script gắn nhãn revert, chạy thử trên file `.jsonl.gz` đã thu | Khung FastAPI + trang tĩnh với dữ liệu giả |
| 07–08/11 | `job-stream` stage 2: cào diff, feature, silver | Train GBT + luật thủ công, bảng PR-AUC | Dashboard thật (đọc Postgres) |
| 09–10/11 | Burst detection + 5 bảng Postgres + job dọn rác | `model_metrics`, chia tập theo thời gian, bằng chứng #4 #5 | Trang tra cứu + 5 nút hỏi đáp |
| 11/11 | `OPTIMIZE`/`VACUUM` + bằng chứng #2 #3 #6 #8 | Bằng chứng #7, 4 test code | Hoàn thiện, chạy thử toàn hệ end-to-end |
| 12/11 sáng | Slide, diễn tập Q&A — **cả 3 người phải trả lời được mọi phần** | | |

**Nguyên tắc ưu tiên khi trượt lịch:** cắt theo thứ tự — (1) hỏi đáp LLM, (2) Lift Wing, (3) trang tra cứu chủ đề, (4) tóm tắt. **Không bao giờ cắt:** model tự train, chia tập theo thời gian, PR-AUC, dashboard, bằng chứng #4 #5.

---

## 11. Rủi ro đã biết

| Rủi ro | Xác suất | Giảm thiểu |
|---|---|---|
| VM Oracle ARM báo "out of capacity" | Cao | Thử region Singapore/Seoul, script retry; dự phòng Azure for Students; `collector.py` chạy laptop nên không chặn tiến độ dữ liệu |
| Spark + Delta trên ARM gặp lỗi image | Trung bình | Dùng image arm64 chính thức, kiểm tra ngày 05/11 — nếu tắc, chuyển Spark về chạy trực tiếp (không Docker) |
| Không kịp phần web | Trung bình | Dashboard làm trước, trang tra cứu và hỏi đáp làm sau; dữ liệu giả cho phép làm web song song từ ngày đầu |
| Model có số liệu quá đẹp | Trung bình | `test_features_no_future_leak` chặn; nếu PR-AUC > 0.95 thì mặc định là có rò rỉ, phải điều tra |
| Dữ liệu huấn luyện quá ít | Thấp | `collector.py` chạy từ 04/11 → đến 10/11 có ~1 triệu edit, ~40–70 nghìn mẫu dương |

---

## 12. Tuyên bố sử dụng AI

Theo mục "Source and AI-use integrity" của kế hoạch môn học: nhóm ghi rõ trong slide cuối rằng Claude được dùng để hỗ trợ thiết kế kiến trúc, viết mã và soạn nội dung. Mọi số liệu, biểu đồ, bằng chứng thực nghiệm và trích dẫn do nhóm tự tạo và tự kiểm tra. Không trích dẫn phản hồi của AI làm bằng chứng kỹ thuật.

---

## 13. Nguồn tham khảo

- [API:Recent changes stream](https://www.mediawiki.org/wiki/API:Recent_changes_stream) — MediaWiki
- [Event Platform/EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams) — Wikitech
- [Apache Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Apache Kafka Documentation — Design](https://kafka.apache.org/documentation/#design)
- [Delta Lake Transaction Log Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [Apache Spark ML Pipelines](https://spark.apache.org/docs/latest/ml-pipeline.html) · [ML Tuning](https://spark.apache.org/docs/latest/ml-tuning.html)
- [NIST Privacy Framework](https://www.nist.gov/privacy-framework)
- [NIST AI Risk Management Framework 1.0](https://www.nist.gov/itl/ai-risk-management-framework)
- Reis & Housley, *Fundamentals of Data Engineering* (2022), ch. 3, 5, 6, 7

*Ghi lại phiên bản thực tế đã dùng của Spark, Kafka, Delta khi demo.*
