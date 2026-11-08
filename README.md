# WikiPulse

Hệ thống phân tích Wikipedia thời gian thực. Đồ án Big Data Analytics (72ITDS40303), Đại học Văn Lang, chủ đề 6.

Hệ thống thu mọi lượt chỉnh sửa trên Wikipedia tiếng Anh ngay khi chúng xảy ra, lấy nội dung thay đổi qua API chính thức, chấm điểm rủi ro bằng model nhóm tự huấn luyện, và phục vụ một website gồm dashboard live, tra cứu theo trang và hỏi đáp.

Thiết kế đầy đủ nằm ở [docs/superpowers/specs/2026-11-04-realtime-wikipedia-design.md](docs/superpowers/specs/2026-11-04-realtime-wikipedia-design.md). Kế hoạch triển khai từng bước nằm ở [docs/superpowers/plans/2026-11-04-realtime-wikipedia.md](docs/superpowers/plans/2026-11-04-realtime-wikipedia.md).

## Kiến trúc

```
Wikimedia EventStreams (SSE)          collector.py (chạy laptop, ghi data/*.jsonl.gz)
        │                                     │
        ▼                                     ▼ make replay
  ingest  ──────────────►  Kafka rc.raw (3 partition, key = page_title, 24h)
  lọc enwiki/edit/không bot             │
  băm IP + Last-Event-ID                ▼
                           job-stream (Spark Structured Streaming, trigger 10 giây)
                           ├─ stage 1: Delta bronze, append, không dedup
                           └─ stage 2: diff MediaWiki → feature → risk_score
                                       → MERGE silver → sự kiện nóng → Postgres
                                        │
                           job-batch (cron 15 phút): nhãn revert → train GBT
                           → gold/model_metrics → page_hourly → dọn TTL
                                        │
                                        ▼
                           PostgreSQL (tầng phục vụ) → FastAPI → website
```

API chỉ đọc PostgreSQL. Spark không bao giờ nằm trên đường xử lý request của website.

| Thư mục | Nội dung |
|---|---|
| `src/ingest` | SSE → Kafka, phát lại file `.jsonl.gz` |
| `src/stream` | lấy diff, feature, chấm điểm, MERGE silver, phát hiện sự kiện nóng, ghi Postgres |
| `src/batch` | gắn nhãn revert, huấn luyện và đánh giá, `page_hourly`, OPTIMIZE/VACUUM |
| `src/api`, `src/web` | FastAPI 6 endpoint, 3 trang HTML + JavaScript thuần |
| `evidence` | script sinh 6 bằng chứng thực nghiệm |
| `sql/schema.sql` | 5 bảng tầng phục vụ |

## Chạy trên VM

Yêu cầu: Docker Compose. Mọi image dùng `platform: linux/arm64` (Oracle Cloud Ampere). Trên máy x86, xoá các dòng `platform:`.

1. Cấu hình. Điền email thật vào `WIKI_CONTACT`; Wikimedia yêu cầu liên hệ trong User-Agent.

   ```bash
   cp .env.example .env
   ```

2. Dựng hạ tầng và cài thư viện cho container Spark (một lần):

   ```bash
   make up
   ```

   ```bash
   make spark-deps
   ```

3. Chạy job streaming ở nền, xem log:

   ```bash
   make stream
   ```

   ```bash
   make stream-logs
   ```

4. Job batch theo cron 15 phút, bảo trì lúc 2 giờ sáng (`crontab -e` trên VM, sửa đường dẫn):

   ```
   */15 * * * * cd /home/ubuntu/realtime-wiki && make batch >> batch.log 2>&1
   0 2 * * *    cd /home/ubuntu/realtime-wiki && make nightly >> nightly.log 2>&1
   ```

5. Website: <http://IP-VM:8000>. Mở cổng 8000 trong Security List của Oracle Cloud.

Model đầu tiên chỉ được huấn luyện khi tập train có ít nhất 20 edit bị revert. Trước đó, cột điểm rủi ro hiển thị "–" (null), không phải số bịa.

### Dữ liệu thu trước khi có VM

`collector.py` chạy trên laptop, không cần Kafka hay Spark. Điền `CONTACT` trong file rồi:

```bash
make collect
```

Khi hạ tầng sẵn sàng, chép thư mục `data/` lên VM và phát lại vào Kafka:

```bash
make replay
```

Chép cả `state/salt.txt` lên VM. Salt phải giữ nguyên giữa collector, ingest và mọi lần chạy, nếu không mã băm người dùng không còn nhất quán và feature `user_edits_prev_1h` sai với dữ liệu cũ. **Không commit salt** (đã có trong `.gitignore`).

### Biến môi trường

| Biến | Bắt buộc | Mặc định | Ý nghĩa |
|---|---|---|---|
| `KAFKA_BOOTSTRAP` | có | | địa chỉ Kafka |
| `KAFKA_TOPIC` | có | | `rc.raw` |
| `PG_DSN` | có | | PostgreSQL tầng phục vụ |
| `DELTA_ROOT` | có | | gốc Delta Lake (bronze/silver/gold/models) |
| `WIKI_CONTACT` | có | | email trong User-Agent |
| `WIKIS` | | `enwiki` | wiki theo dõi |
| `DIFF_MAX_RPS` | | `10` | trần số request/giây tới MediaWiki API |
| `DIFF_MAX_CHARS` | | `10000` | cắt diff dài hơn mức này |
| `WATERMARK_MINUTES` | | `10` | watermark event-time |
| `LABEL_CUTOFF_MINUTES` | | `60` | mốc gắn nhãn revert |
| `RISK_QUEUE_SIZE` | | `50` | số edit giữ trong bảng rủi ro |
| `SUMMARY_MODE` | | `template` | `template` hoặc `llm` |
| `SALT_PATH`, `STATE_PATH`, `DATA_DIR`, `CHECKPOINT_ROOT` | | `state/...` | file trạng thái cục bộ |

## Chạy test

```bash
pip install -r requirements.txt pgserver httpx
```

```bash
make test
```

- Test Spark cần Java 17 (Spark 3.5 không chạy với Java 21+) và Python ≤ 3.12.
- Test Postgres dùng `PG_DSN` nếu có, nếu không tự khởi động Postgres nhúng bằng `pgserver`.
- Test giao diện cần trình duyệt Playwright: `playwright install chromium`. Thiếu trình duyệt thì các test này được bỏ qua.
- Thiếu gói nào thì nhóm test cần gói đó được bỏ qua, phần còn lại vẫn chạy.

Bốn test bắt buộc theo spec §8.1: `test_features_no_future_leak`, `test_silver_merge_idempotent`, `test_label_logic` (`tests/test_labeling.py`), `test_replay_end_to_end`.

### Làm web không cần Docker

```bash
make web-demo
```

Lệnh này khởi động Postgres nhúng, nạp dữ liệu giả rồi mở API và website tại <http://localhost:8000>.

## Bằng chứng thực nghiệm

Mỗi script trong `evidence/` ghi một file markdown vào `evidence/out/`. Không truyền `--delta-root` thì script chạy trên dữ liệu tổng hợp và ghi rõ "chế độ demo"; **số đưa vào báo cáo phải lấy từ chế độ thật trên VM.**

| # | Script | Lệnh chế độ thật |
|---|---|---|
| 1 | `ev1_trace.py` | `python -m evidence.ev1_trace --delta-root /data/delta --rev-id <REV> --api http://localhost:8000` |
| 2 | `ev2_watermark.py` | `python -m evidence.ev2_watermark --delta-root /data/delta --day <YYYY-MM-DD>` |
| 3 | `ev3_optimize.py` | `python -m evidence.ev3_optimize --delta-root /data/delta --day <hôm qua>` |
| 4 | bảng `gold/model_metrics` | PR-AUC, precision@50, ma trận nhầm lẫn, so luật thủ công; job batch ghi mỗi lần train |
| 5 | `ev5_leakage.py` | `python -m evidence.ev5_leakage --delta-root /data/delta` |
| 6 | `ev6_skew.py` | `python -m evidence.ev6_skew --delta-root /data/delta` |
| 7 | bảng `gold/label_audit` | `make nightly` ghi tỉ lệ revert muộn bị nhãn 60 phút bỏ sót |
| 8 | `ev8_recovery.py` | quy trình `docker kill` trong docstring của script, chạy `--phase before` rồi `--phase after` |

Các lệnh chạy trong container Spark: `docker compose exec spark <lệnh>`.

## Chat hỏi đáp

Trang `chat.html` nhận câu hỏi tiếng Việt hoặc tiếng Anh, sinh SQL rồi hiện cả câu SQL lẫn bảng kết quả.

**Đây không phải AI nhóm huấn luyện.** SQL do một mô hình ngôn ngữ có sẵn (Claude, gọi qua API) sinh ra. Model nhóm tự huấn luyện là GBT chấm `risk_score`, nằm ở panel "Edit rủi ro cao". Mô hình chỉ nhận lược đồ bảng và câu hỏi, không nhận dữ liệu trong bảng.

Cài đặt: điền `ANTHROPIC_API_KEY` vào `.env` rồi `make up`. Nếu volume Postgres đã tồn tại từ trước, chạy thêm một lần để tạo role chỉ đọc:

```bash
make chat-role
```

Bảy lớp rào chắn nằm ở `src/api/nl2sql.py` và `sql/chat_role.sql`: chỉ SELECT, chặn từ khoá ghi, chặn nhiều câu lệnh, chỉ 5 bảng đã biết, LIMIT tối đa 100, `statement_timeout` 5 giây, và role `wiki_chat` chỉ có quyền SELECT. Ngoài ra chỉ cho phép một danh sách hàm SQL cố định, và toàn hệ thống nhận tối đa 20 câu chat mỗi phút để giới hạn chi phí API.

Khi thiếu API key, hết quota, mất mạng hoặc SQL bị chặn, trang hiện lỗi rõ ràng kèm link tới 5 câu hỏi bấm sẵn. Demo không phụ thuộc vào LLM.

## Pháp lý và đạo đức thu thập dữ liệu

Rút từ spec §2, áp dụng cho mọi thành phần gọi ra ngoài:

1. **User-Agent khai báo thật** kèm email liên hệ. Code từ chối chạy nếu UA giống trình duyệt.
2. **Không proxy xoay vòng.** Tải của nhóm thấp hơn ngưỡng của Wikimedia rất xa.
3. **Tối đa 10 request/giây** tới MediaWiki API, timeout 3 giây, backoff khi lỗi.
4. **Không lưu thông tin cá nhân.** Edit vô danh dùng địa chỉ IP làm tên tác giả; IP và tên người dùng bị băm SHA-256 kèm salt ngay ở tầng ingest, trước khi ghi xuống bất kỳ đâu.
5. **Ghi nguồn CC BY-SA 4.0.** Mọi diff trên website kèm link về bài gốc. Chỉ dùng cho mục đích học thuật.
6. **Chỉ dùng API chính thức**, không cào HTML.
7. **Có lưu vết và tự khôi phục:** `Last-Event-ID`, Kafka offset, Spark checkpoint.

Giới hạn của model phải nói rõ khi trình bày: **"bị revert" không đồng nghĩa "phá hoại".** Model học "edit có khả năng bị cộng đồng gỡ bỏ", rộng hơn phá hoại.

## Phiên bản

Cột "khi demo" điền lúc chạy thật trên VM (spec §13). Lấy bằng `docker compose exec spark spark-submit --version`, `docker compose exec kafka kafka-topics.sh --version` và `pip show delta-spark`.

| Thành phần | Ghim trong repo | Khi demo |
|---|---|---|
| Spark / PySpark | 3.5.3 | |
| Delta Lake (`delta-spark`) | 3.2.1 | |
| Kafka (image `bitnamilegacy/kafka`) | 3.9 | |
| PostgreSQL | 16 | |
| Python (ingest, api) | 3.11 | |
| FastAPI | 0.115.6 | |

## Sử dụng AI

Xem [docs/ai-use-declaration.md](docs/ai-use-declaration.md).
