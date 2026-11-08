.PHONY: up down logs psql test collect replay stream batch nightly evidence web-demo spark-deps stream-logs chat-role
up:        ; docker compose up -d
down:      ; docker compose down
logs:      ; docker compose logs -f --tail=100
psql:      ; docker compose exec postgres psql -U wiki -d wiki
test:      ; PYTHONPATH=. python3 -m pytest -v
collect:   ; python3 collector.py
replay:    ; PYTHONPATH=. python3 -m src.ingest.replay
# --- chạy trong container spark (xem README) ---
SPARK_EXEC = docker compose exec spark
spark-deps: ; $(SPARK_EXEC) pip install --user -q delta-spark==3.2.1 'psycopg[binary]==3.2.3' numpy==1.26.4 pandas==2.2.3 setuptools==75.6.0
stream:    ; docker compose exec -d spark bash -c 'python -m src.stream.main >> /state/stream.log 2>&1'
stream-logs: ; $(SPARK_EXEC) tail -f /state/stream.log
batch:     ; $(SPARK_EXEC) python -m src.batch.main
nightly:   ; $(SPARK_EXEC) python -m src.batch.main --nightly
evidence:  ; $(SPARK_EXEC) bash -c 'for m in ev2_watermark ev5_leakage ev6_skew; do python -m evidence.$$m --delta-root /data/delta --out-dir /state/evidence; done'
web-demo:  ; PYTHONPATH=. python3 -m tests.demo_server
chat-role: ; docker compose exec -T postgres psql -U wiki -d wiki -v ON_ERROR_STOP=1 < sql/chat_role.sql
