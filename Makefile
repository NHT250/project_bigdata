.PHONY: up down logs psql test collect replay nightly
up:        ; docker compose up -d
down:      ; docker compose down
logs:      ; docker compose logs -f --tail=100
psql:      ; docker compose exec postgres psql -U wiki -d wiki
test:      ; PYTHONPATH=. python3 -m pytest -v
collect:   ; python3 collector.py
replay:    ; PYTHONPATH=. python3 -m src.ingest.replay
nightly:   ; PYTHONPATH=. python3 -m src.batch.main --nightly
