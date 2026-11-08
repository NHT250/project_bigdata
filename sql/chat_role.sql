-- Lớp rào chắn cuối của chat hỏi đáp (Task 24): role chỉ có SELECT trên 5 bảng.
-- Kể cả khi SQL do LLM sinh vượt qua mọi kiểm tra chuỗi, database vẫn từ chối ghi.
-- Chạy được nhiều lần. Áp dụng cho lược đồ hiện tại (current_schema).

DO $role$
DECLARE
    t text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wiki_chat') THEN
        CREATE ROLE wiki_chat LOGIN PASSWORD 'wiki_chat'
            NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    ALTER ROLE wiki_chat SET statement_timeout = '5s';
    ALTER ROLE wiki_chat SET default_transaction_read_only = on;

    EXECUTE format('GRANT USAGE ON SCHEMA %I TO wiki_chat', current_schema());
    FOREACH t IN ARRAY ARRAY['edits_live', 'hot_pages', 'risk_queue', 'metrics_minute', 'page_hourly']
    LOOP
        EXECUTE format('GRANT SELECT ON %I TO wiki_chat', t);
    END LOOP;
END
$role$;
