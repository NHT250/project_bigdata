#!/usr/bin/env python3
"""
collector.py - Thu du lieu tho tu Wikimedia EventStreams xuong file .jsonl.gz

Do an Big Data Analytics - Realtime Wikipedia
Van Lang University, 72ITDS40303

Muc dich: BAT DAU THU DU LIEU NGAY, doc lap hoan toan voi Kafka/Spark/Delta.
Cac file .jsonl.gz sinh ra se duoc phat lai (replay) vao Kafka khi stack san sang.

Chay:   python3 collector.py
Dung:   Ctrl+C (an toan - flush het du lieu truoc khi thoat)

Khong can cai them thu vien nao: chi dung thu vien chuan cua Python 3.
"""

import gzip
import hashlib
import json
import os
import secrets
import signal
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# CAU HINH - sua o day
# ---------------------------------------------------------------------------

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# BAT BUOC: dien lien he cua nhom (email nhom hoac link repo GitHub).
# Wikimedia yeu cau User-Agent khai bao that - gia dang trinh duyet se bi chan.
CONTACT = ""   # <<< DIEN VAO DAY, vi du: "nhom6.bigdata@gmail.com"

UA = f"VLU-BigData-RealtimeWiki/1.0 (Van Lang University course project; {CONTACT})"

WIKIS = {"enwiki"}          # chi theo doi Wikipedia tieng Anh
KEEP_TYPES = {"edit"}       # chi lay luot chinh sua
DROP_BOTS = True            # bo cac edit do bot thuc hien

DATA_DIR = "data"           # noi ghi file .jsonl.gz
STATE_DIR = "state"         # noi luu Last-Event-ID va salt

FLUSH_EVERY_EVENTS = 200    # flush sau moi N su kien
FLUSH_EVERY_SECONDS = 10    # ... hoac sau moi N giay
STATS_EVERY_SECONDS = 60    # in thong ke moi N giay

MAX_BACKOFF = 30            # giay, gioi han thoi gian cho khi reconnect

# ---------------------------------------------------------------------------

STATE_FILE = os.path.join(STATE_DIR, "last_event_id.txt")
SALT_FILE = os.path.join(STATE_DIR, "salt.txt")

running = True


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def handle_signal(signum, frame):
    global running
    running = False
    log("Nhan tin hieu dung - dang flush du lieu...")


def load_or_create_salt():
    """
    Salt dung de bam IP/username. PHAI co dinh giua cac lan chay, neu khong
    cung mot nguoi se cho ra hash khac nhau va feature dem hanh vi se sai.
    KHONG commit file nay len git.
    """
    os.makedirs(STATE_DIR, exist_ok=True)
    if os.path.exists(SALT_FILE):
        with open(SALT_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    salt = secrets.token_hex(16)
    with open(SALT_FILE, "w", encoding="utf-8") as f:
        f.write(salt)
    log(f"Da tao salt moi tai {SALT_FILE} - GIU LAI file nay, dung xoa, dung commit")
    return salt


def load_last_event_id():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            v = f.read().strip()
            return v or None
    return None


def save_last_event_id(event_id):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(event_id)
    os.replace(tmp, STATE_FILE)   # ghi nguyen tu, khong bi file rong khi kill


def hash_user(user, salt):
    """Bam ten/IP nguoi dung. IP la du lieu ca nhan -> khong bao gio ghi xuong dia."""
    if user is None:
        return None
    h = hashlib.sha256((salt + "|" + str(user)).encode("utf-8")).hexdigest()
    return h[:16]


def sanitize(ev, salt):
    """
    Bo moi dau vet PII khoi payload tho truoc khi luu.
    Edit vo danh tren Wikipedia dung DIA CHI IP lam ten tac gia.
    """
    raw = dict(ev)
    raw.pop("user", None)
    raw.pop("user_text", None)
    return raw


def to_record(ev, salt):
    """Chuyen su kien EventStreams thanh mot dong record theo schema bronze."""
    length = ev.get("length") or {}
    revision = ev.get("revision") or {}
    meta = ev.get("meta") or {}

    is_anon = bool(ev.get("user_is_anon", False))
    if not is_anon:
        # EventStreams cu khong co user_is_anon -> suy ra tu dang IP
        u = str(ev.get("user", ""))
        is_anon = u.count(".") == 3 or ":" in u

    return {
        "event_id": meta.get("id"),
        "wiki": ev.get("wiki"),
        "page_id": ev.get("id"),
        "page_title": ev.get("title"),
        "namespace": ev.get("namespace"),
        "rev_id": revision.get("new"),
        "rev_parent_id": revision.get("old"),
        "user_hash": hash_user(ev.get("user"), salt),
        "is_anon": is_anon,
        "is_minor": bool(ev.get("minor", False)),
        "is_bot": bool(ev.get("bot", False)),
        "comment": ev.get("comment"),
        "byte_old": length.get("old"),
        "byte_new": length.get("new"),
        "tags": ev.get("tags") or [],
        "server_name": ev.get("server_name"),
        # event_time: luc luot sua xay ra TREN Wikipedia
        "event_time": datetime.fromtimestamp(
            ev["timestamp"], tz=timezone.utc
        ).isoformat() if ev.get("timestamp") else meta.get("dt"),
        # ingest_time: luc may cua nhom nhan duoc
        "ingest_time": datetime.now(timezone.utc).isoformat(),
        "raw": sanitize(ev, salt),
    }


class RotatingWriter:
    """
    Ghi vao data/YYYY-MM-DD.jsonl.gz, tu doi file khi sang ngay moi (UTC).

    Chien luoc GHI THEO LO: giu record trong bo nho, moi lan flush thi noi
    them MOT gzip member hoan chinh vao file. File .gz noi nhieu member van
    la file .gz hop le va doc binh thuong bang gzip/zcat/pandas.

    Vi sao khong ghi truc tiep roi flush(Z_SYNC_FLUSH): cach do giu duoc byte
    tren dia nhung file thieu end-of-stream marker, nen neu may sap thi trinh
    doc thong thuong bao EOFError. Ghi theo lo thi moi member deu dong tron
    ven -> may sap chi mat toi da mot lo (<= FLUSH_EVERY_EVENTS record).
    """

    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.day = None
        self.buf = []
        os.makedirs(data_dir, exist_ok=True)

    def _path(self, day):
        return os.path.join(self.data_dir, f"{day}.jsonl.gz")

    def write(self, record):
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if day != self.day:
            self.flush()          # dong lo cuoi cua ngay cu truoc khi doi file
            self.day = day
            log(f"Ghi vao {self._path(day)}")
        self.buf.append(json.dumps(record, ensure_ascii=False))

    def flush(self):
        if not self.buf or not self.day:
            return
        blob = ("\n".join(self.buf) + "\n").encode("utf-8")
        self.buf = []
        with gzip.GzipFile(self._path(self.day), "ab") as gz:
            gz.write(blob)

    def current_size(self):
        if self.day:
            p = self._path(self.day)
            if os.path.exists(p):
                return os.path.getsize(p)
        return 0

    def pending(self):
        return len(self.buf)

    def close(self):
        self.flush()


def open_stream(last_event_id):
    req = urllib.request.Request(STREAM_URL)
    req.add_header("User-Agent", UA)
    req.add_header("Accept", "text/event-stream")
    if last_event_id:
        req.add_header("Last-Event-ID", last_event_id)
        log("Reconnect kem Last-Event-ID -> Wikimedia phat lai tu cho cu")
    return urllib.request.urlopen(req, timeout=60)


def main():
    if not CONTACT.strip():
        print(
            "LOI: chua dien CONTACT trong collector.py.\n"
            "Wikimedia yeu cau User-Agent co lien he that (email nhom hoac link repo).\n"
            "Mo file, dien vao bien CONTACT, roi chay lai.",
            file=sys.stderr,
        )
        return 1

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    salt = load_or_create_salt()
    last_event_id = load_last_event_id()
    writer = RotatingWriter(DATA_DIR)

    kept = skipped = 0
    since_flush = 0
    last_flush = last_stats = time.time()
    backoff = 1

    log(f"Bat dau thu du lieu: {sorted(WIKIS)} | type={sorted(KEEP_TYPES)} | bo bot={DROP_BOTS}")
    log(f"User-Agent: {UA}")

    while running:
        try:
            resp = open_stream(last_event_id)
            backoff = 1
            log("Da ket noi EventStreams")

            event_id = None
            data_lines = []

            for raw_line in resp:
                if not running:
                    break
                line = raw_line.decode("utf-8", errors="replace").rstrip("\n")

                if line.startswith("id:"):
                    event_id = line[3:].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[5:].strip())
                elif line == "":
                    # dong trong = ket thuc mot su kien
                    if data_lines:
                        payload = "".join(data_lines)
                        data_lines = []
                        try:
                            ev = json.loads(payload)
                        except json.JSONDecodeError:
                            continue

                        if (
                            ev.get("wiki") in WIKIS
                            and ev.get("type") in KEEP_TYPES
                            and not (DROP_BOTS and ev.get("bot"))
                        ):
                            writer.write(to_record(ev, salt))
                            kept += 1
                            since_flush += 1
                        else:
                            skipped += 1

                    if event_id:
                        last_event_id = event_id

                now = time.time()
                if since_flush >= FLUSH_EVERY_EVENTS or now - last_flush >= FLUSH_EVERY_SECONDS:
                    writer.flush()
                    if last_event_id:
                        save_last_event_id(last_event_id)
                    since_flush = 0
                    last_flush = now

                if now - last_stats >= STATS_EVERY_SECONDS:
                    elapsed = now - last_stats
                    mb = writer.current_size() / 1_048_576
                    log(
                        f"giu={kept}  bo={skipped}  "
                        f"toc_do={kept / max(elapsed, 1):.2f}/s (phut vua roi)  "
                        f"file={mb:.1f} MB"
                    )
                    kept = skipped = 0
                    last_stats = now

            resp.close()
            if running:
                log("Stream dong phia server - se ket noi lai")

        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as e:
            if not running:
                break
            log(f"Loi ket noi: {e.__class__.__name__}: {e} -> cho {backoff}s")
            writer.flush()
            if last_event_id:
                save_last_event_id(last_event_id)
            # ngu theo tung giay nho de Ctrl+C phan hoi ngay
            waited = 0
            while waited < backoff and running:
                time.sleep(0.5)
                waited += 0.5
            backoff = min(backoff * 2, MAX_BACKOFF)

    writer.flush()
    if last_event_id:
        save_last_event_id(last_event_id)
    writer.close()
    log("Da dung an toan. Du lieu nam trong thu muc 'data/'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
