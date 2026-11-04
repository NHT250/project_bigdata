import sys
from dataclasses import replace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import Settings
from src.common.hashing import hash_user, load_salt

ENV = {
    "KAFKA_BOOTSTRAP": "localhost:9092",
    "KAFKA_TOPIC": "rc.raw",
    "PG_DSN": "postgresql://u:p@localhost/db",
    "DELTA_ROOT": "/data/delta",
    "WIKI_CONTACT": "nhom6@example.com",
}


def set_env(monkeypatch, **overrides):
    for k, v in {**ENV, **overrides}.items():
        if v is None:
            monkeypatch.delenv(k, raising=False)
        else:
            monkeypatch.setenv(k, v)


def test_hash_is_16_hex_chars_and_stable():
    assert hash_user("203.113.45.7", "s") == hash_user("203.113.45.7", "s")
    assert len(hash_user("203.113.45.7", "s")) == 16
    assert all(c in "0123456789abcdef" for c in hash_user("203.113.45.7", "s"))


def test_different_salt_gives_different_hash():
    assert hash_user("203.113.45.7", "s1") != hash_user("203.113.45.7", "s2")


def test_different_user_gives_different_hash():
    assert hash_user("203.113.45.7", "s") != hash_user("203.113.45.8", "s")


def test_none_user_returns_none():
    assert hash_user(None, "s") is None


def test_hash_does_not_contain_the_original_value():
    assert "203.113" not in hash_user("203.113.45.7", "s")


def test_load_salt_creates_then_reuses(tmp_path):
    p = tmp_path / "salt.txt"
    first = load_salt(str(p))
    assert len(first) >= 32
    assert load_salt(str(p)) == first


def test_load_salt_creates_parent_directory(tmp_path):
    p = tmp_path / "nested" / "dir" / "salt.txt"
    assert len(load_salt(str(p))) >= 32
    assert p.exists()


def test_settings_requires_wiki_contact(monkeypatch):
    set_env(monkeypatch, WIKI_CONTACT=None)
    with pytest.raises(ValueError, match="WIKI_CONTACT"):
        Settings.load()


def test_settings_requires_pg_dsn(monkeypatch):
    set_env(monkeypatch, PG_DSN=None)
    with pytest.raises(ValueError, match="PG_DSN"):
        Settings.load()


def test_settings_defaults_match_spec(monkeypatch):
    set_env(monkeypatch)
    s = Settings.load()
    assert s.wikis == {"enwiki"}
    assert s.diff_max_rps == 10
    assert s.watermark_minutes == 10
    assert s.label_cutoff_minutes == 60
    assert s.summary_mode == "template"
    assert s.kafka_partitions == 3


def test_settings_user_agent_declares_contact_and_is_not_a_browser(monkeypatch):
    set_env(monkeypatch)
    ua = Settings.load().user_agent
    assert "nhom6@example.com" in ua
    assert "VLU-BigData-RealtimeWiki/1.0" in ua
    for forbidden in ("Mozilla", "Chrome", "Safari", "AppleWebKit"):
        assert forbidden not in ua


def test_settings_rejects_unknown_summary_mode(monkeypatch):
    set_env(monkeypatch, SUMMARY_MODE="magic")
    with pytest.raises(ValueError, match="SUMMARY_MODE"):
        Settings.load()


def test_settings_is_frozen(monkeypatch):
    set_env(monkeypatch)
    s = Settings.load()
    with pytest.raises(Exception):
        s.diff_max_rps = 999
    assert replace(s, diff_max_rps=5).diff_max_rps == 5
