"""Passwort- und Session-Handling.

Ausgelegt auf den Betrieb im Heimnetz über HTTP: ein einzelnes Passwort für den
Haushalt, serverseitige Sessions in SQLite (damit Logout wirklich abmeldet) und
ein HttpOnly-Cookie mit SameSite=Lax.

Das Passwort wird nicht über Umgebungsvariablen gesetzt, sondern beim ersten
Aufruf über die Setup-Seite – so liegt es nirgends im Klartext in einer
docker-compose.yml.
"""

import base64
import hashlib
import hmac
import os
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone

from app.database import get_db

COOKIE_NAME = "hb_session"
PASSWORD_SETTING_KEY = "auth_password_hash"

# PBKDF2-SHA256. 200k Iterationen brauchen auf einem Pi 4 grob 0,3–0,5 s –
# vertretbar für einen Login, aber teuer genug für Brute-Force. Die Iterationszahl
# steckt im Hash-String, sie lässt sich also später anheben, ohne alte Hashes
# ungültig zu machen.
PBKDF2_ITERATIONS = 200_000
MIN_PASSWORD_LENGTH = 8

SESSION_LIFETIME_DEFAULT = timedelta(hours=12)
SESSION_LIFETIME_REMEMBER = timedelta(days=30)

# Brute-Force-Bremse. Prozesslokal – reicht, weil auf dem Pi ein einzelner
# uvicorn-Worker läuft. Ein Neustart setzt die Zähler zurück; im Heimnetz
# akzeptabel.
LOGIN_MAX_ATTEMPTS = 10
LOGIN_WINDOW_SECONDS = 900

_failed_attempts: dict[str, list[float]] = {}
_attempts_lock = threading.Lock()


# ─── Passwort ──────────────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    """→ 'pbkdf2_sha256$<iterationen>$<salt_b64>$<hash_b64>'"""
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, PBKDF2_ITERATIONS)
    return "pbkdf2_sha256${}${}${}".format(
        PBKDF2_ITERATIONS,
        base64.b64encode(salt).decode(),
        base64.b64encode(dk).decode(),
    )


def verify_password(password: str, stored: str) -> bool:
    try:
        algo, iterations, salt_b64, hash_b64 = stored.split("$")
        if algo != "pbkdf2_sha256":
            return False
        salt = base64.b64decode(salt_b64)
        expected = base64.b64decode(hash_b64)
    except (ValueError, TypeError):
        return False
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, int(iterations))
    return hmac.compare_digest(dk, expected)


def get_password_hash(conn) -> str | None:
    row = conn.execute(
        "SELECT value FROM settings WHERE key = ?", (PASSWORD_SETTING_KEY,)
    ).fetchone()
    return row["value"] if row and row["value"] else None


def set_password(conn, password: str) -> None:
    conn.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (PASSWORD_SETTING_KEY, hash_password(password)),
    )


def is_configured() -> bool:
    conn = get_db()
    try:
        return get_password_hash(conn) is not None
    finally:
        conn.close()


# ─── Sessions ──────────────────────────────────────────────────────────────────

def _token_hash(token: str) -> str:
    """In der DB liegt nur der Hash – ein DB-Leak verschafft damit keine Session."""
    return hashlib.sha256(token.encode()).hexdigest()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def create_session(remember: bool) -> tuple[str, int]:
    """Legt eine Session an. → (Token fürs Cookie, Lebensdauer in Sekunden)"""
    token = secrets.token_urlsafe(32)
    lifetime = SESSION_LIFETIME_REMEMBER if remember else SESSION_LIFETIME_DEFAULT
    expires = _now() + lifetime
    conn = get_db()
    try:
        # Abgelaufene Sessions hier aufräumen statt bei jedem Request – ein
        # Schreibvorgang pro Login statt einem pro Seitenaufruf.
        conn.execute("DELETE FROM sessions WHERE expires_at <= ?", (_now().isoformat(),))
        conn.execute(
            "INSERT INTO sessions (token_hash, created_at, expires_at) VALUES (?, ?, ?)",
            (_token_hash(token), _now().isoformat(), expires.isoformat()),
        )
        conn.commit()
    finally:
        conn.close()
    return token, int(lifetime.total_seconds())


def validate_session(token: str | None) -> bool:
    """Nur lesend – auf der SD-Karte soll ein Seitenaufruf keinen Write auslösen."""
    if not token:
        return False
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT expires_at FROM sessions WHERE token_hash = ?", (_token_hash(token),)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return False
    try:
        return datetime.fromisoformat(row["expires_at"]) > _now()
    except ValueError:
        return False


def delete_session(token: str | None) -> None:
    if not token:
        return
    conn = get_db()
    try:
        conn.execute("DELETE FROM sessions WHERE token_hash = ?", (_token_hash(token),))
        conn.commit()
    finally:
        conn.close()


def delete_all_sessions() -> None:
    """Nach einem Passwortwechsel: alle Geräte abmelden."""
    conn = get_db()
    try:
        conn.execute("DELETE FROM sessions")
        conn.commit()
    finally:
        conn.close()


# ─── Rate-Limiting ─────────────────────────────────────────────────────────────

def login_blocked(client: str) -> int:
    """→ verbleibende Sperrzeit in Sekunden, 0 wenn frei."""
    now = time.monotonic()
    with _attempts_lock:
        attempts = [t for t in _failed_attempts.get(client, []) if now - t < LOGIN_WINDOW_SECONDS]
        _failed_attempts[client] = attempts
        if len(attempts) < LOGIN_MAX_ATTEMPTS:
            return 0
        return int(LOGIN_WINDOW_SECONDS - (now - attempts[0])) + 1


def record_failed_login(client: str) -> None:
    with _attempts_lock:
        _failed_attempts.setdefault(client, []).append(time.monotonic())


def reset_failed_logins(client: str) -> None:
    with _attempts_lock:
        _failed_attempts.pop(client, None)
