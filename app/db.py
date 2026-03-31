from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from app.auth import hash_password, verify_password


@dataclass(slots=True)
class UserRecord:
    id: int
    username: str


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    def __init__(self, db_file: str) -> None:
        self.db_file = db_file
        self._write_lock = asyncio.Lock()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        return conn

    def init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_salt BLOB NOT NULL,
                    password_hash BLOB NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    connected_at TEXT NOT NULL,
                    disconnected_at TEXT,
                    remote_addr TEXT,
                    close_reason TEXT,
                    FOREIGN KEY(user_id) REFERENCES users(id)
                );
                CREATE TABLE IF NOT EXISTS locations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    session_id INTEGER NOT NULL,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    accuracy REAL,
                    client_ts TEXT,
                    server_ts TEXT NOT NULL,
                    seq INTEGER,
                    FOREIGN KEY(user_id) REFERENCES users(id),
                    FOREIGN KEY(session_id) REFERENCES sessions(id)
                );
                CREATE TABLE IF NOT EXISTS security_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_ts TEXT NOT NULL,
                    remote_addr TEXT,
                    username TEXT,
                    event_type TEXT NOT NULL,
                    details TEXT
                );
                """
            )

    def create_user(self, username: str, password: str) -> None:
        salt, pwh = hash_password(password)
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO users(username, password_salt, password_hash, created_at) VALUES (?, ?, ?, ?)",
                (username, salt, pwh, utc_now()),
            )

    def authenticate(self, username: str, password: str) -> UserRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, username, password_salt, password_hash FROM users WHERE username = ?",
                (username,),
            ).fetchone()
        if not row:
            return None
        if not verify_password(password, row["password_salt"], row["password_hash"]):
            return None
        return UserRecord(id=row["id"], username=row["username"])

    async def log_security_event(self, remote_addr: str, username: str | None, event_type: str, details: str) -> None:
        async with self._write_lock:
            with self._connect() as conn:
                conn.execute(
                    "INSERT INTO security_events(event_ts, remote_addr, username, event_type, details) VALUES (?, ?, ?, ?, ?)",
                    (utc_now(), remote_addr, username, event_type, details),
                )

    async def create_session(self, user_id: int, remote_addr: str) -> int:
        async with self._write_lock:
            with self._connect() as conn:
                cursor = conn.execute(
                    "INSERT INTO sessions(user_id, connected_at, remote_addr) VALUES (?, ?, ?)",
                    (user_id, utc_now(), remote_addr),
                )
                return int(cursor.lastrowid)

    async def close_session(self, session_id: int, close_reason: str) -> None:
        async with self._write_lock:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE sessions SET disconnected_at = ?, close_reason = ? WHERE id = ?",
                    (utc_now(), close_reason, session_id),
                )

    async def store_location(
        self,
        *,
        user_id: int,
        session_id: int,
        lat: float,
        lon: float,
        accuracy: float | None,
        client_ts: str | None,
        seq: int | None,
    ) -> None:
        async with self._write_lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO locations(user_id, session_id, latitude, longitude, accuracy, client_ts, server_ts, seq)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (user_id, session_id, lat, lon, accuracy, client_ts, utc_now(), seq),
                )

    def admin_dump(self) -> dict[str, list[dict]]:
        with self._connect() as conn:
            users = [
                dict(r)
                for r in conn.execute(
                    "SELECT id, username, created_at FROM users ORDER BY id DESC"
                ).fetchall()
            ]
            sessions = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT id, user_id, connected_at, disconnected_at, remote_addr, close_reason
                    FROM sessions
                    ORDER BY id DESC
                    """
                ).fetchall()
            ]
            locations = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT id, user_id, session_id, latitude, longitude, accuracy, client_ts, server_ts, seq
                    FROM locations
                    ORDER BY id DESC
                    """
                ).fetchall()
            ]
            events = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT id, event_ts, remote_addr, username, event_type, details
                    FROM security_events
                    ORDER BY id DESC
                    """
                ).fetchall()
            ]
        return {
            "Users": users,
            "Sessions": sessions,
            "Locations": locations,
            "Logs": events,
        }