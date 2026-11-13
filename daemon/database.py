"""SQLite persistence for StreapRous accounts and sessions."""

import os
import sqlite3
import threading
from pathlib import Path


class Database:
    """Thread-safe SQLite store for authentication data."""

    def __init__(self, path):
        self.path = os.fspath(path)
        if self.path != ":memory:":
            path = Path(self.path).expanduser()
            path.parent.mkdir(parents=True, exist_ok=True)
            self.path = os.fspath(path)

        self._lock = threading.RLock()
        self._connection = sqlite3.connect(
            self.path,
            timeout=5.0,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA busy_timeout = 5000")
        if self.path != ":memory:":
            self._connection.execute("PRAGMA journal_mode = WAL")
        self._create_schema()

    def _create_schema(self):
        with self._lock, self._connection:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    username TEXT NOT NULL COLLATE NOCASE UNIQUE,
                    password_hash BLOB NOT NULL,
                    salt BLOB NOT NULL,
                    iterations INTEGER NOT NULL CHECK (iterations > 0),
                    created_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sessions (
                    token_hash BLOB PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id)
                        ON DELETE CASCADE,
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS sessions_expires_at_idx
                    ON sessions(expires_at);
                """
            )

    @staticmethod
    def _user_record(row):
        if row is None:
            return None
        return {
            "id": row["id"],
            "username": row["username"],
            "password_hash": bytes(row["password_hash"]),
            "salt": bytes(row["salt"]),
            "iterations": row["iterations"],
            "created_at": row["created_at"],
        }

    def create_user(self, username, password_hash, salt, iterations, created_at):
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO users
                    (username, password_hash, salt, iterations, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (username, password_hash, salt, iterations, created_at),
            )
            row = self._connection.execute(
                """
                SELECT id, username, created_at
                FROM users WHERE id = ?
                """,
                (cursor.lastrowid,),
            ).fetchone()
            return dict(row)

    def get_user(self, username):
        with self._lock:
            row = self._connection.execute(
                """
                SELECT id, username, password_hash, salt, iterations, created_at
                FROM users WHERE username = ? COLLATE NOCASE
                """,
                (username,),
            ).fetchone()
        return self._user_record(row)

    def get_user_by_id(self, user_id):
        with self._lock:
            row = self._connection.execute(
                """
                SELECT id, username, created_at
                FROM users WHERE id = ?
                """,
                (user_id,),
            ).fetchone()
        return dict(row) if row is not None else None

    def create_session(self, token_hash, user_id, created_at, expires_at):
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO sessions (token_hash, user_id, created_at, expires_at)
                VALUES (?, ?, ?, ?)
                """,
                (token_hash, user_id, created_at, expires_at),
            )

    def get_session_user(self, token_hash, now):
        with self._lock, self._connection:
            row = self._connection.execute(
                """
                SELECT users.id, users.username, users.created_at
                FROM sessions
                JOIN users ON users.id = sessions.user_id
                WHERE sessions.token_hash = ? AND sessions.expires_at > ?
                """,
                (token_hash, now),
            ).fetchone()
            self._connection.execute(
                "DELETE FROM sessions WHERE token_hash = ? AND expires_at <= ?",
                (token_hash, now),
            )
        return dict(row) if row is not None else None

    def delete_session(self, token_hash):
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "DELETE FROM sessions WHERE token_hash = ?",
                (token_hash,),
            )
            return cursor.rowcount > 0

    def delete_expired_sessions(self, now):
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "DELETE FROM sessions WHERE expires_at <= ?",
                (now,),
            )
            return cursor.rowcount

    def close(self):
        with self._lock:
            self._connection.close()
