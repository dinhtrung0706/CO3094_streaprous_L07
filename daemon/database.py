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

                CREATE TABLE IF NOT EXISTS channels (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL COLLATE NOCASE UNIQUE,
                    owner_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                    created_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS channel_members (
                    channel_id TEXT NOT NULL REFERENCES channels(id)
                        ON DELETE CASCADE,
                    user_id INTEGER NOT NULL REFERENCES users(id)
                        ON DELETE CASCADE,
                    joined_at INTEGER NOT NULL,
                    PRIMARY KEY (channel_id, user_id)
                );

                """
            )
            self._ensure_messages()
            self._migrate_schema()

    def _columns(self, table):
        with self._lock:
            return {
                row["name"]
                for row in self._connection.execute(
                    f"PRAGMA table_info({table})"
                ).fetchall()
            }

    def _migrate_schema(self):
        """Upgrade existing databases while retaining stored records."""
        with self._connection:
            self._ensure_messages()
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS peers (
                    username TEXT NOT NULL COLLATE NOCASE,
                    ip TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    http_port INTEGER NOT NULL DEFAULT 0,
                    last_seen INTEGER NOT NULL,
                    PRIMARY KEY (username, ip, port)
                )"""
                )
            self._ensure_direct_messages()
            user_columns = self._columns("users")
            if "digest_ha1" not in user_columns:
                self._connection.execute(
                    "ALTER TABLE users ADD COLUMN digest_ha1 TEXT"
                )

            channel_columns = self._columns("channels")
            if "is_private" not in channel_columns:
                self._connection.execute(
                    "ALTER TABLE channels ADD COLUMN is_private INTEGER "
                    "NOT NULL DEFAULT 0 CHECK (is_private IN (0, 1))"
                )
            if "owner_name" not in channel_columns:
                self._connection.execute(
                    "ALTER TABLE channels ADD COLUMN owner_name TEXT "
                    "NOT NULL DEFAULT ''"
                )
            peer_columns = self._columns("peers")
            if "http_port" not in peer_columns:
                self._connection.execute(
                    "ALTER TABLE peers ADD COLUMN http_port INTEGER "
                    "NOT NULL DEFAULT 0"
                )
            if "owner_id" in self._columns("channels"):
                self._connection.execute(
                    """UPDATE channels SET owner_name = COALESCE(
                        (SELECT username FROM users
                         WHERE users.id = channels.owner_id), '')
                       WHERE owner_name = ''"""
                )

            member_columns = self._columns("channel_members")
            if "username" not in member_columns:
                self._connection.execute(
                    "ALTER TABLE channel_members RENAME TO channel_members_old"
                )
                self._connection.execute(
                    """
                    CREATE TABLE channel_members (
                        channel_id TEXT NOT NULL REFERENCES channels(id)
                            ON DELETE CASCADE,
                        username TEXT NOT NULL COLLATE NOCASE,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        joined_at INTEGER NOT NULL,
                        PRIMARY KEY (channel_id, username)
                    )
                    """
                )
                self._connection.execute(
                    """
                    INSERT INTO channel_members
                        (channel_id, username, user_id, joined_at)
                    SELECT old.channel_id, users.username, old.user_id,
                           old.joined_at
                    FROM channel_members_old AS old
                    JOIN users ON users.id = old.user_id
                    """
                )
                self._connection.execute("DROP TABLE channel_members_old")
            elif "username" not in member_columns:
                self._connection.execute(
                    "ALTER TABLE channel_members ADD COLUMN username TEXT "
                    "NOT NULL DEFAULT ''"
                )
                self._connection.execute(
                    """UPDATE channel_members SET username = COALESCE(
                        (SELECT username FROM users WHERE users.id =
                         channel_members.user_id), '')"""
                )

            self._ensure_messages()
            rows = self._connection.execute(
                """
                SELECT id, sender, recipient, body, created_at
                FROM messages WHERE recipient <> '*'
                ORDER BY created_at, seq, id
                """
            ).fetchall()
            for row in rows:
                self.append_direct_message(
                    row["id"], row["sender"], row["recipient"], row["body"],
                    row["created_at"],
                )
                self._connection.execute(
                    "DELETE FROM messages WHERE id = ?", (row["id"],)
                )
            if "owner_name" in self._columns("channels"):
                self._connection.execute(
                    """UPDATE messages SET
                       owner = COALESCE((SELECT owner_name FROM channels
                           WHERE channels.id = messages.channel_id), ''),
                       channel_name = COALESCE((SELECT name FROM channels
                           WHERE channels.id = messages.channel_id), ''),
                       is_private = COALESCE((SELECT is_private FROM channels
                           WHERE channels.id = messages.channel_id), 0)
                       WHERE channel_name = ''"""
                )
            self._connection.execute("PRAGMA user_version = 2")

    def _ensure_messages(self):
        with self._lock:
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
                sender TEXT NOT NULL,
                owner TEXT NOT NULL DEFAULT '',
                channel_name TEXT NOT NULL DEFAULT '',
                is_private INTEGER NOT NULL DEFAULT 0,
                recipient TEXT NOT NULL DEFAULT '*',
                body TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                seq INTEGER NOT NULL,
                    UNIQUE(channel_id, seq))"""
            )
            for column, declaration in (
                ("recipient", "TEXT NOT NULL DEFAULT '*'"),
                ("owner", "TEXT NOT NULL DEFAULT ''"),
                ("channel_name", "TEXT NOT NULL DEFAULT ''"),
                ("is_private", "INTEGER NOT NULL DEFAULT 0"),
            ):
                if column not in self._columns("messages"):
                    self._connection.execute(
                        f"ALTER TABLE messages ADD COLUMN {column} {declaration}"
                    )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS messages_channel_seq_idx "
                "ON messages(channel_id, seq)"
            )

    @staticmethod
    def _user_record(row):
        if row is None:
            return None
        record = {
            "id": row["id"],
            "username": row["username"],
            "password_hash": bytes(row["password_hash"]),
            "salt": bytes(row["salt"]),
            "iterations": row["iterations"],
            "created_at": row["created_at"],
        }
        if "digest_ha1" in row:
            record["digest_ha1"] = row["digest_ha1"]
        return record

    def create_user(self, username, password_hash, salt, iterations, created_at,
                    digest_ha1=None):
        with self._lock, self._connection:
            if "digest_ha1" in self._columns("users"):
                cursor = self._connection.execute(
                    """INSERT INTO users
                        (username, password_hash, salt, iterations, created_at,
                         digest_ha1)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (username, password_hash, salt, iterations, created_at,
                     digest_ha1),
                )
            else:
                cursor = self._connection.execute(
                    """INSERT INTO users
                        (username, password_hash, salt, iterations, created_at)
                       VALUES (?, ?, ?, ?, ?)""",
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
                SELECT *
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

    def get_digest_ha1(self, username):
        """Return the stored Digest HA1 verifier for one account."""
        with self._lock:
            if "digest_ha1" not in self._columns("users"):
                return None
            row = self._connection.execute(
                "SELECT digest_ha1 FROM users WHERE username = ? COLLATE NOCASE",
                (username,),
            ).fetchone()
        return row["digest_ha1"] if row is not None else None

    def _ensure_digest_column(self):
        with self._lock:
            if "digest_ha1" not in self._columns("users"):
                with self._connection:
                    self._connection.execute(
                        "ALTER TABLE users ADD COLUMN digest_ha1 TEXT"
                    )

    def set_digest_ha1(self, user_id, digest_ha1):
        """Store the Digest verifier for an account after password login."""
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE users SET digest_ha1 = ? WHERE id = ?",
                (digest_ha1, user_id),
            )

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

    @staticmethod
    def _rows(cursor):
        return [dict(row) for row in cursor.fetchall()]

    def create_channel(self, channel_id, name, owner_id, created_at,
                       is_private=False, owner_name=None):
        if owner_name is None and owner_id is not None:
            owner = self.get_user_by_id(owner_id)
            owner_name = owner["username"] if owner else ""
        with self._lock, self._connection:
            columns = self._columns("channels")
            if {"owner_name", "is_private"}.issubset(columns):
                self._connection.execute(
                    """INSERT INTO channels
                        (id, name, owner_id, owner_name, created_at, is_private)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (channel_id, name, owner_id, owner_name or "", created_at,
                     int(bool(is_private))),
                )
            else:
                self._connection.execute(
                    """INSERT INTO channels (id, name, owner_id, created_at)
                       VALUES (?, ?, ?, ?)""",
                    (channel_id, name, owner_id, created_at),
                )
            row = self._connection.execute(
                "SELECT * FROM channels WHERE id = ?", (channel_id,)
            ).fetchone()
        return dict(row)

    def get_channel(self, channel_id):
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM channels WHERE id = ?", (channel_id,)
            ).fetchone()
        return dict(row) if row is not None else None

    def get_channel_by_name(self, name):
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM channels WHERE name = ? COLLATE NOCASE", (name,)
            ).fetchone()
        return dict(row) if row is not None else None

    def list_channels(self, user_id):
        user = self.get_user_by_id(user_id)
        username = user["username"] if user else ""
        with self._lock:
            channel_columns = self._columns("channels")
            member_columns = self._columns("channel_members")
            if "is_private" in channel_columns and "username" in member_columns:
                cursor = self._connection.execute(
                    """SELECT channels.*,
                        (SELECT COUNT(*) FROM channel_members AS members
                         WHERE members.channel_id = channels.id) AS member_count,
                        EXISTS(SELECT 1 FROM channel_members AS mine
                         WHERE mine.channel_id = channels.id
                           AND mine.username = ? COLLATE NOCASE) AS joined
                       FROM channels
                       WHERE channels.is_private = 0 OR EXISTS(
                         SELECT 1 FROM channel_members AS mine
                         WHERE mine.channel_id = channels.id
                           AND mine.username = ? COLLATE NOCASE)
                       ORDER BY channels.name COLLATE NOCASE""",
                    (username, username),
                )
            else:
                cursor = self._connection.execute(
                    """SELECT channels.*,
                        (SELECT COUNT(*) FROM channel_members AS members
                         WHERE members.channel_id = channels.id) AS member_count,
                        EXISTS(SELECT 1 FROM channel_members AS mine
                         WHERE mine.channel_id = channels.id
                           AND mine.user_id = ?) AS joined
                       FROM channels ORDER BY channels.name COLLATE NOCASE""",
                    (user_id,),
                )
        return self._rows(cursor)

    def add_channel_member(self, channel_id, user_id, joined_at):
        user = self.get_user_by_id(user_id) if isinstance(user_id, int) else None
        username = user["username"] if user else str(user_id)
        member_id = user_id if user else None
        with self._lock, self._connection:
            if "username" in self._columns("channel_members"):
                cursor = self._connection.execute(
                    """INSERT OR IGNORE INTO channel_members
                       (channel_id, username, user_id, joined_at)
                       VALUES (?, ?, ?, ?)""",
                    (channel_id, username, member_id, joined_at),
                )
            elif member_id is not None:
                cursor = self._connection.execute(
                    """INSERT OR IGNORE INTO channel_members
                       (channel_id, user_id, joined_at) VALUES (?, ?, ?)""",
                    (channel_id, member_id, joined_at),
                )
            else:
                raise ValueError("username membership requires migrated database")
            return cursor.rowcount > 0

    def remove_channel_member(self, channel_id, user_id):
        user = self.get_user_by_id(user_id) if isinstance(user_id, int) else None
        username = user["username"] if user else str(user_id)
        member_id = user_id if user else None
        with self._lock, self._connection:
            if "username" in self._columns("channel_members"):
                cursor = self._connection.execute(
                    """DELETE FROM channel_members WHERE channel_id = ?
                       AND username = ? COLLATE NOCASE""",
                    (channel_id, username),
                )
            else:
                cursor = self._connection.execute(
                    "DELETE FROM channel_members WHERE channel_id = ? AND user_id = ?",
                    (channel_id, member_id),
                )
            return cursor.rowcount > 0

    def is_channel_member(self, channel_id, user_id):
        user = self.get_user_by_id(user_id) if isinstance(user_id, int) else None
        username = user["username"] if user else str(user_id)
        member_id = user_id if user else None
        with self._lock:
            if "username" in self._columns("channel_members"):
                row = self._connection.execute(
                    """SELECT 1 FROM channel_members WHERE channel_id = ?
                       AND username = ? COLLATE NOCASE""",
                    (channel_id, username),
                ).fetchone()
            else:
                row = self._connection.execute(
                    "SELECT 1 FROM channel_members WHERE channel_id = ? AND user_id = ?",
                    (channel_id, member_id),
                ).fetchone()
        return row is not None

    def get_message(self, message_id):
        with self._lock:
            self._ensure_messages()
            row = self._connection.execute(
                "SELECT * FROM messages WHERE id = ?", (message_id,)
            ).fetchone()
        return dict(row) if row is not None else None

    def append_message(self, message_id, channel_id, sender, body, created_at,
                       recipient="*", owner="", channel_name="",
                       is_private=False):
        """Insert one message, assigning the next per-channel sequence number.

        The insert is idempotent: posting the same ``message_id`` twice returns
        the stored row instead of creating a duplicate.
        """
        recipient = recipient or "*"
        with self._lock, self._connection:
            self._ensure_messages()
            existing = self._connection.execute(
                "SELECT * FROM messages WHERE id = ?", (message_id,)
            ).fetchone()
            if existing is not None:
                return dict(existing)

            self._connection.execute(
                """
                INSERT INTO messages
                    (id, channel_id, sender, owner, channel_name, is_private,
                     recipient, body, created_at, seq)
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       (SELECT COALESCE(MAX(seq), 0) + 1
                        FROM messages WHERE channel_id = ?)
                """,
                (message_id, channel_id, sender, owner, channel_name,
                 int(bool(is_private)), recipient, body, created_at, channel_id),
            )
            row = self._connection.execute(
                "SELECT * FROM messages WHERE id = ?", (message_id,)
            ).fetchone()
        return dict(row)

    @staticmethod
    def conversation_key(first, second):
        """Return a stable case-insensitive key for a two-user conversation."""
        return "\0".join(sorted((first.casefold(), second.casefold())))

    def append_direct_message(self, message_id, sender, recipient, body,
                              created_at):
        key = self.conversation_key(sender, recipient)
        with self._lock, self._connection:
            if "recipient" not in self._columns("messages"):
                self._connection.execute(
                    "ALTER TABLE messages ADD COLUMN recipient TEXT "
                    "NOT NULL DEFAULT '*'"
                )
            self._ensure_direct_messages()
            existing = self._connection.execute(
                "SELECT * FROM direct_messages WHERE id = ?", (message_id,)
            ).fetchone()
            if existing is not None:
                return dict(existing), False
            self._connection.execute(
                """
                INSERT INTO direct_messages
                    (id, conversation_key, sender, recipient, body, created_at,
                     seq)
                SELECT ?, ?, ?, ?, ?, ?,
                       (SELECT COALESCE(MAX(seq), 0) + 1
                        FROM direct_messages WHERE conversation_key = ?)
                """,
                (message_id, key, sender, recipient, body, created_at, key),
            )
            row = self._connection.execute(
                "SELECT * FROM direct_messages WHERE id = ?", (message_id,)
            ).fetchone()
        return dict(row), True

    def _ensure_direct_messages(self):
        with self._lock:
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS direct_messages (
                id TEXT PRIMARY KEY,
                conversation_key TEXT NOT NULL,
                sender TEXT NOT NULL COLLATE NOCASE,
                recipient TEXT NOT NULL COLLATE NOCASE,
                body TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                seq INTEGER NOT NULL,
                    UNIQUE(conversation_key, seq))"""
            )
            self._connection.execute(
                """CREATE INDEX IF NOT EXISTS direct_messages_conversation_seq_idx
                   ON direct_messages(conversation_key, seq)"""
            )

    def get_direct_message(self, message_id):
        with self._lock:
            self._ensure_direct_messages()
            row = self._connection.execute(
                "SELECT * FROM direct_messages WHERE id = ?", (message_id,)
            ).fetchone()
        return dict(row) if row is not None else None

    def list_direct_messages(self, first, second, since=0, limit=100):
        key = self.conversation_key(first, second)
        with self._lock:
            self._ensure_direct_messages()
            cursor = self._connection.execute(
                """
                SELECT * FROM direct_messages
                WHERE conversation_key = ? AND seq > ?
                ORDER BY seq LIMIT ?
                """,
                (key, int(since), int(limit)),
            )
            return self._rows(cursor)

    def insert_direct_message(self, message):
        """Insert one remotely synchronized direct message idempotently."""
        key = self.conversation_key(message["sender"], message["recipient"])
        with self._lock, self._connection:
            self._ensure_direct_messages()
            existing = self._connection.execute(
                "SELECT * FROM direct_messages WHERE id = ?", (message["id"],)
            ).fetchone()
            if existing is not None:
                return dict(existing), False
            self._connection.execute(
                """
                INSERT INTO direct_messages
                    (id, conversation_key, sender, recipient, body, created_at,
                     seq)
                SELECT ?, ?, ?, ?, ?, ?,
                       (SELECT COALESCE(MAX(seq), 0) + 1
                        FROM direct_messages WHERE conversation_key = ?)
                """,
                (message["id"], key, message["sender"], message["recipient"],
                 message["body"], int(message["created_at"]), key),
            )
            row = self._connection.execute(
                "SELECT * FROM direct_messages WHERE id = ?", (message["id"],)
            ).fetchone()
        return dict(row), True

    def list_messages(self, channel_id, since=0, limit=100, viewer=None):
        self._ensure_messages()
        query = (
            "SELECT * FROM messages "
            "WHERE channel_id = ? AND seq > ?"
        )
        params = [channel_id, int(since)]
        if viewer is not None and self._columns("channels") and self._columns("messages"):
                query += (
                    " AND (recipient = '*' OR recipient = ? COLLATE NOCASE"
                    " OR sender = ? COLLATE NOCASE)"
                )
                params.extend([viewer, viewer])
        query += " ORDER BY seq LIMIT ?"
        params.append(int(limit))
        with self._lock:
            cursor = self._connection.execute(query, params)
            return self._rows(cursor)

    def upsert_peer(self, username, ip, port, last_seen, http_port=0):
        with self._lock, self._connection:
            if "http_port" not in self._columns("peers"):
                self._connection.execute(
                    "ALTER TABLE peers ADD COLUMN http_port INTEGER "
                    "NOT NULL DEFAULT 0"
                )
            self._connection.execute(
                """
                INSERT OR REPLACE INTO peers
                    (username, ip, port, http_port, last_seen)
                VALUES (?, ?, ?, ?, ?)
                """,
                (username, ip, int(port), int(http_port), int(last_seen)),
            )
            row = self._connection.execute(
                """
                SELECT * FROM peers
                WHERE username = ? COLLATE NOCASE AND ip = ? AND port = ?
                """,
                (username, ip, int(port)),
            ).fetchone()
        return dict(row)

    def list_peers(self, not_before, exclude_username=None):
        if "http_port" not in self._columns("peers"):
            with self._lock, self._connection:
                self._connection.execute(
                    "ALTER TABLE peers ADD COLUMN http_port INTEGER "
                    "NOT NULL DEFAULT 0"
                )
        query = "SELECT * FROM peers WHERE last_seen >= ?"
        params = [int(not_before)]
        if exclude_username is not None:
            query += " AND username <> ? COLLATE NOCASE"
            params.append(exclude_username)
        query += " ORDER BY username COLLATE NOCASE, port"
        with self._lock:
            cursor = self._connection.execute(query, params)
            return self._rows(cursor)

    def expire_peers(self, not_before):
        with self._lock, self._connection:
            if "http_port" not in self._columns("peers"):
                self._connection.execute(
                    "ALTER TABLE peers ADD COLUMN http_port INTEGER "
                    "NOT NULL DEFAULT 0"
                )
            cursor = self._connection.execute(
                "DELETE FROM peers WHERE last_seen < ?", (int(not_before),)
            )
            return cursor.rowcount

    def close(self):
        with self._lock:
            self._connection.close()
