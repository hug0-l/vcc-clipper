"""Unified persistence layer for Clipper server data."""
import json
import sqlite3
import hashlib
import hmac
import threading
from datetime import datetime, timezone


DB_PATH = "clipper_data.db"


class Persistence:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        """Create tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS admin (key TEXT PRIMARY KEY, value TEXT)"
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS room_data (
                room_id TEXT PRIMARY KEY,
                data TEXT  -- JSON blob of {noticePosts, checklists, chatMessages, keyManagements, ...}
            )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS device_peers (
                device_id TEXT PRIMARY KEY,
                peer_id TEXT NOT NULL,
                room_id TEXT DEFAULT '',
                display_name TEXT DEFAULT '',
                last_seen TEXT NOT NULL
            )"""
            )
            conn.commit()
        finally:
            conn.close()

    # --- Admin password ---

    def init_admin_password(self, default_pw="12345"):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "INSERT OR IGNORE INTO admin VALUES ('password', ?)",
                (self._hash(default_pw),),
            )
            conn.commit()
        finally:
            conn.close()

    def verify_admin_password(self, pw):
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT value FROM admin WHERE key='password'"
            ).fetchone()
            if not row:
                return False
            return hmac.compare_digest(self._hash(pw), row[0])
        finally:
            conn.close()

    def set_admin_password(self, new_pw):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "INSERT OR REPLACE INTO admin VALUES ('password', ?)",
                (self._hash(new_pw),),
            )
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _hash(pw):
        return hashlib.sha256(pw.encode()).hexdigest()

    # --- Room data ---

    def save_room_data(self, room_id, room_data):
        """Persist a room's full data (notices, checklists, keys, chat)."""
        conn = sqlite3.connect(self.db_path)
        try:
            blob = json.dumps(room_data, ensure_ascii=False)
            conn.execute(
                "INSERT OR REPLACE INTO room_data VALUES (?, ?)", (room_id, blob)
            )
            conn.commit()
        finally:
            conn.close()

    def load_all_rooms(self):
        """Load all room data from DB. Returns {room_id: {data...}}."""
        conn = sqlite3.connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT room_id, data FROM room_data"
            ).fetchall()
            result = {}
            for rid, blob in rows:
                try:
                    result[rid] = json.loads(blob)
                except json.JSONDecodeError:
                    continue
            return result
        finally:
            conn.close()

    def delete_room(self, room_id):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "DELETE FROM room_data WHERE room_id=?", (room_id,)
            )
            conn.commit()
        finally:
            conn.close()

    # --- Config ---

    def save_config(self, key, value):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "INSERT OR REPLACE INTO config VALUES (?, ?)",
                (key, json.dumps(value)),
            )
            conn.commit()
        finally:
            conn.close()

    def load_config(self, key, default=None):
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT value FROM config WHERE key=?", (key,)
            ).fetchone()
            if row:
                return json.loads(row[0])
            return default
        finally:
            conn.close()

    # --- Device-Peer mapping (thread-safe) ---

    def get_peer_for_device(self, device_id):
        """Look up a peer_id previously assigned to this device.
        Returns (peer_id, room_id, display_name) tuple, or None.
        """
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                row = conn.execute(
                    "SELECT peer_id, room_id, display_name FROM device_peers WHERE device_id=?",
                    (device_id,)
                ).fetchone()
                return row if row else None
            finally:
                conn.close()

    def save_device_peer(self, device_id, peer_id, room_id='', display_name=''):
        """Save or update a device→peer mapping. Thread-safe."""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute(
                    """INSERT OR REPLACE INTO device_peers
                    (device_id, peer_id, room_id, display_name, last_seen)
                    VALUES (?,?,?,?,?)""",
                    (device_id, peer_id, room_id, display_name,
                     datetime.now(timezone.utc).isoformat())
                )
                conn.commit()
            finally:
                conn.close()
