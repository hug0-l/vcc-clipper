#!/usr/bin/env python3
"""Clipper signaling server - N-peer WebSocket room for WebRTC Full Mesh."""

import asyncio
import hashlib
import json
import logging
import os
import random
import secrets
import signal
import socket
import sqlite3
import struct
import string
import time
from datetime import datetime, timezone, timedelta

import websockets
import mimetypes


# room_id -> {peerId: {"ws": websocket, "joinedAt": "ISO timestamp"}}
rooms = {}
peer_ids = set()  # all assigned peerIds across all rooms

MAX_PEERS_PER_ROOM = 50
# room_id -> {"noticePosts": [...], "checklists": [...], "chatMessages": [...]}
room_data = {}

CHAT_RETENTION_DAYS = 7    # How long to keep chat backups (adjustable)
DB_PATH = "clipper_data.db"
LOG_DIR = "logs"
LOG_RETENTION_HOURS = 24
DEFAULT_ADMIN_PASSWORD = "12345"
DEBUG = True   # Toggle verbose debug output
_config = {"chatRetentionDays": CHAT_RETENTION_DAYS, "stunServer": "stun:stun.l.google.com:19302"}  # mutable config for runtime changes
_ntp_config = {"server": "stdtime.gov.hk", "offset": 0.0, "enabled": True}
_sessions = {}  # token -> {"createdAt": timestamp}
_login_attempts = {}  # websocket_id -> {"count": int, "first": timestamp}
MAX_LOGIN_ATTEMPTS = 5
LOGIN_COOLDOWN = 30  # seconds
SESSION_TIMEOUT = 1800  # 30 minutes


def _setup_logging():
    global _log_file
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"clipper_{datetime.now().strftime('%Y%m%d')}.log")
    _log._file = open(log_path, 'a', encoding='utf-8')
    return log_path

def _rotate_logs():
    """Remove log files older than LOG_RETENTION_HOURS."""
    now = time.time()
    cutoff = now - LOG_RETENTION_HOURS * 3600
    if os.path.isdir(LOG_DIR):
        for fname in os.listdir(LOG_DIR):
            fpath = os.path.join(LOG_DIR, fname)
            if fname.endswith('.log') and os.path.getmtime(fpath) < cutoff:
                os.remove(fpath)
                print(f"[Log rotation] Removed old log: {fname}")

def _log(category, message):
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] [{category}] {message}"
    print(line)
    if hasattr(_log, '_file') and _log._file:
        try:
            _log._file.write(line + '\n')
            _log._file.flush()
        except:
            pass


_log._file = None

def _hash_password(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

def _init_admin_password():
    """Insert default admin password if not set."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS admin (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute("INSERT OR IGNORE INTO admin VALUES ('password', ?)", (_hash_password(DEFAULT_ADMIN_PASSWORD),))
        conn.commit()
    finally:
        conn.close()

def _verify_admin_password(pw):
    """Constant-time password comparison to prevent timing attacks."""
    import hmac
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute("SELECT value FROM admin WHERE key='password'").fetchone()
        if not row:
            return False
        return hmac.compare_digest(_hash_password(pw), row[0])
    finally:
        conn.close()

def _set_admin_password(new_pw):
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("INSERT OR REPLACE INTO admin VALUES ('password', ?)", (_hash_password(new_pw),))
        conn.commit()
        return True
    finally:
        conn.close()

def _generate_session():
    """Generate a secure random session token."""
    token = secrets.token_hex(32)
    _sessions[token] = {"createdAt": time.time()}
    return token

def _verify_session(token):
    """Verify session token and check expiry."""
    if not token:
        return False
    entry = _sessions.get(token)
    if not entry:
        return False
    if time.time() - entry["createdAt"] > SESSION_TIMEOUT:
        _sessions.pop(token, None)
        return False
    return True

def _check_login_rate(ws_id):
    """Rate limit logins: max 5 attempts per 30 seconds per connection."""
    now = time.time()
    entry = _login_attempts.get(ws_id, {"count": 0, "first": now})
    if now - entry["first"] > LOGIN_COOLDOWN:
        entry["count"] = 0
        entry["first"] = now
    entry["count"] += 1
    _login_attempts[ws_id] = entry
    return entry["count"] <= MAX_LOGIN_ATTEMPTS

def _get_logs(count=50):
    """Return the last N lines from today's log file."""
    today_log = os.path.join(LOG_DIR, f"clipper_{datetime.now().strftime('%Y%m%d')}.log")
    if not os.path.exists(today_log):
        return ["(no logs yet)"]
    with open(today_log, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    return [l.rstrip('\n') for l in lines[-count:]]


def _debug(message):
    if DEBUG:
        ts = datetime.now(timezone.utc).strftime('%H:%M:%S.%f')[:12]
        print(f"  └─ [{ts}] {message}")


def _init_db():
    """Create SQLite tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS rooms (
        room_id TEXT PRIMARY KEY,
        notice_posts TEXT NOT NULL DEFAULT '[]',
        checklists TEXT NOT NULL DEFAULT '[]',
        chat_messages TEXT NOT NULL DEFAULT '[]',
        key_managements TEXT NOT NULL DEFAULT '[]'
    )""")
    conn.commit()
    conn.close()


def _load_state():
    """Load all rooms from SQLite into memory."""
    global room_data
    room_data = {}
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute("SELECT room_id, notice_posts, checklists, chat_messages, key_managements FROM rooms").fetchall()
        for rid, np, cl, cm, km in rows:
            room_data[rid] = {
                "noticePosts": json.loads(np),
                "checklists": json.loads(cl),
                "chatMessages": json.loads(cm),
                "keyManagements": json.loads(km),
            }
    except sqlite3.OperationalError:
        pass  # table doesn't exist yet
    conn.close()
    # Migrate old JSON if exists
    OLD_JSON = "vcc_server_state.json"
    if os.path.exists(OLD_JSON):
        try:
            with open(OLD_JSON, 'r') as f:
                legacy = json.load(f)
            for rid, data in legacy.items():
                if rid not in room_data:
                    room_data[rid] = data
            _save_state()
            os.rename(OLD_JSON, OLD_JSON + ".bak")
            _log('MIGRATE', f'Imported {len(legacy)} rooms from legacy JSON, backed up as {OLD_JSON}.bak')
        except Exception as e:
            _log('MIGRATE', f'Failed to migrate legacy JSON: {e}')


def _save_state():
    """Write all rooms to SQLite atomically."""
    conn = sqlite3.connect(DB_PATH)
    try:
        with conn:
            for rid, data in room_data.items():
                conn.execute(
                    "INSERT OR REPLACE INTO rooms VALUES (?,?,?,?,?)",
                    (rid,
                     json.dumps(data.get("noticePosts", [])),
                     json.dumps(data.get("checklists", [])),
                     json.dumps(data.get("chatMessages", [])),
                     json.dumps(data.get("keyManagements", [])))
                )
    finally:
        conn.close()


def _migrate_room_data():
    for rid in room_data:
        if "checklistItems" in room_data[rid]:
            old_items = room_data[rid].pop("checklistItems", [])
            if old_items and "checklists" not in room_data[rid]:
                room_data[rid]["checklists"] = [{
                    "id": "legacy-" + rid,
                    "title": "舊檢查清單",
                    "category": "",
                    "tags": [],
                    "color": "#38bdf8",
                    "pinned": False,
                    "createdBy": "系統",
                    "createdAt": int(time.time() * 1000),
                    "items": old_items
                }]
        if "checklists" not in room_data[rid]:
            room_data[rid]["checklists"] = []
        if "keyManagements" not in room_data[rid]:
            room_data[rid]["keyManagements"] = []


NTP_PORT = 123
NTP_PACKET = b'\x1b' + 47 * b'\x00'  # NTP v4, client mode

def _ntp_query(server=None):
    """Query NTP server and return (offset, is_valid). is_valid=False on failure."""
    if server is None:
        server = _ntp_config["server"]
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(5)
        t0 = time.time()
        sock.sendto(NTP_PACKET, (server, NTP_PORT))
        data, addr = sock.recvfrom(1024)
        t3 = time.time()
        sock.close()
        if len(data) < 48:
            return (0, False)
        # Extract transmit timestamp (bytes 40-47)
        tx_ts = struct.unpack('!I', data[40:44])[0]
        tx_frac = struct.unpack('!I', data[44:48])[0]
        tx_time = tx_ts + tx_frac / 2**32 - 2208988800  # NTP epoch to Unix
        # Calculate offset: ((t1 - t0) + (t2 - t3)) / 2  simplified
        rtt = t3 - t0
        offset = (tx_time - t0 - rtt / 2)
        return (offset, True)
    except socket.timeout:
        _debug(f"NTP query timeout for {server}")
        return (0, False)
    except Exception as e:
        _debug(f"NTP query failed: {e}")
        return (0, False)


def _ensure_room_data(rid):
    if rid not in room_data:
        room_data[rid] = {"noticePosts": [], "checklists": [], "chatMessages": [], "keyManagements": []}
    if "deletedPostIds" not in room_data[rid]:
        room_data[rid]["deletedPostIds"] = []
    if "deletedChecklistIds" not in room_data[rid]:
        room_data[rid]["deletedChecklistIds"] = []
    if "deletedKeyIds" not in room_data[rid]:
        room_data[rid]["deletedKeyIds"] = []


def _generate_peer_id():
    """Generate a unique 4-char uppercase alphanumeric peer ID."""
    chars = string.ascii_uppercase + string.digits
    while True:
        pid = "".join(random.choices(chars, k=4))
        if pid not in peer_ids:
            peer_ids.add(pid)
            return pid


# Mini HTTP server — serves static files + REST API on port 8766
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _api_error_response(status, message):
    return {"error": message}, status


def _api_health():
    """GET /api/health"""
    return {
        "status": "ok",
        "version": "1.1.0",
        "uptime": int(time.time() - _start_time),
        "activeRooms": len(rooms),
        "onlinePeers": sum(len(p) for p in rooms.values()),
    }, 200


def _api_room_state(room_id):
    """GET /api/rooms/:room/state"""
    _ensure_room_data(room_id)
    rd = room_data[room_id]
    return {
        "noticePosts": rd.get("noticePosts", []),
        "checklists": rd.get("checklists", []),
        "keyManagements": rd.get("keyManagements", []),
        "deletedNoticeIds": rd.get("deletedPostIds", []),
        "deletedChecklistIds": rd.get("deletedChecklistIds", []),
        "deletedKeyIds": rd.get("deletedKeyIds", []),
    }, 200


def _api_room_notice(room_id, method, query, body):
    """POST/PUT/DELETE /api/rooms/:room/notice"""
    _ensure_room_data(room_id)

    if method == "POST":
        data = json.loads(body) if body else {}
        now_ms = int(time.time() * 1000)
        post = {
            "id": data.get("id", str(now_ms)),
            "title": data.get("title", ""),
            "content": data.get("content", ""),
            "category": data.get("category", ""),
            "tags": data.get("tags", []),
            "author": data.get("author", ""),
            "createdAt": now_ms,
        }
        room_data[room_id]["noticePosts"].append(post)
        _save_state()
        return {"success": True, "post": post}, 200

    elif method == "PUT":
        data = json.loads(body) if body else {}
        post_id = query.get("id") or data.get("id")
        if not post_id:
            return _api_error_response(400, "id is required")
        found = None
        for post in room_data[room_id]["noticePosts"]:
            if post.get("id") == post_id:
                for key in ("title", "content", "category", "tags", "color"):
                    if key in data:
                        post[key] = data[key]
                post["editedAt"] = data.get("editedAt", int(time.time() * 1000))
                found = post
                break
        if not found:
            return _api_error_response(404, "notice not found")
        _save_state()
        return {"success": True, "post": found}, 200

    elif method == "DELETE":
        del_id = query.get("id")
        if not del_id:
            return _api_error_response(400, "id is required")
        room_data[room_id]["noticePosts"] = [
            p for p in room_data[room_id]["noticePosts"] if p.get("id") != del_id
        ]
        if del_id not in room_data[room_id].get("deletedPostIds", []):
            room_data[room_id].setdefault("deletedPostIds", []).append(del_id)
        _save_state()
        return {"success": True, "id": del_id}, 200

    return _api_error_response(405, "method not allowed")


def _api_room_checklist(room_id, method, query, body):
    """POST/PUT/DELETE /api/rooms/:room/checklist"""
    _ensure_room_data(room_id)

    if method == "POST":
        board = json.loads(body) if body else {}
        room_data[room_id]["checklists"].append(board)
        _save_state()
        return {"success": True, "board": board}, 200

    elif method == "PUT":
        data = json.loads(body) if body else {}
        board_id = query.get("id") or data.get("id")
        if not board_id:
            return _api_error_response(400, "id is required")
        found = None
        for board in room_data[room_id]["checklists"]:
            if board.get("id") == board_id:
                for key in ("title", "category", "tags", "color"):
                    if key in data:
                        board[key] = data[key]
                found = board
                break
        if not found:
            return _api_error_response(404, "checklist not found")
        _save_state()
        return {"success": True, "board": found}, 200

    elif method == "DELETE":
        del_id = query.get("id")
        if not del_id:
            return _api_error_response(400, "id is required")
        room_data[room_id]["checklists"] = [
            b for b in room_data[room_id]["checklists"] if b.get("id") != del_id
        ]
        if del_id not in room_data[room_id].get("deletedChecklistIds", []):
            room_data[room_id].setdefault("deletedChecklistIds", []).append(del_id)
        _save_state()
        return {"success": True, "id": del_id}, 200

    return _api_error_response(405, "method not allowed")


def _api_room_keymgmt(room_id, method, query, body):
    """POST/PUT/DELETE /api/rooms/:room/keymgmt"""
    _ensure_room_data(room_id)

    if method == "POST":
        entry = json.loads(body) if body else {}
        room_data[room_id].setdefault("keyManagements", []).append(entry)
        _save_state()
        return {"success": True, "entry": entry}, 200

    elif method == "PUT":
        data = json.loads(body) if body else {}
        entry_id = query.get("id") or data.get("id")
        if not entry_id:
            return _api_error_response(400, "id is required")
        found = None
        for entry in room_data[room_id].get("keyManagements", []):
            if entry.get("id") == entry_id:
                for key in ("label", "streamKey", "streamUrl", "currentProgram"):
                    if key in data:
                        entry[key] = data[key]
                entry["updatedAt"] = data.get("updatedAt", int(time.time() * 1000))
                found = entry
                break
        if not found:
            return _api_error_response(404, "key management entry not found")
        _save_state()
        return {"success": True, "entry": found}, 200

    elif method == "DELETE":
        del_id = query.get("id")
        if not del_id:
            return _api_error_response(400, "id is required")
        room_data[room_id]["keyManagements"] = [
            e for e in room_data[room_id].get("keyManagements", []) if e.get("id") != del_id
        ]
        if del_id not in room_data[room_id].get("deletedKeyIds", []):
            room_data[room_id].setdefault("deletedKeyIds", []).append(del_id)
        _save_state()
        return {"success": True, "id": del_id}, 200

    return _api_error_response(405, "method not allowed")


def _api_room_chats(room_id):
    """GET /api/rooms/:room/chats"""
    _ensure_room_data(room_id)
    return {"messages": room_data[room_id].get("chatMessages", [])}, 200


async def _mini_http(reader, writer):
    """Serve HTTP requests — REST API + static files."""
    try:
        line = await asyncio.wait_for(reader.readline(), timeout=5)
        if not line:
            writer.close(); return
        parts = line.decode(errors="replace").strip().split(" ")
        if len(parts) < 2:
            writer.close(); return
        method = parts[0].upper()
        raw_path = parts[1]

        path = raw_path
        query_params = {}
        if "?" in raw_path:
            path, qs = raw_path.split("?", 1)
            for pair in qs.split("&"):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    query_params[k] = v

        headers = {}
        while True:
            hdr = await reader.readline()
            if hdr == b"\r\n" or not hdr:
                break
            hdr_str = hdr.decode(errors="replace").strip()
            if ":" in hdr_str:
                hk, hv = hdr_str.split(":", 1)
                headers[hk.strip().lower()] = hv.strip()

        body = b""
        cl = headers.get("content-length", "0")
        try:
            content_length = int(cl)
        except ValueError:
            content_length = 0
        if content_length > 0:
            body = await asyncio.wait_for(reader.readexactly(content_length), timeout=5)

        if method == "OPTIONS":
            resp = (
                b"HTTP/1.1 200 OK\r\n"
                b"Access-Control-Allow-Origin: *\r\n"
                b"Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n"
                b"Access-Control-Allow-Headers: Content-Type, Authorization\r\n"
                b"Content-Length: 0\r\n"
                b"Connection: close\r\n\r\n"
            )
            writer.write(resp); await writer.drain()
            writer.close(); return

        if path.startswith("/api/"):
            status_code = 200
            response_data = None

            try:
                rest_path = path[len("/api/"):]

                if rest_path == "health":
                    response_data, status_code = _api_health()
                elif rest_path.startswith("rooms/"):
                    sub = rest_path[len("rooms/"):]
                    slash_idx = sub.find("/")
                    if slash_idx == -1:
                        room_id = sub
                        sub_resource = None
                    else:
                        room_id = sub[:slash_idx]
                        sub_resource = sub[slash_idx+1:]

                    if sub_resource is None:
                        response_data, status_code = {"error": "missing resource"}, 404
                    elif sub_resource == "state" and method == "GET":
                        response_data, status_code = _api_room_state(room_id)
                    elif sub_resource == "notice":
                        response_data, status_code = _api_room_notice(room_id, method, query_params, body)
                    elif sub_resource == "checklist":
                        response_data, status_code = _api_room_checklist(room_id, method, query_params, body)
                    elif sub_resource == "keymgmt":
                        response_data, status_code = _api_room_keymgmt(room_id, method, query_params, body)
                    elif sub_resource == "chats" and method == "GET":
                        response_data, status_code = _api_room_chats(room_id)
                    else:
                        response_data, status_code = {"error": "not found"}, 404
                else:
                    response_data, status_code = {"error": "not found"}, 404
            except json.JSONDecodeError:
                response_data, status_code = {"error": "invalid JSON"}, 400
            except Exception as e:
                _log('REST-API', f'Error: {e}')
                response_data, status_code = {"error": "internal server error"}, 500

            body_out = json.dumps(response_data).encode()
            status_msg = {
                200: b"200 OK", 201: b"201 Created", 400: b"400 Bad Request",
                404: b"404 Not Found", 405: b"405 Method Not Allowed",
                500: b"500 Internal Server Error",
            }.get(status_code, b"500 Internal Server Error")
            resp = (
                b"HTTP/1.1 " + status_msg + b"\r\n"
                b"Access-Control-Allow-Origin: *\r\n"
                b"Content-Type: application/json\r\n"
                b"Content-Length: " + str(len(body_out)).encode() + b"\r\n"
                b"Connection: close\r\n\r\n"
            ) + body_out
            writer.write(resp); await writer.drain()
            writer.close(); return

        if path == "/":
            path = "/clipper.html"
        safe_path = os.path.normpath(os.path.join(_SCRIPT_DIR, path.lstrip("/")))
        if not safe_path.startswith(_SCRIPT_DIR) or not os.path.isfile(safe_path):
            body_out = b"Not Found"; status = b"404 Not Found"; ct = b"text/plain"
        else:
            ct_val, _ = mimetypes.guess_type(safe_path)
            ct = (ct_val or "text/html").encode()
            with open(safe_path, "rb") as f:
                body_out = f.read()
            status = b"200 OK"
        resp = (b"HTTP/1.1 " + status + b"\r\n"
                b"Content-Type: " + ct + b"\r\n"
                b"Content-Length: " + str(len(body_out)).encode() + b"\r\n"
                b"Cache-Control: no-store, no-cache, must-revalidate\r\n"
                b"Pragma: no-cache\r\n"
                b"Expires: 0\r\n"
                b"Access-Control-Allow-Origin: *\r\n"
                b"Connection: close\r\n\r\n") + body_out
        writer.write(resp); await writer.drain()
    except Exception:
        pass
    finally:
        try: writer.close()
        except: pass


def _generate_peer_id():
    """Generate a unique 4-char uppercase alphanumeric peer ID."""
    chars = string.ascii_uppercase + string.digits
    while True:
        pid = "".join(random.choices(chars, k=4))
        if pid not in peer_ids:
            peer_ids.add(pid)
            return pid


def _ts_val(v):
    """Convert timestamp (epoch ms number or ISO string) to float (epoch ms)."""
    if v is None:
        return 0.0
    if isinstance(v, str):
        try:
            return float(v)
        except ValueError:
            # ISO format: "2026-06-20T05:00:00.000Z"
            return datetime.fromisoformat(v.replace('Z','+00:00')).timestamp() * 1000
    return float(v)

async def handler(websocket):
    """Handle a WebSocket connection."""
    room_id = None
    my_peer_id = None

    try:
        async for message in websocket:
            data = json.loads(message)
            msg_type = data.get("type")
            _debug(f"← RX type={msg_type} room={data.get('room','?')} from={my_peer_id}")

            if msg_type == "generate":
                code = str(random.randint(1000, 9999))

            elif msg_type == "join":
                rid = data.get("room")
                if not rid:
                    await websocket.send(json.dumps({"type": "error", "message": "room is required"}))
                    continue

                if rid in rooms and len(rooms[rid]) >= MAX_PEERS_PER_ROOM:
                    await websocket.send(json.dumps({"type": "room_full", "room": rid}))
                    continue

                # Leave previous room if any
                if room_id and room_id in rooms and my_peer_id:
                    rooms[room_id].pop(my_peer_id, None)
                    peer_ids.discard(my_peer_id)
                    if not rooms[room_id]:
                        del rooms[room_id]
                    else:
                        _broadcast(rooms[room_id], {"type": "peer_left", "peerId": my_peer_id}, exclude=websocket)

                # Assign peer ID and join
                rid = data["room"]
                my_peer_id = _generate_peer_id()
                now_iso = datetime.now(timezone.utc).isoformat()

                if rid not in rooms:
                    rooms[rid] = {}

                rooms[rid][my_peer_id] = {"ws": websocket, "joinedAt": now_iso, "lastHeartbeat": time.time(), "displayName": data.get("displayName", my_peer_id)}
                room_id = rid

                # Send joined confirmation
                await websocket.send(json.dumps({
                    "type": "joined",
                    "room": room_id,
                    "peerId": my_peer_id,
                }))
                _debug(f"→ TX joined room={room_id} peerId={my_peer_id}")

                # If others are in the room, send room_peers to joiner
                # and peer_joined to all existing members
                other_peers = {pid: info for pid, info in rooms[room_id].items() if pid != my_peer_id}
                if other_peers:
                    peers_list = [
                        {"peerId": pid, "joinedAt": info["joinedAt"], "displayName": info.get("displayName", pid)}
                        for pid, info in other_peers.items()
                    ]
                    await websocket.send(json.dumps({
                        "type": "room_peers",
                        "peers": peers_list,
                    }))
                    _debug(f"→ TX room_peers count={len(peers_list)} to={my_peer_id}")

                    # Notify all existing peers
                    joiner_name = rooms[room_id][my_peer_id].get("displayName", my_peer_id)
                    _broadcast(
                        rooms[room_id],
                        {"type": "peer_joined", "peerId": my_peer_id, "displayName": joiner_name},
                        exclude=websocket,
                    )

                # Broadcast updated peer list to all (including joiner)
                await _broadcast_peer_list(room_id)
                _log('JOIN', f'{my_peer_id} joined room {room_id} ({len(rooms[room_id])} peers)')

            elif msg_type in ("offer", "answer", "ice-candidate"):
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue

                target = data.get("to")
                if target and target in rooms[rid]:
                    # Targeted routing
                    out = {
                        "type": msg_type,
                        "from": my_peer_id,
                        "data": data.get("data"),
                    }
                    ws = rooms[rid][target]["ws"]
                    try:
                        await ws.send(json.dumps(out))
                        _debug(f"→ TX {msg_type} to={target} from={my_peer_id}")
                    except websockets.exceptions.ConnectionClosed:
                        pass
                elif not target and len(rooms[rid]) == 2:
                    # Backwards compat: 2-peer room, no target → send to the other peer
                    for pid, info in rooms[rid].items():
                        if pid != my_peer_id:
                            out = {
                                "type": msg_type,
                                "from": my_peer_id,
                                "data": data.get("data"),
                            }
                            try:
                                await info["ws"].send(json.dumps(out))
                                _debug(f"→ TX {msg_type} to={pid} from={my_peer_id} (2-peer compat)")
                            except websockets.exceptions.ConnectionClosed:
                                pass
                            break
                else:
                    await websocket.send(json.dumps({
                        "type": "error",
                        "message": f"target peer '{target}' not found in room",
                    }))

            elif msg_type == "chat-backup":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                backup_msg = {
                    "text": data.get("text", ""),
                    "from": data.get("from", ""),
                    "timestamp": data.get("timestamp", time.time() * 1000),
                    "serverReceivedAt": time.time() * 1000,
                }
                room_data[rid]["chatMessages"].append(backup_msg)
                # Enforce retention: remove messages older than configured period
                retention = _config.get("chatRetentionDays", CHAT_RETENTION_DAYS)
                cutoff = (time.time() - retention * 86400) * 1000
                room_data[rid]["chatMessages"] = [
                    m for m in room_data[rid]["chatMessages"]
                    if _ts_val(m["timestamp"]) > cutoff
                ]
                _save_state()
                _log('CHAT-BACKUP', f'{my_peer_id} backed up chat msg in {rid} ({len(room_data[rid]["chatMessages"])} stored)')

            elif msg_type == "notice-create":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                post = data.get("post", {})
                # Accept optional v2 fields: category, tags, color
                room_data[rid]["noticePosts"].append(post)
                _broadcast(
                    rooms[rid],
                    {"type": "notice-create", "post": post},
                    exclude=websocket,
                )
                _log('NOTICE', f'{my_peer_id} created post in {rid}')
                _save_state()

            elif msg_type == "notice-edit":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                post_id = data.get("id")
                for post in room_data[rid]["noticePosts"]:
                    if post.get("id") == post_id:
                        post["title"] = data.get("title", post.get("title", ""))
                        post["content"] = data.get("content", post.get("content", ""))
                        post["editedAt"] = data.get("editedAt", time.time() * 1000)
                        # Optional v2 fields
                        if "category" in data:
                            post["category"] = data["category"]
                        if "tags" in data:
                            post["tags"] = data["tags"]
                        if "color" in data:
                            post["color"] = data["color"]
                        break
                broadcast_msg = {
                    "type": "notice-edit",
                    "id": post_id,
                    "title": data.get("title"),
                    "content": data.get("content"),
                    "editedAt": data.get("editedAt"),
                }
                if "category" in data:
                    broadcast_msg["category"] = data["category"]
                if "tags" in data:
                    broadcast_msg["tags"] = data["tags"]
                if "color" in data:
                    broadcast_msg["color"] = data["color"]
                _broadcast(
                    rooms[rid],
                    broadcast_msg,
                    exclude=websocket,
                )
                _save_state()

            elif msg_type == "notice-delete":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                del_id = data.get("id")
                room_data[rid]["noticePosts"] = [
                    p for p in room_data[rid]["noticePosts"] if p.get("id") != del_id
                ]
                if del_id and del_id not in room_data[rid]["deletedPostIds"]:
                    room_data[rid]["deletedPostIds"].append(del_id)
                _broadcast(
                    rooms[rid],
                    {"type": "notice-delete", "id": del_id},
                    exclude=websocket,
                )
                _save_state()

            elif msg_type == "notice-pin":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                pin_id = data.get("id")
                pin_val = data.get("pinned", False)
                for post in room_data[rid]["noticePosts"]:
                    if post.get("id") == pin_id:
                        post["pinned"] = pin_val
                        break
                _broadcast(
                    rooms[rid],
                    {"type": "notice-pin", "id": pin_id, "pinned": pin_val},
                    exclude=websocket,
                )
                _save_state()

            elif msg_type == "checklistboard-create":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                room_data[rid]["checklists"].append(data.get("board", {}))
                _broadcast(
                    rooms[rid],
                    {"type": "checklistboard-create", "board": data.get("board", {})},
                    exclude=websocket,
                )
                _save_state()
                _log('CHECKLIST', f'{my_peer_id} created board in {rid}')

            elif msg_type == "checklistboard-edit":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                board_id = data.get("id")
                for board in room_data[rid]["checklists"]:
                    if board.get("id") == board_id:
                        if "title" in data:
                            board["title"] = data["title"]
                        if "category" in data:
                            board["category"] = data["category"]
                        if "tags" in data:
                            board["tags"] = data["tags"]
                        if "color" in data:
                            board["color"] = data["color"]
                        break
                broadcast_msg = {
                    "type": "checklistboard-edit",
                    "id": board_id,
                    "title": data.get("title"),
                    "category": data.get("category"),
                    "tags": data.get("tags"),
                    "color": data.get("color"),
                }
                _broadcast(rooms[rid], broadcast_msg, exclude=websocket)
                _save_state()

            elif msg_type == "checklistboard-delete":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                del_id = data.get("id")
                room_data[rid]["checklists"] = [
                    b for b in room_data[rid]["checklists"] if b.get("id") != del_id
                ]
                if del_id and del_id not in room_data[rid]["deletedChecklistIds"]:
                    room_data[rid]["deletedChecklistIds"].append(del_id)
                _broadcast(
                    rooms[rid],
                    {"type": "checklistboard-delete", "id": del_id},
                    exclude=websocket,
                )
                _save_state()

            elif msg_type == "checklistboard-pin":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                pin_id = data.get("id")
                pin_val = data.get("pinned", False)
                for board in room_data[rid]["checklists"]:
                    if board.get("id") == pin_id:
                        board["pinned"] = pin_val
                        break
                _broadcast(
                    rooms[rid],
                    {"type": "checklistboard-pin", "id": pin_id, "pinned": pin_val},
                    exclude=websocket,
                )
                _save_state()

            elif msg_type == "checklistboard-remind":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                remind_id = data.get("id")
                remind_at = data.get("reminderAt")      # epoch ms or null to clear
                remind_title = data.get("reminderTitle", "")
                for board in room_data[rid]["checklists"]:
                    if board.get("id") == remind_id:
                        board["reminderAt"] = remind_at
                        board["reminderTitle"] = remind_title
                        break
                _broadcast(
                    rooms[rid],
                    {"type": "checklistboard-remind", "id": remind_id, "reminderAt": remind_at, "reminderTitle": remind_title},
                    exclude=websocket,
                )
                _save_state()
                _log('CHECKLIST', f'{my_peer_id} set reminder for board {remind_id} in {rid}')

            elif msg_type == "checklist-add":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                checklist_id = data.get("checklistId")
                item = data.get("item", {})
                for board in room_data[rid]["checklists"]:
                    if board.get("id") == checklist_id:
                        board.setdefault("items", []).append(item)
                        break
                _broadcast(
                    rooms[rid],
                    {"type": "checklist-add", "checklistId": checklist_id, "item": item},
                    exclude=websocket,
                )
                _save_state()

            elif msg_type == "checklist-toggle":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                checklist_id = data.get("checklistId")
                toggle_id = data.get("id")
                checked = data.get("checked", False)
                checked_at = data.get("checkedAt", time.time() * 1000)
                for board in room_data[rid]["checklists"]:
                    if board.get("id") == checklist_id:
                        for item in board.get("items", []):
                            if item.get("id") == toggle_id:
                                item["checked"] = checked
                                item["checkedAt"] = checked_at
                                break
                        break
                _broadcast(
                    rooms[rid],
                    {"type": "checklist-toggle", "checklistId": checklist_id, "id": toggle_id, "checked": checked, "checkedAt": checked_at},
                    exclude=websocket,
                )
                _save_state()

            elif msg_type == "checklist-delete":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                checklist_id = data.get("checklistId")
                del_id = data.get("id")
                for board in room_data[rid]["checklists"]:
                    if board.get("id") == checklist_id:
                        board["items"] = [
                            i for i in board.get("items", []) if i.get("id") != del_id
                        ]
                        break
                _broadcast(
                    rooms[rid],
                    {"type": "checklist-delete", "checklistId": checklist_id, "id": del_id},
                    exclude=websocket,
                )
                _save_state()

            elif msg_type == "checklist-reset":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                board_id = data.get("id") or data.get("checklistId")
                if not board_id:
                    await websocket.send(json.dumps({"type": "error", "message": "checklistId required"}))
                    continue
                for board in room_data[rid]["checklists"]:
                    if board.get("id") == board_id:
                        for item in board.get("items", []):
                            item["checked"] = False
                            item["checkedAt"] = None
                        break
                _broadcast(
                    rooms[rid],
                    {"type": "checklist-reset", "id": board_id},
                    exclude=websocket,
                )
                _save_state()
                _log('CHECKLIST', f'{my_peer_id} reset all items in board {board_id} in {rid}')

            elif msg_type == "state-get":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                posts_count = len(room_data[rid].get("noticePosts", []))
                boards_count = len(room_data[rid].get("checklists", []))
                await websocket.send(json.dumps({
                    "type": "room-state",
                    "noticePosts": room_data[rid].get("noticePosts", []),
                    "checklists": room_data[rid].get("checklists", []),
                    "keyManagements": room_data[rid].get("keyManagements", []),
                    "deletedNoticeIds": room_data[rid].get("deletedPostIds", []),
                    "deletedChecklistIds": room_data[rid].get("deletedChecklistIds", []),
                    "deletedKeyIds": room_data[rid].get("deletedKeyIds", []),
                }))
                _log('STATE', f'{my_peer_id} requested state in {rid}')
                _debug(f"→ TX room-state: {posts_count} posts, {boards_count} boards to {my_peer_id}")

            elif msg_type == "chat-history":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                since = data.get("since")
                if since is None:
                    retention = _config.get("chatRetentionDays", CHAT_RETENTION_DAYS)
                    cutoff = (time.time() - retention * 86400) * 1000
                    filtered = [
                        m for m in room_data[rid].get("chatMessages", [])
                        if _ts_val(m["timestamp"]) > cutoff
                    ]
                else:
                    since_f = _ts_val(since) if since is not None else 0
                    filtered = [
                        m for m in room_data[rid].get("chatMessages", [])
                        if _ts_val(m["timestamp"]) > since_f
                    ]
                await websocket.send(json.dumps({
                    "type": "chat-history-result",
                    "messages": filtered,
                    "room": rid,
                }))

            elif msg_type == "ping":
                if my_peer_id and room_id and room_id in rooms and my_peer_id in rooms[room_id]:
                    rooms[room_id][my_peer_id]["lastHeartbeat"] = time.time()
                    try:
                        await websocket.send(json.dumps({"type": "pong"}))
                    except:
                        pass

            elif msg_type == "time-request":
                # Client requests current server time
                ntp_on = _ntp_config["enabled"]
                if ntp_on:
                    server_ts = (time.time() + _ntp_config["offset"]) * 1000
                else:
                    server_ts = time.time() * 1000
                try:
                    await websocket.send(json.dumps({
                        "type": "time-sync",
                        "serverTime": server_ts,
                        "ntpEnabled": ntp_on,
                        "ntpServer": _ntp_config["server"],
                        "ntpValid": _ntp_config.get("_ntp_valid", False),
                    }))
                except:
                    pass

            elif msg_type == "ntp-config":
                if not _verify_session(data.get("token", "")):
                    await websocket.send(json.dumps({"type": "error", "message": "unauthorized"}))
                    continue
                if "ntpServer" in data:
                    _ntp_config["server"] = data["ntpServer"]
                if "ntpEnabled" in data:
                    _ntp_config["enabled"] = bool(data["ntpEnabled"])
                ntp_valid = False
                if _ntp_config["enabled"]:
                    offset, ntp_valid = _ntp_query()
                    _ntp_config["offset"] = offset
                    if ntp_valid:
                        _log('NTP', 'NTP sync from ' + _ntp_config["server"] + ': offset=' + str(_ntp_config["offset"]))
                    else:
                        _log('NTP', 'NTP sync FAILED from ' + _ntp_config["server"])
                else:
                    _ntp_config["offset"] = 0
                await websocket.send(json.dumps({
                    "type": "ntp-config-result",
                    "ntpServer": _ntp_config["server"],
                    "ntpEnabled": _ntp_config["enabled"],
                    "ntpOffset": round(_ntp_config["offset"], 3),
                    "ntpValid": ntp_valid,
                }))
                rid = data.get("room")
                name = data.get("displayName", "").strip()
                if not rid or rid not in rooms or not name or not my_peer_id:
                    await websocket.send(json.dumps({"type": "error", "message": "invalid register-name"}))
                    continue
                # Check for duplicate names — use _N suffix starting from 2
                final_name = name
                counter = 1
                while True:
                    has_conflict = any(info.get("displayName", "") == final_name for pid, info in rooms[rid].items() if pid != my_peer_id)
                    if not has_conflict:
                        break
                    counter += 1
                    final_name = f"{name}_{counter}"
                rooms[rid][my_peer_id]["displayName"] = final_name
                # Notify the client of their resolved name
                await websocket.send(json.dumps({"type": "name-resolved", "displayName": final_name, "wasConflict": final_name != name}))
                _log('NAME', f'{my_peer_id} registered as "{final_name}"{" (was conflict: " + name + ")" if final_name != name else ""} in {rid}')
                # Broadcast updated peer list
                await _broadcast_peer_list(rid)

            elif msg_type == "relay-data":
                rid = data.get("room")
                target = data.get("to")
                payload = data.get("data", {})
                if not rid or rid not in rooms or not target or target not in rooms[rid]:
                    await websocket.send(json.dumps({"type": "error", "message": "relay target not found"}))
                    continue
                out = {"type": "relay-data", "from": my_peer_id, "data": payload}
                try:
                    await rooms[rid][target]["ws"].send(json.dumps(out))
                    _debug(f"→ TX relay-data to={target} from={my_peer_id} ({payload.get('type','?')})")
                    _log('RELAY', f'{my_peer_id} → {target} ({payload.get("type","?")})')
                except websockets.exceptions.ConnectionClosed:
                    pass

            elif msg_type == "relay-chunk":
                rid = data.get("room")
                target = data.get("to")
                chunk_data = {
                    "type": "relay-chunk",
                    "from": my_peer_id,
                    "fileId": data.get("fileId"),
                    "chunk": data.get("chunk"),
                    "index": data.get("index"),
                    "total": data.get("total"),
                }
                if not rid or rid not in rooms or not target or target not in rooms[rid]:
                    continue
                try:
                    await rooms[rid][target]["ws"].send(json.dumps(chunk_data))
                except websockets.exceptions.ConnectionClosed:
                    pass

            elif msg_type == "file-cancel":
                rid = data.get("room")
                target = data.get("to") or data.get("sender")
                if rid and target and target in rooms.get(rid, {}):
                    try:
                        await rooms[rid][target]["ws"].send(json.dumps({
                            "type": "file-cancel",
                            "from": my_peer_id,
                            "fileId": data.get("fileId"),
                        }))
                        _debug(f"→ TX file-cancel to={target} from={my_peer_id} fileId={data.get('fileId')}")
                    except websockets.exceptions.ConnectionClosed:
                        pass

            elif msg_type == "admin-login":
                # Rate limit check
                ws_id = str(websocket.remote_address) if hasattr(websocket, "remote_address") else str(id(websocket))
                if not _check_login_rate(ws_id):
                    await websocket.send(json.dumps({"type": "admin-login-result", "success": False, "message": "登入嘗試過於頻繁，請 30 秒後再試"}))
                    _log("ADMIN", f"Rate limit hit for {ws_id}")
                    continue
                pw = data.get("password", "")
                if _verify_admin_password(pw):
                    token = _generate_session()
                    await websocket.send(json.dumps({
                        "type": "admin-login-result",
                        "success": True,
                        "message": "Authenticated",
                        "token": token,
                        "serverInfo": {
                            "version": "1.1.0",
                            "uptime": int(time.time() - _start_time) if hasattr(_log, '_start') else 0,
                            "activeRooms": len(rooms),
                            "activePeers": sum(len(p) for p in rooms.values()),
                            "dataRooms": len(room_data),
                            "chatRetentionDays": CHAT_RETENTION_DAYS,
                            "debugMode": DEBUG,
                            "ntpServer": _ntp_config["server"],
                            "ntpEnabled": _ntp_config["enabled"],
                            "ntpOffset": round(_ntp_config["offset"], 3),
                            "ntpValid": _ntp_config.get("_ntp_valid", False),
                            "stunServer": _config["stunServer"],
                        },
                        "config": {
                            "chatRetentionDays": CHAT_RETENTION_DAYS,
                            "stunServer": _config["stunServer"],
                            "logDir": LOG_DIR,
                            "dataFile": DB_PATH,
                        }
                    }))
                    _log('ADMIN', f'{my_peer_id} logged in successfully')
                else:
                    await websocket.send(json.dumps({"type": "admin-login-result", "success": False, "message": "密碼錯誤"}))
                    _log('ADMIN', f'{my_peer_id} login FAILED')

            elif msg_type == "admin-logs":
                if not _verify_session(data.get("token", "")):
                    await websocket.send(json.dumps({"type": "error", "message": "unauthorized"}))
                    continue
                count = data.get("count", 50)
                logs = _get_logs(count)
                await websocket.send(json.dumps({"type": "admin-logs-result", "logs": logs}))

            elif msg_type == "admin-log-download":
                if not _verify_session(data.get("token", "")):
                    await websocket.send(json.dumps({"type": "error", "message": "unauthorized"}))
                    continue
                today_log = os.path.join(LOG_DIR, f"clipper_{datetime.now().strftime('%Y%m%d')}.log")
                if os.path.exists(today_log):
                    with open(today_log, 'r', encoding='utf-8') as f:
                        log_text = f.read()
                else:
                    log_text = "(no logs yet)"
                await websocket.send(json.dumps({"type": "admin-log-download-result", "logText": log_text, "logName": f"clipper_{datetime.now().strftime('%Y%m%d')}.log"}))

            elif msg_type == "admin-change-password":
                old_pw = data.get("oldPassword", "")
                new_pw = data.get("newPassword", "")
                if not new_pw or len(new_pw) < 4:
                    await websocket.send(json.dumps({"type": "admin-change-password-result", "success": False, "message": "新密碼至少需要 4 個字元"}))
                    continue
                if not _verify_admin_password(old_pw):
                    await websocket.send(json.dumps({"type": "admin-change-password-result", "success": False, "message": "舊密碼錯誤"}))
                    continue
                _set_admin_password(new_pw)
                await websocket.send(json.dumps({"type": "admin-change-password-result", "success": True, "message": "密碼已更改"}))
                _log('ADMIN', f'{my_peer_id} changed password')

            elif msg_type == "admin-get-config":
                if not _verify_session(data.get("token", "")):
                    await websocket.send(json.dumps({"type": "error", "message": "unauthorized"}))
                    continue
                await websocket.send(json.dumps({
                    "type": "admin-config",
                    "config": {
                        "chatRetentionDays": _config["chatRetentionDays"],
                        "maxPeersPerRoom": MAX_PEERS_PER_ROOM,
                        "debug": DEBUG,
                        "logRetentionHours": LOG_RETENTION_HOURS,
                        "dataFile": DB_PATH,
                        "logDir": LOG_DIR,
                        "ntpServer": _ntp_config["server"],
                        "ntpEnabled": _ntp_config["enabled"],
                        "ntpOffset": round(_ntp_config["offset"], 3),
                        "stunServer": _config["stunServer"],                        "ntpOffset": round(_ntp_config["offset"], 3),
                        "ntpValid": _ntp_config.get("_ntp_valid", False),
                        "stunServer": _config["stunServer"],
                    }
                }))

            elif msg_type == "admin-set-config":
                if not _verify_session(data.get("token", "")):
                    await websocket.send(json.dumps({"type": "error", "message": "unauthorized"}))
                    continue
                cfg = data.get("config", {})
                # Only allow safe overrides (use mutable dict to avoid global issues)
                if "chatRetentionDays" in cfg:
                    _config["chatRetentionDays"] = int(cfg["chatRetentionDays"])
                if "stunServer" in cfg:
                    _config["stunServer"] = str(cfg["stunServer"])
                response = {"type": "admin-set-config-result", "success": True, "message": "設定已更新", "config": _config}
                await websocket.send(json.dumps(response))
                _log('ADMIN', f'{my_peer_id} updated server config')

            elif msg_type == "admin-export":
                if not _verify_session(data.get("token", "")):
                    await websocket.send(json.dumps({"type": "error", "message": "unauthorized"}))
                    continue
                dump_data = {
                    "exportedAt": datetime.now(timezone.utc).isoformat(),
                    "version": "1.1.0",
                    "config": {
                        "chatRetentionDays": _config["chatRetentionDays"],
                        "ntpServer": _ntp_config["server"],
                        "ntpEnabled": _ntp_config["enabled"],
                    },
                    "rooms": room_data,
                }
                await websocket.send(json.dumps({"type": "admin-export-result", "dump": json.dumps(dump_data)}))
                _log('ADMIN', f'{my_peer_id} exported config dump ({len(room_data)} rooms)')

            elif msg_type == "admin-import":
                if not _verify_session(data.get("token", "")):
                    await websocket.send(json.dumps({"type": "error", "message": "unauthorized"}))
                    continue
                dump_raw = data.get("dump", "")
                try:
                    dump_data = json.loads(dump_raw)
                    if "rooms" in dump_data:
                        imported = 0
                        for rid, rdata in dump_data["rooms"].items():
                            if isinstance(rdata, dict):
                                room_data[rid] = rdata
                                imported += 1
                        _save_state()
                        _log('ADMIN', f'{my_peer_id} imported {imported} rooms from config dump')
                        await websocket.send(json.dumps({"type": "admin-import-result", "success": True, "message": f"成功匯入 {imported} 個房間的資料", "count": imported}))
                    else:
                        await websocket.send(json.dumps({"type": "admin-import-result", "success": False, "message": "無效的備份檔案：缺少 rooms 資料"}))
                except json.JSONDecodeError as e:
                    await websocket.send(json.dumps({"type": "admin-import-result", "success": False, "message": f"無效的 JSON 格式：{e}"}))

            elif msg_type == "keymgmt-create":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                entry = data.get("entry", {})
                if "keyManagements" not in room_data[rid]:
                    room_data[rid]["keyManagements"] = []
                room_data[rid]["keyManagements"].append(entry)
                _broadcast(
                    rooms[rid],
                    {"type": "keymgmt-create", "entry": entry},
                    exclude=websocket,
                )
                _log('KEYMGMT', f'{my_peer_id} created key entry in {rid}')
                _save_state()

            elif msg_type == "keymgmt-edit":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                edit_id = data.get("id")
                for entry in room_data[rid].get("keyManagements", []):
                    if entry.get("id") == edit_id:
                        if "label" in data:
                            entry["label"] = data["label"]
                        if "streamKey" in data:
                            entry["streamKey"] = data["streamKey"]
                        if "streamUrl" in data:
                            entry["streamUrl"] = data["streamUrl"]
                        if "currentProgram" in data:
                            entry["currentProgram"] = data["currentProgram"]
                        entry["updatedAt"] = data.get("updatedAt", time.time() * 1000)
                        break
                _broadcast(
                    rooms[rid],
                    {"type": "keymgmt-edit", "id": edit_id, "label": data.get("label"), "streamKey": data.get("streamKey"), "streamUrl": data.get("streamUrl"), "currentProgram": data.get("currentProgram")},
                    exclude=websocket,
                )
                _save_state()
                _log('KEYMGMT', f'{my_peer_id} edited key entry {edit_id} in {rid}')

            elif msg_type == "keymgmt-toggle-active":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                toggle_id = data.get("id")
                new_active = False
                for entry in room_data[rid].get("keyManagements", []):
                    if entry.get("id") == toggle_id:
                        entry["isActive"] = not entry.get("isActive", False)
                        new_active = entry["isActive"]
                        break
                _broadcast(
                    rooms[rid],
                    {"type": "keymgmt-toggle-active", "id": toggle_id, "isActive": new_active},
                    exclude=websocket,
                )
                _save_state()
                _log('KEYMGMT', f'{my_peer_id} toggled key entry {toggle_id} in {rid}')

            elif msg_type == "keymgmt-set-program":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                prog_id = data.get("id")
                current_program = data.get("currentProgram", "")
                for entry in room_data[rid].get("keyManagements", []):
                    if entry.get("id") == prog_id:
                        entry["currentProgram"] = current_program
                        entry["updatedAt"] = time.time() * 1000
                        break
                _broadcast(
                    rooms[rid],
                    {"type": "keymgmt-set-program", "id": prog_id, "currentProgram": current_program},
                    exclude=websocket,
                )
                _save_state()
                _log('KEYMGMT', f'{my_peer_id} set program for key entry {prog_id} in {rid}')

            elif msg_type == "keymgmt-delete":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                del_id = data.get("id")
                room_data[rid]["keyManagements"] = [
                    e for e in room_data[rid].get("keyManagements", []) if e.get("id") != del_id
                ]
                if del_id and del_id not in room_data[rid]["deletedKeyIds"]:
                    room_data[rid]["deletedKeyIds"].append(del_id)
                _broadcast(
                    rooms[rid],
                    {"type": "keymgmt-delete", "id": del_id},
                    exclude=websocket,
                )
                _save_state()
                _log('KEYMGMT', f'{my_peer_id} deleted key entry {del_id} in {rid}')

            elif msg_type == "dump":
                iso_ts = datetime.now(timezone.utc).isoformat()
                rooms_diag = {}
                total_notices = 0
                total_boards = 0
                for rid_key, rdata in room_data.items():
                    n = len(rdata.get("noticePosts", []))
                    b = len(rdata.get("checklists", []))
                    total_notices += n
                    total_boards += b
                    rooms_diag[rid_key] = {
                        "peerCount": len(rooms.get(rid_key, {})),
                        "noticePosts": rdata.get("noticePosts", []),
                        "checklists": rdata.get("checklists", []),
                        "chatMessageCount": len(rdata.get("chatMessages", [])),
                    }
                await websocket.send(json.dumps({
                    "type": "dump-result",
                    "timestamp": iso_ts,
                    "retention_days": CHAT_RETENTION_DAYS,
                    "room_count": len(room_data),
                    "rooms": rooms_diag,
                }))
                _log('DUMP', f'Dump requested by {my_peer_id}')
                _debug(f"→ TX dump: {len(room_data)} rooms, {total_notices} notices, {total_boards} boards")

    except websockets.exceptions.ConnectionClosed:
        _debug(f"WebSocket connection closed for {my_peer_id}")
        pass
    finally:
        if room_id and room_id in rooms and my_peer_id:
            rooms[room_id].pop(my_peer_id, None)
            peer_ids.discard(my_peer_id)
            if rooms[room_id]:
                remaining = len(rooms[room_id])
                _debug(f"peer_left broadcast: {my_peer_id} left, {remaining} remaining in {room_id}")
                _broadcast(
                    rooms[room_id],
                    {"type": "peer_left", "peerId": my_peer_id},
                )
            else:
                _debug(f"Room {room_id} now empty, deleting")
                del rooms[room_id]

        if room_id and room_id in rooms and len(rooms[room_id]) > 0:
            asyncio.create_task(_broadcast_peer_list(room_id))
        _log('DISCONNECT', f'{my_peer_id} disconnected (room: {room_id})')


def _broadcast(room_peers, message, exclude=None):
    """Send a message to all peers in a room, optionally excluding one."""
    payload = json.dumps(message)
    target_ids = []
    for info in room_peers.values():
        if exclude and info["ws"] == exclude:
            continue
        target_ids.append('?')
        try:
            asyncio.create_task(info["ws"].send(payload))
        except websockets.exceptions.ConnectionClosed:
            pass
    if DEBUG:
        mtype = message.get("type", "?")
        _debug(f"→ TX broadcast type={mtype} to={len(target_ids)} peers")


async def _broadcast_peer_list(rid):
    """Broadcast the current online peer list for a room."""
    if rid not in rooms:
        return
    peer_list = []
    for pid, info in rooms[rid].items():
        peer_list.append({
            "peerId": pid,
            "displayName": info.get("displayName", pid),
            "joinedAt": info.get("joinedAt", ""),
            "alive": True,
        })
    _broadcast(rooms[rid], {"type": "peer-list", "peers": peer_list})


HEARTBEAT_TIMEOUT = 20  # seconds without heartbeat = stale

async def _heartbeat_check():
    """Periodic heartbeat check. Remove stale peers and broadcast lists."""
    while True:
        await asyncio.sleep(10)
        now = time.time()
        for rid in list(rooms.keys()):
            stale = []
            for pid, info in list(rooms[rid].items()):
                if now - info.get("lastHeartbeat", 0) > HEARTBEAT_TIMEOUT:
                    stale.append(pid)
            for pid in stale:
                _log('HEARTBEAT', f'{pid} timed out in room {rid}')
                try:
                    await rooms[rid][pid]["ws"].close()
                except:
                    pass
                rooms[rid].pop(pid, None)
                peer_ids.discard(pid)
            if stale:
                if rooms[rid]:
                    await _broadcast_peer_list(rid)
                else:
                    del rooms[rid]


async def _periodic_log_rotation():
    """Rotate logs every hour."""
    while True:
        await asyncio.sleep(3600)
        _rotate_logs()


async def _session_cleanup():
    """Clean up expired sessions every 5 minutes."""
    while True:
        await asyncio.sleep(300)
        now = time.time()
        expired = [t for t, e in _sessions.items() if now - e["createdAt"] > SESSION_TIMEOUT]
        for t in expired:
            _sessions.pop(t, None)
        if expired and DEBUG:
            _debug(f"Cleaned up {len(expired)} expired sessions")

async def _login_attempt_cleanup():
    """Clean up old login attempt records every 60 seconds."""
    while True:
        await asyncio.sleep(60)
        now = time.time()
        stale = [k for k, v in _login_attempts.items() if now - v["first"] > LOGIN_COOLDOWN]
        for k in stale:
            _login_attempts.pop(k, None)


async def _ntp_sync_loop():
    """Periodically sync NTP every 60 seconds."""
    while True:
        await asyncio.sleep(60)
        if _ntp_config["enabled"]:
            offset, valid = _ntp_query()
            _ntp_config["offset"] = offset
            _ntp_config["_ntp_valid"] = valid
            if valid:
                _debug('NTP re-sync: offset=%.3fs' % _ntp_config['offset'])

async def main():
    global _start_time
    _start_time = time.time()
    log_path = _setup_logging()
    _rotate_logs()
    _init_db()
    # Add key_managements column for existing databases
    _migrate_conn = sqlite3.connect(DB_PATH)
    try:
        _migrate_conn.execute("ALTER TABLE rooms ADD COLUMN key_managements TEXT NOT NULL DEFAULT '[]'")
        _migrate_conn.commit()
    except sqlite3.OperationalError:
        pass  # column already exists
    finally:
        _migrate_conn.close()
    _init_admin_password()
    _load_state()
    _migrate_room_data()
    asyncio.create_task(_heartbeat_check())
    asyncio.create_task(_periodic_log_rotation())
    asyncio.create_task(_session_cleanup())
    asyncio.create_task(_login_attempt_cleanup())
    asyncio.create_task(_ntp_sync_loop())
    # Initial NTP sync
    if _ntp_config["enabled"]:
        offset, valid = _ntp_query()
        _ntp_config["offset"] = offset
        _ntp_config["_ntp_valid"] = valid
        if valid:
            _log('NTP', 'Initial NTP sync from ' + _ntp_config["server"] + ': offset=' + str(_ntp_config["offset"]))
        else:
            _log('NTP', 'Initial NTP sync FAILED from ' + _ntp_config["server"])
    total_notices = sum(len(r.get("noticePosts", [])) for r in room_data.values())
    total_boards = sum(len(r.get("checklists", [])) for r in room_data.values())
    total_chats = sum(len(r.get("chatMessages", [])) for r in room_data.values())
    _log('STARTUP', f'Loaded {len(room_data)} rooms from SQLite ({DB_PATH})')
    _log('STARTUP', f'Data: {total_notices} notices, {total_boards} boards, {total_chats} chat backups')
    _log('STARTUP', f'Chat retention: {CHAT_RETENTION_DAYS} days')
    _log('STARTUP', f'DEBUG mode: {"ON" if DEBUG else "OFF"}')
    _log('STARTUP', 'listening on ws://localhost:8765  |  http://localhost:8766')

    loop = asyncio.get_running_loop()
    stop = loop.create_future()

    # Start HTTP server on port 8766
    http_server = await asyncio.start_server(_mini_http, "0.0.0.0", 8766)

    async with websockets.serve(handler, "0.0.0.0", 8765):
        await stop


if __name__ == "__main__":
    asyncio.run(main())
