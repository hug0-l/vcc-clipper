#!/usr/bin/env python3
"""VCC_Clipper signaling server - N-peer WebSocket room for WebRTC Full Mesh."""

import asyncio
import json
import os
import random
import signal
import string
import time
from datetime import datetime, timezone

import websockets


# room_id -> {peerId: {"ws": websocket, "joinedAt": "ISO timestamp"}}
rooms = {}
peer_ids = set()  # all assigned peerIds across all rooms

MAX_PEERS_PER_ROOM = 50
# room_id -> {"noticePosts": [...], "checklistItems": [...], "chatMessages": [...]}
room_data = {}

CHAT_RETENTION_DAYS = 7    # How long to keep chat backups (adjustable)
DATA_FILE = "vcc_server_state.json"


def _log(category, message):
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] [{category}] {message}")


def _load_state():
    global room_data
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f:
            room_data = json.load(f)
    else:
        room_data = {}


def _save_state():
    with open(DATA_FILE, 'w') as f:
        json.dump(room_data, f)


def _ensure_room_data(rid):
    if rid not in room_data:
        room_data[rid] = {"noticePosts": [], "checklistItems": [], "chatMessages": []}


def _generate_peer_id():
    """Generate a unique 4-char uppercase alphanumeric peer ID."""
    chars = string.ascii_uppercase + string.digits
    while True:
        pid = "".join(random.choices(chars, k=4))
        if pid not in peer_ids:
            peer_ids.add(pid)
            return pid


async def handler(websocket):
    """Handle a WebSocket connection."""
    room_id = None
    my_peer_id = None

    try:
        async for message in websocket:
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "generate":
                code = str(random.randint(1000, 9999))
                while code in rooms:
                    code = str(random.randint(1000, 9999))
                await websocket.send(json.dumps({"type": "generated", "room": code}))

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

                rooms[rid][my_peer_id] = {"ws": websocket, "joinedAt": now_iso}
                room_id = rid

                # Send joined confirmation
                await websocket.send(json.dumps({
                    "type": "joined",
                    "room": room_id,
                    "peerId": my_peer_id,
                }))

                # If others are in the room, send room_peers to joiner
                # and peer_joined to all existing members
                other_peers = {pid: info for pid, info in rooms[room_id].items() if pid != my_peer_id}
                if other_peers:
                    peers_list = [
                        {"peerId": pid, "joinedAt": info["joinedAt"]}
                        for pid, info in other_peers.items()
                    ]
                    await websocket.send(json.dumps({
                        "type": "room_peers",
                        "peers": peers_list,
                    }))

                    # Notify all existing peers
                    _broadcast(
                        rooms[room_id],
                        {"type": "peer_joined", "peerId": my_peer_id},
                        exclude=websocket,
                    )

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
                # Enforce retention: remove messages older than CHAT_RETENTION_DAYS
                cutoff = (time.time() - CHAT_RETENTION_DAYS * 86400) * 1000
                room_data[rid]["chatMessages"] = [
                    m for m in room_data[rid]["chatMessages"]
                    if m["timestamp"] > cutoff
                ]
                _save_state()
                _log('CHAT-BACKUP', f'{my_peer_id} backed up chat msg in {rid} ({len(room_data[rid]["chatMessages"])} stored)')

            elif msg_type == "notice-create":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                room_data[rid]["noticePosts"].append(data.get("post", {}))
                _broadcast(
                    rooms[rid],
                    {"type": "notice-create", "post": data.get("post", {})},
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
                        break
                _broadcast(
                    rooms[rid],
                    {
                        "type": "notice-edit",
                        "id": post_id,
                        "title": data.get("title"),
                        "content": data.get("content"),
                        "editedAt": data.get("editedAt"),
                    },
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

            elif msg_type == "checklist-add":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                room_data[rid]["checklistItems"].append(data.get("item", {}))
                _broadcast(
                    rooms[rid],
                    {"type": "checklist-add", "item": data.get("item", {})},
                    exclude=websocket,
                )
                _save_state()

            elif msg_type == "checklist-toggle":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                toggle_id = data.get("id")
                checked = data.get("checked", False)
                checked_at = data.get("checkedAt", time.time() * 1000)
                for item in room_data[rid]["checklistItems"]:
                    if item.get("id") == toggle_id:
                        item["checked"] = checked
                        item["checkedAt"] = checked_at
                        break
                _broadcast(
                    rooms[rid],
                    {"type": "checklist-toggle", "id": toggle_id, "checked": checked, "checkedAt": checked_at},
                    exclude=websocket,
                )
                _save_state()

            elif msg_type == "checklist-delete":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                del_id = data.get("id")
                room_data[rid]["checklistItems"] = [
                    i for i in room_data[rid]["checklistItems"] if i.get("id") != del_id
                ]
                _broadcast(
                    rooms[rid],
                    {"type": "checklist-delete", "id": del_id},
                    exclude=websocket,
                )
                _save_state()

            elif msg_type == "state-get":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                await websocket.send(json.dumps({
                    "type": "room-state",
                    "noticePosts": room_data[rid].get("noticePosts", []),
                    "checklistItems": room_data[rid].get("checklistItems", []),
                }))
                _log('STATE', f'{my_peer_id} requested state in {rid}')

            elif msg_type == "chat-history":
                rid = data.get("room")
                if not rid or rid not in rooms:
                    await websocket.send(json.dumps({"type": "error", "message": "room not found"}))
                    continue
                _ensure_room_data(rid)
                since = data.get("since")
                if since is None:
                    cutoff = (time.time() - CHAT_RETENTION_DAYS * 86400) * 1000
                    filtered = [
                        m for m in room_data[rid].get("chatMessages", [])
                        if m["timestamp"] > cutoff
                    ]
                else:
                    filtered = [
                        m for m in room_data[rid].get("chatMessages", [])
                        if m["timestamp"] > since
                    ]
                await websocket.send(json.dumps({
                    "type": "chat-history-result",
                    "messages": filtered,
                    "room": rid,
                }))

            elif msg_type == "dump":
                iso_ts = datetime.now(timezone.utc).isoformat()
                rooms_diag = {}
                for rid_key, rdata in room_data.items():
                    rooms_diag[rid_key] = {
                        "peerCount": len(rooms.get(rid_key, {})),
                        "noticePosts": rdata.get("noticePosts", []),
                        "checklistItems": rdata.get("checklistItems", []),
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

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if room_id and room_id in rooms and my_peer_id:
            rooms[room_id].pop(my_peer_id, None)
            peer_ids.discard(my_peer_id)
            if rooms[room_id]:
                _broadcast(
                    rooms[room_id],
                    {"type": "peer_left", "peerId": my_peer_id},
                )
            else:
                del rooms[room_id]

        _log('DISCONNECT', f'{my_peer_id} disconnected (room: {room_id})')


def _broadcast(room_peers, message, exclude=None):
    """Send a message to all peers in a room, optionally excluding one."""
    payload = json.dumps(message)
    for info in room_peers.values():
        if exclude and info["ws"] == exclude:
            continue
        try:
            asyncio.create_task(info["ws"].send(payload))
        except websockets.exceptions.ConnectionClosed:
            pass


async def main():
    _load_state()
    _log('STARTUP', f'Loaded {len(room_data)} rooms from {DATA_FILE}')
    _log('STARTUP', f'Chat retention: {CHAT_RETENTION_DAYS} days')
    _log('STARTUP', 'listening on ws://localhost:8765')

    loop = asyncio.get_running_loop()
    stop = loop.create_future()

    def _shutdown():
        _save_state()
        _log('SHUTDOWN', 'Server shutting down, state saved')
        stop.set_result(None)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    async with websockets.serve(handler, "0.0.0.0", 8765):
        await stop


if __name__ == "__main__":
    asyncio.run(main())
