"""Room service for Clipper — owns all room management and peer tracking logic."""
import random
import string
import time
from datetime import datetime, timezone
import logging


class RoomService:
    def __init__(self, persistence, rooms, peer_ids, max_peers=50):
        self.persistence = persistence
        self.rooms = rooms
        self.peer_ids = peer_ids
        self.MAX_PEERS_PER_ROOM = max_peers

    def generate_room_code(self):
        """Generate a unique 4-digit room code."""
        while True:
            code = str(random.randint(1000, 9999))
            if code not in self.rooms:
                return code

    def generate_peer_id(self):
        """Generate a unique 4-char alphanumeric peer ID."""
        chars = string.ascii_uppercase + string.digits
        while True:
            pid = "".join(random.choices(chars, k=4))
            if pid not in self.peer_ids:
                self.peer_ids.add(pid)
                return pid

    def is_room_full(self, rid):
        """Check if a room has reached max peers."""
        return rid in self.rooms and len(self.rooms[rid]) >= self.MAX_PEERS_PER_ROOM

    def leave_previous_room(self, old_room_id, old_peer_id):
        """Leave previous room. Returns (was_left, room_now_empty)."""
        if old_room_id and old_room_id in self.rooms and old_peer_id and old_peer_id in self.rooms[old_room_id]:
            self.rooms[old_room_id].pop(old_peer_id, None)
            self.peer_ids.discard(old_peer_id)
            if not self.rooms[old_room_id]:
                del self.rooms[old_room_id]
                return True, True
            return True, False
        return False, False

    def add_peer(self, rid, websocket, display_name=None, device_id=None):
        """Add a peer to a room. Returns (room_id, my_peer_id, peer_info, reused).

        If device_id is provided, checks if a previous peer_id exists for this
        device and reuses it (stable peer identity across reconnections).
        Falls back to random peer_id if the device is unknown or its previous
        peer_id is currently online (defense against duplicate sessions).
        """
        reused = False
        my_peer_id = None

        if device_id and hasattr(self, 'persistence') and self.persistence:
            mapping = self.persistence.get_peer_for_device(device_id)
            if mapping:
                old_peer_id, old_room_id, old_display = mapping
                # Reuse peer_id only if it's not currently online
                if old_peer_id not in self.peer_ids:
                    my_peer_id = old_peer_id
                    self.peer_ids.add(my_peer_id)
                    reused = True
                    logging.debug("Reused peer_id %s for device %s", my_peer_id, device_id[:8])

        if not my_peer_id:
            my_peer_id = self.generate_peer_id()

        now_iso = datetime.now(timezone.utc).isoformat()
        if rid not in self.rooms:
            self.rooms[rid] = {}
        info = {
            "ws": websocket,
            "joinedAt": now_iso,
            "lastHeartbeat": time.time(),
            "displayName": display_name or my_peer_id,
        }
        self.rooms[rid][my_peer_id] = info

        # Persist device→peer mapping for future reconnections
        if device_id and hasattr(self, 'persistence') and self.persistence:
            self.persistence.save_device_peer(device_id, my_peer_id, rid, display_name or my_peer_id)

        return rid, my_peer_id, info, reused

    def get_other_peers(self, room_id, my_peer_id):
        """Get peers in a room excluding self."""
        return [
            {"peerId": pid, "joinedAt": info["joinedAt"], "displayName": info.get("displayName", pid)}
            for pid, info in self.rooms.get(room_id, {}).items() if pid != my_peer_id
        ]

    def resolve_display_name(self, room_id, my_peer_id, desired_name):
        """Resolve display name with _N suffix for conflicts. Returns (final_name, was_conflict)."""
        final_name = desired_name
        counter = 1
        while True:
            has_conflict = any(
                info.get("displayName", "") == final_name
                for pid, info in self.rooms.get(room_id, {}).items()
                if pid != my_peer_id
            )
            if not has_conflict:
                break
            counter += 1
            final_name = f"{desired_name}_{counter}"
        self.rooms[room_id][my_peer_id]["displayName"] = final_name
        return final_name, final_name != desired_name

    def update_heartbeat(self, room_id, my_peer_id):
        """Update peer heartbeat timestamp. Returns True if updated."""
        if room_id and room_id in self.rooms and my_peer_id and my_peer_id in self.rooms[room_id]:
            self.rooms[room_id][my_peer_id]["lastHeartbeat"] = time.time()
            return True
        return False

    def remove_peer(self, room_id, my_peer_id):
        """Remove a peer from a room. Returns (was_removed, room_now_empty)."""
        was_removed = False
        room_now_empty = False
        if room_id and room_id in self.rooms and my_peer_id and my_peer_id in self.rooms[room_id]:
            self.rooms[room_id].pop(my_peer_id, None)
            self.peer_ids.discard(my_peer_id)
            was_removed = True
            if not self.rooms[room_id]:
                del self.rooms[room_id]
                room_now_empty = True
        return was_removed, room_now_empty
