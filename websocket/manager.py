# =============================================================================
# NOTIFICATION ENGINE - CONNECTION MANAGER
# =============================================================================
# Manages WebSocket connections and user session mappings
# Handles both local memory and Redis-based session storage
# =============================================================================

import logging
from typing import Dict, Optional, List, Set
from dataclasses import dataclass, field
from datetime import datetime

from redis_manager import online_status_manager

logger = logging.getLogger(__name__)


@dataclass
class UserConnection:
    """
    Represents a user's WebSocket connection.
    """
    user_id: int
    socket_id: str
    email: str
    connected_at: datetime = field(default_factory=datetime.utcnow)
    rooms: Set[str] = field(default_factory=set)

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "socket_id": self.socket_id,
            "email": self.email,
            "connected_at": self.connected_at.isoformat(),
            "rooms": list(self.rooms),
        }


class ConnectionManager:
    """
    Manages WebSocket connections for the notification engine.

    Features:
    - Local memory storage for fast lookups
    - Redis sync for distributed deployments
    - Room management for group notifications
    - Connection tracking and statistics
    """

    def __init__(self):
        # Local mappings for fast access
        self._user_to_socket: Dict[int, str] = {}  # user_id -> socket_id
        self._socket_to_user: Dict[str, int] = {}  # socket_id -> user_id
        self._connections: Dict[str, UserConnection] = {}  # socket_id -> UserConnection

        # Room management
        self._user_rooms: Dict[int, Set[str]] = {}  # user_id -> set of room names
        self._room_users: Dict[str, Set[int]] = {}  # room_name -> set of user_ids

    # -------------------------------------------------------------------------
    # CONNECTION LIFECYCLE
    # -------------------------------------------------------------------------

    def add_connection(
        self,
        user_id: int,
        socket_id: str,
        email: str
    ) -> UserConnection:
        """
        Register a new WebSocket connection.

        Args:
            user_id: User's unique identifier
            socket_id: Socket.IO session ID
            email: User's email

        Returns:
            UserConnection object
        """
        # Create connection object
        connection = UserConnection(
            user_id=user_id,
            socket_id=socket_id,
            email=email
        )

        # Store in local mappings
        self._user_to_socket[user_id] = socket_id
        self._socket_to_user[socket_id] = user_id
        self._connections[socket_id] = connection

        # Store in Redis for distributed access
        online_status_manager.set_user_online(user_id, socket_id)

        logger.info(f"Connection added: user={user_id}, socket={socket_id}")
        return connection

    def remove_connection(self, socket_id: str) -> Optional[int]:
        """
        Remove a WebSocket connection.

        Args:
            socket_id: Socket.IO session ID

        Returns:
            User ID if connection existed, None otherwise
        """
        user_id = self._socket_to_user.pop(socket_id, None)

        if user_id is not None:
            # Remove from local mappings
            self._user_to_socket.pop(user_id, None)
            self._connections.pop(socket_id, None)

            # Remove from all rooms
            self._remove_user_from_all_rooms(user_id)

            # Remove from Redis
            online_status_manager.set_user_offline(user_id)

            logger.info(f"Connection removed: user={user_id}, socket={socket_id}")

        return user_id

    def remove_connection_by_user(self, user_id: int) -> Optional[str]:
        """
        Remove connection by user ID.

        Args:
            user_id: User's unique identifier

        Returns:
            Socket ID if connection existed, None otherwise
        """
        socket_id = self._user_to_socket.get(user_id)
        if socket_id:
            self.remove_connection(socket_id)
        return socket_id

    # -------------------------------------------------------------------------
    # CONNECTION LOOKUPS
    # -------------------------------------------------------------------------

    def get_socket_id(self, user_id: int) -> Optional[str]:
        """
        Get socket ID for a user.

        Args:
            user_id: User's unique identifier

        Returns:
            Socket ID if user is connected, None otherwise
        """
        # Check local first (faster)
        socket_id = self._user_to_socket.get(user_id)
        if socket_id:
            return socket_id

        # Fall back to Redis (for distributed deployments)
        return online_status_manager.get_user_socket(user_id)

    def get_user_id(self, socket_id: str) -> Optional[int]:
        """
        Get user ID for a socket.

        Args:
            socket_id: Socket.IO session ID

        Returns:
            User ID if socket exists, None otherwise
        """
        return self._socket_to_user.get(socket_id)

    def get_connection(self, socket_id: str) -> Optional[UserConnection]:
        """
        Get connection details for a socket.

        Args:
            socket_id: Socket.IO session ID

        Returns:
            UserConnection if exists, None otherwise
        """
        return self._connections.get(socket_id)

    def is_user_online(self, user_id: int) -> bool:
        """
        Check if user is currently connected.

        Args:
            user_id: User's unique identifier

        Returns:
            True if user is online, False otherwise
        """
        # Check local first
        if user_id in self._user_to_socket:
            return True

        # Fall back to Redis
        return online_status_manager.is_user_online(user_id)

    def get_online_users(self, user_ids: List[int]) -> Dict[int, str]:
        """
        Get online status for multiple users.

        Args:
            user_ids: List of user IDs to check

        Returns:
            Dict mapping user_id to socket_id for online users
        """
        result = {}

        # Check local mappings first
        for user_id in user_ids:
            if user_id in self._user_to_socket:
                result[user_id] = self._user_to_socket[user_id]

        # For remaining users, check Redis
        remaining = [uid for uid in user_ids if uid not in result]
        if remaining:
            redis_result = online_status_manager.get_online_users(remaining)
            result.update(redis_result)

        return result

    # -------------------------------------------------------------------------
    # ROOM MANAGEMENT
    # -------------------------------------------------------------------------

    def join_room(self, user_id: int, room: str) -> bool:
        """
        Add user to a room.

        Args:
            user_id: User's unique identifier
            room: Room name

        Returns:
            True if successful, False otherwise
        """
        # Add to user's rooms
        if user_id not in self._user_rooms:
            self._user_rooms[user_id] = set()
        self._user_rooms[user_id].add(room)

        # Add to room's users
        if room not in self._room_users:
            self._room_users[room] = set()
        self._room_users[room].add(user_id)

        # Update connection object
        socket_id = self._user_to_socket.get(user_id)
        if socket_id and socket_id in self._connections:
            self._connections[socket_id].rooms.add(room)

        logger.debug(f"User {user_id} joined room {room}")
        return True

    def leave_room(self, user_id: int, room: str) -> bool:
        """
        Remove user from a room.

        Args:
            user_id: User's unique identifier
            room: Room name

        Returns:
            True if successful, False otherwise
        """
        # Remove from user's rooms
        if user_id in self._user_rooms:
            self._user_rooms[user_id].discard(room)

        # Remove from room's users
        if room in self._room_users:
            self._room_users[room].discard(user_id)
            # Clean up empty rooms
            if not self._room_users[room]:
                del self._room_users[room]

        # Update connection object
        socket_id = self._user_to_socket.get(user_id)
        if socket_id and socket_id in self._connections:
            self._connections[socket_id].rooms.discard(room)

        logger.debug(f"User {user_id} left room {room}")
        return True

    def _remove_user_from_all_rooms(self, user_id: int) -> None:
        """Remove user from all rooms (on disconnect)."""
        rooms = self._user_rooms.pop(user_id, set())
        for room in rooms:
            if room in self._room_users:
                self._room_users[room].discard(user_id)
                if not self._room_users[room]:
                    del self._room_users[room]

    def get_room_users(self, room: str) -> Set[int]:
        """
        Get all users in a room.

        Args:
            room: Room name

        Returns:
            Set of user IDs in the room
        """
        return self._room_users.get(room, set()).copy()

    def get_user_rooms(self, user_id: int) -> Set[str]:
        """
        Get all rooms a user is in.

        Args:
            user_id: User's unique identifier

        Returns:
            Set of room names
        """
        return self._user_rooms.get(user_id, set()).copy()

    # -------------------------------------------------------------------------
    # STATISTICS
    # -------------------------------------------------------------------------

    @property
    def total_connections(self) -> int:
        """Get total number of active connections."""
        return len(self._connections)

    @property
    def total_rooms(self) -> int:
        """Get total number of active rooms."""
        return len(self._room_users)

    def get_stats(self) -> dict:
        """
        Get connection statistics.

        Returns:
            Dict containing connection stats
        """
        return {
            "total_connections": self.total_connections,
            "total_rooms": self.total_rooms,
            "connections": [conn.to_dict() for conn in self._connections.values()],
            "rooms": {room: len(users) for room, users in self._room_users.items()},
        }


# =============================================================================
# GLOBAL CONNECTION MANAGER INSTANCE
# =============================================================================

connection_manager = ConnectionManager()
