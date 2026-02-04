# =============================================================================
# NOTIFICATION ENGINE - WEBSOCKET SERVER
# =============================================================================
# Socket.IO server for real-time notification delivery
# Handles authentication, connection management, and event emission
# =============================================================================

import logging
from typing import Dict, Any, Optional, List

import socketio
import msgpack

from config.settings import settings
from websocket.auth import verify_websocket_token, WebSocketAuthError
from websocket.manager import connection_manager

logger = logging.getLogger(__name__)

# =============================================================================
# HELPER VARIABLES (Same pattern as NextGen-FastAPI)
# =============================================================================

IS_DEBUG = settings.DEBUG and settings.APP_ENV != "production"
CORS_ORIGINS = ["*"] if settings.WEBSOCKET_CORS_ORIGINS == "*" else [o.strip() for o in settings.WEBSOCKET_CORS_ORIGINS.split(",")]

# =============================================================================
# SOCKET.IO SERVER SETUP
# =============================================================================

# Create Socket.IO server
sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins=CORS_ORIGINS,
    ping_timeout=settings.WEBSOCKET_PING_TIMEOUT,
    ping_interval=settings.WEBSOCKET_PING_INTERVAL,
    logger=IS_DEBUG,
    engineio_logger=IS_DEBUG,
)

# Create ASGI app (socketio_path="" means handle at mount root)
socket_app = socketio.ASGIApp(sio, socketio_path="")


# =============================================================================
# SERIALIZATION
# =============================================================================

#new line

# Binary format flags (compatible with main project)
BINARY_FORMAT_FLAG = b"\x01"
JSON_FORMAT_FLAG = b"\x00"


def serialize_data(data: Dict[str, Any]) -> bytes:
    """
    Serialize data to binary format using MessagePack.

    Args:
        data: Dictionary to serialize

    Returns:
        Binary data with format flag
    """
    try:
        msgpack_data = msgpack.packb(data, use_bin_type=True)
        return BINARY_FORMAT_FLAG + msgpack_data
    except Exception as e:
        logger.error(f"Serialization failed: {e}")
        # Return empty dict as fallback
        return BINARY_FORMAT_FLAG + msgpack.packb({}, use_bin_type=True)


def deserialize_data(data: bytes) -> Dict[str, Any]:
    """
    Deserialize binary data to dictionary.

    Args:
        data: Binary data to deserialize

    Returns:
        Deserialized dictionary
    """
    if isinstance(data, bytes) and len(data) > 1:
        format_flag = data[:1]
        if format_flag == BINARY_FORMAT_FLAG:
            try:
                return msgpack.unpackb(data[1:], raw=False)
            except Exception as e:
                logger.error(f"Deserialization failed: {e}")
                return {}

    return data if isinstance(data, dict) else {}


# =============================================================================
# SOCKET EVENTS
# =============================================================================

@sio.event
async def connect(sid: str, environ: dict, auth: Optional[dict] = None):
    """
    Handle WebSocket connection with JWT authentication.

    Args:
        sid: Socket.IO session ID
        environ: WSGI environment
        auth: Authentication data containing JWT token

    Returns:
        True if connection accepted, False otherwise
    """
    logger.info(f"Connection attempt: {sid}")

    # Validate auth data
    if not auth or "token" not in auth:
        logger.warning(f"Connection rejected for {sid}: Missing token")
        return False

    token = auth["token"]

    try:
        # Verify JWT token
        user_id, email, payload = verify_websocket_token(token)

        # Register connection
        connection = connection_manager.add_connection(
            user_id=user_id,
            socket_id=sid,
            email=email
        )

        # Save session data
        await sio.save_session(sid, {
            "user_id": user_id,
            "email": email,
            "session_id": payload.get("sid"),
        })

        # Join user-specific room
        await sio.enter_room(sid, f"user:{user_id}")

        # Send authentication success
        auth_response = {
            "authenticated": True,
            "user_id": user_id,
            "email": email,
        }
        await sio.emit("authenticated", serialize_data(auth_response), room=sid)

        logger.info(f"User {user_id} ({email}) connected: {sid}")
        return True

    except WebSocketAuthError as e:
        logger.warning(f"Auth failed for {sid}: {e.message}")
        # Send error before rejecting
        await sio.emit("auth_error", {"error": e.message, "code": e.code}, room=sid)
        return False

    except Exception as e:
        logger.error(f"Connection error for {sid}: {e}")
        return False


@sio.event
async def disconnect(sid: str):
    """
    Handle WebSocket disconnection.

    Args:
        sid: Socket.IO session ID
    """
    user_id = connection_manager.remove_connection(sid)

    if user_id:
        logger.info(f"User {user_id} disconnected: {sid}")
    else:
        logger.debug(f"Unknown socket disconnected: {sid}")


@sio.event
async def ping(sid: str):
    """
    Handle ping event (heartbeat).

    Args:
        sid: Socket.IO session ID
    """
    await sio.emit("pong", {"timestamp": __import__("time").time()}, room=sid)


# =============================================================================
# NOTIFICATION EMISSION FUNCTIONS
# =============================================================================

async def emit_to_user(
    user_id: int,
    event: str,
    data: Dict[str, Any],
    serialize: bool = True
) -> bool:
    """
    Send event to a specific user.

    Args:
        user_id: Target user ID
        event: Event name
        data: Event data
        serialize: Whether to serialize data (default True)

    Returns:
        True if user was online and received the event, False otherwise
    """
    socket_id = connection_manager.get_socket_id(user_id)

    if not socket_id:
        logger.debug(f"User {user_id} not online, skipping emit")
        return False

    try:
        payload = serialize_data(data) if serialize else data
        await sio.emit(event, payload, room=socket_id)
        logger.debug(f"Emitted '{event}' to user {user_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to emit to user {user_id}: {e}")
        return False


async def emit_to_users(
    user_ids: List[int],
    event: str,
    data: Dict[str, Any],
    serialize: bool = True
) -> Dict[int, bool]:
    """
    Send event to multiple users.

    Args:
        user_ids: List of target user IDs
        event: Event name
        data: Event data
        serialize: Whether to serialize data

    Returns:
        Dict mapping user_id to delivery success
    """
    results = {}
    online_users = connection_manager.get_online_users(user_ids)

    payload = serialize_data(data) if serialize else data

    for user_id in user_ids:
        socket_id = online_users.get(user_id)
        if socket_id:
            try:
                await sio.emit(event, payload, room=socket_id)
                results[user_id] = True
                logger.debug(f"Emitted '{event}' to user {user_id}")
            except Exception as e:
                logger.error(f"Failed to emit to user {user_id}: {e}")
                results[user_id] = False
        else:
            results[user_id] = False

    return results


async def emit_to_room(
    room: str,
    event: str,
    data: Dict[str, Any],
    serialize: bool = True
) -> None:
    """
    Send event to all users in a room.

    Args:
        room: Room name
        event: Event name
        data: Event data
        serialize: Whether to serialize data
    """
    try:
        payload = serialize_data(data) if serialize else data
        await sio.emit(event, payload, room=room)
        logger.debug(f"Emitted '{event}' to room {room}")
    except Exception as e:
        logger.error(f"Failed to emit to room {room}: {e}")


async def broadcast(
    event: str,
    data: Dict[str, Any],
    serialize: bool = True
) -> None:
    """
    Broadcast event to all connected users.

    Args:
        event: Event name
        data: Event data
        serialize: Whether to serialize data
    """
    try:
        payload = serialize_data(data) if serialize else data
        await sio.emit(event, payload)
        logger.debug(f"Broadcasted '{event}' to all users")
    except Exception as e:
        logger.error(f"Failed to broadcast '{event}': {e}")


# =============================================================================
# NOTIFICATION-SPECIFIC FUNCTIONS
# =============================================================================

async def push_notification(
    user_id: int,
    notification: Dict[str, Any]
) -> bool:
    """
    Push a notification to a user.

    Args:
        user_id: Target user ID
        notification: Notification data (should match WebSocketNotification schema)

    Returns:
        True if notification was delivered, False otherwise
    """
    return await emit_to_user(
        user_id=user_id,
        event="new_notification",
        data=notification
    )


async def push_notifications_batch(
    notifications: List[Dict[str, Any]]
) -> Dict[int, bool]:
    """
    Push multiple notifications to different users.

    Args:
        notifications: List of dicts with 'user_id' and 'notification' keys

    Returns:
        Dict mapping user_id to delivery success
    """
    results = {}

    for item in notifications:
        user_id = item.get("user_id")
        notification = item.get("notification")

        if user_id and notification:
            results[user_id] = await push_notification(user_id, notification)

    return results
