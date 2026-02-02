# =============================================================================
# NOTIFICATION ENGINE - ONLINE STATUS MANAGER
# =============================================================================
# Redis operations for tracking online users
# =============================================================================

import logging
from typing import Optional, Dict, List

from redis.exceptions import RedisError

from config.settings import settings
from redis_manager.connection import get_redis

logger = logging.getLogger(__name__)


class OnlineStatusManager:
    """
    Manager for tracking user online status in Redis.

    Responsibilities:
    - Set user online/offline
    - Check if user is online
    - Get socket ID for online users
    - Batch check multiple users
    """

    def __init__(self):
        self._redis = get_redis()
        self.prefix = settings.REDIS_PREFIX_ONLINE
        self.ttl = settings.REDIS_ONLINE_TTL

    def set_user_online(self, user_id: int, socket_id: str) -> bool:
        """
        Mark user as online with their socket ID.

        Args:
            user_id: User's unique identifier
            socket_id: Socket.IO session ID

        Returns:
            bool: True if successful
        """
        try:
            key = f"{self.prefix}:{user_id}"
            self._redis.setex(key, self.ttl, socket_id)
            logger.debug(f"User {user_id} marked online with socket {socket_id}")
            return True
        except RedisError as e:
            logger.error(f"Failed to set user {user_id} online: {e}")
            return False

    def set_user_offline(self, user_id: int) -> bool:
        """
        Mark user as offline.

        Args:
            user_id: User's unique identifier

        Returns:
            bool: True if successful
        """
        try:
            key = f"{self.prefix}:{user_id}"
            self._redis.delete(key)
            logger.debug(f"User {user_id} marked offline")
            return True
        except RedisError as e:
            logger.error(f"Failed to set user {user_id} offline: {e}")
            return False

    def get_user_socket(self, user_id: int) -> Optional[str]:
        """
        Get user's socket ID if they are online.

        Args:
            user_id: User's unique identifier

        Returns:
            str: Socket ID if online, None otherwise
        """
        try:
            key = f"{self.prefix}:{user_id}"
            return self._redis.get(key)
        except RedisError as e:
            logger.error(f"Failed to get socket for user {user_id}: {e}")
            return None

    def is_user_online(self, user_id: int) -> bool:
        """
        Check if user is currently online.

        Args:
            user_id: User's unique identifier

        Returns:
            bool: True if online
        """
        return self.get_user_socket(user_id) is not None

    def get_online_users(self, user_ids: List[int]) -> Dict[int, str]:
        """
        Get online status for multiple users.

        Args:
            user_ids: List of user IDs to check

        Returns:
            Dict mapping user_id to socket_id (only online users)
        """
        if not user_ids:
            return {}

        try:
            pipeline = self._redis.pipeline()
            for user_id in user_ids:
                key = f"{self.prefix}:{user_id}"
                pipeline.get(key)

            results = pipeline.execute()

            online_users = {}
            for user_id, socket_id in zip(user_ids, results):
                if socket_id:
                    online_users[user_id] = socket_id

            return online_users
        except RedisError as e:
            logger.error(f"Failed to get online users: {e}")
            return {}

    def refresh_user_online(self, user_id: int) -> bool:
        """
        Refresh TTL for user's online status (heartbeat).

        Args:
            user_id: User's unique identifier

        Returns:
            bool: True if successful
        """
        try:
            key = f"{self.prefix}:{user_id}"
            return self._redis.expire(key, self.ttl)
        except RedisError as e:
            logger.error(f"Failed to refresh status for user {user_id}: {e}")
            return False


# Singleton instance
online_status_manager = OnlineStatusManager()
