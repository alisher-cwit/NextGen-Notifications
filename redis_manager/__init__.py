# =============================================================================
# NOTIFICATION ENGINE - REDIS MANAGER MODULE
# =============================================================================
# Redis operations for notification engine
#
# Components:
# - connection: Redis connection manager (singleton)
# - online_status: User online/offline status tracking
# =============================================================================

from redis_manager.connection import (
    get_redis,
    redis_connection,
    check_redis_connection,
)
from redis_manager.online_status import (
    online_status_manager,
    OnlineStatusManager,
)

__all__ = [
    # Connection
    "get_redis",
    "redis_connection",
    "check_redis_connection",

    # Online Status
    "online_status_manager",
    "OnlineStatusManager",
]
