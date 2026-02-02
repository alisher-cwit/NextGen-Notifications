# =============================================================================
# NOTIFICATION ENGINE - CORE MODULE (DEPRECATED)
# =============================================================================
# This module is kept for backwards compatibility only.
# Use direct imports instead:
#   from database import Base, get_db, get_db_session
#   from redis_manager import redis_connection, online_status_manager
# =============================================================================

from database import (
    engine,
    SessionLocal,
    Base,
    get_db,
    get_db_session,
)
from redis_manager import (
    redis_connection,
    online_status_manager,
)

__all__ = [
    # Database (use 'from database import ...' instead)
    "engine",
    "SessionLocal",
    "Base",
    "get_db",
    "get_db_session",
    # Redis (use 'from redis_manager import ...' instead)
    "redis_connection",
    "online_status_manager",
]
