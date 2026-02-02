# =============================================================================
# NOTIFICATION ENGINE - HELPER UTILITIES
# =============================================================================
# Common utility functions used across the application
# =============================================================================

import json
import logging
from uuid import uuid4
from datetime import datetime, timezone
from typing import Any, Optional, Dict

logger = logging.getLogger(__name__)


def safe_json_loads(data: str, default: Any = None) -> Any:
    """
    Safely parse JSON string.

    Args:
        data: JSON string to parse
        default: Default value if parsing fails

    Returns:
        Parsed data or default value
    """
    try:
        return json.loads(data)
    except (json.JSONDecodeError, TypeError) as e:
        logger.debug(f"JSON parse failed: {e}")
        return default


def safe_json_dumps(data: Any, default: str = "{}") -> str:
    """
    Safely serialize data to JSON string.

    Args:
        data: Data to serialize
        default: Default value if serialization fails

    Returns:
        JSON string or default value
    """
    try:
        return json.dumps(data, default=str, ensure_ascii=False)
    except (TypeError, ValueError) as e:
        logger.debug(f"JSON dump failed: {e}")
        return default


def generate_uuid() -> str:
    """
    Generate a new UUID string.

    Returns:
        UUID string (36 characters)
    """
    return str(uuid4())


def get_current_timestamp() -> datetime:
    """
    Get current UTC timestamp.

    Returns:
        Current datetime in UTC
    """
    return datetime.now(timezone.utc)


def format_timestamp(dt: Optional[datetime]) -> Optional[str]:
    """
    Format datetime to ISO format string.

    Args:
        dt: Datetime object to format

    Returns:
        ISO format string or None
    """
    if dt is None:
        return None
    return dt.isoformat()


def safe_int(value: Any, default: int = 0) -> int:
    """
    Safely convert value to integer.

    Args:
        value: Value to convert
        default: Default value if conversion fails

    Returns:
        Integer value or default
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def truncate_string(s: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Truncate string to maximum length.

    Args:
        s: String to truncate
        max_length: Maximum length
        suffix: Suffix to add if truncated

    Returns:
        Truncated string
    """
    if len(s) <= max_length:
        return s
    return s[:max_length - len(suffix)] + suffix


def merge_dicts(*dicts: Dict) -> Dict:
    """
    Merge multiple dictionaries.
    Later dicts override earlier ones.

    Args:
        dicts: Dictionaries to merge

    Returns:
        Merged dictionary
    """
    result = {}
    for d in dicts:
        if d:
            result.update(d)
    return result
