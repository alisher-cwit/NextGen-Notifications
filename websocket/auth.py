# =============================================================================
# NOTIFICATION ENGINE - WEBSOCKET AUTHENTICATION
# =============================================================================
# JWT token verification for WebSocket connections
# Uses the same SECRET_KEY as main project for compatibility
# =============================================================================

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple

from jose import jwt, JWTError, ExpiredSignatureError

from config.settings import settings
from database import get_db_session
from models.user import User, UserSession

logger = logging.getLogger(__name__)


class WebSocketAuthError(Exception):
    """Custom exception for WebSocket authentication errors."""

    def __init__(self, message: str, code: str = "AUTH_ERROR"):
        self.message = message
        self.code = code
        super().__init__(self.message)


def decode_jwt_token(token: str) -> Dict[str, Any]:
    """
    Decode and validate JWT token.

    Args:
        token: JWT token string

    Returns:
        Dict containing token payload

    Raises:
        WebSocketAuthError: If token is invalid or expired
    """
    try:
        # Remove Bearer prefix if present
        if token.startswith("Bearer "):
            token = token[7:]

        payload = jwt.decode(
            token,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM]
        )
        return payload

    except ExpiredSignatureError:
        logger.warning("Token has expired")
        raise WebSocketAuthError("Token has expired", "TOKEN_EXPIRED")

    except JWTError as e:
        logger.warning(f"Invalid token: {e}")
        raise WebSocketAuthError("Invalid token", "INVALID_TOKEN")


def verify_websocket_token(token: str) -> Tuple[int, str, Dict[str, Any]]:
    """
    Verify WebSocket authentication token.

    This function:
    1. Decodes the JWT token
    2. Extracts user email and session ID
    3. Validates session in database
    4. Returns user information

    Args:
        token: JWT token from WebSocket auth

    Returns:
        Tuple of (user_id, email, payload)

    Raises:
        WebSocketAuthError: If authentication fails
    """
    # Decode token
    payload = decode_jwt_token(token)

    # Extract required fields
    email = payload.get("sub")
    session_id = payload.get("sid")

    if not email:
        raise WebSocketAuthError("Token missing user identifier", "INVALID_TOKEN")

    if not session_id:
        raise WebSocketAuthError("Token missing session ID", "INVALID_TOKEN")

    # Get user from database
    with get_db_session() as db:
        # Find user by email
        user = db.query(User).filter(User.email == email).first()

        if not user:
            logger.warning(f"User not found for email: {email}")
            raise WebSocketAuthError("User not found", "USER_NOT_FOUND")

        # Validate session
        now = datetime.now(timezone.utc)
        session = db.query(UserSession).filter(
            UserSession.id == session_id,
            UserSession.user_id == user.id,
            UserSession.revoked_at.is_(None),
            UserSession.expires_at > now
        ).first()

        if not session:
            logger.warning(f"Invalid or expired session for user {user.id}")
            raise WebSocketAuthError("Session expired or invalid", "SESSION_INVALID")

        logger.info(f"WebSocket auth successful for user {user.id} ({email})")

        return user.id, email, payload


def verify_token_with_redis(token: str, validate_session: bool = True) -> Tuple[int, str, Dict[str, Any]]:
    """
    Verify WebSocket token with optional Redis session validation.

    This is a faster alternative that can optionally skip database checks
    if session validation is stored in Redis.

    Args:
        token: JWT token
        validate_session: Whether to validate session in database

    Returns:
        Tuple of (user_id, email, payload)

    Raises:
        WebSocketAuthError: If authentication fails
    """
    # Use database validation directly
    return verify_websocket_token(token)


def extract_user_id_from_token(token: str) -> Optional[int]:
    """
    Quick extraction of user ID from token without full validation.
    Use only for logging/debugging purposes.

    Args:
        token: JWT token

    Returns:
        User ID if extractable, None otherwise
    """
    try:
        if token.startswith("Bearer "):
            token = token[7:]

        # Decode without verification for quick extraction
        payload = jwt.decode(
            token,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM],
            options={"verify_exp": False}
        )

        email = payload.get("sub")
        if email:
            with get_db_session() as db:
                user = db.query(User).filter(User.email == email).first()
                return user.id if user else None

    except Exception:
        pass

    return None
