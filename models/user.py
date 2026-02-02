# =============================================================================
# NOTIFICATION ENGINE - USER MODEL (MINIMAL)
# =============================================================================
# Minimal User model for JWT validation and notification targeting
# Only includes fields necessary for the notification engine
# =============================================================================

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Column,
    BigInteger,
    String,
    DateTime,
    Boolean,
    Integer,
)
from sqlalchemy.sql import func

from database import Base


class User(Base):
    """
    Minimal User model for notification engine.
    Contains only fields needed for authentication and notification delivery.

    Note: This is a read-only view of the users table.
    User management is handled by the main project.
    """

    __tablename__ = "users"

    # Primary key
    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Basic info
    name = Column(String(255), nullable=True)
    email = Column(String(255), unique=True, nullable=False, index=True)

    # Status
    status_id = Column(Integer, nullable=True)

    # Organizational info (for scoped notifications)
    role_id = Column(BigInteger, nullable=True)
    department_id = Column(BigInteger, nullable=True)
    company_department_id = Column(BigInteger, nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<User(id={self.id}, email={self.email})>"

    def to_dict(self) -> dict:
        """
        Convert user to dictionary.

        Returns:
            Dict containing user data
        """
        return {
            "id": self.id,
            "name": self.name,
            "email": self.email,
            "role_id": self.role_id,
            "department_id": self.department_id,
            "company_department_id": self.company_department_id,
        }


class UserSession(Base):
    """
    User session model for JWT session validation.
    Used to verify if a session is still valid (not revoked).
    """

    __tablename__ = "user_sessions"

    # Primary key
    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # User reference
    user_id = Column(BigInteger, nullable=False, index=True)

    # Session info
    ip = Column(String(45), nullable=True)
    user_agent = Column(String(500), nullable=True)

    # Location (optional)
    lat = Column(String(50), nullable=True)
    lon = Column(String(50), nullable=True)
    accuracy = Column(Integer, nullable=True)

    # Revocation
    revoked_at = Column(DateTime(timezone=True), nullable=True)
    revoked_by = Column(BigInteger, nullable=True)

    # Expiration
    expires_at = Column(DateTime(timezone=True), nullable=False)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self) -> str:
        return f"<UserSession(id={self.id}, user_id={self.user_id})>"

    @property
    def is_valid(self) -> bool:
        """Check if session is valid (not revoked and not expired)."""
        if self.revoked_at is not None:
            return False
        if self.expires_at and self.expires_at < datetime.utcnow():
            return False
        return True
