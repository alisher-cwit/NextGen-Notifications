# =============================================================================
# NOTIFICATION ENGINE - NOTIFICATION MODEL
# =============================================================================
# Database model for notifications table
# Same structure as NextGen-FastAPI notification model
#
# Note: Enums are in enums/notification.py
# Note: Response schemas (to_dict, to_websocket) are in schemas/notification.py
# =============================================================================

from sqlalchemy import (
    Column,
    BigInteger,
    String,
    Text,
    DateTime,
    JSON,
    Boolean,
    Index,
    ARRAY,
)
from sqlalchemy.sql import func

from database import Base


class Notification(Base):
    __tablename__ = "notifications"

    # Primary key - UUID string
    id = Column(String(36), primary_key=True)

    # Notification type and target
    type = Column(String(255), nullable=False)
    notifiable_type = Column(String(255), nullable=False)
    notifiable_id = Column(BigInteger, nullable=False)

    # Notifiable targets (array columns)
    notifiable_users = Column(ARRAY(BigInteger), nullable=True)
    notifiable_teams = Column(ARRAY(BigInteger), nullable=True)
    notifiable_departments = Column(ARRAY(BigInteger), nullable=True)
    notifiable_companies = Column(ARRAY(BigInteger), nullable=True)

    # Related entities
    ticket_id = Column(BigInteger, nullable=True)
    task_id = Column(BigInteger, nullable=True)
    work_item_type_id = Column(BigInteger, nullable=True)
    company_id = Column(BigInteger, nullable=True)

    # Notification content
    data = Column(Text, nullable=False)
    meta_data = Column(JSON, nullable=True)

    # Status and timing
    read_at = Column(DateTime, nullable=True)
    schedule_at = Column(DateTime(timezone=True), nullable=True)
    auto_reminder = Column(Boolean, nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)

    # Indexes for fast queries
    __table_args__ = (
        Index("idx_notif_user_unread", "notifiable_id", "read_at"),
        Index("idx_notif_user_created", "notifiable_id", "created_at"),
        Index("idx_notif_type_created", "type", "created_at"),
    )
