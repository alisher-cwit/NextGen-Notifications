# =============================================================================
# NOTIFICATION ENGINE - PYDANTIC SCHEMAS
# =============================================================================
# Request/Response schemas for notification data validation
# =============================================================================

from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from enum import Enum

from pydantic import BaseModel, Field, field_validator


class NotificationPriority(str, Enum):
    """Notification priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


# Legacy scope type enum (kept for backward compatibility)
class NotificationScopeType(str, Enum):
    USER = "user"
    TEAM = "team"
    DEPARTMENT = "department"
    COMPANY = "company"


# =============================================================================
# RABBITMQ PAYLOAD SCHEMAS
# =============================================================================

class NotificationRecipient(BaseModel):
    """
    Schema for a notification recipient.
    """
    user_id: int = Field(..., description="User ID to notify")
    # Legacy scope_type - kept for backward compatibility
    scope_type: Optional[str] = Field(default="user", description="Scope type (legacy)")
    team_id: Optional[int] = Field(default=None, description="Team ID if scope is team")
    department_id: Optional[int] = Field(default=None, description="Department ID if scope is department")

    class Config:
        use_enum_values = True


class NotificationData(BaseModel):
    """
    Schema for notification content data.
    """
    title: Optional[str] = Field(default=None, description="Notification title")
    message: str = Field(..., description="Notification message")
    action_url: Optional[str] = Field(default=None, description="URL for notification action")
    image_url: Optional[str] = Field(default=None, description="Image URL for notification")
    extra: Optional[Dict[str, Any]] = Field(default=None, description="Extra data")

    # Change tracking (for ticket/task updates)
    changes: Optional[Dict[str, Any]] = Field(default=None, description="What changed")
    from_status: Optional[int] = Field(default=None, description="Previous status ID")
    to_status: Optional[int] = Field(default=None, description="New status ID")
    from_agent: Optional[int] = Field(default=None, description="Previous agent ID")
    to_agent: Optional[int] = Field(default=None, description="New agent ID")


class NotificationPayload(BaseModel):
    """
    Schema for notification payload received from RabbitMQ.
    This is the main payload structure that services publish.
    """
    # Event identification
    event_id: Optional[str] = Field(default=None, description="Unique event ID")
    event_type: str = Field(..., description="Type of notification event")
    source_service: Optional[str] = Field(default=None, description="Service that generated the event")
    timestamp: Optional[datetime] = Field(default=None, description="Event timestamp")

    # Recipients
    recipients: List[Union[NotificationRecipient, int]] = Field(
        ...,
        description="List of recipients (can be NotificationRecipient objects or user IDs)"
    )

    # Notification content
    notification: NotificationData = Field(..., description="Notification content")

    # Entity references
    ticket_id: Optional[int] = Field(default=None, description="Related ticket ID")
    task_id: Optional[int] = Field(default=None, description="Related task ID")
    company_id: Optional[int] = Field(default=None, description="Company ID")
    work_item_type_id: Optional[int] = Field(default=None, description="Work item type ID")

    # Metadata
    triggered_by: Optional[int] = Field(default=None, description="User ID who triggered the event")
    priority: NotificationPriority = Field(
        default=NotificationPriority.NORMAL,
        description="Notification priority"
    )

    # Options
    options: Optional[Dict[str, bool]] = Field(
        default=None,
        description="Delivery options (save_to_db, push_realtime, etc.)"
    )

    @field_validator("recipients", mode="before")
    @classmethod
    def normalize_recipients(cls, v):
        """Convert integer recipients to NotificationRecipient objects."""
        if not v:
            return v

        normalized = []
        for recipient in v:
            if isinstance(recipient, int):
                normalized.append(NotificationRecipient(user_id=recipient))
            elif isinstance(recipient, dict):
                normalized.append(NotificationRecipient(**recipient))
            else:
                normalized.append(recipient)
        return normalized

    class Config:
        use_enum_values = True


# =============================================================================
# RESPONSE SCHEMAS
# =============================================================================

class NotificationResponse(BaseModel):
    """
    Schema for notification API response.
    """
    id: str = Field(..., description="Notification ID")
    type: str = Field(..., description="Notification type")
    notifiable_type: str = Field(..., description="Notifiable type")
    notifiable_id: int = Field(..., description="User ID")
    # New array columns
    notifiable_users: Optional[List[int]] = Field(default=None, description="Target user IDs")
    notifiable_teams: Optional[List[int]] = Field(default=None, description="Target team IDs")
    notifiable_departments: Optional[List[int]] = Field(default=None, description="Target department IDs")
    notifiable_companies: Optional[List[int]] = Field(default=None, description="Target company IDs")
    data: Dict[str, Any] = Field(..., description="Notification data")
    ticket_id: Optional[int] = Field(default=None)
    task_id: Optional[int] = Field(default=None)
    is_read: bool = Field(..., description="Read status")
    read_at: Optional[datetime] = Field(default=None)
    created_at: datetime = Field(...)

    class Config:
        from_attributes = True


class WebSocketNotification(BaseModel):
    """
    Schema for WebSocket notification payload.
    Optimized for real-time delivery.
    """
    id: str = Field(..., description="Notification ID")
    type: str = Field(..., description="Notification type")
    notifiable_type: str = Field(..., description="Notifiable type")
    data: Dict[str, Any] = Field(..., description="Notification data")
    ticket_id: Optional[int] = Field(default=None)
    task_id: Optional[int] = Field(default=None)
    created_at: str = Field(..., description="ISO format timestamp")
    priority: str = Field(default="normal", description="Notification priority")

    class Config:
        from_attributes = True


# =============================================================================
# INTERNAL SCHEMAS
# =============================================================================

class ProcessedNotification(BaseModel):
    """
    Internal schema for a processed notification.
    Used after saving to database.
    """
    notification_id: str
    user_id: int
    socket_id: Optional[str] = None
    is_online: bool = False
    delivered: bool = False
    payload: WebSocketNotification
