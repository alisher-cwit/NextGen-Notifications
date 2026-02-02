# =============================================================================
# NOTIFICATION ENGINE - NOTIFICATION PROCESSOR
# =============================================================================
# Core business logic for processing notifications
# Handles database storage and real-time delivery
# =============================================================================

import json
import logging
from uuid import uuid4
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

from sqlalchemy.orm import Session

from config.settings import settings
from database import get_db_session
from redis_manager import online_status_manager
from models.notification import Notification
# Note: scope_type is used as string, not enum (for flexibility)
from schemas.notification import (
    NotificationPayload,
    NotificationRecipient,
    WebSocketNotification,
)

logger = logging.getLogger(__name__)


@dataclass
class SavedNotification:
    """Holds notification data after saving to database."""
    id: str
    user_id: int
    type: str
    data: dict
    ticket_id: Optional[int]
    task_id: Optional[int]
    created_at: datetime


@dataclass
class ProcessingResult:
    """Result of notification processing."""
    total_recipients: int
    saved_count: int
    online_count: int
    delivered_count: int
    failed_count: int
    notification_ids: List[str]

    def to_dict(self) -> dict:
        return {
            "total_recipients": self.total_recipients,
            "saved_count": self.saved_count,
            "online_count": self.online_count,
            "delivered_count": self.delivered_count,
            "failed_count": self.failed_count,
            "notification_ids": self.notification_ids,
        }


class NotificationProcessor:
    """
    Processes notification messages from RabbitMQ.

    Responsibilities:
    1. Parse and validate incoming messages
    2. Save notifications to database
    3. Check online status via Redis
    4. Deliver to online users via WebSocket
    """

    def __init__(self, websocket_emitter=None):
        """
        Initialize the notification processor.

        Args:
            websocket_emitter: Function to emit WebSocket events.
                             Should be: async (user_id, notification) -> bool
        """
        self._websocket_emitter = websocket_emitter

    def set_websocket_emitter(self, emitter) -> None:
        """Set the WebSocket emitter function."""
        self._websocket_emitter = emitter

    # -------------------------------------------------------------------------
    # MAIN PROCESSING
    # -------------------------------------------------------------------------

    async def process(self, message: dict) -> ProcessingResult:
        """
        Process a notification message.

        Args:
            message: Raw message dictionary from RabbitMQ

        Returns:
            ProcessingResult with processing statistics
        """
        try:
            # Parse and validate message
            payload = self._parse_payload(message)

            if not payload:
                logger.error("Failed to parse notification payload")
                return ProcessingResult(0, 0, 0, 0, 1, [])

            logger.info(
                f"Processing notification: type={payload.event_type}, "
                f"recipients={len(payload.recipients)}, ticket_id={payload.ticket_id}"
            )

            # Extract recipient user IDs
            recipient_ids = [r.user_id for r in payload.recipients]

            # Step 1: Save notifications to database
            notifications = self._save_notifications(payload)
            saved_count = len(notifications)

            # Step 2: Check online status
            online_users = online_status_manager.get_online_users(recipient_ids)
            online_count = len(online_users)

            # Step 3: Deliver to online users
            delivered_count = 0
            if self._websocket_emitter and online_users:
                delivered_count = await self._deliver_notifications(
                    notifications, online_users
                )

            # Build result
            result = ProcessingResult(
                total_recipients=len(recipient_ids),
                saved_count=saved_count,
                online_count=online_count,
                delivered_count=delivered_count,
                failed_count=len(recipient_ids) - saved_count,
                notification_ids=[n.id for n in notifications]  # Now works - SavedNotification has id
            )

            logger.info(
                f"Notification processed: saved={saved_count}, "
                f"online={online_count}, delivered={delivered_count}"
            )

            return result

        except Exception as e:
            logger.error(f"Error processing notification: {e}", exc_info=True)
            return ProcessingResult(0, 0, 0, 0, 1, [])

    # -------------------------------------------------------------------------
    # PAYLOAD PARSING
    # -------------------------------------------------------------------------

    def _parse_payload(self, message: dict) -> Optional[NotificationPayload]:
        """
        Parse and validate notification payload.

        Args:
            message: Raw message dictionary

        Returns:
            NotificationPayload if valid, None otherwise
        """
        try:
            return NotificationPayload(**message)
        except Exception as e:
            logger.error(f"Failed to parse payload: {e}")

            # Try to extract minimal required fields
            try:
                recipients = message.get("recipients", [])
                notification = message.get("notification", {})
                event_type = message.get("event_type", "unknown")

                if recipients and notification:
                    return NotificationPayload(
                        event_type=event_type,
                        recipients=recipients,
                        notification=notification,
                        ticket_id=message.get("ticket_id"),
                        task_id=message.get("task_id"),
                        company_id=message.get("company_id"),
                    )
            except Exception as e2:
                logger.error(f"Failed to parse minimal payload: {e2}")

            return None

    # -------------------------------------------------------------------------
    # DATABASE OPERATIONS
    # -------------------------------------------------------------------------

    def _save_notifications(self, payload: NotificationPayload) -> List[SavedNotification]:
        """
        Save notifications to database for all recipients.

        Args:
            payload: Validated notification payload

        Returns:
            List of SavedNotification with data captured before session closes
        """
        saved_notifications = []

        with get_db_session() as db:
            notifications = []
            for recipient in payload.recipients:
                try:
                    notification = self._create_notification(
                        db=db,
                        recipient=recipient,
                        payload=payload
                    )
                    notifications.append(notification)

                except Exception as e:
                    logger.error(
                        f"Failed to save notification for user {recipient.user_id}: {e}"
                    )

            # Commit all notifications in a single transaction
            try:
                db.commit()

                # Capture data BEFORE session closes (objects will be detached after)
                for n in notifications:
                    # Parse data JSON string to dict
                    data = n.data
                    if isinstance(data, str):
                        try:
                            data = json.loads(data)
                        except:
                            data = {}

                    saved_notifications.append(SavedNotification(
                        id=n.id,
                        user_id=n.notifiable_id,
                        type=n.type,
                        data=data,
                        ticket_id=n.ticket_id,
                        task_id=n.task_id,
                        created_at=n.created_at,
                    ))

                logger.debug(f"Committed {len(saved_notifications)} notifications to database")
            except Exception as e:
                logger.error(f"Failed to commit notifications: {e}")
                db.rollback()
                return []

        return saved_notifications

    def _create_notification(
        self,
        db: Session,
        recipient: NotificationRecipient,
        payload: NotificationPayload
    ) -> Notification:
        """
        Create a single notification record.

        Args:
            db: Database session
            recipient: Recipient information
            payload: Notification payload

        Returns:
            Created Notification object
        """
        # Build notification data
        notification_data = {
            "title": payload.notification.title,
            "message": payload.notification.message,
            "action_url": payload.notification.action_url,
            "changes": payload.notification.changes,
            "extra": payload.notification.extra,
        }

        # Remove None values
        notification_data = {k: v for k, v in notification_data.items() if v is not None}

        # Determine which array to populate based on scope_type
        notifiable_users = None
        notifiable_teams = None
        notifiable_departments = None
        notifiable_companies = None

        scope_type = getattr(recipient, 'scope_type', 'user') or 'user'
        if scope_type == 'user':
            notifiable_users = [recipient.user_id]
        elif scope_type == 'team':
            notifiable_teams = [recipient.user_id]  # user_id contains team_id in this case
        elif scope_type == 'department':
            notifiable_departments = [recipient.user_id]  # user_id contains dept_id
        elif scope_type == 'company':
            notifiable_companies = [recipient.user_id]  # user_id contains company_id
        else:
            notifiable_users = [recipient.user_id]  # Default to user

        notification = Notification(
            id=str(uuid4()),
            type=payload.event_type,
            notifiable_type=payload.event_type,
            notifiable_id=recipient.user_id,
            notifiable_users=notifiable_users,
            notifiable_teams=notifiable_teams,
            notifiable_departments=notifiable_departments,
            notifiable_companies=notifiable_companies,
            ticket_id=payload.ticket_id,
            task_id=payload.task_id,
            company_id=payload.company_id,
            work_item_type_id=payload.work_item_type_id,
            data=json.dumps(notification_data),
            auto_reminder=False,
            created_at=datetime.utcnow(),
        )

        db.add(notification)
        return notification

    # -------------------------------------------------------------------------
    # WEBSOCKET DELIVERY
    # -------------------------------------------------------------------------

    async def _deliver_notifications(
        self,
        notifications: List[SavedNotification],
        online_users: Dict[int, str]
    ) -> int:
        """
        Deliver notifications to online users via WebSocket.

        Args:
            notifications: List of SavedNotification objects
            online_users: Dict mapping user_id to socket_id

        Returns:
            Number of successfully delivered notifications
        """
        delivered_count = 0

        for notification in notifications:
            user_id = notification.user_id

            if user_id not in online_users:
                continue

            try:
                # Build WebSocket payload
                ws_payload = self._build_websocket_payload(notification)

                # Emit to user
                success = await self._websocket_emitter(user_id, ws_payload)

                if success:
                    delivered_count += 1
                    logger.debug(f"Delivered notification {notification.id} to user {user_id}")

            except Exception as e:
                logger.error(f"Failed to deliver notification to user {user_id}: {e}")

        return delivered_count

    def _build_websocket_payload(self, notification: SavedNotification) -> dict:
        """
        Build WebSocket payload from SavedNotification.

        Args:
            notification: SavedNotification instance

        Returns:
            Dictionary for WebSocket emission
        """
        return {
            "id": notification.id,
            "type": notification.type,
            "notifiable_type": notification.type,
            "data": notification.data,
            "ticket_id": notification.ticket_id,
            "task_id": notification.task_id,
            "created_at": notification.created_at.isoformat() if notification.created_at else None,
        }


# =============================================================================
# MODULE-LEVEL PROCESSOR INSTANCE
# =============================================================================

# Global processor instance (will have websocket_emitter set later)
notification_processor = NotificationProcessor()


async def process_notification_message(message: dict) -> bool:
    """
    Process a notification message.
    This is the main entry point called by the RabbitMQ consumer.

    Args:
        message: Raw message dictionary from RabbitMQ

    Returns:
        True if processing successful, False otherwise
    """
    try:
        result = await notification_processor.process(message)
        return result.failed_count == 0
    except Exception as e:
        logger.error(f"Failed to process notification message: {e}", exc_info=True)
        return False
