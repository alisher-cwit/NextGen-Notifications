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
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from sqlalchemy.orm import Session

from config.settings import settings
from database import get_db_session
from redis_manager import online_status_manager
from models.notification import Notification
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
    type: str
    data: dict
    ticket_id: Optional[int]
    task_id: Optional[int]
    created_at: datetime
    # Aggregated target arrays
    notifiable_users: Optional[List[int]]
    notifiable_teams: Optional[List[int]]
    notifiable_departments: Optional[List[int]]
    notifiable_companies: Optional[List[int]]


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
    2. Save SINGLE notification to database with aggregated targets
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

            # Extract all user IDs from recipients (only users, not teams/depts/companies)
            user_recipient_ids = [
                r.user_id for r in payload.recipients 
                if (getattr(r, 'scope_type', 'user') or 'user') == 'user'
            ]

            # Step 1: Save SINGLE notification to database with aggregated targets
            saved_notification = self._save_notification(payload)
            saved_count = 1 if saved_notification else 0

            # Step 2: Check online status for user recipients
            online_users = {}
            if user_recipient_ids:
                online_users = online_status_manager.get_online_users(user_recipient_ids)
            online_count = len(online_users)

            # Step 3: Deliver to online users via WebSocket
            delivered_count = 0
            if self._websocket_emitter and online_users and saved_notification:
                delivered_count = await self._deliver_to_users(
                    saved_notification, online_users
                )

            # Build result
            result = ProcessingResult(
                total_recipients=len(payload.recipients),
                saved_count=saved_count,
                online_count=online_count,
                delivered_count=delivered_count,
                failed_count=0 if saved_count > 0 else 1,
                notification_ids=[saved_notification.id] if saved_notification else []
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
                        work_item_type_id=message.get("work_item_type_id"),
                    )
            except Exception as e2:
                logger.error(f"Failed to parse minimal payload: {e2}")

            return None

    # -------------------------------------------------------------------------
    # DATABASE OPERATIONS
    # -------------------------------------------------------------------------

    def _save_notification(self, payload: NotificationPayload) -> Optional[SavedNotification]:
        """
        Save a SINGLE notification to database with all recipients aggregated.

        Args:
            payload: Validated notification payload

        Returns:
            SavedNotification or None if failed
        """
        with get_db_session() as db:
            try:
                notification = self._create_single_notification(db=db, payload=payload)

                # Commit the single notification
                db.commit()

                # Parse data JSON string to dict
                data = notification.data
                if isinstance(data, str):
                    try:
                        data = json.loads(data)
                    except:
                        data = {}

                saved = SavedNotification(
                    id=notification.id,
                    type=notification.type,
                    data=data,
                    ticket_id=notification.ticket_id,
                    task_id=notification.task_id,
                    created_at=notification.created_at,
                    notifiable_users=notification.notifiable_users,
                    notifiable_teams=notification.notifiable_teams,
                    notifiable_departments=notification.notifiable_departments,
                    notifiable_companies=notification.notifiable_companies,
                )

                logger.info(
                    f"Saved notification {notification.id}: "
                    f"users={len(notification.notifiable_users or [])}, "
                    f"teams={len(notification.notifiable_teams or [])}, "
                    f"depts={len(notification.notifiable_departments or [])}, "
                    f"companies={len(notification.notifiable_companies or [])}"
                )

                return saved

            except Exception as e:
                logger.error(f"Failed to save notification: {e}", exc_info=True)
                db.rollback()
                return None

    def _create_single_notification(
        self,
        db: Session,
        payload: NotificationPayload
    ) -> Notification:
        """
        Create a SINGLE notification with all recipients in array columns.

        Args:
            db: Database session
            payload: Notification payload with recipients list

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

        # Aggregate all recipients into their respective arrays
        notifiable_users = []
        notifiable_teams = []
        notifiable_departments = []
        notifiable_companies = []

        for recipient in payload.recipients:
            scope_type = getattr(recipient, 'scope_type', 'user') or 'user'
            recipient_id = recipient.user_id

            if scope_type == 'user':
                if recipient_id and recipient_id not in notifiable_users:
                    notifiable_users.append(recipient_id)
            elif scope_type == 'team':
                if recipient_id and recipient_id not in notifiable_teams:
                    notifiable_teams.append(recipient_id)
            elif scope_type == 'department':
                if recipient_id and recipient_id not in notifiable_departments:
                    notifiable_departments.append(recipient_id)
            elif scope_type == 'company':
                if recipient_id and recipient_id not in notifiable_companies:
                    notifiable_companies.append(recipient_id)
            else:
                # Default to user
                if recipient_id and recipient_id not in notifiable_users:
                    notifiable_users.append(recipient_id)

        # Get primary user_id for notifiable_id (backward compatibility)
        primary_user_id = notifiable_users[0] if notifiable_users else 0

        notification = Notification(
            id=str(uuid4()),
            type=payload.event_type,
            notifiable_type=payload.event_type,
            notifiable_id=primary_user_id,
            notifiable_users=notifiable_users if notifiable_users else None,
            notifiable_teams=notifiable_teams if notifiable_teams else None,
            notifiable_departments=notifiable_departments if notifiable_departments else None,
            notifiable_companies=notifiable_companies if notifiable_companies else None,
            ticket_id=payload.ticket_id,
            task_id=payload.task_id,
            company_id=payload.company_id,
            work_item_type_id=payload.work_item_type_id,
            data=json.dumps(notification_data),
            auto_reminder=False,
            created_at=datetime.utcnow(),
        )

        db.add(notification)
        db.flush()  # Ensure notification is flushed to DB before returning
        return notification

    # -------------------------------------------------------------------------
    # WEBSOCKET DELIVERY
    # -------------------------------------------------------------------------

    async def _deliver_to_users(
        self,
        notification: SavedNotification,
        online_users: Dict[int, str]
    ) -> int:
        """
        Deliver notification to all online users in the notifiable_users array.

        Args:
            notification: SavedNotification with aggregated targets
            online_users: Dict mapping user_id to socket_id

        Returns:
            Number of successfully delivered notifications
        """
        delivered_count = 0

        # Get all target user IDs
        target_user_ids = notification.notifiable_users or []

        for user_id in target_user_ids:
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
