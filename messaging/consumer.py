# =============================================================================
# NOTIFICATION ENGINE - RABBITMQ CONSUMER
# =============================================================================
# Consumes notification messages from RabbitMQ and processes them
# Handles connection management, message parsing, and retry logic
# =============================================================================

import json
import time
import logging
import asyncio
import threading
from typing import Callable, Optional, Any
from dataclasses import dataclass

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from config.settings import settings

logger = logging.getLogger(__name__)

# Suppress pika debug logs
logging.getLogger("pika").setLevel(logging.WARNING)


@dataclass
class ConsumerConfig:
    """Configuration for the notification consumer."""
    exchange: str = settings.RABBITMQ_EXCHANGE
    queue: str = settings.RABBITMQ_QUEUE
    routing_key: str = settings.RABBITMQ_ROUTING_KEY
    prefetch_count: int = settings.RABBITMQ_PREFETCH_COUNT
    durable: bool = True
    auto_ack: bool = False


class NotificationConsumer:
    """
    RabbitMQ consumer for processing notification messages.

    Features:
    - Automatic reconnection on failure
    - Configurable prefetch for load balancing
    - Message acknowledgment handling
    - Graceful shutdown support
    - Queue statistics tracking
    """

    def __init__(
        self,
        message_handler: Callable[[dict], bool],
        config: Optional[ConsumerConfig] = None
    ):
        """
        Initialize the notification consumer.

        Args:
            message_handler: Async function to process messages.
                            Should return True on success, False on failure.
            config: Consumer configuration (uses defaults if not provided)
        """
        self.message_handler = message_handler
        self.config = config or ConsumerConfig()

        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[BlockingChannel] = None
        self._is_running = False
        self._should_stop = False

        # Event loop for async handler
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Queue statistics
        self._total_consumed = 0
        self._total_processed = 0
        self._total_failed = 0

    # -------------------------------------------------------------------------
    # CONNECTION MANAGEMENT
    # -------------------------------------------------------------------------

    def _get_connection_params(self) -> pika.ConnectionParameters:
        """Build RabbitMQ connection parameters."""
        credentials = pika.PlainCredentials(
            settings.RABBITMQ_USER,
            settings.RABBITMQ_PASS
        )

        return pika.ConnectionParameters(
            host=settings.RABBITMQ_HOST,
            port=settings.RABBITMQ_PORT,
            virtual_host=settings.RABBITMQ_VHOST,
            credentials=credentials,
            heartbeat=settings.RABBITMQ_HEARTBEAT,
            blocked_connection_timeout=settings.RABBITMQ_BLOCKED_TIMEOUT,
            connection_attempts=settings.RABBITMQ_CONNECTION_ATTEMPTS,
            retry_delay=settings.RABBITMQ_RETRY_DELAY,
            socket_timeout=10,
        )

    def _connect(self) -> bool:
        """
        Establish connection to RabbitMQ.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            logger.info(f"Connecting to RabbitMQ at {settings.RABBITMQ_HOST}:{settings.RABBITMQ_PORT}")

            self._connection = pika.BlockingConnection(self._get_connection_params())
            self._channel = self._connection.channel()

            # Set prefetch count for load balancing
            self._channel.basic_qos(prefetch_count=self.config.prefetch_count)

            # Declare exchange (this is message hub)
            self._channel.exchange_declare(
                exchange=self.config.exchange,
                exchange_type="direct",
                durable=self.config.durable
            )

            # Declare queue (message storage)
            self._channel.queue_declare(
                queue=self.config.queue,
                durable=self.config.durable
            )

            # Bind queue to exchange
            self._channel.queue_bind(
                queue=self.config.queue,
                exchange=self.config.exchange,
                routing_key=self.config.routing_key
            )

            logger.info(f"Connected to RabbitMQ. Queue: {self.config.queue}")

            # Log initial queue stats
            stats = self.get_queue_stats()
            if stats:
                logger.info(f"Queue status: messages_waiting={stats['messages']}, consumers={stats['consumers']}")

            return True

        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            return False

    def get_queue_stats(self) -> Optional[dict]:
        """
        Get current queue statistics.

        Returns:
            Dict with queue stats or None if unavailable
        """
        try:
            if self._channel and self._channel.is_open:
                result = self._channel.queue_declare(
                    queue=self.config.queue,
                    durable=self.config.durable,
                    passive=True  # Don't create, just check
                )
                return {
                    "messages": result.method.message_count,
                    "consumers": result.method.consumer_count,
                }
        except Exception as e:
            logger.debug(f"Could not get queue stats: {e}")
        return None

    def _disconnect(self) -> None:
        """Close RabbitMQ connection."""
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
            if self._connection and self._connection.is_open:
                self._connection.close()
            logger.info("Disconnected from RabbitMQ")
        except Exception as e:
            logger.error(f"Error disconnecting from RabbitMQ: {e}")
        finally:
            self._channel = None
            self._connection = None

    # -------------------------------------------------------------------------
    # MESSAGE HANDLING
    # -------------------------------------------------------------------------

    def _on_message(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes
    ) -> None:
        """
        Callback for processing incoming messages.

        Args:
            channel: RabbitMQ channel
            method: Delivery method with routing info
            properties: Message properties
            body: Message body as bytes
        """
        delivery_tag = method.delivery_tag
        self._total_consumed += 1

        try:
            # Get queue stats before processing
            stats_before = self.get_queue_stats()
            queue_remaining = stats_before['messages'] if stats_before else '?'

            # Parse message body
            message = self._parse_message(body)
            if message is None:
                logger.error(f"Failed to parse message: {delivery_tag}")
                channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
                self._total_failed += 1
                return

            # Log incoming message
            logger.info(f"Message received: tag={delivery_tag}, type={message.get('event_type', 'unknown')}, queue_remaining={queue_remaining}")

            # Process message using async handler
            success = self._process_message_sync(message)

            if success:
                channel.basic_ack(delivery_tag=delivery_tag)
                self._total_processed += 1

                # Log processing success with stats
                stats_after = self.get_queue_stats()
                queue_after = stats_after['messages'] if stats_after else '?'
                logger.info(f"Message processed: tag={delivery_tag}, queue_remaining={queue_after}, consumed={self._total_consumed}, processed={self._total_processed}, failed={self._total_failed}")
            else:
                # Don't requeue failed messages - they will loop infinitely
                # In production, use Dead Letter Queue (DLQ) for failed messages
                channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
                self._total_failed += 1
                logger.error(f"Message failed: tag={delivery_tag}, consumed={self._total_consumed}, processed={self._total_processed}, failed={self._total_failed}")

        except Exception as e:
            logger.error(f"Message exception: tag={delivery_tag}, error={e}, consumed={self._total_consumed}, processed={self._total_processed}, failed={self._total_failed + 1}", exc_info=True)
            channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
            self._total_failed += 1

    def _parse_message(self, body: bytes) -> Optional[dict]:
        """
        Parse message body to dictionary.

        Args:
            body: Message body as bytes

        Returns:
            Parsed dictionary or None if parsing fails
        """
        try:
            return json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Failed to parse message body: {e}")
            return None

    def _process_message_sync(self, message: dict) -> bool:
        """
        Process message synchronously, handling async handler.

        Args:
            message: Parsed message dictionary

        Returns:
            True if processing successful, False otherwise
        """
        try:
            # If handler is async, run it using asyncio.run()
            if asyncio.iscoroutinefunction(self.message_handler):
                # Use asyncio.run() for each message - simpler and thread-safe
                return asyncio.run(self.message_handler(message))
            else:
                return self.message_handler(message)

        except Exception as e:
            logger.error(f"Message handler error: {e}", exc_info=True)
            return False

    def _should_requeue(self, properties: BasicProperties) -> bool:
        """
        Determine if message should be requeued based on retry count.

        Args:
            properties: Message properties

        Returns:
            True if should requeue, False otherwise
        """
        headers = properties.headers or {}
        retry_count = headers.get("x-retry-count", 0)
        max_retries = 3

        return retry_count < max_retries

    # -------------------------------------------------------------------------
    # CONSUMER LIFECYCLE
    # -------------------------------------------------------------------------

    def start(self) -> None:
        """
        Start consuming messages.
        Runs in a loop with automatic reconnection.
        """
        self._is_running = True
        self._should_stop = False
        retry_delay = settings.RABBITMQ_RETRY_DELAY

        logger.info("Starting notification consumer...")

        while not self._should_stop:
            try:
                # Connect to RabbitMQ
                if not self._connect():
                    logger.warning(f"Connection failed, retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    continue

                # Start consuming
                self._channel.basic_consume(
                    queue=self.config.queue,
                    on_message_callback=self._on_message,
                    auto_ack=self.config.auto_ack
                )

                logger.info(f"Consumer started. Waiting for messages on queue: {self.config.queue}")

                # Start consuming (blocking)
                self._channel.start_consuming()

            except KeyboardInterrupt:
                logger.info("Consumer interrupted by user")
                break

            except pika.exceptions.ConnectionClosedByBroker:
                logger.warning("Connection closed by broker, reconnecting...")
                time.sleep(retry_delay)

            except pika.exceptions.AMQPChannelError as e:
                logger.error(f"Channel error: {e}, reconnecting...")
                time.sleep(retry_delay)

            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"Connection error: {e}, reconnecting...")
                time.sleep(retry_delay)

            except Exception as e:
                logger.error(f"Unexpected error: {e}", exc_info=True)
                time.sleep(retry_delay)

            finally:
                self._disconnect()

        self._is_running = False
        logger.info("Consumer stopped")

    def stop(self) -> None:
        """Stop the consumer gracefully."""
        logger.info("Stopping consumer...")
        self._should_stop = True

        if self._channel and self._channel.is_open:
            try:
                self._channel.stop_consuming()
            except Exception as e:
                logger.error(f"Error stopping consumer: {e}")

    @property
    def is_running(self) -> bool:
        """Check if consumer is currently running."""
        return self._is_running

    def get_stats(self) -> dict:   #return consumer performance
        """
        Get consumer and queue statistics.

        Returns:
            Dict containing consumer stats
        """
        queue_stats = self.get_queue_stats()
        return {
            "is_running": self._is_running,
            "total_consumed": self._total_consumed,
            "total_processed": self._total_processed,
            "total_failed": self._total_failed,
            "success_rate": f"{(self._total_processed / self._total_consumed * 100):.1f}%" if self._total_consumed > 0 else "N/A",
            "queue": {
                "name": self.config.queue,
                "messages_waiting": queue_stats['messages'] if queue_stats else None,
                "consumers_active": queue_stats['consumers'] if queue_stats else None,
            }
        }


# =============================================================================
# CONSUMER FACTORY FUNCTION
# =============================================================================

def start_consumer(
    message_handler: Callable[[dict], bool],
    config: Optional[ConsumerConfig] = None,
    run_in_thread: bool = False
) -> NotificationConsumer:
    """
    Create and start a notification consumer.

    Args:
        message_handler: Function to process messages
        config: Consumer configuration
        run_in_thread: If True, run consumer in a separate thread

    Returns:
        NotificationConsumer instance
    """
    consumer = NotificationConsumer(message_handler, config)

    if run_in_thread:
        thread = threading.Thread(target=consumer.start, daemon=True)
        thread.start()
        logger.info("Consumer started in background thread")
    else:
        consumer.start()

    return consumer
