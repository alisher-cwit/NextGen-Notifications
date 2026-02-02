"""Notification Engine - Main Entry Point."""

import os
import sys
import signal
import threading
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import settings
from config.logging_config import setup_logging, get_logger
from database import check_database_connection, dispose_engine
from redis_manager import check_redis_connection, redis_connection
from websocket.server import socket_app, push_notification
from messaging.consumer import NotificationConsumer
from services.processor import notification_processor, process_notification_message
from routes import health_router
from routes.health_routes import set_consumer

# Initialize logging
setup_logging()
logger = get_logger(__name__)

# Global state
consumer: NotificationConsumer = None
consumer_thread: threading.Thread = None

# Helper variables (same as NextGen-FastAPI pattern)
IS_DEBUG = settings.DEBUG and settings.APP_ENV != "production"
IS_PRODUCTION = settings.APP_ENV == "production"
CORS_ORIGINS = ["*"] if settings.WEBSOCKET_CORS_ORIGINS == "*" else [o.strip() for o in settings.WEBSOCKET_CORS_ORIGINS.split(",")]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Application lifespan - startup and shutdown events."""
    global consumer, consumer_thread

    logger.info("=" * 60)
    logger.info(f"Starting {settings.APP_NAME}...")
    logger.info(f"Environment: {settings.APP_ENV}")
    logger.info("=" * 60)

    # Startup
    if not check_database_connection():
        logger.error("Database connection failed!")
        raise RuntimeError("Cannot connect to database")

    if not check_redis_connection():
        logger.error("Redis connection failed!")
        raise RuntimeError("Cannot connect to Redis")

    # Configure processor
    notification_processor.set_websocket_emitter(push_notification)
    logger.info("Notification processor configured")

    # Start RabbitMQ consumer
    consumer = NotificationConsumer(message_handler=process_notification_message)
    consumer_thread = threading.Thread(target=consumer.start, daemon=True, name="RabbitMQConsumer")
    consumer_thread.start()
    logger.info("RabbitMQ consumer started")

    # Set consumer reference for health routes
    set_consumer(consumer)

    logger.info("=" * 60)
    logger.info(f"Notification Engine ready on port {settings.ENGINE_PORT}")
    logger.info("=" * 60)

    yield

    # Shutdown
    logger.info("Shutting down Notification Engine...")

    if consumer:
        consumer.stop()
        logger.info("RabbitMQ consumer stopped")

    redis_connection.close()
    logger.info("Redis connections closed")

    dispose_engine()
    logger.info("Database connections closed")

    logger.info("Notification Engine shutdown complete")


# FastAPI application
app = FastAPI(
    title=settings.APP_NAME,
    description="Real-time notification delivery service",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs" if IS_DEBUG else None,
    redoc_url="/redoc" if IS_DEBUG else None,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health_router)

# Mount WebSocket
app.mount("/socket.io", socket_app)


# Signal handler
def handle_shutdown(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    if consumer:
        consumer.stop()
    sys.exit(0)


def main():
    """Main entry point."""
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    print("=" * 60)
    print(f"  {settings.APP_NAME}")
    print(f"  Environment: {settings.APP_ENV}")
    print(f"  Port: {settings.ENGINE_PORT}")
    print("=" * 60)

    uvicorn.run(
        "main:app",
        host=settings.ENGINE_HOST,
        port=settings.ENGINE_PORT,
        reload=IS_DEBUG,
        log_level=settings.LOG_LEVEL.lower(),
        access_log=IS_DEBUG,
    )


if __name__ == "__main__":
    main()
