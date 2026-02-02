"""
Logging Configuration - Simple daily rotating log file.

Features:
- Single log file per day
- Auto cleanup old logs
- Console + File output
"""

import os
import logging
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from config.settings import settings


def setup_logging() -> None:
    """Setup application-wide logging configuration."""
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)

    # Create logs directory
    log_dir = Path(settings.LOG_DIRECTORY)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Log file path
    log_file = log_dir / settings.LOG_FILE

    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Clear existing handlers
    root_logger.handlers.clear()

    # Formatter
    formatter = logging.Formatter(
        fmt=settings.LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File Handler
    file_handler = TimedRotatingFileHandler(
        filename=str(log_file),
        when="midnight",
        interval=1,
        backupCount=settings.LOG_RETENTION_DAYS,
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Suppress noisy loggers
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("socketio").setLevel(logging.WARNING)
    logging.getLogger("engineio").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for a module."""
    return logging.getLogger(name)


def cleanup_old_logs() -> int:
    """Remove log files older than retention period."""
    log_dir = Path(settings.LOG_DIRECTORY)
    if not log_dir.exists():
        return 0

    cutoff_date = datetime.now() - timedelta(days=settings.LOG_RETENTION_DAYS)
    removed_count = 0

    for log_file in log_dir.glob("*.log*"):
        try:
            file_time = datetime.fromtimestamp(log_file.stat().st_mtime)
            if file_time < cutoff_date:
                log_file.unlink()
                removed_count += 1
        except (ValueError, OSError):
            continue

    return removed_count
