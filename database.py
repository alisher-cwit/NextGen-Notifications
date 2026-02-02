# =============================================================================
# NOTIFICATION ENGINE - DATABASE CONNECTION
# =============================================================================
# Root level database configuration (same pattern as chat_application)
# =============================================================================

import logging
import urllib.parse
from typing import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

from config.settings import settings

logger = logging.getLogger(__name__)

# =============================================================================
# BUILD DATABASE URL
# =============================================================================

DB_DRIVER = settings.DB_DRIVER
DB_CONNECTION = settings.DB_CONNECTION
DB_USERNAME = settings.DB_USERNAME
DB_PASSWORD = settings.DB_PASSWORD
DB_HOST = settings.DB_HOST
DB_PORT = str(settings.DB_PORT)
DB_DATABASE = settings.DB_DATABASE

if DB_PASSWORD:
    encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
    DATABASE_URL = f"{DB_CONNECTION}+{DB_DRIVER}://{DB_USERNAME}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"
else:
    DATABASE_URL = f"{DB_CONNECTION}+{DB_DRIVER}://{DB_USERNAME}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"

# =============================================================================
# SQLALCHEMY ENGINE
# =============================================================================

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_timeout=settings.DB_POOL_TIMEOUT,
    pool_recycle=settings.DB_POOL_RECYCLE,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# =============================================================================
# SESSION MANAGEMENT
# =============================================================================

def get_db() -> Generator[Session, None, None]:
    """Dependency for FastAPI."""
    db = SessionLocal()
    try:
        yield db
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        db.rollback()
        raise
    finally:
        db.close()


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """Context manager for non-FastAPI usage."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        db.rollback()
        raise
    finally:
        db.close()


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def check_database_connection() -> bool:
    """Check if database is connected."""
    try:
        with get_db_session() as db:
            db.execute(text("SELECT 1"))
            logger.info("Database connection OK")
            return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False


def dispose_engine() -> None:
    """Dispose database engine on shutdown."""
    logger.info("Disposing database engine...")
    engine.dispose()
    logger.info("Database engine disposed")
