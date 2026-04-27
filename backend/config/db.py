import os
import threading
from pathlib import Path
from psycopg2 import pool, OperationalError
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

DB_URL = os.getenv("DB_URL")

if not DB_URL:
    raise RuntimeError("Environment variable DB_URL is not set")

# [FIX-Performance] กำหนด pool size จาก env ได้
_POOL_MIN = int(os.getenv("DB_POOL_MIN", "2"))
_POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))

connection_pool: pool.SimpleConnectionPool | None = None
_pool_lock = threading.Lock()  # [FIX-Performance] thread-safe init


def init_db_pool() -> None:
    """สร้าง Connection Pool ตอนเริ่มแอปพลิเคชัน"""
    global connection_pool
    with _pool_lock:
        if connection_pool is not None:
            return  # already initialized
        try:
            connection_pool = pool.SimpleConnectionPool(_POOL_MIN, _POOL_MAX, DB_URL)
            logger.info(f"✅ DB pool initialized (min={_POOL_MIN}, max={_POOL_MAX})")
        except OperationalError as e:
            logger.error(f"❌ Failed to create DB pool: {e}")
            raise


def close_db_pool() -> None:
    """ปิด Connection ทั้งหมดตอนแอปพลิเคชันหยุดทำงาน"""
    global connection_pool
    with _pool_lock:
        if connection_pool:
            connection_pool.closeall()
            connection_pool = None
            logger.info("🛑 DB pool closed")


def get_connection():
    """ดึง connection จาก pool"""
    if connection_pool is None:
        raise RuntimeError("Database connection pool is not initialized.")
    try:
        conn = connection_pool.getconn()
        if conn is None:
            raise RuntimeError("No available connections in pool")
        return conn
    except pool.PoolError as e:
        logger.error(f"❌ Pool exhausted: {e}")
        raise RuntimeError(f"Connection pool exhausted: {e}") from e


def release_connection(conn) -> None:
    """คืน connection กลับ pool — ถ้า pool ปิดแล้วก็ close ตรงๆ"""
    if connection_pool:
        try:
            connection_pool.putconn(conn)
        except Exception as e:
            logger.warning(f"⚠️ Could not return connection to pool: {e}")
            try:
                conn.close()
            except Exception:
                pass
    else:
        # pool ถูก close แล้ว (shutdown) — close connection ตรงๆ
        try:
            conn.close()
        except Exception:
            pass