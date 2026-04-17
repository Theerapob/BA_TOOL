import os
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DB_URL")

if not DB_URL:
    raise RuntimeError("Environment variable DB_URL is not set")

# เริ่มต้นเป็น None เพื่อรอการ Initialize จาก Startup
connection_pool = None

def init_db_pool():
    """สร้าง Connection Pool ตอนเริ่มแอปพลิเคชัน"""
    global connection_pool
    if connection_pool is None:
        # กำหนด min 1, max 10 connections
        connection_pool = pool.SimpleConnectionPool(1, 10, DB_URL)

def close_db_pool():
    """ปิด Connection ทั้งหมดตอนแอปพลิเคชันหยุดทำงาน"""
    global connection_pool
    if connection_pool:
        connection_pool.closeall()
        connection_pool = None

def get_connection():
    if connection_pool is None:
        raise RuntimeError("Database connection pool is not initialized.")
    return connection_pool.getconn()

def release_connection(conn):
    if connection_pool:
        connection_pool.putconn(conn)