import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import List

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from backend.repository.mapping_repo import MappingRepository
from backend.core.converter import DataTypeConverter
from backend.parser.sql_parser import parse_sql
from backend.config.logger import logger
from backend.config.db import init_db_pool, close_db_pool
from backend.core.cache_store import result_cache

mapping_data = {}
converter = None


# ── Lifecycle ─────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Starting up...")
    global mapping_data, converter

    try:
        init_db_pool()
        repo = MappingRepository()
        mapping_data = repo.get_all()
        converter = DataTypeConverter(mapping_data)

    except Exception as e:
        logger.warning(f"⚠️ Startup skipped (test mode): {e}")
        mapping_data = {}
        converter = DataTypeConverter({})

    yield

    logger.info("🛑 Shutdown")
    close_db_pool()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ⚠️ production ควร fix domain
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Model ─────────────────────────────
class OverrideRequest(BaseModel):
    table: str
    column: str
    new_type: str


# ── Helpers ─────────────────────────────
def cleanup_expired_sessions():
    now = datetime.now()
    for sid in list(result_cache.keys()):
        if now - result_cache[sid]["created_at"] > timedelta(hours=1):
            del result_cache[sid]


def get_cached_data(session_id: str):
    session = result_cache.get(session_id)
    if not session:
        raise HTTPException(404, "Session not found")
    return session["data"]


# ── API ─────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/convert")
async def convert(files: List[UploadFile] = File(...)):
    if converter is None:
        raise HTTPException(500, "Converter not initialized")

    logger.info(f"📥 Convert {len(files)} file(s)")

    tables = {}
    unknown = {}

    for file in files:
        logger.info(f"  → Processing: {file.filename}")

        try:
            await file.seek(0)  # ✅ กัน pointer เพี้ยน
            content = await file.read()
            sql_text = content.decode("utf-8-sig")
        except Exception as e:
            logger.error(f"❌ Error reading {file.filename}: {e}")
            raise HTTPException(400, f"Invalid file: {file.filename}")

        parsed = parse_sql(sql_text)
        if not parsed:
            logger.warning(f"  ⚠️ No table found in: {file.filename}")
            continue

        for row in parsed:
            res = converter.convert(row["type"])
            table = row["table"]

            tables.setdefault(table, []).append({
                "column_name": row["column"],
                "file": file.filename,  # ✅ เพิ่มว่า column มาจากไฟล์ไหน
                "raw_type": res.get("raw"),
                "logical_type": res.get("logical"),
                "final_type": res.get("final") if res.get("status") == "ok" else row["type"],
                "source_sql_type": row["type"]
            })

            if res.get("status") != "ok":
                unknown.setdefault(table, []).append({
                    "column_name": row["column"],
                    "file": file.filename,
                    "reason": res.get("reason")
                })

    if not tables:
        raise HTTPException(400, "No table found in any file")

    cleanup_expired_sessions()

    session_id = str(uuid.uuid4())

    result_cache[session_id] = {
        "data": {"tables": tables, "unknown": unknown},
        "created_at": datetime.now()
    }

    return {
        "session_id": session_id,
        "file_count": len(files),
        "tables": tables
    }


@app.get("/result/{session_id}")
def get_result(session_id: str):
    return get_cached_data(session_id)


@app.post("/override/{session_id}")
def override(session_id: str, body: OverrideRequest):
    data = get_cached_data(session_id)

    for col in data["tables"].get(body.table, []):
        if col["column_name"] == body.column:
            col["final_type"] = body.new_type
            return {"updated_column": col}

    raise HTTPException(404, "Column not found")


@app.delete("/session/{session_id}")
def delete(session_id: str):
    if session_id in result_cache:
        del result_cache[session_id]
        return {"status": "deleted"}
    raise HTTPException(404, "Session not found")