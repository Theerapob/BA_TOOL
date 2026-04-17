import io
import pytest
from fastapi.testclient import TestClient

# ✅ ใช้ import ปกติ (ต้องมี __init__.py แล้ว)
from backend.api.main import app, result_cache
from backend.parser.sql_parser import parse_sql
from backend.core.converter import DataTypeConverter
from backend.exporter.excel_exporter import (
    export_all_xlsx,
    export_all_csv
)

# ======================================================================
# TEST: SQL PARSER
# ======================================================================

def test_parser_logic():
    print("\n[1/3] 🔍 Testing SQL Parser")

    sql = "CREATE TABLE test (id INT);"
    result = parse_sql(sql)

    assert isinstance(result, list)
    assert len(result) == 1

    print("✔ Parser OK")


# ======================================================================
# TEST: DATA TYPE CONVERTER
# ======================================================================

def test_converter_logic():
    print("[2/3] 🔄 Testing DataType Converter")

    mock_mapping = {
        "int": {
            "raw": "Number",
            "logical": "Integer",
            "final": "INTEGER"
        }
    }

    conv = DataTypeConverter(mock_mapping)
    res = conv.convert("INT")

    assert res["status"] == "ok"
    assert res["final"] == "INTEGER"

    print("✔ Converter OK")


# ======================================================================
# TEST: FULL API FLOW
# ======================================================================

def test_full_api_journey():
    print("[3/3] 🚀 Testing Full API Journey")

    with TestClient(app) as client:

        # ── A. Health Check ─────────────────────
        res = client.get("/health")
        assert res.status_code == 200

        # ── B. Convert SQL ─────────────────────
        sql_text = "CREATE TABLE test_api (id INT);"
        file = ("test.sql", io.BytesIO(sql_text.encode()), "text/plain")

        resp = client.post("/convert", files={"file": file})
        assert resp.status_code == 200

        data = resp.json()
        session_id = data["session_id"]

        print(f"✔ Session created: {session_id}")

        # ── C. Override ─────────────────────
        override_payload = {
            "table": "test_api",
            "column": "id",
            "new_type": "BIGINT"
        }

        ov = client.post(f"/override/{session_id}", json=override_payload)
        assert ov.status_code == 200

        updated = ov.json()["updated_column"]
        assert updated["final_type"] == "BIGINT"

        print("✔ Override success")

        # ── D. Cleanup ─────────────────────
        del_res = client.delete(f"/session/{session_id}")
        assert del_res.status_code == 200

        print("✔ Session cleaned")


# ======================================================================
# RUN
# ======================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])