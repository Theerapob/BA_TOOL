import re

# ประเภทที่ถือว่า "ปกติ" เมื่อแปลงเป็น bytes/byte array
# (ต้นทางต้องเป็น decimal-family จึงจะ OK)
_DECIMAL_FAMILY = {
    "decimal", "numeric", "money", "smallmoney",
    "float", "real", "double", "number",
}

# raw_type ที่ถือว่าเป็น "byte output"
_BYTE_RAW_TYPES = {"bytes", "byte", "byte[]", "binary"}


class DataTypeConverter:
    def __init__(self, mapping: dict):
        self.mapping = mapping

    def normalize(self, sql_type: str) -> str:
        t = sql_type.lower().strip()
        return re.split(r"[\(\s]", t)[0]

    def apply_precision(self, sql_type: str, base: str, final_type: str) -> str:
        """
        ต่อ (n) หรือ (p,s) กลับเข้า final_type จาก sql_type ต้นทาง
        base  = normalized base type เช่น 'varchar', 'money'
        """
        # money / smallmoney / timestamp / rowversion
        # มี precision fixed ใน DB แล้ว → ห้าม append ซ้ำ
        if base in ("money", "smallmoney", "timestamp", "rowversion"):
            return final_type

        # ถ้า final_type จาก DB มี (...) อยู่แล้ว (เช่น nvarchar(max), datetime2(7))
        # → ใช้ตรงๆ ห้าม append ซ้ำเด็ดขาด
        if "(" in final_type:
            return final_type

        match = re.search(r"\(([^)]+)\)", sql_type)

        # ใช้ base type ต้นทางในการตัดสิน ไม่ใช่ final_type
        needs_precision = {
            "decimal", "numeric",
            "varchar", "char",
            "nvarchar", "nchar",
            "binary", "varbinary",
        }

        if base in needs_precision and match:
            return f"{final_type}({match.group(1)})"

        # decimal/numeric ต้นทางไม่ระบุ precision → SQL Server default = (18,0)
        if base in ("decimal", "numeric") and not match:
            return f"{final_type}(18,0)"

        return final_type

    def convert(self, sql_type: str) -> dict:
        base = self.normalize(sql_type)
        data = self.mapping.get(base)

        if not data:
            return {
                "input": sql_type, "raw": None, "logical": None, "final": None,
                "status": "unknown", "reason": f"Type '{base}' not found in mapping"
            }

        final = self.apply_precision(sql_type, base, data["final"])

        # ── ตรวจ byte anomaly ────────────────────────────────────
        # raw_type เป็น bytes แต่ต้นทางไม่ใช่ decimal-family → ผิดปกติ
        raw_type_lower = (data["raw"] or "").lower().strip()
        is_byte_output = raw_type_lower in _BYTE_RAW_TYPES
        is_decimal_src = base in _DECIMAL_FAMILY
        byte_anomaly   = is_byte_output and not is_decimal_src

        return {
            "input": sql_type,
            "raw": data["raw"],
            "logical": data["logical"],
            "final": final,
            "status": "ok",
            "reason": None,
            "byte_anomaly": byte_anomaly,
            "byte_anomaly_detail": (
                f"คอลัมน์ถูกแปลงเป็น '{data['raw']}' "
                f"แต่ type ต้นทาง '{base}' ไม่ใช่ decimal — "
                f"กรุณาตรวจสอบ mapping"
            ) if byte_anomaly else None,
        }