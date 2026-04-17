import re

class DataTypeConverter:
    def __init__(self, mapping: dict):
        self.mapping = mapping

    def normalize(self, sql_type: str) -> str:
        # ตัดคำแรกมาหาใน mapping เช่น "VARCHAR(50)" -> "varchar"
        t = sql_type.lower().strip()
        return re.split(r"[\(\s]", t)[0]

    def apply_precision(self, sql_type: str, final_type: str) -> str:
        # ดึงค่าในวงเล็บ (ถ้ามี)
        match = re.search(r"\(([^)]+)\)", sql_type)
        
        # รายชื่อประเภทข้อมูลที่ควรคงค่าความยาวไว้
        needs_precision = ["decimal", "numeric", "varchar", "char", "character varying", "nvarchar"]
        
        target_lower = final_type.lower()
        
        if match:
            if any(p in target_lower for p in needs_precision):
                return f"{final_type}({match.group(1)})"
        
        # กรณี decimal ใน mapping แต่ต้นฉบับไม่ได้ระบุมา ให้ใส่ default
        if target_lower in ["decimal", "numeric"] and not match:
            return f"{final_type}(18,2)"
            
        return final_type

    def convert(self, sql_type: str) -> dict:
        base = self.normalize(sql_type)
        data = self.mapping.get(base)

        if not data:
            return {
                "input": sql_type, "raw": None, "logical": None, "final": None,
                "status": "unknown", "reason": f"Type '{base}' not found in mapping"
            }

        final = self.apply_precision(sql_type, data["final"])
        return {
            "input": sql_type,
            "raw": data["raw"],
            "logical": data["logical"],
            "final": final,
            "status": "ok",
            "reason": None
        }