import re
import sys
import pandas as pd
from tabulate import tabulate

KNOWN_TYPES = [
    "bigint", "int", "decimal", "numeric",
    "date", "time", "datetime", "timestamp",
    "char", "text", "varchar", "nvarchar",
    "bit", "boolean",
]
#แบบ raw-type, logical-type
def type_mapping (sql_type):
    t = sql_type.lower()
    # ตัด precision ออก เช่น decimal(10,2) → decimal
    base = re.split(r"[\(\s]", t)[0]

    if "bigint" in base:
        return "long", "long"
    elif base in ("int", "integer", "smallint", "tinyint"):
        return "int", "int"
    elif base in ("decimal", "numeric", "money", "smallmoney"):
        return "bytes", "decimal"
    elif base in ("datetime", "timestamp"):
        return "long", "timestamp-millis"
    elif base in ("char", "text", "varchar", "nvarchar", "nchar","json",):
        return "string", "string"
    elif base in ("bit", "boolean", "bool"):
        return "boolean", "boolean"
    elif base in ("float"):
        return "float", "float"
    elif base in ("double", "real"):
        return "double", "double"
    elif base in ("date"):
        return "int", "date"
    elif base in ("time"):
        return "int", "time-millis"
    else:
        return None, None  # ← ไม่รู้จัก type นี้


def get_action(logical):
    if logical == "timestamp-millis":
        return "Convert datetime → Unix timestamp"
    return "Direct move"


def extract_columns(sql: str):
    columns = []
    invalid_columns = []  # ← เก็บ column ที่มี type ไม่ถูกต้อง

    match = re.search(r"\((.*)\)", sql, re.DOTALL)
    if not match:
        return columns, invalid_columns

    body = match.group(1)

    # แยก line โดย track depth ของ parenthesis
    lines = []
    depth = 0
    buf = ""

    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            lines.append(buf.strip())
            buf = ""
        else:
            buf += ch
    if buf:
        lines.append(buf.strip())

    clean_lines = []
    for line in lines:
        if not line:
            continue
        if re.match(r"^(PRIMARY|CONSTRAINT|UNIQUE|FOREIGN|CHECK|KEY)", line, re.I):
            continue
        if len(line.split()) < 2:
            continue
        clean_lines.append(line)

    for i, line in enumerate(clean_lines):
        parts = line.split()
        col_name = parts[0].strip("[]`\"")
        sql_type = parts[1]

        raw, logical = type_mapping(sql_type)

        # ถ้า type ไม่ถูกต้อง → บันทึกไว้แล้วข้ามไป
        if raw is None:
            invalid_columns.append({
                "NO":       i + 1,
                "Name":     col_name,
                "SQL Type": sql_type,
                "Reason":   f"Unknown type '{sql_type}' — ไม่มีใน mapping",
            })
            continue

        columns.append({
            "NO":      i + 1,
            "Name":    col_name,
            "Raw":     raw,
            "Logical": logical,
            "Action":  get_action(logical),
        })

    return columns, invalid_columns


def main():
    print("📌 วาง SQL แล้วกด Enter 2 ครั้ง:\n")

    lines = []
    empty_count = 0

    while True:
        line = input()
        if line == "":
            empty_count += 1
            if empty_count == 2:
                break
        else:
            empty_count = 0
        lines.append(line)

    sql = "\n".join(lines).strip()
    cols, invalid_cols = extract_columns(sql)

# แสดง column ที่ type ไม่ถูกต้อง (ถ้ามี)
    if invalid_cols:
        print("\n⚠️  พบ datatype ที่ไม่รองรับ:\n")
        df_invalid = pd.DataFrame(invalid_cols)
        print(tabulate(df_invalid, headers="keys", tablefmt="outline", showindex=False))
        print("\n❌ หยุดโปรแกรม: กรุณาแก้ไข datatype ก่อนดำเนินการต่อ")
        sys.exit(1)  # ← exit code 1 = error

    if not cols:
        print("❌ อ่าน SQL ไม่ได้")
        sys.exit(1)
    
# แสดงผลปกติ
    df = pd.DataFrame(cols)
    print("\n✅ RESULT:\n")
    print(tabulate(df, headers="keys", tablefmt="outline", showindex=False))


if __name__ == "__main__":
    main()