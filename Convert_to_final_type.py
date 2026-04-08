import re
import sys
import pandas as pd
from tabulate import tabulate

KNOWN_TYPES = [
    "bigint", "int", "decimal", "numeric",
    "date", "time", "datetime", "timestamp",
    "char", "text", "varchar", "nvarchar",
    "bit", "boolean", "float", "double", "real",
    "smallint", "tinyint", "money", "smallmoney",
]

#แปลง SQL type → raw type, logical type
def type_mapping(sql_type):
    t = sql_type.lower()
    base = re.split(r"[\(\s]", t)[0]

    if base == "bigint":
        return "long", "long"
    elif base in ("int", "integer", "smallint", "tinyint"):
        return "int", "int"
    elif base in ("decimal", "numeric"):
        return "bytes", "decimal"
    elif base in ("money", "smallmoney"):
        return "bytes", "decimal"
    elif base == "bit":
        return "boolean", "boolean"
    elif base in ("float", "real"):
        return "float", "float"
    elif base == "double":
        return "double", "double"
    elif base in ("datetime", "smalldatetime"):
        return "long", "timestamp-millis"
    elif base == "datetime2":
        return "long", "timestamp-micros" 
    elif base == "date":
        return "int", "date"
    elif base == "time":
        return "int", "time-millis"
    elif base in ("char", "varchar", "text"):
        return "string", "string"
    elif base in ("nchar", "nvarchar", "ntext"):
        return "string", "string"
    elif base in ("binary", "varbinary", "image"):
        return "bytes", "bytes"
    elif base == "rowversion":
        return "bytes", "bytes"
    elif base == "timestamp":      
        return "bytes", "bytes"
    elif base == "uniqueidentifier":
        return "string", "uuid"
    elif base == "xml":
        return "string", "string"
    elif base in ("sql_variant",):
        return "string", "string"           
    elif base in ("geography", "geometry", "hierarchyid"):
        return "string", "string"          
    else:
        return None, None

#แปลง logical type → final type 
def get_final_type(sql_type: str, logical: str) -> str:
    t = sql_type.lower()
    base = re.split(r"[\(\s]", t)[0]

    # ดึง precision/scale จาก sql_type เดิม เช่น decimal(10,2)
    precision_match = re.search(r"\(([^)]+)\)", t)
    precision_scale = precision_match.group(1) if precision_match else "18,2"

    type_mapping = {
        # Logical types
        "timestamp-millis": "datetime",
        "timestamp-micros": "datetime2(6)",
        "date":             "date",
        "time-millis":      "time",
        "decimal":          f"decimal({precision_scale})",
        "uuid":             "uniqueidentifier",

        # Primitive types
        "boolean":          "bit",
        "long":             "bigint",
        "int":              "int",
        "float":            "float",
        "double":           "float(53)",
        "string":           "nvarchar(max)",
        "bytes":            "varbinary(max)",
    }

    # varchar ถ้า base type เดิมเป็น varchar หรือ char (ไม่ใช่ unicode)
    if logical == "string" and base in ("varchar", "char"):
        return "varchar"

    return type_mapping.get(logical, "unknown")


def get_action(logical):
    if logical == "timestamp-millis":
        return "Convert datetime → Unix timestamp"
    return "Direct move"


def extract_columns(sql: str):
    columns = []
    invalid_columns = []

    match = re.search(r"\((.*)\)", sql, re.DOTALL)
    if not match:
        return columns, invalid_columns

    body = match.group(1)

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

        if raw is None:
            invalid_columns.append({
                "NO":       i + 1,
                "Name":     col_name,
                "SQL Type": sql_type,
                "Reason":   f"Unknown type '{sql_type}' — ไม่มีใน mapping",
            })
            continue

#ส่วนตารางเเสดงผลลัพธ์
        final = get_final_type(sql_type, logical)

        columns.append({
            "NO":      i + 1,
            "Name":    col_name,
            "RAW TYPE":     raw,
            "LOGICAL TYPE": logical,
            "TARGET TYPE":   final,
            "Action":  get_action(logical),
        })

    return columns, invalid_columns

#ส่วน main function
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

    if invalid_cols:
        print("\n⚠️  พบ datatype ที่ไม่รองรับ:\n")
        df_invalid = pd.DataFrame(invalid_cols)
        print(tabulate(df_invalid, headers="keys", tablefmt="outline", showindex=False))
        print("\n❌ หยุดโปรแกรม: กรุณาแก้ไข datatype ก่อนดำเนินการต่อ")
        sys.exit(1)

    if not cols:
        print("❌ อ่าน SQL ไม่ได้")
        sys.exit(1)

    df = pd.DataFrame(cols)
    print("\n✅ RESULT:\n")
    print(tabulate(df, headers="keys", tablefmt="outline", showindex=False))


if __name__ == "__main__":
    main()