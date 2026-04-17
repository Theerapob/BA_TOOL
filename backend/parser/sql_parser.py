import re

def parse_sql(sql_text: str) -> list[dict]:
    tables = []

    # รองรับ CREATE TABLE + IF NOT EXISTS + schema
    pattern = re.compile(
        r"create\s+table\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_.]+)\s*\((.*?)\)\s*;",
        re.IGNORECASE | re.DOTALL
    )

    matches = pattern.findall(sql_text)

    CONSTRAINT_KEYWORDS = {
        "not", "null", "default", "unique", "check",
        "references", "primary", "foreign", "constraint"
    }

    for table_name, body in matches:
        # normalize table name
        clean_table_name = table_name.split(".")[-1].lower().strip()

        # แยก column ด้วย comma ที่อยู่นอกวงเล็บ
        lines = re.split(r',\s*(?![^()]*\))', body)

        for line in lines:
            line = line.strip()
            if not line:
                continue

            parts = line.split()
            if not parts:
                continue

            first_word = parts[0].lower()

            # ข้าม constraint ระดับ table
            if first_word in ("primary", "foreign", "constraint"):
                continue

            if len(parts) < 2:
                continue

            # normalize column name
            column_name = parts[0].strip('"').lower().strip()

            # เก็บเฉพาะ datatype (หยุดเมื่อเจอ constraint keyword)
            type_tokens = []
            for token in parts[1:]:
                if token.lower() in CONSTRAINT_KEYWORDS:
                    break
                type_tokens.append(token)

            if not type_tokens:
                continue

            sql_type = " ".join(type_tokens).strip()

            tables.append({
                "table": clean_table_name,
                "column": column_name,
                "type": sql_type
            })

    return tables