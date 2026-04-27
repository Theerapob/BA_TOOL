import re
import logging

logger = logging.getLogger(__name__)

# [FIX-Bug] เพิ่ม identity/generated เป็น stop keyword ใน type parsing
TYPE_STOP_KEYWORDS = {
    "default", "unique", "check", "references",
    "primary", "foreign", "constraint",
    "identity", "generated", "collate", "comment",
    "key", "index", "as",
}

LINE_SKIP_KEYWORDS = {"primary", "foreign", "constraint", "unique", "check", "index", "key"}

# [FIX-Bug] regex compile ครั้งเดียว ไม่ compile ในลูป
_TABLE_PATTERN = re.compile(
    r"create\s+table\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_.\"'\[\]]+)\s*\((.*?)\)\s*;",
    re.IGNORECASE | re.DOTALL
)
_PAREN_CONTENT   = re.compile(r"\(([^)]+)\)")
_PK_INLINE       = re.compile(r"\bPRIMARY\s+KEY\b")
_NOT_NULL        = re.compile(r"\bNOT\s+NULL\b")
_REFERENCES      = re.compile(
    r"REFERENCES\s+([a-zA-Z0-9_.\"'\[\]]+)\s*(?:\(([^)]*)\))?",
    re.IGNORECASE
)
_FK_LINE         = re.compile(
    r"FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+([a-zA-Z0-9_.\"']+)\s*(?:\(([^)]*)\))?",
    re.IGNORECASE
)


def _clean_name(s: str) -> str:
    """ลบ quote ทุกชนิด + lowercase"""
    return re.sub(r'[\"\'\[\]]', '', s).strip().lower()


def parse_sql(sql_text: str) -> list[dict]:
    tables = []
    matches = _TABLE_PATTERN.findall(sql_text)

    for table_name, body in matches:
        clean_table_name = _clean_name(table_name.split(".")[-1])
        lines = _split_columns(body)

        # ── Pass 1: scan table-level PK / FK constraints ──────
        pk_cols: set[str]  = set()
        fk_map: dict[str, dict] = {}

        for line in lines:
            line_s = line.strip()
            upper  = line_s.upper()

            # PRIMARY KEY (col1, col2, ...)
            if re.match(r"PRIMARY\s+KEY", upper):
                m = _PAREN_CONTENT.search(line_s)
                if m:
                    for c in m.group(1).split(","):
                        pk_cols.add(_clean_name(c))

            # FOREIGN KEY (col) REFERENCES tbl(col)
            fk_m = _FK_LINE.search(line_s)
            if fk_m:
                fk_col    = _clean_name(fk_m.group(1))
                ref_table = _clean_name(fk_m.group(2).split(".")[-1])
                ref_col   = _clean_name(fk_m.group(3)) if fk_m.group(3) else None
                fk_map[fk_col] = {"ref_table": ref_table, "ref_column": ref_col}

        # ── Pass 2: parse columns ──────────────────────────────
        for line in lines:
            line = line.strip()
            if not line:
                continue

            parts = line.split()
            if not parts:
                continue

            first_word = parts[0].lower().strip('"\'[]').rstrip(",")
            if first_word in LINE_SKIP_KEYWORDS:
                continue

            if len(parts) < 2:
                continue

            column_name = _clean_name(parts[0])

            # ── parse type ──────────────────────────────────────
            type_tokens:  list[str] = []
            type_end_idx: int = 1
            paren_depth:  int = 0

            for token in parts[1:]:
                paren_depth += token.count("(") - token.count(")")
                clean_token  = token.lower().rstrip(",") if paren_depth == 0 else token.lower()

                if paren_depth == 0:
                    if clean_token in ("not", "null"):
                        break
                    if clean_token in TYPE_STOP_KEYWORDS:
                        break

                type_tokens.append(token)
                type_end_idx += 1

            if not type_tokens:
                continue

            sql_type = " ".join(type_tokens).rstrip(",").strip()

            # ── parse nullable ──────────────────────────────────
            remaining_tokens = [t.rstrip(",") for t in parts[type_end_idx:]]
            remaining_clean  = _strip_collate(remaining_tokens).upper()

            nullable = "NOT NULL" if _NOT_NULL.search(remaining_clean) else "NULL"

            # ── inline PRIMARY KEY ──────────────────────────────
            if _PK_INLINE.search(remaining_clean):
                pk_cols.add(column_name)

            # ── inline REFERENCES ───────────────────────────────
            ref_m = _REFERENCES.search(line)
            if ref_m and column_name not in fk_map:
                ref_table = _clean_name(ref_m.group(1).split(".")[-1])
                ref_col   = _clean_name(ref_m.group(2)) if ref_m.group(2) else None
                fk_map[column_name] = {"ref_table": ref_table, "ref_column": ref_col}

            tables.append({
                "table":    clean_table_name,
                "column":   column_name,
                "type":     sql_type,
                "nullable": nullable,
                "is_pk":    column_name in pk_cols,
                "fk":       fk_map.get(column_name),
            })

    # [FIX-Bug] เปลี่ยน print → logger.debug (ไม่ spam production logs)
    logger.debug("parsed rows:")
    for t in tables:
        pk = " PK" if t["is_pk"] else ""
        fk = (f" FK->{t['fk']['ref_table']}.{t['fk']['ref_column'] or '?'}"
              if t["fk"] else "")
        logger.debug(f"  {t['table']}.{t['column']} -> {t['type']} | {t['nullable']}{pk}{fk}")

    return tables


def validate_fk(tables: list[dict]) -> list[dict]:
    """ตรวจสอบ FK reference ว่าชี้ไปตาราง/คอลัมน์ที่มีอยู่จริง และเป็น PK"""
    errors: list[dict] = []
    schema: dict[str, set]  = {}
    pk_idx: dict[str, set]  = {}

    for row in tables:
        t = row["table"]
        schema.setdefault(t, set()).add(row["column"])
        if row.get("is_pk"):
            pk_idx.setdefault(t, set()).add(row["column"])

    for row in tables:
        fk = row.get("fk")
        if not fk:
            continue

        ref_t = fk["ref_table"]
        ref_c = fk["ref_column"]
        src   = f"{row['table']}.{row['column']}"

        if ref_t not in schema:
            errors.append({"level": "error", "src": src,
                           "msg": f"ตาราง '{ref_t}' ไม่มีใน schema"})
            continue

        if ref_c and ref_c not in schema[ref_t]:
            errors.append({"level": "error", "src": src,
                           "msg": f"column '{ref_t}.{ref_c}' ไม่มีใน schema"})
            continue

        if ref_c and ref_c not in pk_idx.get(ref_t, set()):
            errors.append({"level": "warn", "src": src,
                           "msg": f"'{ref_t}.{ref_c}' ไม่ใช่ PRIMARY KEY"})

    return errors


def _strip_collate(tokens: list[str]) -> str:
    result:    list[str] = []
    skip_next: bool      = False
    for token in tokens:
        clean = token.lower().rstrip(",")
        if skip_next:
            skip_next = False
            continue
        if clean == "collate":
            skip_next = True
            continue
        result.append(token)
    return " ".join(result)


def _split_columns(body: str) -> list[str]:
    """แยก column definitions ด้วย comma โดยไม่สนใจ comma ใน parenthesis"""
    parts: list[str] = []
    depth: int       = 0
    buf:   list[str] = []

    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)

    if buf:
        parts.append("".join(buf).strip())
    return parts


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    sql = """
    CREATE TABLE users (
        id         INT NOT NULL PRIMARY KEY,
        email      VARCHAR(100) NOT NULL UNIQUE,
        name       VARCHAR(255) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE orders (
        order_id   BIGINT NOT NULL,
        user_id    INT NOT NULL REFERENCES users(id),
        note       TEXT NULL,
        PRIMARY KEY (order_id),
        FOREIGN KEY (user_id) REFERENCES users(id)
    );
    CREATE TABLE order_items (
        item_id    INT NOT NULL,
        order_id   BIGINT NOT NULL,
        product_id INT NOT NULL REFERENCES products(id),
        PRIMARY KEY (item_id),
        CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(order_id)
    );
    """
    rows   = parse_sql(sql)
    errors = validate_fk(rows)
    print("\n[FK VALIDATION]")
    if not errors:
        print("  OK — no issues")
    for e in errors:
        icon = "ERR" if e["level"] == "error" else "WARN"
        print(f"  [{icon}] {e['src']} -- {e['msg']}")