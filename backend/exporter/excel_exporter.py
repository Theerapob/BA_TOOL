import pandas as pd
import io


def export_all_xlsx(tables: dict) -> io.BytesIO:
    """Export ทุกตาราง — แต่ละตารางเป็น 1 sheet"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for table_name, columns in tables.items():
            df = pd.DataFrame(columns)
            df.to_excel(writer, sheet_name=table_name[:31], index=False)
    output.seek(0)
    return output


def export_all_csv(tables: dict) -> io.BytesIO:
    """Export ทุกตาราง รวมเป็น CSV เดียว มี column Table นำหน้า"""
    rows = []
    for table_name, columns in tables.items():
        for col in columns:
            rows.append({"Table": table_name, **col})
    df = pd.DataFrame(rows)
    output = io.BytesIO()
    df.to_csv(output, index=False, encoding="utf-8-sig")
    output.seek(0)
    return output


def export_table_xlsx(columns: list) -> io.BytesIO:
    """Export ตารางเดียวเป็น xlsx"""
    df = pd.DataFrame(columns)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return output


def export_table_csv(columns: list) -> io.BytesIO:
    """Export ตารางเดียวเป็น csv"""
    df = pd.DataFrame(columns)
    output = io.BytesIO()
    df.to_csv(output, index=False, encoding="utf-8-sig")
    output.seek(0)
    return output