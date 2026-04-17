import re

#SQL → raw and Logical
def map_to_logical(sql_type: str):
    t = sql_type.lower()
    base = re.split(r"[\(\s]", t)[0]

    if base == "bigint":
        return "long", "long"
    elif base in ("int", "integer", "smallint", "tinyint"):
        return "int", "int"
    elif base in ("decimal", "numeric", "money", "smallmoney"):
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
    elif base in ("char", "varchar", "text", "nchar", "nvarchar", "ntext"):
        return "string", "string"
    elif base in ("binary", "varbinary", "image", "rowversion", "timestamp"):
        return "bytes", "bytes"
    elif base == "uniqueidentifier":
        return "string", "uuid"
    elif base in ("xml", "sql_variant", "geography", "geometry", "hierarchyid"):
        return "string", "string"
    else:
        return None, None
    
#Logical → Final format
def map_to_final(sql_type: str, logical: str) -> str:
    t = sql_type.lower()

    precision_match = re.search(r"\(([^)]+)\)", t)
    precision_scale = precision_match.group(1) if precision_match else "18,2"

    mapping = {
        "timestamp-millis": "datetime",
        "timestamp-micros": "datetime2(6)",
        "date":             "date",
        "time-millis":      "time",
        "decimal":          f"decimal({precision_scale})",
        "uuid":             "uniqueidentifier",
        "boolean":          "bit",
        "long":             "bigint",
        "int":              "int",
        "float":            "float",
        "double":           "float(53)",
        "string":           "nvarchar(max)",
        "bytes":            "varbinary(max)",
    }

    return mapping.get(logical, "nvarchar(max)")


def convert_type(sql_type: str):
    primitive, logical = map_to_logical(sql_type)

    if logical is None:
        return {
            "input": sql_type,
            "primitive": None,
            "logical": None,
            "final": None,
            "status": "unknown"
        }

    final = map_to_final(sql_type, logical)

    return {
        "input": sql_type,
        "primitive": primitive,
        "logical": logical,
        "final": final,
        "status": "ok"
    }