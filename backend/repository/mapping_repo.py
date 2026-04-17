from backend.config.db import get_connection, release_connection


class MappingRepository:
    def get_all(self) -> dict:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT source_type, raw_type, logical_type, final_type
                FROM datatype_mapping
            """)
            rows = cur.fetchall()
            cur.close()

            mapping = {}
            for source, raw, logical, final in rows:
                mapping[source] = {
                    "raw": raw,
                    "logical": logical,
                    "final": final
                }

            return mapping

        finally:
            release_connection(conn)