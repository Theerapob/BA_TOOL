from backend.config.db import get_connection, release_connection


class MappingRepository:
    def get_all(self) -> dict:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT sql_type, raw_type, logical_type, target_type AS final_type
                    FROM sql_type_mapping
                """)
                rows = cur.fetchall()

            mapping = {}
            for source, raw, logical, final in rows:
                mapping[source.lower()] = {
                    "raw": raw,
                    "logical": logical,
                    "final": final
                }

            return mapping

        finally:
            release_connection(conn)