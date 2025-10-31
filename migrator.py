import pyodbc
import psycopg2
from psycopg2 import sql
from io import StringIO


def migrate_access_to_postgres(access_path, pg_params, schema, progress_callback=None):
    """
    Migra todas las tablas de una base Access hacia PostgreSQL, dentro de un esquema específico.
    Compatible con caracteres ANSI (Windows-1252 → UTF-8).
    """

    # --- Conexión Access ---
    access_conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={access_path};'
    )
    access_conn = pyodbc.connect(access_conn_str)
    access_cursor = access_conn.cursor()

    # --- Conexión PostgreSQL ---
    pg_conn = psycopg2.connect(**pg_params)
    pg_cursor = pg_conn.cursor()

    # Crear esquema si no existe
    pg_cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    pg_conn.commit()

    # Listar tablas de Access
    tables = [t.table_name for t in access_cursor.tables(tableType='TABLE')]

    for t_index, table in enumerate(tables, 1):
        if progress_callback:
            progress_callback(f"Migrando tabla {table}...", 0)

        # --- Obtener columnas ---
        columns_info = list(access_cursor.columns(table=table))
        cols = []
        for col in columns_info:
            name = col.column_name
            type_name = col.type_name
            if 'CHAR' in type_name or 'TEXT' in type_name:
                pg_type = 'TEXT'
            elif 'INT' in type_name:
                pg_type = 'INTEGER'
            elif 'DOUBLE' in type_name or 'NUMERIC' in type_name or 'DECIMAL' in type_name:
                pg_type = 'NUMERIC'
            elif 'DATE' in type_name or 'TIME' in type_name:
                pg_type = 'TIMESTAMP'
            else:
                pg_type = 'TEXT'
            cols.append((name, pg_type))

        # --- Crear tabla destino ---
        create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(', ').join(sql.SQL("{} {}").format(sql.Identifier(c[0]), sql.SQL(c[1])) for c in cols)
        )
        pg_cursor.execute(create_stmt)
        pg_conn.commit()

        # --- Extraer datos de Access ---
        col_names = [c[0] for c in cols]
        access_cursor.execute(f"SELECT * FROM [{table}]")
        rows = access_cursor.fetchall()
        total = len(rows)

        if total == 0:
            if progress_callback:
                progress_callback(f"{table}: sin datos", 100)
            continue

        # --- Convertir filas y cargar en buffer ---
        buffer = StringIO()
        for i, row in enumerate(rows, 1):
            values = []
            for v in row:
                if v is None:
                    values.append("\\N")
                else:
                    # 🔧 Conversión segura Windows-1252 → UTF-8
                    try:
                        if isinstance(v, bytes):
                            val = v.decode('windows-1252', errors='ignore')
                        else:
                            val = str(v).encode('windows-1252', errors='ignore').decode('utf-8', errors='ignore')
                    except Exception:
                        val = str(v)
                    val = val.replace("\n", " ").replace("\r", "")
                    values.append(val)

            buffer.write("\t".join(values) + "\n")

            if progress_callback and i % max(1, total // 50) == 0:
                progress_callback(f"{table} ({i}/{total})", int(i * 100 / total))

        buffer.seek(0)

        # --- Cargar con COPY FROM ---
        copy_sql = sql.SQL(
            "COPY {}.{} ({}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
        ).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, col_names))
        )

        pg_cursor.copy_expert(copy_sql.as_string(pg_conn), buffer)
        pg_conn.commit()

        if progress_callback:
            progress_callback(f"{table}: completada", 100)

    # --- Cierre de conexiones ---
    access_conn.close()
    pg_conn.close()

    if progress_callback:
        progress_callback("✅ Migración completada", 100)
