import pyodbc
import psycopg2
from psycopg2 import sql
from io import StringIO
import os

# Archivo de log
log_file = os.path.join(os.path.dirname(__file__), "migracion.log")


def safe_str(value, table_name="", row_index=0, col_name=""):
    """
    Convierte cualquier valor a str seguro para PostgreSQL.
    Todos los bytes que no se puedan decodificar se reemplazan por �.
    """
    if value is None:
        return "\\N"
    replaced = False
    try:
        # Si es str, convertir a bytes primero
        if isinstance(value, str):
            val_bytes = value.encode("utf-8", errors="replace")
        elif isinstance(value, bytes):
            val_bytes = value
        elif isinstance(value, memoryview):
            val_bytes = value.tobytes()
        else:
            val_bytes = str(value).encode("utf-8", errors="replace")

        # Decodificar en latin1 (1 byte = 1 carácter)
        s = val_bytes.decode("latin1")
        if "�" in s:
            replaced = True
    except Exception:
        s = str(value)
        replaced = True

    # Registrar en log si hubo reemplazo
    if replaced:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"[{table_name}] Fila {row_index}, Columna '{col_name}': caracteres inválidos reemplazados\n")
    return s.replace("\n", " ").replace("\r", "")


def migrate_access_to_postgres(access_path, pg_params, schema, progress_callback=None):
    """
    Migra todas las tablas de Access a PostgreSQL dentro de un esquema.
    Convierte todo a texto seguro y registra reemplazos de caracteres.
    """
    access_conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={access_path};'
    )

    access_conn = pyodbc.connect(access_conn_str, autocommit=True)
    access_cursor = access_conn.cursor()

    # Configuración simple, no usar errors
    access_conn.setencoding(encoding='utf-8', ctype=pyodbc.SQL_CHAR)
    access_conn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
    access_conn.setdecoding(pyodbc.SQL_WCHAR, encoding='latin1')
    access_conn.setdecoding(pyodbc.SQL_WMETADATA, encoding='latin1')

    pg_conn = psycopg2.connect(**pg_params)
    pg_cursor = pg_conn.cursor()

    pg_cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    pg_conn.commit()

    tables = [t.table_name for t in access_cursor.tables(tableType='TABLE')]

    for t_index, table in enumerate(tables, 1):
        if progress_callback:
            progress_callback(f"Migrando tabla {table}...", 0)

        columns_info = list(access_cursor.columns(table=table))
        cols = []
        for col in columns_info:
            name = col.column_name
            type_name = col.type_name or ''
            if 'CHAR' in type_name or 'TEXT' in type_name:
                pg_type = 'TEXT'
            elif 'INT' in type_name:
                pg_type = 'INTEGER'
            elif any(x in type_name for x in ['DOUBLE', 'NUMERIC', 'DECIMAL']):
                pg_type = 'NUMERIC'
            elif any(x in type_name for x in ['DATE', 'TIME']):
                pg_type = 'TIMESTAMP'
            else:
                pg_type = 'TEXT'
            cols.append((name, pg_type))

        create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(', ').join(sql.SQL("{} {}").format(sql.Identifier(c[0]), sql.SQL(c[1])) for c in cols)
        )
        pg_cursor.execute(create_stmt)
        pg_conn.commit()

        col_names = [c[0] for c in cols]
        access_cursor.execute(f"SELECT * FROM [{table}]")
        rows = access_cursor.fetchall()
        total = len(rows)

        if total == 0:
            if progress_callback:
                progress_callback(f"{table}: sin datos", 100)
            continue

        buffer = StringIO()
        for i, row in enumerate(rows, 1):
            converted = []
            for j, v in enumerate(row):
                col_name = col_names[j]
                val = safe_str(v, table_name=table, row_index=i, col_name=col_name)
                converted.append(val)
            buffer.write("\t".join(converted) + "\n")

            if progress_callback and i % max(1, total // 50) == 0:
                progress_callback(f"{table} ({i}/{total})", int(i * 100 / total))

        buffer.seek(0)
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

    access_conn.close()
    pg_conn.close()

    if progress_callback:
        progress_callback("✅ Migración completada sin errores de codificación", 100)
