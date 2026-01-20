import os
import re
import shutil
import hashlib
from pathlib import Path

from sqlalchemy import create_engine, text
#This script saves all routines from the chosen schema into the separate folder

connection_string = (
    "snowflake://<USER>:<PASSWORD>@<ACCOUNT>/<DATABASE>/<SCHEMA>?warehouse=<WH>&role=<ROLE>"
)

TARGET_DIR = r"C:\Users\ADMIN\MyProjects\Snowflake_task\save_from_snowflake"
DB_NAME = "BASE_SCHEMA" # your Database name
SCHEMA_NAME = "AIR_TEST" # your schema name


def normalize_filename(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^A-Za-z0-9_().,-]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_") or "proc"


def ensure_clean_dir(path: str):
    try:
        shutil.rmtree(path)
        print(f"Directory '{path}' removed")
    except FileNotFoundError:
        pass
    os.makedirs(path, exist_ok=True)
    print(f"Directory '{path}' created")


def unique_path(dir_path: str, base_name: str, ext: str = ".sql") -> str:
    p = Path(dir_path) / f"{base_name}{ext}"
    if not p.exists():
        return str(p)

    for i in range(2, 10_000):
        pi = Path(dir_path) / f"{base_name}_{i}{ext}"
        if not pi.exists():
            return str(pi)

    h = hashlib.md5(base_name.encode("utf-8")).hexdigest()[:10]
    return str(Path(dir_path) / f"{base_name}_{h}{ext}")


def fetch_procedure_definitions(engine, db: str, schema: str):
    q = text(f"""
        SELECT
            procedure_catalog,
            procedure_schema,
            procedure_name,
            procedure_definition
        FROM {db}.information_schema.procedures
        --WHERE procedure_schema = :schema
        ORDER BY procedure_name, argument_signature
    """) # use there the SQL script that extracts all routines from DB
    with engine.connect() as conn:
        return conn.execute(q, {"schema": schema.upper()}).fetchall()


def save_all_procedures_from_definition():
    engine = create_engine(connection_string)
    ensure_clean_dir(TARGET_DIR)

    rows = fetch_procedure_definitions(engine, DB_NAME, SCHEMA_NAME)
    print(f"Found procedure rows (incl overloads): {len(rows)}")

    saved = 0
    skipped = 0

    for catalog, schema, proc_name, proc_def in rows:
        proc_name = (proc_name or "").strip()
        proc_def = proc_def or ""

        if not proc_name:
            skipped += 1
            continue

        if not proc_def.strip():
            print(f"SKIP: {catalog}.{schema}.{proc_name} (empty procedure_definition)")
            skipped += 1
            continue

        file_base = normalize_filename(proc_name)
        file_path = unique_path(TARGET_DIR, file_base, ".sql")

        # procedure_definition — это тело/дефиниция, не полный CREATE
        # для удобства добавим простой хедер
        header = f"-- {catalog}.{schema}.{proc_name}\n\n"
        content = header + proc_def.replace("\r\n", "\n")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

        saved += 1

    print(f"Done. Saved: {saved}, Skipped: {skipped}")


if __name__ == "__main__":
    save_all_procedures_from_definition()
