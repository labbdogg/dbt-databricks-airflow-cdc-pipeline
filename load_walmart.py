"""
Creates the `raw` schema in your Postgres database, creates the Walmart tables
from the DDL file, and loads each CSV into its table with COPY.

Setup:   uv add "psycopg[binary]"
         set DATABASE_URL in this terminal first
Run:     uv run python load_walmart.py <ddl_file.sql> <data_folder> [--reset]

--reset  drops the `raw` schema first, so you can re-run from scratch.

Expects one CSV per table, named after the table (customers.csv, stores.csv,
products.csv, employees.csv, orders.csv, order_items.csv) with a header row.
"""
import csv
import os
import sys
from pathlib import Path

import psycopg

SCHEMA = "raw"
TABLES = ["customers", "stores", "products", "employees", "orders", "order_items"]


def main(ddl_path, data_dir, reset=False):
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("DATABASE_URL is not set in this terminal.")

    ddl = Path(ddl_path).read_text(encoding="utf-8-sig")
    data_dir = Path(data_dir)

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            if reset:
                cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
                print(f"dropped schema {SCHEMA}")

            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            cur.execute(f"SET search_path TO {SCHEMA}")

            # run the DDL one statement at a time
            for statement in ddl.split(";"):
                if statement.strip():
                    cur.execute(statement)
            print(f"created tables in schema {SCHEMA}")

            for table in TABLES:
                csv_path = data_dir / f"{table}.csv"
                if not csv_path.exists():
                    print(f"skipped {table}: {csv_path} not found")
                    continue

                # use the CSV header as the column list, so column order doesn't matter
                with csv_path.open(newline="", encoding="utf-8-sig") as f:
                    columns = ", ".join(next(csv.reader(f)))

                copy_sql = f"COPY {table} ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true)"
                with csv_path.open("rb") as f, cur.copy(copy_sql) as copy:
                    while chunk := f.read(65536):
                        copy.write(chunk)

                cur.execute(f"SELECT count(*) FROM {table}")
                print(f"loaded {table}: {cur.fetchone()[0]} rows")

    print("done")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(args) != 2:
        sys.exit(__doc__)
    main(args[0], args[1], reset="--reset" in sys.argv)