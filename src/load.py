from __future__ import annotations

import csv
from pathlib import Path

from psycopg import sql

from db import connect


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MIGRATIONS_DIR = ROOT / "migrations"


TABLE_SPECS: dict[str, dict] = {
    "customers": {
        "path": DATA_DIR / "customers.csv",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS public.customers (
              customer_id INTEGER NOT NULL,
              full_name   TEXT NOT NULL,
              email       TEXT NOT NULL
            );
        """,
        "truncate_sql": "TRUNCATE TABLE public.customers;",
    },
    "products": {
        "path": DATA_DIR / "products.csv",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS public.products (
              product_id       INTEGER NOT NULL,
              sku              TEXT NOT NULL,
              product_name     TEXT NOT NULL,
              unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0)
            );
        """,
        "truncate_sql": "TRUNCATE TABLE public.products;",
    },
    "orders": {
        "path": DATA_DIR / "orders.csv",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS public.orders (
              order_id    INTEGER NOT NULL,
              customer_id INTEGER NOT NULL,
              order_date  DATE NOT NULL
            );
        """,
        "truncate_sql": "TRUNCATE TABLE public.orders;",
    },
    "order_items": {
        "path": DATA_DIR / "order_items.csv",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS public.order_items (
              order_id    INTEGER NOT NULL,
              line_number INTEGER NOT NULL,
              product_id  INTEGER NOT NULL,
              quantity    INTEGER NOT NULL CHECK (quantity > 0)
            );
        """,
        "truncate_sql": "TRUNCATE TABLE public.order_items;",
    },
}


def _ensure_dirs() -> None:
    if not DATA_DIR.exists():
        raise FileNotFoundError(f"Missing data directory: {DATA_DIR}")
    if not MIGRATIONS_DIR.exists():
        raise FileNotFoundError(f"Missing migrations directory: {MIGRATIONS_DIR}")


def _csv_columns(path: Path) -> list[str]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            raise ValueError(f"Empty CSV header: {path}")
        return [h.strip() for h in header]


def _copy_csv(conn, *, table: str, csv_path: Path) -> None:
    cols = _csv_columns(csv_path)
    ident_cols = [sql.Identifier(c) for c in cols]
    q = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(
        sql.Identifier("public", table),
        sql.SQL(", ").join(ident_cols),
    )
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        with conn.cursor() as cur:
            with cur.copy(q) as copy:
                for line in f:
                    copy.write(line)


def _run_migrations(conn) -> None:
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migration_files:
        return
    with conn.cursor() as cur:
        for p in migration_files:
            cur.execute(p.read_text(encoding="utf-8"))


def main() -> int:
    _ensure_dirs()
    with connect() as conn:
        with conn.cursor() as cur:
            for spec in TABLE_SPECS.values():
                cur.execute(spec["create_sql"])

            # Load in dependency-safe order (truncate child tables first).
            cur.execute(TABLE_SPECS["order_items"]["truncate_sql"])
            cur.execute(TABLE_SPECS["orders"]["truncate_sql"])
            cur.execute(TABLE_SPECS["products"]["truncate_sql"])
            cur.execute(TABLE_SPECS["customers"]["truncate_sql"])

        _copy_csv(conn, table="customers", csv_path=TABLE_SPECS["customers"]["path"])
        _copy_csv(conn, table="products", csv_path=TABLE_SPECS["products"]["path"])
        _copy_csv(conn, table="orders", csv_path=TABLE_SPECS["orders"]["path"])
        _copy_csv(conn, table="order_items", csv_path=TABLE_SPECS["order_items"]["path"])

        _run_migrations(conn)
        conn.commit()

    print("Load complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

