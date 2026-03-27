from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from db import connect


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"


@dataclass(frozen=True)
class CheckFailure:
    message: str


def _csv_row_count(path: Path) -> int:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return 0
        return sum(1 for _ in reader)


def _db_scalar(conn, query: str, params: tuple = ()) -> int:
    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        assert row is not None
        return int(row[0])


def _validate_row_counts(conn) -> list[CheckFailure]:
    failures: list[CheckFailure] = []
    mapping = {
        "customers": DATA_DIR / "customers.csv",
        "products": DATA_DIR / "products.csv",
        "orders": DATA_DIR / "orders.csv",
        "order_items": DATA_DIR / "order_items.csv",
    }
    for table, path in mapping.items():
        expected = _csv_row_count(path)
        actual = _db_scalar(conn, f"SELECT COUNT(*) FROM public.{table};")
        if expected != actual:
            failures.append(
                CheckFailure(
                    f"Row count mismatch for {table}: csv={expected} db={actual}"
                )
            )
    return failures


def _validate_nulls(conn) -> list[CheckFailure]:
    failures: list[CheckFailure] = []
    checks = [
        ("customers", "customer_id"),
        ("customers", "full_name"),
        ("customers", "email"),
        ("products", "product_id"),
        ("products", "sku"),
        ("products", "product_name"),
        ("products", "unit_price_cents"),
        ("orders", "order_id"),
        ("orders", "customer_id"),
        ("orders", "order_date"),
        ("order_items", "order_id"),
        ("order_items", "line_number"),
        ("order_items", "product_id"),
        ("order_items", "quantity"),
    ]
    for table, col in checks:
        n = _db_scalar(
            conn,
            f"SELECT COUNT(*) FROM public.{table} WHERE {col} IS NULL;",
        )
        if n != 0:
            failures.append(CheckFailure(f"Null check failed: {table}.{col} has {n} NULLs"))
    return failures


def _validate_foreign_keys(conn) -> list[CheckFailure]:
    failures: list[CheckFailure] = []

    orphan_orders = _db_scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM public.orders o
        LEFT JOIN public.customers c ON c.customer_id = o.customer_id
        WHERE c.customer_id IS NULL;
        """,
    )
    if orphan_orders:
        failures.append(CheckFailure(f"FK check failed: orders.customer_id has {orphan_orders} orphans"))

    orphan_items_orders = _db_scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM public.order_items oi
        LEFT JOIN public.orders o ON o.order_id = oi.order_id
        WHERE o.order_id IS NULL;
        """,
    )
    if orphan_items_orders:
        failures.append(CheckFailure(f"FK check failed: order_items.order_id has {orphan_items_orders} orphans"))

    orphan_items_products = _db_scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM public.order_items oi
        LEFT JOIN public.products p ON p.product_id = oi.product_id
        WHERE p.product_id IS NULL;
        """,
    )
    if orphan_items_products:
        failures.append(CheckFailure(f"FK check failed: order_items.product_id has {orphan_items_products} orphans"))

    return failures


def main() -> int:
    failures: list[CheckFailure] = []
    with connect() as conn:
        failures.extend(_validate_row_counts(conn))
        failures.extend(_validate_nulls(conn))
        failures.extend(_validate_foreign_keys(conn))

    if failures:
        print("Validation failed:")
        for f in failures:
            print(f"- {f.message}")
        return 1

    print("Validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

