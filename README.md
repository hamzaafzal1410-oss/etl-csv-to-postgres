# ETL CSV → Postgres (Docker)

Loads CSV files into Postgres (creating tables if missing), then validates row counts, nulls, and foreign-key consistency.

## Prereqs

- Docker Desktop
- Python 3.10+

## Setup

Create your `.env`:

```bash
Copy-Item .env.example .env
```

Start Postgres:

```bash
docker compose up -d
```

Create a venv + install deps:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Run

Load CSVs + run migration:

```bash
python src/load.py
```

Validate:

```bash
python src/validate.py
```

Done when both commands succeed.

## What gets created

- **CSVs**: `data/customers.csv`, `data/products.csv`, `data/orders.csv`, `data/order_items.csv`
- **Migration**: `migrations/001_constraints_indexes.sql`
- **Tables**: `customers`, `products`, `orders`, `order_items`

## Screenshot (tables)

![DB tables](docs/db_tables.svg)
