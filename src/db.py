from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv
import psycopg


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def load_config() -> DbConfig:
    load_dotenv()
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    dbname = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    return DbConfig(host=host, port=port, dbname=dbname, user=user, password=password)


def connect(*, autocommit: bool = False) -> psycopg.Connection:
    cfg = load_config()
    conn = psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )
    conn.autocommit = autocommit
    return conn

