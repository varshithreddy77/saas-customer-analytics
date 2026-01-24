from sqlalchemy import text
from sqlalchemy.engine import Engine

import pandas as pd

def create_table(engine: Engine) -> None:
    ddl = """
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.raw_user_attributes (
      user_id     TEXT PRIMARY KEY,
      usage_score INTEGER,
      base_mrr    NUMERIC(10,2),
      nps_score   INTEGER,
      updated_at  TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS raw.raw_events (
      event_id        TEXT PRIMARY KEY,
      user_id         TEXT NOT NULL REFERENCES raw.raw_users(user_id),
      event_time      TIMESTAMP NOT NULL,
      event_name      TEXT NOT NULL,
      properties_json JSONB
    );

    CREATE TABLE IF NOT EXISTS raw.raw_invoices (
      invoice_id      TEXT PRIMARY KEY,
      subscription_id TEXT NOT NULL REFERENCES raw.raw_subscriptions(subscription_id),
      amount_usd      NUMERIC(10,2) NOT NULL,
      issued_at       TIMESTAMP NOT NULL,
      paid_at         TIMESTAMP,
      failed_at       TIMESTAMP,
      failure_reason  TEXT
    );

    CREATE TABLE IF NOT EXISTS raw.raw_tickets (
      ticket_id   TEXT PRIMARY KEY,
      user_id     TEXT NOT NULL REFERENCES raw.raw_users(user_id),
      created_at  TIMESTAMP NOT NULL,
      category    TEXT,
      resolved_at TIMESTAMP,
      csat        INTEGER
    );

    CREATE TABLE IF NOT EXISTS raw.raw_etl_run_log (
      pipeline    TEXT PRIMARY KEY,
      last_run_at TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_events_user_time ON raw.raw_events(user_id, event_time);
    CREATE INDEX IF NOT EXISTS idx_invoices_issued ON raw.raw_invoices(issued_at);
    CREATE INDEX IF NOT EXISTS idx_tickets_user_time ON raw.raw_tickets(user_id, created_at);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def load_user_attributes(engine: Engine, csv_path: str) -> int:
    df = pd.read_csv(csv_path)

    keep = df[["customer_id", "usage_score", "monthly_revenue", "nps_score"]].copy()
    keep.rename(columns={
        "customer_id": "user_id",
        "monthly_revenue": "base_mrr"
    }, inplace=True)

    with engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE raw.raw_user_attributes;")

    keep.to_sql(
        "raw_user_attributes",
        engine,
        schema="raw",
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi",
    )
    return len(keep)