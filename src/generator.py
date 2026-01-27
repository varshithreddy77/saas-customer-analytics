from __future__ import annotations
import random
from typing import Optional
from datetime import datetime, timedelta, time, date
import json
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import JSONB

PIPELINE_NAME = "generate"

def get_last_run(engine: Engine) -> Optional[datetime]:
    with engine.connect() as conn:
        result = conn.execute(
            text(" SELECT last_run_at FROM raw.raw_etl_run_log WHERE pipeline = :p "),
            {"p": PIPELINE_NAME},
        )
        row = result.fetchone()
    return row[0] if row and row[0] else None

def set_last_run(engine: Engine, ts: datetime) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO raw.raw_etl_run_log (pipeline, last_run_at)
                VALUES (:p, :t)
                ON CONFLICT (pipeline) DO UPDATE SET last_run_at = EXCLUDED.last_run_at;
                """
            ),
            {"p": PIPELINE_NAME, "t": ts},
        )

def date_range(start_date: date, end_date:date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)

def month_starts_between(start_date: date, end_date: date):
    current = start_date.replace(day=1)
    last = end_date.replace(day=1)
    while current <= last:
        yield current
        current = (current.replace(day = 28) + timedelta(days=4)).replace(day=1)

def generate(
    engine: Engine,
    *,
    lookback_days: int = 7,
    sample_users: int = 5000,   # set 0 to use ALL users (can be heavy)
    seed: int = 42,
    force_rebuild: bool = False,
) -> dict[str, int]:
    """
    Generates realistic time-series raw data:
      - raw_events (daily product usage)
      - raw_invoices (monthly billing)
      - raw_tickets (support)
    Uses a watermark (raw_etl_run_log) so future runs add only new days.
    """
    rng = random.Random(seed)
    now = datetime.now()

    if force_rebuild:
        with engine.begin() as conn:
            conn.exec_driver_sql("TRUNCATE raw.raw_events, raw.raw_invoices, raw.raw_tickets;")
            conn.execute(text("DELETE FROM raw.raw_etl_run_log WHERE pipeline=:p"), {"p": PIPELINE_NAME})
        last_run = None
    else:
        last_run = get_last_run(engine)

    if last_run is None:
        start_dt = now - timedelta(days=lookback_days)
    else:
        # next day (avoid regenerating same day)
        start_dt = datetime.combine(last_run.date() + timedelta(days=1), time(0, 0, 0))

    end_dt = now

    # --- Load base data
    users = pd.read_sql("SELECT user_id, created_at FROM raw.raw_users", engine)
    attrs = pd.read_sql(
        "SELECT user_id, usage_score, base_mrr, nps_score FROM raw.raw_user_attributes",
        engine,
    )
    subs = pd.read_sql(
        """
        SELECT s.subscription_id, s.user_id, s.plan_id, s.start_at, s.end_at, s.status,
               p.price_usd
        FROM raw.raw_subscriptions s
        JOIN raw.raw_plans p ON p.plan_id = s.plan_id
        """,
        engine,
    )

    u = users.merge(attrs, on="user_id", how="left").fillna(
        {"usage_score": 50, "nps_score": 7, "base_mrr": 0}
    )

    # Optional sampling (for speed on laptops)
    if sample_users and sample_users > 0 and sample_users < len(u):
        u = u.sample(n=sample_users, random_state=seed).reset_index(drop=True)

        # Keep only subs for sampled users
        keep_ids = set(u["user_id"].tolist())
        subs = subs[subs["user_id"].isin(keep_ids)].reset_index(drop=True)

    start_d = start_dt.date()
    end_d = end_dt.date()

    # EVENTS
    event_names = ["login", "feature_used", "dashboard_view", "export_report", "settings_update"]
    features = ["automation", "crm_sync", "analytics", "billing", "alerts"]

    event_rows: list[dict] = []
    for d in date_range(start_d, end_d):
        base_day = datetime.combine(d, time(0, 0, 0))
        for _, r in u.iterrows():
            usage = float(r["usage_score"])
            # usage 0..100 => approx 0..4 events/day (capped)
            expected = 0.2 + (usage / 100.0) * 3.8
            n_events = min(6, int(rng.random() * expected * 2.0))

            for i in range(n_events):
                ev_time = base_day + timedelta(minutes=rng.randint(0, 1439))
                ev_name = rng.choice(event_names)
                ev_id = f"evt_{r['user_id']}_{ev_time.strftime('%Y%m%d%H%M')}_{ev_name}_{i}"
                props = {"source": "sim", "usage_score": int(usage)}
                if ev_name == "feature_used":
                    props["feature"] = rng.choice(features)

                event_rows.append(
                    {
                        "event_id": ev_id,
                        "user_id": r["user_id"],
                        "event_time": ev_time,
                        "event_name": ev_name,
                        "properties_json": props,
                    }
                )

    events_df = pd.DataFrame(event_rows)

    # INVOICES (monthly)
    invoice_rows: list[dict] = []
    # Build quick lookup for usage + nps
    u_lookup = u.set_index("user_id")[["usage_score", "nps_score"]].to_dict("index")

    for _, s in subs.iterrows():
        sub_start = pd.to_datetime(s["start_at"]).date()
        sub_end = (
            pd.to_datetime(s["end_at"]).date()
            if pd.notna(s["end_at"])
            else end_d
        )

        window_start = max(sub_start, start_d)
        window_end = min(sub_end, end_d)

        for month_start in month_starts_between(window_start, window_end):
            issued_at = datetime.combine(month_start, time(9, 0, 0))
            inv_id = f"inv_{s['subscription_id']}_{month_start.strftime('%Y%m')}"
            amount = float(s["price_usd"])

            info = u_lookup.get(s["user_id"], {"usage_score": 50, "nps_score": 7})
            usage = float(info["usage_score"])
            nps = float(info["nps_score"])

            # fail probability rises when usage or nps is low
            fail_prob = 0.05 + (max(0, 50 - usage) / 200.0) + (max(0, 7 - nps) / 20.0)
            failed = rng.random() < min(0.35, fail_prob)

            if failed:
                paid_at = None
                failed_at = issued_at + timedelta(hours=rng.randint(2, 48))
                reason = rng.choice(["expired_card", "insufficient_funds", "bank_declined"])
            else:
                paid_at = issued_at + timedelta(hours=rng.randint(1, 24))
                failed_at = None
                reason = None

            invoice_rows.append(
                {
                    "invoice_id": inv_id,
                    "subscription_id": s["subscription_id"],
                    "amount_usd": amount,
                    "issued_at": issued_at,
                    "paid_at": paid_at,
                    "failed_at": failed_at,
                    "failure_reason": reason,
                }
            )

    invoices_df = pd.DataFrame(invoice_rows)

    # TICKETS (daily)
    ticket_rows: list[dict] = []
    categories = ["billing", "bug", "how_to", "performance"]

    for d in date_range(start_d, end_d):
        base_day = datetime.combine(d, time(0, 0, 0))
        for _, r in u.iterrows():
            usage = float(r["usage_score"])
            nps = float(r["nps_score"])

            # baseline tiny chance + bumps for low usage/nps
            p = 0.002 + (max(0, 40 - usage) / 20000.0) + (max(0, 6 - nps) / 5000.0)
            if rng.random() < min(0.02, p):
                created_at = base_day + timedelta(minutes=rng.randint(0, 1439))
                resolved_at = created_at + timedelta(hours=rng.randint(2, 72))
                csat = max(1, min(5, int(round((nps / 2) + rng.choice([-1, 0, 0, 1])))))
                tkt_id = f"tkt_{r['user_id']}_{created_at.strftime('%Y%m%d%H%M')}"

                ticket_rows.append(
                    {
                        "ticket_id": tkt_id,
                        "user_id": r["user_id"],
                        "created_at": created_at,
                        "category": rng.choice(categories),
                        "resolved_at": resolved_at,
                        "csat": csat,
                    }
                )

    tickets_df = pd.DataFrame(ticket_rows)

    # --- Load to Postgres (append)
    inserted = {"raw_events": 0, "raw_invoices": 0, "raw_tickets": 0}

    if len(events_df) > 0:
        events_df["properties_json"] = events_df["properties_json"].apply(json.dumps)
        events_df.to_sql(
            "raw_events",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi",
            dtype={"properties_json": JSONB()},
        )
        inserted["raw_events"] = len(events_df)

    if len(invoices_df) > 0:
        invoices_df.to_sql(
            "raw_invoices",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi",
        )
        inserted["raw_invoices"] = len(invoices_df)

    if len(tickets_df) > 0:
        tickets_df.to_sql(
            "raw_tickets",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi",
        )
        inserted["raw_tickets"] = len(tickets_df)

    set_last_run(engine, now)
    return inserted