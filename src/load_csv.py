import pandas as pd
from sqlalchemy.engine import Engine

def _plan_id(plan_type: str) -> str:
    return f"{str(plan_type).strip().lower()}_m"

def load_csv_to_raw(engine: Engine, csv_path: str, force_reload: bool) -> dict[str, int]:
    df = pd.read_csv(csv_path)

    expected = {
        "customer_id","industry","region","signup_date","plan_type","monthly_revenue",
        "churned","renewal_date","sales_rep","usage_score","nps_score"
    }
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing columns: {sorted(missing)}")

    df["signup_date"] = pd.to_datetime(df["signup_date"]).dt.date
    df["renewal_date"] = pd.to_datetime(df["renewal_date"], errors="coerce").dt.date

    if force_reload:
        with engine.begin() as conn:
            conn.exec_driver_sql(
                "TRUNCATE raw.raw_subscriptions, raw.raw_nps, raw.raw_users, raw.raw_plans "
                "RESTART IDENTITY CASCADE;"
            )

    with engine.connect() as conn:
        existing = conn.exec_driver_sql("SELECT COUNT(*) FROM raw.raw_users;").scalar() or 0
    if existing > 0 and not force_reload:
        return {"raw_users": 0, "raw_plans": 0, "raw_subscriptions": 0, "raw_nps": 0}

    users = df[["customer_id","signup_date","industry","region","sales_rep"]].copy()
    users.rename(columns={"customer_id":"user_id", "signup_date":"created_at"}, inplace=True)

    plans = (
        df.groupby("plan_type", as_index=False)["monthly_revenue"]
          .median()
          .rename(columns={"monthly_revenue":"price_usd"})
    )
    plans["plan_id"] = plans["plan_type"].apply(_plan_id)
    plans.rename(columns={"plan_type":"plan_name"}, inplace=True)
    plans["billing_period"] = "monthly"
    plans = plans[["plan_id","plan_name","price_usd","billing_period"]]

    subs = df[["customer_id","plan_type","signup_date","renewal_date","churned"]].copy()
    subs["subscription_id"] = subs["customer_id"].apply(lambda x: f"sub_{x}")
    subs["user_id"] = subs["customer_id"]
    subs["plan_id"] = subs["plan_type"].apply(_plan_id)
    subs["start_at"] = subs["signup_date"]
    subs["status"] = subs["churned"].apply(lambda x: "canceled" if int(x) == 1 else "active")
    subs["end_at"] = subs.apply(lambda r: r["renewal_date"] if r["status"] == "canceled" else None, axis=1)
    subs["cancel_reason"] = None
    subs = subs[["subscription_id","user_id","plan_id","start_at","end_at","status","cancel_reason"]]

    nps = df[["customer_id","renewal_date","signup_date","nps_score"]].copy()
    nps["nps_id"] = nps["customer_id"].apply(lambda x: f"nps_{x}")
    nps["user_id"] = nps["customer_id"]
    nps["survey_at"] = nps["renewal_date"].fillna(nps["signup_date"])
    nps = nps[["nps_id","user_id","survey_at","nps_score"]]

    plans.to_sql("raw_plans", engine, schema="raw", if_exists="append", index=False, method="multi", chunksize=5000)
    users.to_sql("raw_users", engine, schema="raw", if_exists="append", index=False, method="multi", chunksize=5000)
    subs.to_sql("raw_subscriptions", engine, schema="raw", if_exists="append", index=False, method="multi", chunksize=5000)
    nps.to_sql("raw_nps", engine, schema="raw", if_exists="append", index=False, method="multi", chunksize=5000)

    return {
        "raw_users": len(users),
        "raw_plans": len(plans),
        "raw_subscriptions": len(subs),
        "raw_nps": len(nps),
    }
