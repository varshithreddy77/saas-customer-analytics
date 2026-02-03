from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class BIReport:
    user_daily: int
    subscription_monthly: int
    kpi_daily: int
    cohort_retention: int


def fetch_bi_report(engine: Engine, *, rebuild: bool = True, days_back: int = 90) -> BIReport:
    """
    Step 4: Build BI-friendly tables (bi_report schema)

    Tables:
    - bi_report.user_daily                  (last N days only; can be big)
    - bi_report.subscription_monthly
    - bi_report.kpi_daily
    - bi_report.cohort_retention
    """

    statements: list[str] = []

    # Create Schema
    statements.append("CREATE SCHEMA IF NOT EXISTS bi_report;")

    if rebuild:
        statements += [
            "DROP TABLE IF EXISTS bi_report.user_daily;",
            "DROP TABLE IF EXISTS bi_report.subscription_monthly;",
            "DROP TABLE IF EXISTS bi_report.kpi_daily;",
            "DROP TABLE IF EXISTS bi_report.cohort_retention;",
        ]

    # 1) bi_report.user_daily (FILTER dates first, then cross join)
    statements.append(
        """
        CREATE TABLE bi_report.user_daily AS
        WITH dd AS (
          SELECT date_day
          FROM analytics.dim_date
          WHERE date_day >= (CURRENT_DATE - make_interval(days => :days_back))
        )
        SELECT
          dd.date_day,
          u.user_id,
          u.industry,
          u.region,
          u.sales_rep,
          u.signup_date,
          u.usage_score,
          u.nps_score,
          u.base_mrr,

          COALESCE(a.event_count, 0) AS event_count,
          COALESCE(a.active_flag, 0) AS active_flag,
          COALESCE(a.feature_used_count, 0) AS feature_used_count,

          COALESCE(s.tickets_created, 0) AS tickets_created,
          s.avg_resolution_hours,
          s.avg_csat
        FROM dd
        CROSS JOIN analytics.dim_user u
        LEFT JOIN analytics.fact_daily_activity a
          ON a.activity_date = dd.date_day
         AND a.user_id = u.user_id
        LEFT JOIN analytics.fact_daily_support s
          ON s.support_date = dd.date_day
         AND s.user_id = u.user_id
        ;
        """
    )

    statements += [
        "CREATE INDEX IF NOT EXISTS idx_bi_user_daily_day ON bi_report.user_daily(date_day);",
        "CREATE INDEX IF NOT EXISTS idx_bi_user_daily_user ON bi_report.user_daily(user_id);",
    ]

    # 2) bi_report.subscription_monthly
    statements.append(
        """
        CREATE TABLE bi_report.subscription_monthly AS
        SELECT
          b.month_start,
          b.subscription_id,
          b.user_id,
          b.plan_id,
          p.plan_name,
          p.price_usd AS plan_price_usd,

          b.mrr_usd,
          b.invoices_issued,
          b.invoices_paid,
          b.invoices_failed,
          b.amount_paid_usd,
          b.amount_failed_usd,

          CASE WHEN c.subscription_id IS NOT NULL THEN 1 ELSE 0 END AS churned_flag,
          c.churn_date
        FROM analytics.fact_monthly_billing b
        JOIN analytics.dim_plan p
          ON p.plan_id = b.plan_id
        LEFT JOIN analytics.fact_churn c
          ON c.subscription_id = b.subscription_id
         AND date_trunc('month', c.churn_date)::date = b.month_start
        ;
        """
    )

    statements += [
        "CREATE INDEX IF NOT EXISTS idx_bi_sub_month_user ON bi_report.subscription_monthly(user_id);",
        "CREATE INDEX IF NOT EXISTS idx_bi_sub_month_month ON bi_report.subscription_monthly(month_start);",
    ]

    # 3) bi_report.kpi_daily
    statements.append(
        """
        CREATE TABLE bi_report.kpi_daily AS
        SELECT * FROM analytics.kpi_daily;
        """
    )
    statements.append("CREATE INDEX IF NOT EXISTS idx_bi_kpi_day ON bi_report.kpi_daily(date_day);")

    # 4) bi_report.cohort_retention
    statements.append(
        """
        CREATE TABLE bi_report.cohort_retention AS
        SELECT * FROM analytics.retention_cohort_monthly;
        """
    )
    statements.append(
        "CREATE INDEX IF NOT EXISTS idx_bi_cohort_month ON bi_report.cohort_retention(cohort_month);"
    )

    # Execute everything (IMPORTANT: use begin() so DDL commits)
    with engine.begin() as conn:
        for stmt in statements:
            if ":days_back" in stmt:
                conn.execute(text(stmt), {"days_back": int(days_back)})
            else:
                conn.execute(text(stmt))

        c1 = conn.execute(text("SELECT COUNT(*) FROM bi_report.user_daily;")).scalar_one()
        c2 = conn.execute(text("SELECT COUNT(*) FROM bi_report.subscription_monthly;")).scalar_one()
        c3 = conn.execute(text("SELECT COUNT(*) FROM bi_report.kpi_daily;")).scalar_one()
        c4 = conn.execute(text("SELECT COUNT(*) FROM bi_report.cohort_retention;")).scalar_one()

    return BIReport(
        user_daily=int(c1),
        subscription_monthly=int(c2),
        kpi_daily=int(c3),
        cohort_retention=int(c4),
    )


def export_bireport_to_csv(
    engine: Engine,
    *,
    out_dir: str = "outputs/powerbi",
    include_user_daily: bool = False,
    user_daily_limit: Optional[int] = None,
) -> dict[str, str]:
    """
    Export bi_report tables to CSV.

    NOTE: user_daily can be huge (millions). Default include_user_daily=False.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    exports: dict[str, str] = {
        "bi_report_subscription_monthly": "SELECT * FROM bi_report.subscription_monthly",
        "bi_report_kpi_daily": "SELECT * FROM bi_report.kpi_daily",
        "bi_report_cohort_retention": "SELECT * FROM bi_report.cohort_retention",
    }

    if include_user_daily:
        sql = "SELECT * FROM bi_report.user_daily"
        if user_daily_limit and user_daily_limit > 0:
            sql += f" LIMIT {int(user_daily_limit)}"
        exports["bi_report_user_daily"] = sql

    paths: dict[str, str] = {}

    for name, sql in exports.items():
        df = pd.read_sql(sql, engine)
        path = out / f"{name}.csv"
        df.to_csv(path, index=False)
        paths[name] = str(path)

    return paths
