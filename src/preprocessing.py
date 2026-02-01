from __future__ import annotations

from dataclasses import dataclass
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class schema:
    dim_user: int
    dim_plan: int
    dim_date: int
    fact_daily_activity: int
    fact_daily_support: int
    fact_monthly_billing: int
    fact_churn: int
    kpi_daily: int
    retention_cohort_monthly: int


def create_schema(engine: Engine, *, rebuild: bool = True) -> schema:
    statements: list[str] = []

    # Schemas
    statements += [
        "CREATE SCHEMA IF NOT EXISTS stg;",
        "CREATE SCHEMA IF NOT EXISTS analytics;",
    ]

    # -------------------------
    # Staging views
    # -------------------------
    statements += [
        """
        CREATE OR REPLACE VIEW stg.users AS
        SELECT
          u.user_id,
          u.created_at::date AS signup_date,
          u.industry,
          u.region,
          u.sales_rep,
          a.usage_score,
          a.nps_score,
          a.base_mrr
        FROM raw.raw_users u
        LEFT JOIN raw.raw_user_attributes a
          ON a.user_id = u.user_id;
        """,
        """
        CREATE OR REPLACE VIEW stg.plans AS
        SELECT
          plan_id,
          plan_name,
          price_usd::numeric(10,2) AS price_usd,
          billing_period
        FROM raw.raw_plans;
        """,
        """
        CREATE OR REPLACE VIEW stg.subscriptions AS
        SELECT
          s.subscription_id,
          s.user_id,
          s.plan_id,
          s.start_at::date AS start_date,
          s.end_at::date AS end_date,
          s.status,
          p.price_usd::numeric(10,2) AS plan_price_usd
        FROM raw.raw_subscriptions s
        JOIN stg.plans p
          ON p.plan_id = s.plan_id;
        """,
        """
        CREATE OR REPLACE VIEW stg.events AS
        SELECT
          e.event_id,
          e.user_id,
          e.event_time,
          e.event_time::date AS event_date,
          e.event_name,
          (e.properties_json->>'feature') AS feature
        FROM raw.raw_events e;
        """,
        """
        CREATE OR REPLACE VIEW stg.invoices AS
        SELECT
          i.invoice_id,
          i.subscription_id,
          i.amount_usd::numeric(10,2) AS amount_usd,
          i.issued_at,
          i.issued_at::date AS issued_date,
          date_trunc('month', i.issued_at)::date AS month_start,
          i.paid_at,
          i.failed_at,
          CASE
            WHEN i.paid_at IS NOT NULL THEN 'paid'
            WHEN i.failed_at IS NOT NULL THEN 'failed'
            ELSE 'open'
          END AS invoice_status,
          i.failure_reason
        FROM raw.raw_invoices i;
        """,
        """
        CREATE OR REPLACE VIEW stg.tickets AS
        SELECT
          t.ticket_id,
          t.user_id,
          t.created_at,
          t.created_at::date AS created_date,
          t.category,
          t.resolved_at,
          CASE
            WHEN t.resolved_at IS NOT NULL
              THEN EXTRACT(EPOCH FROM (t.resolved_at - t.created_at)) / 3600.0
            ELSE NULL
          END AS resolution_hours,
          t.csat
        FROM raw.raw_tickets t;
        """,
    ]

    # -------------------------
    # Analytics tables
    # -------------------------
    statements += [
        # Dimensions
        """
        CREATE TABLE IF NOT EXISTS analytics.dim_user(
          user_id TEXT PRIMARY KEY,
          signup_date DATE NOT NULL,
          industry TEXT,
          region TEXT,
          sales_rep TEXT,
          usage_score INTEGER,
          nps_score INTEGER,
          base_mrr NUMERIC(10,2)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analytics.dim_plan(
          plan_id TEXT PRIMARY KEY,
          plan_name TEXT NOT NULL,
          price_usd NUMERIC(10,2) NOT NULL,
          billing_period TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analytics.dim_date(
          date_day DATE PRIMARY KEY,
          year INT NOT NULL,
          month INT NOT NULL,
          month_start DATE NOT NULL,
          week INT NOT NULL,
          day_of_week INT NOT NULL
        );
        """,
        # Facts
        """
        CREATE TABLE IF NOT EXISTS analytics.fact_daily_activity (
          activity_date DATE NOT NULL,
          user_id TEXT NOT NULL,
          event_count INT NOT NULL,
          active_flag INT NOT NULL,
          feature_used_count INT NOT NULL,
          PRIMARY KEY (activity_date, user_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analytics.fact_daily_support(
          support_date DATE NOT NULL,
          user_id TEXT NOT NULL,
          tickets_created INT NOT NULL,
          avg_resolution_hours DOUBLE PRECISION,
          avg_csat DOUBLE PRECISION,
          PRIMARY KEY (support_date, user_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analytics.fact_monthly_billing(
          month_start DATE NOT NULL,
          subscription_id TEXT NOT NULL,
          user_id TEXT NOT NULL,
          plan_id TEXT NOT NULL,
          mrr_usd NUMERIC(10,2) NOT NULL,
          invoices_issued INT NOT NULL,
          invoices_paid INT NOT NULL,
          invoices_failed INT NOT NULL,
          amount_paid_usd NUMERIC(10,2) NOT NULL,
          amount_failed_usd NUMERIC(10,2) NOT NULL,
          PRIMARY KEY (month_start, subscription_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analytics.fact_churn(
          churn_date DATE NOT NULL,
          subscription_id TEXT NOT NULL,
          user_id TEXT NOT NULL,
          plan_id TEXT NOT NULL,
          PRIMARY KEY (churn_date, subscription_id)
        );
        """,
        # KPIs
        """
        CREATE TABLE IF NOT EXISTS analytics.kpi_daily(
          date_day DATE PRIMARY KEY,
          dau INT NOT NULL,
          active_customers INT NOT NULL,
          new_signups INT NOT NULL,
          churned_users INT NOT NULL,
          churn_rate DOUBLE PRECISION,
          tickets_created INT NOT NULL,
          invoices_failed INT NOT NULL,
          paid_revenue_usd NUMERIC(10,2) NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analytics.retention_cohort_monthly(
          cohort_month DATE NOT NULL,
          active_month DATE NOT NULL,
          months_since_signup INT NOT NULL,
          cohort_size INT NOT NULL,
          active_users INT NOT NULL,
          retention_rate DOUBLE PRECISION NOT NULL,
          PRIMARY KEY (cohort_month, active_month)
        );
        """,
    ]

    statements += [
        "CREATE INDEX IF NOT EXISTS idx_fda_user ON analytics.fact_daily_activity(user_id);",
        "CREATE INDEX IF NOT EXISTS idx_fmr_user ON analytics.fact_monthly_billing(user_id);",
        "CREATE INDEX IF NOT EXISTS idx_kpi_day ON analytics.kpi_daily(date_day);",
    ]

    # Rebuild data
    if rebuild:
        statements += [
            "TRUNCATE TABLE analytics.dim_user;",
            "TRUNCATE TABLE analytics.dim_plan;",
            "TRUNCATE TABLE analytics.dim_date;",
            "TRUNCATE TABLE analytics.fact_daily_activity;",
            "TRUNCATE TABLE analytics.fact_daily_support;",
            "TRUNCATE TABLE analytics.fact_monthly_billing;",
            "TRUNCATE TABLE analytics.fact_churn;",
            "TRUNCATE TABLE analytics.kpi_daily;",
            "TRUNCATE TABLE analytics.retention_cohort_monthly;",
        ]

    # dim_user
    statements += [
        """
        INSERT INTO analytics.dim_user(user_id, signup_date, industry, region, sales_rep, usage_score, nps_score, base_mrr)
        SELECT user_id, signup_date, industry, region, sales_rep, usage_score, nps_score, base_mrr
        FROM stg.users;
        """
    ]

    # dim_plan
    statements += [
        """
        INSERT INTO analytics.dim_plan(plan_id, plan_name, price_usd, billing_period)
        SELECT plan_id, plan_name, price_usd, billing_period
        FROM stg.plans;
        """
    ]

    # dim_date
    statements += [
        """
        INSERT INTO analytics.dim_date(date_day, year, month, month_start, week, day_of_week)
        WITH bounds AS (
          SELECT
            LEAST(
              (SELECT MIN(signup_date) FROM stg.users),
              (SELECT MIN(event_time)::date FROM stg.events)
            ) AS min_day,
            GREATEST(
              (SELECT MAX(signup_date) FROM stg.users),
              (SELECT MAX(event_time)::date FROM stg.events)
            ) AS max_day
        ),
        spine AS (
          SELECT generate_series(min_day, max_day, interval '1 day')::date AS date_day
          FROM bounds
        )
        SELECT
          date_day,
          EXTRACT(YEAR FROM date_day)::int AS year,
          EXTRACT(MONTH FROM date_day)::int AS month,
          date_trunc('month', date_day)::date AS month_start,
          EXTRACT(WEEK FROM date_day)::int AS week,
          EXTRACT(ISODOW FROM date_day)::int AS day_of_week
        FROM spine;
        """
    ]

    # fact_daily_activity
    statements += [
        """
        INSERT INTO analytics.fact_daily_activity(activity_date, user_id, event_count, active_flag, feature_used_count)
        SELECT
          e.event_date AS activity_date,
          e.user_id,
          COUNT(*)::int AS event_count,
          CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS active_flag,
          SUM(CASE WHEN e.event_name = 'feature_used' THEN 1 ELSE 0 END)::int AS feature_used_count
        FROM stg.events e
        GROUP BY 1,2;
        """
    ]

    # fact_daily_support
    statements += [
        """
        INSERT INTO analytics.fact_daily_support(support_date, user_id, tickets_created, avg_resolution_hours, avg_csat)
        SELECT
          t.created_date AS support_date,
          t.user_id,
          COUNT(*)::int AS tickets_created,
          AVG(t.resolution_hours) AS avg_resolution_hours,
          AVG(t.csat) AS avg_csat
        FROM stg.tickets t
        GROUP BY 1,2;
        """
    ]

    # fact_monthly_billing
    statements += [
        """
        INSERT INTO analytics.fact_monthly_billing(
          month_start, subscription_id, user_id, plan_id, mrr_usd,
          invoices_issued, invoices_paid, invoices_failed,
          amount_paid_usd, amount_failed_usd
        )
        SELECT
          i.month_start,
          s.subscription_id,
          s.user_id,
          s.plan_id,
          p.price_usd AS mrr_usd,
          COUNT(*)::int AS invoices_issued,
          SUM(CASE WHEN i.invoice_status = 'paid' THEN 1 ELSE 0 END)::int AS invoices_paid,
          SUM(CASE WHEN i.invoice_status = 'failed' THEN 1 ELSE 0 END)::int AS invoices_failed,
          SUM(CASE WHEN i.invoice_status = 'paid' THEN i.amount_usd ELSE 0 END)::numeric(12,2) AS amount_paid_usd,
          SUM(CASE WHEN i.invoice_status = 'failed' THEN i.amount_usd ELSE 0 END)::numeric(12,2) AS amount_failed_usd
        FROM stg.invoices i
        JOIN stg.subscriptions s ON s.subscription_id = i.subscription_id
        JOIN stg.plans p ON p.plan_id = s.plan_id
        GROUP BY 1,2,3,4,5;
        """
    ]

    # fact_churn
    statements += [
        """
        INSERT INTO analytics.fact_churn(churn_date, subscription_id, user_id, plan_id)
        SELECT
          s.end_date AS churn_date,
          s.subscription_id,
          s.user_id,
          s.plan_id
        FROM stg.subscriptions s
        WHERE s.status = 'canceled'
          AND s.end_date IS NOT NULL;
        """
    ]

    # kpi_daily
    statements += [
        """
        INSERT INTO analytics.kpi_daily(
          date_day, dau, active_customers, new_signups, churned_users, churn_rate,
          tickets_created, invoices_failed, paid_revenue_usd
        )
        WITH
        dau AS (
          SELECT activity_date AS d, COUNT(DISTINCT user_id)::int AS dau
          FROM analytics.fact_daily_activity
          WHERE active_flag = 1
          GROUP BY 1
        ),
        active_customers AS (
          SELECT dd.date_day AS d, COUNT(DISTINCT s.user_id)::int AS active_customers
          FROM analytics.dim_date dd
          JOIN stg.subscriptions s
            ON s.start_date <= dd.date_day
           AND (s.end_date IS NULL OR s.end_date > dd.date_day)
          GROUP BY 1
        ),
        signups AS (
          SELECT signup_date AS d, COUNT(*)::int AS new_signups
          FROM stg.users
          GROUP BY 1
        ),
        churn AS (
          SELECT churn_date AS d, COUNT(DISTINCT user_id)::int AS churned_users
          FROM analytics.fact_churn
          GROUP BY 1
        ),
        tickets AS (
          SELECT support_date AS d, SUM(tickets_created)::int AS tickets_created
          FROM analytics.fact_daily_support
          GROUP BY 1
        ),
        inv AS (
          SELECT issued_date AS d,
                 SUM(CASE WHEN invoice_status='failed' THEN 1 ELSE 0 END)::int AS invoices_failed,
                 SUM(CASE WHEN invoice_status='paid' THEN amount_usd ELSE 0 END)::numeric(12,2) AS paid_revenue_usd
          FROM stg.invoices
          GROUP BY 1
        )
        SELECT
          dd.date_day,
          COALESCE(dau.dau, 0) AS dau,
          COALESCE(ac.active_customers, 0) AS active_customers,
          COALESCE(su.new_signups, 0) AS new_signups,
          COALESCE(ch.churned_users, 0) AS churned_users,
          CASE
            WHEN COALESCE(ac.active_customers, 0) = 0 THEN NULL
            ELSE (COALESCE(ch.churned_users, 0)::double precision / ac.active_customers::double precision)
          END AS churn_rate,
          COALESCE(ti.tickets_created, 0) AS tickets_created,
          COALESCE(iv.invoices_failed, 0) AS invoices_failed,
          COALESCE(iv.paid_revenue_usd, 0)::numeric(12,2) AS paid_revenue_usd
        FROM analytics.dim_date dd
        LEFT JOIN dau dau ON dau.d = dd.date_day
        LEFT JOIN active_customers ac ON ac.d = dd.date_day
        LEFT JOIN signups su ON su.d = dd.date_day
        LEFT JOIN churn ch ON ch.d = dd.date_day
        LEFT JOIN tickets ti ON ti.d = dd.date_day
        LEFT JOIN inv iv ON iv.d = dd.date_day;
        """
    ]

    # retention_cohort_monthly
    statements += [
        """
        INSERT INTO analytics.retention_cohort_monthly(
          cohort_month, active_month, months_since_signup, cohort_size, active_users, retention_rate
        )
        WITH
        cohorts AS (
          SELECT user_id, date_trunc('month', signup_date)::date AS cohort_month
          FROM stg.users
        ),
        active_months AS (
          SELECT user_id, date_trunc('month', activity_date)::date AS active_month
          FROM analytics.fact_daily_activity
          WHERE active_flag = 1
          GROUP BY 1,2
        ),
        joined AS (
          SELECT
            c.cohort_month,
            a.active_month,
            (DATE_PART('year', AGE(a.active_month, c.cohort_month)) * 12
             + DATE_PART('month', AGE(a.active_month, c.cohort_month)))::int AS months_since_signup,
            c.user_id
          FROM cohorts c
          JOIN active_months a
            ON a.user_id = c.user_id
           AND a.active_month >= c.cohort_month
        ),
        cohort_sizes AS (
          SELECT cohort_month, COUNT(*)::int AS cohort_size
          FROM cohorts
          GROUP BY 1
        ),
        active_counts AS (
          SELECT cohort_month, active_month, months_since_signup, COUNT(DISTINCT user_id)::int AS active_users
          FROM joined
          GROUP BY 1,2,3
        )
        SELECT
          a.cohort_month,
          a.active_month,
          a.months_since_signup,
          s.cohort_size,
          a.active_users,
          (a.active_users::double precision / s.cohort_size::double precision) AS retention_rate
        FROM active_counts a
        JOIN cohort_sizes s ON s.cohort_month = a.cohort_month;
        """
    ]

    # Execute everything
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))

        dim_user = conn.execute(text("SELECT COUNT(*) FROM analytics.dim_user")).scalar_one()
        dim_plan = conn.execute(text("SELECT COUNT(*) FROM analytics.dim_plan")).scalar_one()
        dim_date = conn.execute(text("SELECT COUNT(*) FROM analytics.dim_date")).scalar_one()

        fda = conn.execute(text("SELECT COUNT(*) FROM analytics.fact_daily_activity")).scalar_one()
        fds = conn.execute(text("SELECT COUNT(*) FROM analytics.fact_daily_support")).scalar_one()
        fmr = conn.execute(text("SELECT COUNT(*) FROM analytics.fact_monthly_billing")).scalar_one()
        fch = conn.execute(text("SELECT COUNT(*) FROM analytics.fact_churn")).scalar_one()

        kpi = conn.execute(text("SELECT COUNT(*) FROM analytics.kpi_daily")).scalar_one()
        coh = conn.execute(text("SELECT COUNT(*) FROM analytics.retention_cohort_monthly")).scalar_one()

    return schema(
        dim_user=dim_user,
        dim_plan=dim_plan,
        dim_date=dim_date,
        fact_daily_activity=fda,
        fact_daily_support=fds,
        fact_monthly_billing=fmr,
        fact_churn=fch,
        kpi_daily=kpi,
        retention_cohort_monthly=coh,
    )
