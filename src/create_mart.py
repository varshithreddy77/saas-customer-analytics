from rich import print
from sqlalchemy import text
from src.config import Settings
from src.db import get_engine

def create_analytics_layer():
    s = Settings()
    engine = get_engine(s.sqlalchemy_url)

    print("[bold cyan]Creating 'analytics' schema and tables...[/bold cyan]")
    
    with engine.begin() as conn:
        # 1. Create Schema
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics;"))
        
        # 2. Drop table if exists to allow re-runs
        conn.execute(text("DROP TABLE IF EXISTS analytics.customer_churn_summary;"))
        
        # 3. Create the Summary Table
        # We join Users -> Subscriptions -> Plans -> NPS
        query = text("""
            CREATE TABLE analytics.customer_churn_summary AS
            SELECT
                u.user_id,
                u.industry,
                u.region,
                u.created_at AS signup_date,
                u.sales_rep,
                p.plan_name,
                p.price_usd AS monthly_revenue,
                s.status,
                CASE WHEN s.status = 'canceled' THEN 1 ELSE 0 END AS is_churned,
                s.start_at AS subscription_start,
                s.end_at AS subscription_end,
                n.nps_score
            FROM raw.raw_users u
            JOIN raw.raw_subscriptions s ON u.user_id = s.user_id
            JOIN raw.raw_plans p ON s.plan_id = p.plan_id
            LEFT JOIN raw.raw_nps n ON u.user_id = n.user_id;
        """)
        conn.execute(query)
        
        # 4. Add a primary key/index for good measure (optional but recommended)
        conn.execute(text("ALTER TABLE analytics.customer_churn_summary ADD PRIMARY KEY (user_id);"))
    
    print("[bold green]✅ Table 'analytics.customer_churn_summary' created successfully![/bold green]")

    # Verify by counting
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM analytics.customer_churn_summary")).scalar()
        print(f"Total rows in summary table: [bold]{count}[/bold]")

if __name__ == "__main__":
    create_analytics_layer()
