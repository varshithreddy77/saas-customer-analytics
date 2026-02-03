from rich import print
import traceback
from src.config import Settings
from src.docker_ops import docker_compose_up
from src.db import get_engine, wait_for_db, scalar_int
from src.load_csv import load_csv_to_raw
from src.table_creation import create_table, load_user_attributes
from src.generator import generate
from src.preprocessing import create_schema
from src.reporting import fetch_bi_report, export_bireport_to_csv

def main() -> None:
    s = Settings()

    print("[bold cyan]1) Start Postgres (Docker)[/bold cyan]")
    docker_compose_up()

    print("[bold cyan]2) Connect to DB[/bold cyan]")
    engine = get_engine(s.sqlalchemy_url)
    wait_for_db(engine)

    print("[bold cyan]3) Load CSV -> raw tables[/bold cyan]")
    counts = load_csv_to_raw(engine, s.data_path, s.force_reload)

    if sum(counts.values()) == 0:
        print("[yellow]Already loaded. Set FORCE_RELOAD=1 in .env if you want to reload.[/yellow]")
    else:
        print(f"[green]Loaded rows:[/green] {counts}")

    print("[bold cyan]4) Verify counts[/bold cyan]")
    print("raw_users:", scalar_int(engine, "SELECT COUNT(*) FROM raw.raw_users;"))
    print("raw_plans:", scalar_int(engine, "SELECT COUNT(*) FROM raw.raw_plans;"))
    print("raw_subscriptions:", scalar_int(engine, "SELECT COUNT(*) FROM raw.raw_subscriptions;"))
    print("raw_nps:", scalar_int(engine, "SELECT COUNT(*) FROM raw.raw_nps;"))

    print("[bold green]✅ Step 1 complete[/bold green]")

    print("STEP 2) Create Step2 tables")
    create_table(engine)

    print("STEP 2) Load user attributes")
    n_attr = load_user_attributes(engine, s.data_path)
    print("Loaded raw_user_attributes:", n_attr)

    print("STEP 2) Generate events/invoices/tickets")
    try:
        inserted = generate(
            engine,
            lookback_days=7,      # start small; increase later
            sample_users=5000,    # set 0 for ALL users (can be heavy)
            seed=42,
            force_rebuild=False,  # set True if you want to wipe Step2 tables & regenerate
        )
    except Exception as e:
        print("\n========== REAL ERROR ==========")
        print("TYPE:", type(e))
        print("REPR:", repr(e))
        if hasattr(e, "orig"):
            print("DB ORIG:", repr(e.orig))
        traceback.print_exc()
        print("================================\n")
        raise

    print("Step 3) Build analytical schema")
    result = create_schema(engine, rebuild=True)
    print("STEP 3 done ✅", result)
    print("Inserted:", inserted)

    print("[bold magenta]Step 4) Build MART layer (BI-ready)[/bold magenta]")
    mart_counts = fetch_bi_report(engine, rebuild=True, days_back=90)
    print("[green]Mart row counts:[/green]", mart_counts)

    print("[bold magenta]Step 4) Export MART tables to CSV for Power BI[/bold magenta]")
    paths = export_bireport_to_csv(engine, out_dir="outputs/powerbi")
    print("[green]Exported files:[/green]", paths)

    print("[bold green]✅ Step 4 done[/bold green]")
if __name__ == "__main__":
    main()
