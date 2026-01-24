from rich import print

from src.config import Settings
from src.docker_ops import docker_compose_up
from src.db import get_engine, wait_for_db, scalar_int
from src.load_csv import load_csv_to_raw

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

if __name__ == "__main__":
    main()
