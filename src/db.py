import time
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

def get_engine(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True)

def wait_for_db(engine: Engine, timeout_s: int = 60) -> None:
    start = time.time()
    while True:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception:
            if time.time() - start > timeout_s:
                raise
            time.sleep(1)

def scalar_int(engine: Engine, sql: str) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text(sql)).scalar() or 0)
