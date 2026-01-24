from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", "5432"))
    db: str = os.getenv("DB_NAME", "saas_analytics")
    user: str = os.getenv("DB_USER", "analytics")
    password: str = os.getenv("DB_PASSWORD", "analytics")

    data_path: str = os.getenv("DATA_PATH", "data/saas_customer_data.csv")
    force_reload: bool = os.getenv("FORCE_RELOAD", "0") == "1"

    @property
    def sqlalchemy_url(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
