import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class DatabaseConfig:
    host: str
    port: int
    database: str
    username: str
    password: str
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class ETLConfig:
    source_table: str = "source_table"
    target_table: str = "target_table"
    target_results_for_ge: str = "target_results_for_ge"
    batch_size: int = 1000
    snapshot_date: Optional[str] = None

class Settings:
    def __init__(self):
        self.db = DatabaseConfig(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME", "postgres"),
            username=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "root")
        )
        self.etl = ETLConfig()
        
    @classmethod
    def from_env(cls):
        return cls()

settings = Settings.from_env()
