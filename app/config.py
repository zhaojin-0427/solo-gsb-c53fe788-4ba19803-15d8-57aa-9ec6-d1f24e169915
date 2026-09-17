"""Application settings, all overridable from environment variables."""
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="QUOTA_", env_file=".env",
                                      extra="ignore")

    database_url: str = Field(
        default="postgresql://quota:quota@db:5432/quota",
        description="asyncpg DSN, e.g. postgresql://user:pass@host:5432/db",
    )
    db_min_size: int = 2
    db_max_size: int = 10
    db_connect_timeout: float = 60.0

    # Defaults used when a bucket level is touched for the first time.
    default_capacity: float = 100.0
    default_refill_rate: float = 10.0  # tokens per second

    # Reservations left in 'granted' are auto-cancelled after this TTL.
    reservation_ttl_seconds: float = 60.0

    # Background expiry reaper.
    reaper_interval_seconds: float = 1.0
    reaper_batch_size: int = 200


@lru_cache
def get_settings() -> Settings:
    return Settings()
