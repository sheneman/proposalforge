from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "mysql+asyncmy://proposalforge:proposalforge_pass@db:3306/proposalforge"
    REDIS_URL: str = "redis://redis:6379/0"
    SECRET_KEY: str = "change-me-in-production"
    DEBUG: bool = True
    SYNC_ON_STARTUP: bool = False
    SYNC_INTERVAL_HOURS: int = 6

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
