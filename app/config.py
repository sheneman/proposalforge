from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "mysql+asyncmy://proposalforge:proposalforge_pass@db:3306/proposalforge"
    REDIS_URL: str = "redis://redis:6379/0"
    SECRET_KEY: str = "change-me-in-production"
    DEBUG: bool = True
    SYNC_ON_STARTUP: bool = False
    SYNC_INTERVAL_HOURS: int = 6

    # LLM settings for Chat with My Data
    LLM_BASE_URL: str = "https://mindrouter.uidaho.edu/v1"
    LLM_MODEL: str = "openai/gpt-oss-120b"
    LLM_API_KEY: str = "not-needed"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
