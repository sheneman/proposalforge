from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "mysql+asyncmy://proposalforge:proposalforge_pass@db:3306/proposalforge"
    REDIS_URL: str = "redis://redis:6379/0"
    SECRET_KEY: str = "change-me-in-production"
    DEBUG: bool = True
    SYNC_ON_STARTUP: bool = False
    SYNC_INTERVAL_HOURS: int = 6

    # Admin authentication (must be set in .env — no defaults for security)
    ADMIN_USERNAME: str = "admin"
    ADMIN_PASSWORD: str = ""

    # LLM settings for Chat with My Data
    LLM_BASE_URL: str = "https://mindrouter.uidaho.edu/v1"
    LLM_MODEL: str = "openai/gpt-oss-120b"
    LLM_API_KEY: str = "not-needed"

    # CollabNet Data API (researcher data)
    COLLABNET_API_URL: str = "https://collabnet-api.nkn.uidaho.edu"
    COLLABNET_API_KEY: str = ""
    RESEARCHER_SYNC_ON_STARTUP: bool = False

    # Brave Search API (for MCP web search)
    BRAVE_API_KEY: str = ""

    # VERSO/Esploro API (grants, projects, activities)
    VERSO_API_URL: str = "https://api-na.hosted.exlibrisgroup.com/esploro/v1"
    VERSO_API_KEY: str = ""

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
