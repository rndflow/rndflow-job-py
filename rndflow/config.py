from pydantic import BaseSettings
from typing import Optional
from functools import lru_cache

class Settings(BaseSettings):
    rndflow_api_server: Optional[str]
    rndflow_refresh_token: str

@lru_cache
def get_settings():
    return Settings()
