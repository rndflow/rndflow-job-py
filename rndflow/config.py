from pydantic import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    rndflow_api_server: Optional[str]
    rndflow_refresh_token: str
