from typing import List, Any
from pydantic import BaseModel, Field


class ClickhouseConfig(BaseModel):
    host: str
    port: int
    user: str
    password: str
    secure: bool

class RssSourceConfig(BaseModel):
    name: str
    url: str
    interval: int = Field(gt=0, description="Интервал опроса источника в секундах")
    timezone: str
    cursor: Any = None

class AppConfig(BaseModel):
    clickhouse: ClickhouseConfig
    rss_sources: List[RssSourceConfig]
