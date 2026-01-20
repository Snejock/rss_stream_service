from typing import List, Any
from pydantic import BaseModel, Field


class ClickhouseConfig(BaseModel):
    host: str
    port: int
    user: str
    password: str
    secure: bool

class BrokerConfig(BaseModel):
    host: str
    port: int
    client_id: str
    schema_registry_url: str
    topic: str
    linger_ms: int
    batch_size: int
    compression_type: str
    acks: int

class RssSourceConfig(BaseModel):
    name: str
    url: str
    interval: int = Field(gt=0, description="Интервал опроса источника в секундах")
    timezone: str
    cursor: Any = None

class AppConfig(BaseModel):
    clickhouse: ClickhouseConfig
    broker: BrokerConfig
    rss_sources: List[RssSourceConfig]
