import asyncio
import logging
import yaml
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from config.schema import AppConfig, RssSourceConfig
from packages.providers import RssProvider, ClickhouseProvider
from packages.parser import Parser

logger = logging.getLogger(__name__)


class Application:
    def __init__(self, config_path: str = "./config/config.yml", cursor: int | None = None):
        logger.info("Initialize applications...")
        self.config = self._load_config(config_path)
        self.tz_utc = timezone.utc

        logger.debug("Initializing providers...")
        self.ch_provider = ClickhouseProvider(config=self.config)
        self.rss_provider = RssProvider(timeout_sec=10)
        self.parser = Parser()

        logger.info("All components have been successfully initialized")

    async def main_process(self, source: RssSourceConfig):
        logger.info(f"Starting worker for source: {source.name}")
        if source.cursor is None:
            await self._get_cursor(source)

        try:
            while True:
                try:
                    data = await self.rss_provider.fetch(source.url)
                    raw_entries = self.parser.parse(data)

                    payload = []
                    max_cursor = source.cursor
                    loaded_dttm = datetime.now(self.tz_utc)

                    for entry in raw_entries:
                        try:
                            published_dttm = datetime.strptime(entry["published"], '%a, %d %b %Y %H:%M:%S %z').astimezone(timezone.utc)
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid date format in source {source.name}: {entry.get('published')}")
                            continue

                        if published_dttm > source.cursor:
                            if published_dttm > max_cursor:
                                max_cursor = published_dttm

                            payload.append([
                                loaded_dttm,
                                "RSS",
                                source.name,
                                entry["guid"],
                                published_dttm,
                                entry["title"],
                                entry["summary"],
                                entry["link"]
                            ])

                    # Вставка данных
                    if payload:
                        logger.info(f"Source {source.name}: inserting {len(payload)} new entries. New cursor: {max_cursor}")
                        await self.ch_provider.async_insert(
                            table="stg.rss_news",
                            columns=[
                                "loaded_dttm", "source_system", "feed_nm", "guid",
                                "published_dttm", "title_txt", "summary_txt", "link"
                            ],
                            data=payload
                        )

                        # Обновляем курсор только после успешной вставки
                        source.cursor = max_cursor
                    else:
                        logger.debug(f"Source {source.name}: no new entries found")

                except Exception as e:
                    logger.error(f"Error in processing for {source.name}: {e}", exc_info=True)

                await asyncio.sleep(source.interval)

        except asyncio.CancelledError:
            logger.info(f"Worker for {source.name} was cancelled")

    async def run(self):
        """Запуск приложения: инициализация ресурсов и создание корутин"""
        try:
            await asyncio.gather(
                self.ch_provider.connect(),
                self.rss_provider.connect()
            )
            await self._init_db()

            tasks = [asyncio.create_task(self.main_process(src)) for src in self.config.rss_sources]
            logger.info(f"Started {len(tasks)} workers. Press Ctrl+C to stop.")
            await asyncio.gather(*tasks)

        except asyncio.CancelledError:
            logger.info("Application stopping...")
        finally:
            logger.info("Cleaning up resources...")
            await self.rss_provider.close()
            await self.ch_provider.close()
            logger.info("Providers have been successfully closed")

    @staticmethod
    def _load_config(path: str) -> AppConfig:
        try:
            with open(path, "r") as f:
                data = yaml.safe_load(f)
                return AppConfig(**data)
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found: {path}")

    async def _init_db(self) -> None:
        try:
            await self.ch_provider.query("CREATE DATABASE IF NOT EXISTS stg")

            await self.ch_provider.query("""
                CREATE TABLE IF NOT EXISTS stg.rss_news
                (
                    loaded_dttm         DateTime,
                    source_system       LowCardinality(String),
                    feed_nm             LowCardinality(String),
                    guid                String,
                    published_dttm      DateTime,
                    title_txt           String,
                    summary_txt         String,
                    link                String
                )
                ENGINE = ReplacingMergeTree
                PARTITION BY toYYYYMM(published_dttm)
                ORDER BY (feed_nm, published_dttm, guid)
                SETTINGS index_granularity = 8192
            """)
            logger.info("ClickHouse schema ensured (database/table present)")
        except Exception:
            logger.exception("Failed to initialize ClickHouse schema")
            raise

    async def _get_cursor(self, source):
        logger.info("Getting initial cursor from ClickHouse...")

        while True:
            try:
                result = await self.ch_provider.query(sql=f"SELECT max(published_dttm) FROM stg.rss_news WHERE feed_nm = '{source.name}'")

                if result and result[0] and result[0][0] is not None:
                    source.cursor = result[0][0].replace(tzinfo=timezone.utc)
                else:
                    logger.warning("Table is empty or NULL returned, setting cursor to 0")
                    source.cursor = 0

                logger.info(f"Cursor initialized for {source.name}: {source.cursor}")
                return

            except Exception as e:
                logger.error(f"Failed to get cursor: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
