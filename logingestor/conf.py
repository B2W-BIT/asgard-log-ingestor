import asyncio
import logging
import os

from asgard.sdk.options import get_option
from aioelasticsearch import Elasticsearch

from aiologger.loggers.json import JsonLogger


LOGLEVEL_CONF = os.getenv("ASGARD_LOGINGESTOR_LOGLEVEL", "INFO")
loglevel = getattr(logging, LOGLEVEL_CONF, logging.INFO)

logger = None

async def init_logger():
    global logger
    logger = await JsonLogger.with_default_handlers(level=loglevel, flatten=True)

loop = asyncio.get_event_loop()
init_logger_task = loop.create_task(init_logger())

ELASTIC_SEARCH_ADDRESSES = get_option("ELASTICSEARCH", "ADDRESS")
BULK_INSERT_TIMEOUT = int(os.getenv("INDEXER_BULK_INSERT_TIMEOUT", "30"))

elasticsearch = Elasticsearch(hosts=ELASTIC_SEARCH_ADDRESSES)

RABBITMQ_HOST = os.getenv("LOGS_RABBITMQ_HOST", "127.0.0.1")
RABBITMQ_USER = os.getenv("LOGS_RABBITMQ_USER", "guest")
RABBITMQ_PWD = os.getenv("LOGS_RABBITMQ_PWD", "guest")
RABBITMQ_PREFETCH = int(os.getenv("LOGS_RABBITMQ_PREFETCH", 32))
RABBITMQ_VHOST = os.getenv("LOGS_RABBITMQ_VHOST", "/")
LOGS_QUEUE_NAMES = [item.strip() for item in os.getenv("LOGS_QUEUE_NAMES", "").split(",")]
LOGS_BULK_SIZE = int(os.getenv("LOGS_BULK_SIZE", 1))

STATS_RABBITMQ_HOST = os.getenv("STATS_RABBITMQ_HOST", "127.0.0.1")
STATS_RABBITMQ_USER = os.getenv("STATS_RABBITMQ_USER", "guest")
STATS_RABBITMQ_PWD = os.getenv("STATS_RABBITMQ_PWD", "guest")
STATS_RABBITMQ_PREFETCH = int(os.getenv("STATS_RABBITMQ_PREFETCH", 32))
STATS_RABBITMQ_VHOST = os.getenv("STATS_RABBITMQ_VHOST", "/")
STATS_QUEUE_NAMES = [item.strip() for item in os.getenv("STATS_QUEUE_NAMES", "").split(",")]
STATS_BULK_SIZE = int(os.getenv("STATS_BULK_SIZE", 1))

FLUENTD_INDEXER_RABBITMQ_HOST = os.getenv("FLUENTD_INDEXER_RABBITMQ_HOST", "127.0.0.1")
FLUENTD_INDEXER_RABBITMQ_USER = os.getenv("FLUENTD_INDEXER_RABBITMQ_USER", "guest")
FLUENTD_INDEXER_RABBITMQ_PWD = os.getenv("FLUENTD_INDEXER_RABBITMQ_PWD", "guest")
FLUENTD_INDEXER_RABBITMQ_PREFETCH = int(os.getenv("FLUENTD_INDEXER_RABBITMQ_PREFETCH", 32))
FLUENTD_INDEXER_RABBITMQ_VHOST = os.getenv("FLUENTD_INDEXER_RABBITMQ_VHOST", "/")
FLUENTD_INDEXER_QUEUE_NAMES = [item.strip() for item in os.getenv("FLUENTD_INDEXER_QUEUE_NAMES", "").split(",")]
FLUENTD_INDEXER_BULK_SIZE = int(os.getenv("FLUENTD_INDEXER_BULK_SIZE", 1))
