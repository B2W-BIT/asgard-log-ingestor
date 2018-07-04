from datetime import datetime

from asyncworker import App
from asyncworker.options import Options

from logingestor import conf
from statsindexer.indexer import StatsIndexer

app = App(host=conf.RABBITMQ_HOST, user=conf.RABBITMQ_USER, password=conf.RABBITMQ_PWD, prefetch_count=conf.RABBITMQ_PREFETCH)

indexer = StatsIndexer(conf.elasticsearch)

@app.route(conf.LOGS_QUEUE_NAMES, vhost=conf.RABBITMQ_VHOST, options = {Options.BULK_SIZE: conf.LOGS_BULK_SIZE})
async def app_stats_indexer_handler(messages):
    await indexer.bulk((m.body for m in messages))
