from datetime import datetime

from asyncworker import App
from asyncworker.options import Options

from logingestor import conf
from logingestor.indexer import AppIndexer

app = App(host=conf.RABBITMQ_HOST, user=conf.RABBITMQ_USER, password=conf.RABBITMQ_PWD, prefetch_count=conf.RABBITMQ_PREFETCH)

indexer = AppIndexer(conf.elasticsearch, conf.logger)

@app.route(conf.LOGS_QUEUE_NAMES, vhost=conf.RABBITMQ_VHOST, options = {Options.BULK_SIZE: conf.LOGS_BULK_SIZE})
async def generic_app_log_indexer(messages):
    await indexer.bulk(messages)
