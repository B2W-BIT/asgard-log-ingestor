from datetime import datetime
from functools import partial

from asyncworker import App
from asyncworker.options import Options
from asyncworker.utils import Timeit, TimeitCallback

from logingestor import conf
from logingestor.indexer import AppIndexer

app = App(host=conf.RABBITMQ_HOST, user=conf.RABBITMQ_USER, password=conf.RABBITMQ_PWD, prefetch_count=conf.RABBITMQ_PREFETCH)

indexer = AppIndexer(conf.elasticsearch, conf.logger)
async def logger_function(total_messages, name, elapsed_time, **kwargs):
    await conf.logger.info({name: elapsed_time, "total-messages": total_messages, **kwargs})

@app.route(conf.LOGS_QUEUE_NAMES, vhost=conf.RABBITMQ_VHOST, options = {Options.BULK_SIZE: conf.LOGS_BULK_SIZE})
async def generic_app_log_indexer(messages):
    indexer.logger = conf.logger
    logger_partial = partial(logger_function, len(messages))
    async with Timeit(name="processing-time", callback=logger_partial):
        await indexer.bulk(messages)

