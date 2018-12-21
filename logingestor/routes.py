from datetime import datetime
from functools import partial

from asyncworker import App
from asyncworker.options import Options
from asyncworker.utils import Timeit, TimeitCallback

from logingestor import conf
from logingestor.indexer import AppIndexer

import logging
from aiohttp import web

logging.getLogger('elasticsearch').setLevel(logging.NOTSET)

app = App(host=conf.RABBITMQ_HOST, user=conf.RABBITMQ_USER, password=conf.RABBITMQ_PWD, prefetch_count=conf.RABBITMQ_PREFETCH)

indexer = AppIndexer(conf.elasticsearch, conf.logger)

async def logger_function(logger, total_messages, *args, **kwargs):
    await logger.info({
        "event": "bulk_log_index",
        "messages": total_messages,
        args[0]: args[1],
        **kwargs
    })

@app.route(conf.LOGS_QUEUE_NAMES, vhost=conf.RABBITMQ_VHOST, options = {Options.BULK_SIZE: conf.LOGS_BULK_SIZE})
async def generic_app_log_indexer(messages):
    logger_partial = partial(logger_function, conf.logger, len(messages))
    async with Timeit(name="bulk_index_time", callback=logger_partial):
        await indexer.bulk(messages)

