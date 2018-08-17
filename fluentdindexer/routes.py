from datetime import datetime
from functools import partial

from asyncworker import App
from asyncworker.options import Options
from asyncworker.utils import Timeit, TimeitCallback

from logingestor import conf
from fluentdindexer.indexer import FluentdMonitoringIndexer

import logging

logging.getLogger('elasticsearch').setLevel(100)

app = App(host=conf.FLUENTD_INDEXER_RABBITMQ_HOST,
          user=conf.FLUENTD_INDEXER_RABBITMQ_USER,
          password=conf.FLUENTD_INDEXER_RABBITMQ_PWD,
          prefetch_count=conf.FLUENTD_INDEXER_RABBITMQ_PREFETCH
      )

indexer = FluentdMonitoringIndexer(conf.elasticsearch, conf.logger)

@app.route(conf.FLUENTD_INDEXER_QUEUE_NAMES, vhost=conf.FLUENTD_INDEXER_RABBITMQ_VHOST, options = {Options.BULK_SIZE: conf.FLUENTD_INDEXER_BULK_SIZE})
async def fluentd_monitoring_events_indexer(messages):
    indexer.logger = conf.logger
    await indexer.bulk(messages)
