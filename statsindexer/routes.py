from asyncworker import App
from asyncworker.options import Options

from logingestor import conf
from statsindexer.indexer import StatsIndexer

app = App(
    host=conf.STATS_RABBITMQ_HOST,
    user=conf.STATS_RABBITMQ_USER,
    password=conf.STATS_RABBITMQ_PWD,
    prefetch_count=conf.STATS_RABBITMQ_PREFETCH,
)

indexer = StatsIndexer(conf.elasticsearch, conf.logger)


@app.route(
    conf.STATS_QUEUE_NAMES,
    vhost=conf.STATS_RABBITMQ_VHOST,
    options={Options.BULK_SIZE: conf.STATS_BULK_SIZE},
)
async def app_stats_indexer_handler(messages):
    await indexer.bulk(messages)
