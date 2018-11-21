import asyncio
from datetime import datetime

from asyncworker import App
from asyncworker.options import Options, RouteTypes

from logingestor import conf
from statsindexer.indexer import StatsIndexer

from aiohttp import web

from healthchecker.async_checker import AsyncCheckCase
from healthchecker import check

app = App(host=conf.STATS_RABBITMQ_HOST, user=conf.STATS_RABBITMQ_USER, password=conf.STATS_RABBITMQ_PWD, prefetch_count=conf.STATS_RABBITMQ_PREFETCH)

indexer = StatsIndexer(conf.elasticsearch, conf.logger)

@app.route(conf.STATS_QUEUE_NAMES, type=RouteTypes.AMQP_RABBITMQ, vhost=conf.STATS_RABBITMQ_VHOST, options = {Options.BULK_SIZE: conf.STATS_BULK_SIZE})
async def app_stats_indexer_handler(messages):
    await indexer.bulk(messages)

@app.route(["/health"], type=RouteTypes.HTTP, methods=["GET"])
class HealthCheck(AsyncCheckCase, web.View):
    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self.request.app.loop

    async def get(self) -> web.Response:
        """
        Should return 200 if all dependencies are ok, 500 otherwise.
        :returns: A HTTP response with True or False for each check
        """
        await self.check()

        status_code = 200 if self.has_succeeded() else 500

        return web.json_response(data=self.check_report, status=status_code)

    @check
    async def elasticsearch(self):
        return await conf.elasticsearch.ping()

