from importlib import reload
import os
import asynctest
from aiohttp.test_utils import TestClient, TestServer
import asyncio

from asyncworker.options import RouteTypes
from asynctest.mock import CoroutineMock, ANY, patch
from freezegun import freeze_time
from logingestor import conf
from asyncworker.signals.handlers.http import HTTPServer

from statsindexer.indexer import StatsIndexer
from statsindexer.routes import app


class StatsIndexerTest(asynctest.TestCase):

    def setUp(self):
        self.elasticsearch_mock = CoroutineMock(index=CoroutineMock(), bulk=CoroutineMock())
        self.indexer = StatsIndexer(self.elasticsearch_mock, CoroutineMock())
        self.http_server = HTTPServer()

    @freeze_time("2018-06-27T10:00:00-03:00")
    def test_test_generate_correct_index_name(self):
        """
        Index name: asgard-app-stats-<namespace>-<appname>
        Trocamos "/" por "-"
        Sempre geramos parte da data no nome do índice usanto UTC.
        """
        with patch.dict(os.environ, STATS_INDEX_PREFIX="asgard-app-stats-index"):
            reload(conf)
            self.assertEqual("asgard-app-stats-index-2018-06-27-13", self.indexer._index_name({"appname": "/infra/app/in/some/inner/folder"}))
            self.assertEqual("asgard-app-stats-index-2018-06-27-13", self.indexer._index_name({"appname": "/dev/other/app-with-dashes"}))

    async def test_prepare_document_returns_same_document(self):
        document = {"some-key": "some-value", "appname": "/dev/foo", "timestamp": 1531222358}
        expected_document = self.indexer._prepare_document(document)
        expected_document['timestamp'] = ANY
        self.assertEqual(expected_document, document)

    async def test_prepare_document_format_timestamp(self):
        document = {"some-key": "some-value", "appname": "/dev/foo", "timestamp": 1531223488}
        expected_document = self.indexer._prepare_document(document)
        self.assertEqual(expected_document['timestamp'], "2018-07-10T11:51:28+00:00")

    async def test_extracts_appname(self):
        appname=  self.indexer._app_name_with_namespace({"appname": "/infra/app"})
        self.assertEqual("/infra/app", appname)

    async def test_health_check_OK(self):
        await self.http_server.startup(app)
        async with TestClient(TestServer(app[RouteTypes.HTTP]['http_app']), loop=asyncio.get_event_loop()) as client:
            with patch.multiple(conf, elasticsearch=self.elasticsearch_mock):
                self.elasticsearch_mock.ping = CoroutineMock(return_value=True)
                resp = await client.get("/health")
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                self.assertDictEqual({
                    "elasticsearch": True,
                }, data)
        await self.http_server.shutdown(app)

    async def test_health_check_failing(self):
        await self.http_server.startup(app)
        async with TestClient(TestServer(app[RouteTypes.HTTP]['http_app']), loop=asyncio.get_event_loop()) as client:
            with patch.multiple(conf, elasticsearch=self.elasticsearch_mock):
                self.elasticsearch_mock.ping = CoroutineMock(return_value=False)
                resp = await client.get("/health")
                self.assertEqual(resp.status, 500)
                data = await resp.json()
                self.assertDictEqual({
                    "elasticsearch": False,
                }, data)
        await self.http_server.shutdown(app)

