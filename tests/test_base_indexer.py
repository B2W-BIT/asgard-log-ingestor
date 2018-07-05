import asynctest
from asynctest.mock import CoroutineMock
from asynctest import mock
import json

from freezegun import freeze_time

from logingestor.indexer import Indexer

class BaseIndexerTest(asynctest.TestCase):

    def setUp(self):
        self.elasticsearch_mock = CoroutineMock(index=CoroutineMock(), bulk=CoroutineMock())
        self.logmessage_parse_ok = json.loads(
        """
            {
               "timestamp" : 1529609166,
               "payload" : {
                  "file_path" : "/opt/app/countsingestor/indexer.py",
                  "logged_at" : "2018-06-21T19:26:06.648610+00:00",
                  "level" : "INFO",
                  "line_number" : 37,
                  "index-time" : 0,
                  "function" : "index",
                  "count-type" : "OK"
               },
               "key" : "asgard.app.infra.asgard.logs.counts"
            }

        """)
        self.logmessage_parse_error = json.loads(
        """
            {
               "key" : "errors.asgard.app.sieve.captura.kirby.powerup",
               "payload" : {
                  "fluentd_tag" : "asgard.app.sieve.captura.kirby.powerup",
                  "log" : "  File /usr/local/lib/python3.6/site-packages/aioamqp/protocol.py, line 235, in get_frame",
                  "container_id" : "d1b2c3ef17f86a3e5b6b16b4b6a566310f133ec399d9666ddcf003da0bfa9c77",
                  "container_name" : "/mesos-d7bf305c-2e34-4597-b5dc-e1cb3144c6b9",
                  "source" : "stderr",
                  "parse_error" : "true"
               },
               "timestamp" : 1530108596
            }
        """)


    async def test_generated_right_action_data_for_bulk_insert(self):
        """
        Antes de passar o(s) documento(s) para o elasticsearch, devemos
        chamar `self._prepare_document(document)`.
        E para gerar o nome do índice onde esse documento será indexado, chamamos
        `self._index_name(document)`
        """
        class MyIndexer(Indexer):

            def _prepare_document(self, document):
                del document['key']
                document['new-key'] = 42
                return document

            def _index_name(self, document):
                return "my-important-index"

        self.elasticsearch_mock.bulk.return_value = 42
        indexer = MyIndexer(self.elasticsearch_mock)
        returned_by_elasticsearch = await indexer.bulk([{"key": "errors.asgard.app.sieve.captura.kirby.powerup", "some-value": 10}])
        self.assertEqual([mock.call([
            { "index" : { "_index" : "my-important-index", "_type" : "logs"}},
            {"some-value": 10, "new-key": 42},
        ])], self.elasticsearch_mock.bulk.await_args_list)
        self.assertEqual(42, returned_by_elasticsearch)

    async def test_confirms_default_prepare_document_implementation(self):
        class MyOtherIndexer(Indexer):
            def _index_name(self, document):
                return "my-important-index"

        indexer = MyOtherIndexer(self.elasticsearch_mock)
        await indexer.bulk([{"key": "errors.asgard.app.sieve.captura.kirby.powerup", "some-value": 10}])
        self.assertEqual([mock.call([
            { "index" : { "_index" : "my-important-index", "_type" : "logs"}},
            {"key": "errors.asgard.app.sieve.captura.kirby.powerup", "some-value": 10},
        ])], self.elasticsearch_mock.bulk.await_args_list)

