import json
from freezegun import freeze_time
import asynctest
from asynctest.mock import CoroutineMock, ANY
from asynctest import mock

from logingestor.indexer import Indexer, AppIndexer

class LogIndexerTest(asynctest.TestCase):


    def setUp(self):
        self.elasticsearch_mock = CoroutineMock(index=CoroutineMock(), bulk=CoroutineMock())
        self.indexer = AppIndexer(self.elasticsearch_mock)
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

    @freeze_time("2018-06-27T10:00:00-03:00")
    def test_test_generate_correct_index_name(self):
        """
        Index name: asgard-app-logs-<namespace>-<appname>
        Trocamos "/" por "-"
        Sempre geramos parte da data no nome do índice usanto UTC.
        """
        self.assertEqual("asgard-app-logs-infra-asgard-logs-counts-2018-06-27-13", self.indexer._index_name({"key": "asgard.app.infra.asgard.logs.counts"}))
        self.assertEqual("asgard-app-logs-infra-asgard-logs-counts-2018-06-27-13", self.indexer._index_name({"key": "errors.asgard.app.infra.asgard.logs.counts"}))

    def test_prepare_indexed_document_log_parse_ok(self):
        """

            Exemplo de log que foi corretamente parseado pelo fluentd
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
        """
        expected_document = {
            "timestamp" : "2018-06-21T19:26:06+00:00",
               "file_path" : "/opt/app/countsingestor/indexer.py",
               "logged_at" : "2018-06-21T19:26:06.648610+00:00",
               "level" : "INFO",
               "line_number" : 37,
               "index-time" : 0,
               "function" : "index",
               "count-type" : "OK",
            "asgard_index_delay": ANY,
            "appname": ANY,
        }
        prepared_document = self.indexer._prepare_document(self.logmessage_parse_ok)
        self.assertDictEqual(expected_document, prepared_document)

    @freeze_time("2018-06-21T19:26:06+00:00")
    def test_prepare_indexed_document_do_not_override_special_fields(self):
        self.logmessage_parse_ok['payload']['timestamp'] = 1530120546 # ~qua jun 27 17:28:57 UTC 2018
        self.logmessage_parse_ok['payload']['asgard_index_delay'] = 42
        self.logmessage_parse_ok['payload']['appname'] = "my-app-name"
        expected_document = {
           "timestamp" : "2018-06-21T19:26:06+00:00",
           "appname" : "/infra/asgard/logs/counts",
           "file_path" : "/opt/app/countsingestor/indexer.py",
           "logged_at" : "2018-06-21T19:26:06.648610+00:00",
           "level" : "INFO",
           "line_number" : 37,
           "index-time" : 0,
           "function" : "index",
           "count-type" : "OK",
           "asgard_index_delay": 0.0,
        }
        prepared_document = self.indexer._prepare_document(self.logmessage_parse_ok)
        self.assertDictEqual(expected_document, prepared_document)

    def test_prepare_indexed_document_log_parse_error(self):
        """

            Exemplo de log que gerou parsing error no fluentd.
            {
               "key" : "errors.asgard.app.sieve.captura.kirby.powerup",
               "payload" : {
                  "fluentd_tag" : "asgard.app.sieve.captura.kirby.powerup",
                  "log" : "  File \"/usr/local/lib/python3.6/site-packages/aioamqp/protocol.py\", line 235, in get_frame",
                  "container_id" : "d1b2c3ef17f86a3e5b6b16b4b6a566310f133ec399d9666ddcf003da0bfa9c77",
                  "container_name" : "/mesos-d7bf305c-2e34-4597-b5dc-e1cb3144c6b9",
                  "source" : "stderr",
                  "parse_error" : "true"
               },
               "timestamp" : 1530108596
            }
        """
        expected_document = {
            "timestamp" : "2018-06-27T14:09:56+00:00",
            "asgard_index_delay": ANY,
            "appname": ANY,
            "fluentd_tag" : "asgard.app.sieve.captura.kirby.powerup",
            "log" : "  File /usr/local/lib/python3.6/site-packages/aioamqp/protocol.py, line 235, in get_frame",
            "container_id" : "d1b2c3ef17f86a3e5b6b16b4b6a566310f133ec399d9666ddcf003da0bfa9c77",
            "container_name" : "/mesos-d7bf305c-2e34-4597-b5dc-e1cb3144c6b9",
            "source" : "stderr",
            "parse_error" : "true"
        }
        prepared_document = self.indexer._prepare_document(self.logmessage_parse_error)
        self.assertDictEqual(expected_document, prepared_document)

    def test_prepare_indexed_document_add_delay_key_log_ok(self):
        """
        Vamos adicionar uma nova chave: `asgard_index_delay` que será
        a diferença de tempo entre o `timestamp` (que é o momento em que o log foi processado
        pelo fluentd) e o momento em que o log-ingestor processou o log.
        O tempo será em segundos
        """
        with freeze_time("2018-06-21T19:26:06+00:00"):
            prepared_document = self.indexer._prepare_document(self.logmessage_parse_ok)
            self.assertEqual(0, prepared_document['asgard_index_delay'])

        with freeze_time("2018-06-21T19:26:20+00:00"):
            prepared_document = self.indexer._prepare_document(self.logmessage_parse_ok)
            self.assertEqual(14, prepared_document['asgard_index_delay'])

        with freeze_time("2018-06-21T19:27:10+00:00"):
            prepared_document = self.indexer._prepare_document(self.logmessage_parse_ok)
            self.assertEqual(64, prepared_document['asgard_index_delay'])

    def test_prepare_indexed_document_add_delay_key_log_error(self):
        with freeze_time("2018-06-27T14:09:56+00:00"):
            prepared_document = self.indexer._prepare_document(self.logmessage_parse_error)
            self.assertEqual(0, prepared_document['asgard_index_delay'])

        with freeze_time("2018-06-27T14:10:10+00:00"):
            prepared_document = self.indexer._prepare_document(self.logmessage_parse_error)
            self.assertEqual(14, prepared_document['asgard_index_delay'])

        with freeze_time("2018-06-27T14:11:00+00:00"):
            prepared_document = self.indexer._prepare_document(self.logmessage_parse_error)
            self.assertEqual(64, prepared_document['asgard_index_delay'])

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

