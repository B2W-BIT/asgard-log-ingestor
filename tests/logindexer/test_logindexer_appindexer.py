import json
from freezegun import freeze_time
import asynctest
from asynctest.mock import CoroutineMock, ANY
from asynctest import mock

from logingestor.indexer import Indexer, AppIndexer

class LogIndexerTest(asynctest.TestCase):


    def setUp(self):
        self.elasticsearch_mock = CoroutineMock(index=CoroutineMock(), bulk=CoroutineMock())
        self.indexer = AppIndexer(self.elasticsearch_mock, CoroutineMock())
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
        prepared_document = self.indexer.prepare_document(self.logmessage_parse_ok)
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
        prepared_document = self.indexer.prepare_document(self.logmessage_parse_error)
        self.assertDictEqual(expected_document, prepared_document)

    async def test_extracts_appname(self):
        self.assertEqual("/infra/apps/myapp", self.indexer._app_name_with_namespace({"key": "asgard.app.infra.apps.myapp"}))
        self.assertEqual("/infra/otherapp/app", self.indexer._app_name_with_namespace({"key": "errors.asgard.app.infra.otherapp.app"}))
        self.assertEqual("/fluentd/internal/monitoring", self.indexer._app_name_with_namespace({"key": "fluentd.internal.monitoring"}))

