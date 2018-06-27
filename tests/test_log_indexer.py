import json
from freezegun import freeze_time
import asynctest
from asynctest.mock import CoroutineMock

from logingestor.indexer import AppIndexer

class LogIndexerTest(asynctest.TestCase):


    def setUp(self):
        self.elasticsearch_mock = CoroutineMock(index=CoroutineMock(), bulk=CoroutineMock())
        self.indexer = AppIndexer(self.elasticsearch_mock)
        self.logmessage_parse_ok = json.loads(
        """
            {
               "timestamp" : 1529609166,
               "payload" : {
                  "appname" : "/sieve/captura/seller/feed-buybox/buybox-stream-reader",
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

    @freeze_time("2018-06-27T10:00:00+00:00")
    def test_test_generate_correct_index_name(self):
        """
        Index name: asgard-app-logs-<namespace>-<appname>
        Trocamos "/" por "-"
        """
        self.assertEqual("asgard-app-logs-infra-asgard-logs-counts-2018-06-27", self.indexer._index_name("asgard.app.infra.asgard.logs.counts"))
        self.assertEqual("asgard-app-logs-infra-asgard-logs-counts-2018-06-27", self.indexer._index_name("errors.asgard.app.infra.asgard.logs.counts"))

    def test_prepare_indexed_document_log_parse_ok(self):
        """

            Exemplo de log que foi corretamente parseado pelo fluentd
            {
               "timestamp" : 1529609166,
               "payload" : {
                  "appname" : "/sieve/captura/seller/feed-buybox/buybox-stream-reader",
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
        self.fail()

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
        self.fail()

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

    def test_call_bulk_insert(self):
        """
        Dado uma lista de documentos a serem indexados,
        chamamos o `.bulk()` com o conteúdo correto, que é
        uma linha de "action" para cada documento que está sendo
        indexado.
        Exemplo de Action:
        { "index" : { "_index" : "asgard-app-logs-calculator-timing", "_type" : "logs"}}
        """
        self.fail()
