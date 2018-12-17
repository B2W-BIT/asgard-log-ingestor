import asynctest
from asynctest.mock import CoroutineMock
from asynctest import mock
import json
from freezegun import freeze_time

from freezegun import freeze_time

from logingestor.indexer import Indexer, AppIndexer
from asyncworker.rabbitmq.message import RabbitMQMessage

from logingestor import conf

class BaseIndexerTest(asynctest.TestCase):

    def setUp(self):
        self.elasticsearch_mock = CoroutineMock(index=CoroutineMock(), bulk=CoroutineMock())
        self.elasticsearch_mock.bulk.return_value = {"items": [], "errors": False}
        class MyIndexer(Indexer):
            def _index_name(self, document):
                return "myindex"
            def _extract_appname(self, doc):
                return "/asgard/myapp"

        self.logger_mock = mock.CoroutineMock(info=mock.CoroutineMock(), error=mock.CoroutineMock())
        self.indexer = MyIndexer(self.elasticsearch_mock, self.logger_mock)
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

    async def tearDown(self):
        mock.patch.stopall()

    async def test_call_extract_appname_when_needed(self):
        class MyIndexer(Indexer):
            def _extract_appname(self, document):
                return "/infra/asgard/collector"

        indexer = MyIndexer(self.elasticsearch_mock, self.logger_mock)
        self.assertEqual("/infra/asgard/collector", indexer._app_name_with_namespace({}))

    async def test_calls_index_bulk_passing_received_message_bodies(self):
        """
        Checamos que a preparação do bulk insert usa o message.body de cada RabbitMQMessage
        """
        indexer_bulk_mock = mock.CoroutineMock()
        messages = [
            RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "value"}}, delivery_tag=10),
            RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "other-value"}}, delivery_tag=11),
        ]
        expected_bulk_data = [
            { "index" : { "_index" : "myindex", "_type" : "logs"}},
            messages[0].body,
            { "index" : { "_index" : "myindex", "_type" : "logs"}},
            messages[1].body,
        ]

        indexer_bulk_mock.return_value = {"items": [], "errors": False}
        await self.indexer.bulk(messages)
        self.assertEqual(expected_bulk_data, list(self.elasticsearch_mock.bulk.await_args_list[0][0][0]))

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

        self.elasticsearch_mock.bulk.return_value = {"errors": False}
        indexer = MyIndexer(self.elasticsearch_mock, self.logger_mock)
        with mock.patch.object(conf, "BULK_INSERT_TIMEOUT", 60):
            returned_by_elasticsearch = await indexer.bulk([CoroutineMock(body={"key": "errors.asgard.app.sieve.captura.kirby.powerup", "some-value": 10})])
            self.assertEqual([mock.call([
                { "index" : { "_index" : "my-important-index", "_type" : "logs"}},
                {"some-value": 10, "new-key": 42},
            ], request_timeout=conf.BULK_INSERT_TIMEOUT)], self.elasticsearch_mock.bulk.await_args_list)
            self.assertEqual({"errors": False}, returned_by_elasticsearch)

    async def test_confirms_bulk_insert_uses_timeout_config(self):
        class MyOtherIndexer(Indexer):
            def _index_name(self, document):
                return "my-important-index"

        indexer = MyOtherIndexer(self.elasticsearch_mock, self.logger_mock)
        await indexer.bulk([CoroutineMock(body={"key": "errors.asgard.app.sieve.captura.kirby.powerup", "some-value": 10})])
        self.assertEqual([mock.call([
            { "index" : { "_index" : "my-important-index", "_type" : "logs"}},
            {"key": "errors.asgard.app.sieve.captura.kirby.powerup", "some-value": 10}], request_timeout=conf.BULK_INSERT_TIMEOUT)], self.elasticsearch_mock.bulk.await_args_list)

    async def test_confirms_default_prepare_document_implementation(self):
        class MyOtherIndexer(Indexer):
            def _index_name(self, document):
                return "my-important-index"

        indexer = MyOtherIndexer(self.elasticsearch_mock, self.logger_mock)
        documents = [CoroutineMock(body={"key": "errors.asgard.app.sieve.captura.kirby.powerup", "some-value": 10})]
        await indexer.bulk(documents)
        self.assertEqual([mock.call([
            { "index" : { "_index" : "my-important-index", "_type" : "logs"}},
            {"key": "errors.asgard.app.sieve.captura.kirby.powerup", "some-value": 10},
        ], request_timeout=conf.BULK_INSERT_TIMEOUT)], self.elasticsearch_mock.bulk.await_args_list)

    async def test_if_result_does_not_have_errors_dont_iterate_it(self):
        """
        Se o ES retornar indicando que não há erros, não precisamos iterar todas as mensagens
        chamando `.reject()`, afinal todas foram indexadas com sucesso
        """
        indexer_bulk_mock = mock.CoroutineMock()
        with mock.patch.object(self.indexer, "bulk_", indexer_bulk_mock, create=True):
            messages = [
                CoroutineMock(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "value"}}, delivery_tag=10),
                CoroutineMock(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "other-value"}}, delivery_tag=11),
                CoroutineMock(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "other-value"}}, delivery_tag=12)
            ]
            expected_bodies = list((m.body for m in messages))
            indexer_bulk_mock.return_value = {"errors": False}

            await self.indexer.bulk(messages)
            self.assertEqual(0, messages[0].reject.call_count)
            self.assertEqual(0, messages[1].reject.call_count)
            self.assertEqual(0, messages[2].reject.call_count)

    async def test_should_index_a_different_document_for_messages_with_index_errors(self):
        """
        Para documentos que o ES se recusou a indexar, geramos um novo bulk contendo
        os documentos que causaram erro e o motivo de cada um dos erros.

        O Documento é indexado no índice da app original, e tem a seguinite estrutura:

        {
          "asgard": {
            "original": {
              "msg": "<resultdo do json.dumps(msg_orignial)>"
            },
            "error": {
              {
                "type": "mapper_parsing_exception",
                "reason": "failed to parse [msg.website_id]",
                "caused_by": {
                  "type": "number_format_exception",
                  "reason": "For input string: "abc""
                }
              }
            }
          }
        }
        """
        messages = [
            CoroutineMock(body={"timestamp": 1530132385, "key": "asgard.app.infra.my.app", "payload": {"field": "value"}}, delivery_tag=10),
            CoroutineMock(body={"timestamp": 1530132385, "key": "asgard.app.infra.my.app", "payload": {"field": "other-value"}}, delivery_tag=11),
        ]
        self.elasticsearch_mock.bulk.return_value = {
               "took": 30,
               "errors": True,
               "items": [
                {
                   "index" : {
                      "error" : {
                         "type" : "illegal_argument_exception",
                         "reason" : "Cant merge a non object mapping [marketplace_sellers] with an object mapping [marketplace_sellers]"
                      },
                      "_type" : "logs",
                      "_id" : "AWRDEIdY6_jLl57Ht_Gj",
                      "status" : 400,
                      "_index" : "asgard-app-logs-sieve-captura-wetl-updater-marketplace-2018-06-27"
                   }
                },
                  {
                     "index": {
                        "_index": "test",
                        "_type": "_doc",
                        "_id": "1",
                        "_version": 2,
                        "result": "updated",
                        "_shards": {
                            "total": 2,
                            "successful": 1,
                            "failed": 1
                        },
                        "status": 200,
                        "_seq_no" : 3,
                        "_primary_term" : 4
                     }
                  }
               ]
            }

        indexer = AppIndexer(self.elasticsearch_mock, self.logger_mock)
        await indexer.bulk(messages)
        self.assertEqual(self.elasticsearch_mock.bulk.await_count, 2)
        expected_second_bulk_call = mock.call([
            { "index" : { "_index" : indexer._index_name(messages[0].body), "_type" : "logs"}},
            { "asgard": {
                            "original": {
                                "msg": json.dumps(messages[0].body['payload'])
                            },
                            "error":  {
                                 "type" : "illegal_argument_exception",
                                 "reason" : "Cant merge a non object mapping [marketplace_sellers] with an object mapping [marketplace_sellers]"
                          },
                            "index_error": True,
                        },
                        "timestamp": "2018-06-27T20:46:25+00:00",
                        "appname": "/infra/my/app",
                        "asgard_index_delay": mock.ANY
            }
        ], request_timeout=conf.BULK_INSERT_TIMEOUT)
        self.maxDiff = None
        self.assertEqual(expected_second_bulk_call, self.elasticsearch_mock.bulk.await_args_list[1])

    def test_prepare_indexed_document_add_delay_key_log_error(self):
        with freeze_time("2018-06-27T14:09:56+00:00"):
            prepared_document = self.indexer.prepare_document(self.logmessage_parse_error)
            self.assertEqual(0, prepared_document['asgard_index_delay'])

        with freeze_time("2018-06-27T14:10:10+00:00"):
            prepared_document = self.indexer.prepare_document(self.logmessage_parse_error)
            self.assertEqual(14, prepared_document['asgard_index_delay'])

        with freeze_time("2018-06-27T14:11:00+00:00"):
            prepared_document = self.indexer.prepare_document(self.logmessage_parse_error)
            self.assertEqual(64, prepared_document['asgard_index_delay'])

    @freeze_time("2018-06-21T19:26:06+00:00")
    def test_prepare_indexed_document_do_not_override_special_fields(self):
        self.maxDiff = None
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
        indexer = AppIndexer(self.elasticsearch_mock, self.logger_mock)
        prepared_document = indexer.prepare_document(self.logmessage_parse_ok)
        self.assertDictEqual(expected_document, prepared_document)

    def test_prepare_indexed_document_add_delay_key_log_ok(self):
        """
        Vamos adicionar uma nova chave: `asgard_index_delay` que será
        a diferença de tempo entre o `timestamp` (que é o momento em que o log foi processado
        pelo fluentd) e o momento em que o log-ingestor processou o log.
        O tempo será em segundos
        """
        indexer = AppIndexer(self.elasticsearch_mock, self.logger_mock)
        with freeze_time("2018-06-21T19:26:06+00:00"):
            prepared_document = indexer.prepare_document(self.logmessage_parse_ok)
            self.assertEqual(0, prepared_document['asgard_index_delay'])

        with freeze_time("2018-06-21T19:26:20+00:00"):
            prepared_document = indexer.prepare_document(self.logmessage_parse_ok)
            self.assertEqual(14, prepared_document['asgard_index_delay'])

        with freeze_time("2018-06-21T19:27:10+00:00"):
            prepared_document = indexer.prepare_document(self.logmessage_parse_ok)
            self.assertEqual(64, prepared_document['asgard_index_delay'])
