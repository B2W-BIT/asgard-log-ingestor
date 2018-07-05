import asynctest
from asynctest.mock import CoroutineMock
from asynctest import mock
import json

from freezegun import freeze_time

from logingestor.indexer import Indexer
from asyncworker.rabbitmq.message import RabbitMQMessage

class BaseIndexerTest(asynctest.TestCase):

    def setUp(self):
        self.elasticsearch_mock = CoroutineMock(index=CoroutineMock(), bulk=CoroutineMock())
        class MyIndexer(Indexer):
            def _index_name(self, document):
                return "myindex"

        self.logger_mock = mock.CoroutineMock(info=mock.CoroutineMock(), error=mock.CoroutineMock())
        self.indexer = MyIndexer(self.elasticsearch_mock, self.logger_mock)
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

    async def tearDown(self):
        mock.patch.stopall()

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
        returned_by_elasticsearch = await indexer.bulk([CoroutineMock(body={"key": "errors.asgard.app.sieve.captura.kirby.powerup", "some-value": 10})])
        self.assertEqual([mock.call([
            { "index" : { "_index" : "my-important-index", "_type" : "logs"}},
            {"some-value": 10, "new-key": 42},
        ])], self.elasticsearch_mock.bulk.await_args_list)
        self.assertEqual({"errors": False}, returned_by_elasticsearch)

    async def test_confirms_default_prepare_document_implementation(self):
        class MyOtherIndexer(Indexer):
            def _index_name(self, document):
                return "my-important-index"

        indexer = MyOtherIndexer(self.elasticsearch_mock, self.logger_mock)
        await indexer.bulk([CoroutineMock(body={"key": "errors.asgard.app.sieve.captura.kirby.powerup", "some-value": 10})])
        self.assertEqual([mock.call([
            { "index" : { "_index" : "my-important-index", "_type" : "logs"}},
            {"key": "errors.asgard.app.sieve.captura.kirby.powerup", "some-value": 10},
        ])], self.elasticsearch_mock.bulk.await_args_list)

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

    async def test_only_logs_one_error_per_bulk_insert(self):
        """
        Mesmo que tenhamos mais de uma mensagem rejeitada em um mesmo bulk, logamos apenas o primeiro erro.
        """
        messages = [
            RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "value"}}, delivery_tag=10),
            RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "other-value"}}, delivery_tag=11),
            RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "some-other-value"}}, delivery_tag=11),
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
                "index" : {
                   "_type" : "logs",
                   "_id" : "AWRDEIdY6_jLl57Ht_Gj",
                   "status" : 400,
                   "_index" : "asgard-app-logs-sieve-captura-wetl-updater-marketplace-2018-06-27"
                }
             }
            ]
         }
        await self.indexer.bulk(messages)
        self.assertEqual(1, self.logger_mock.error.await_count)
        self.assertEqual("value", self.logger_mock.error.await_args_list[0][0][0]['original-message']['payload']['field'])

    async def test_log_one_message_per_batch_processed(self):
        indexer_bulk_mock = mock.CoroutineMock()
        messages = [
            RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "value"}}, delivery_tag=10),
            RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "other-value"}}, delivery_tag=11),
        ]
        #expected_bodies = list((m.body for m in messagemsessages))
        self.elasticsearch_mock.bulk.return_value = {"errors": False}
        await self.indexer.bulk(messages)
        self.assertEqual([mock.call({'messages-processed': 2, 'accepted-messages': 2, 'rejected': 0, 'errors': False})], self.logger_mock.info.await_args_list)

    async def test_rejects_some_messages_if_elastic_search_returns_some_errors(self):
        messages = [
            RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "value"}}, delivery_tag=10),
            RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "other-value"}}, delivery_tag=11),
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

        await self.indexer.bulk(messages)
        self.assertTrue(messages[1]._do_ack)
        self.assertFalse(messages[0]._do_ack)
