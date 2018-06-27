import asynctest
from asynctest import mock

from asyncworker.rabbitmq.message import RabbitMQMessage

from logingestor import routes

class RoutesTest(asynctest.TestCase):

    async def test_calls_index_bulk_passing_received_message_bodies(self):
        self.fail()

    def test_if_result_does_not_have_errors_dont_iterate_it(self):
        """
        Se o ES retornar indicando que não há erros, não precisamos iterar todas as mensagens
        chamando `.reject()`, afinal todas foram indexadas com sucesso
        """
        self.fail()

    def test_only_logs_one_error_per_bulk_insert(self):
        """
        Mesmo que tenhamos mais de uma mensagem rejeitada em um mesmo bulk, logamos apenaso primeiro erro.
        """
        self.fail()

    async def test_tejects_some_messages_if_elastic_search_returns_some_errors(self):
        messages = [
            RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "value"}}, delivery_tag=10),
            RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "other-value"}}, delivery_tag=11),
        ]
        es_mock = mock.CoroutineMock(bulk=mock.CoroutineMock())
        es_mock.bulk.return_value = {
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
        with mock.patch.object(routes.indexer, "elasticsearch", es_mock):
            await routes.generic_app_log_indexer(messages)
            self.assertTrue(messages[1]._do_ack)
            self.assertFalse(messages[0]._do_ack)
