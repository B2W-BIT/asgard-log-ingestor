from asynctest import mock
import asynctest


from asyncworker.rabbitmq.message import RabbitMQMessage

from statsindexer import routes

class StatsIndexerRoutesTest(asynctest.TestCase):

    async def test_calls_index_bulk_passing_received_message_bodies(self):
        indexer_bulk_mock = mock.CoroutineMock()
        with mock.patch.object(routes.indexer, "bulk", indexer_bulk_mock):
            messages = [
                RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "value"}}, delivery_tag=10),
                RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "other-value"}}, delivery_tag=11),
            ]
            expected_bodies = list((m.body for m in messages))
            indexer_bulk_mock.return_value = {"items": [], "errors": False}
            await routes.app_stats_indexer_handler(messages)
            indexer_bulk_mock.assert_awaited()
            self.assertEqual(expected_bodies, list(indexer_bulk_mock.await_args_list[0][0][0]))
