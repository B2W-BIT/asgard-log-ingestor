import os
import importlib

from asynctest import mock
import asynctest


from asyncworker.rabbitmq.message import RabbitMQMessage

from statsindexer import routes
from logingestor import conf

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

    async def test_app_uses_right_configs(self):

        with mock.patch.dict(os.environ,
                        STATS_RABBITMQ_HOST="10.0.0.42",
                        STATS_RABBITMQ_USER="myuser",
                        STATS_RABBITMQ_PWD="secret",
                        STATS_RABBITMQ_VHOST="myvhost",
                        STATS_RABBITMQ_PREFETCH="1024",
                        STATS_BULK_SIZE="64",
                        STATS_QUEUE_NAMES="asgard/counts, asgard/counts/errors,  asgard/other   "):
            importlib.reload(conf)
            importlib.reload(routes)
            self.assertEqual("10.0.0.42", routes.app.host)
            self.assertEqual("myuser", routes.app.user)
            self.assertEqual("secret", routes.app.password)
            self.assertEqual(1024, routes.app.prefetch_count)
            self.assertEqual("myvhost", routes.app.routes_registry[routes.app_stats_indexer_handler]['options']['vhost'])
            self.assertEqual(64, routes.app.routes_registry[routes.app_stats_indexer_handler]['options']['bulk_size'])

