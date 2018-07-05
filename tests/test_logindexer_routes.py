import os
import importlib

import asynctest
from asynctest import mock

from asyncworker.rabbitmq.message import RabbitMQMessage

from logingestor import conf
from logingestor import routes

class RoutesTest(asynctest.TestCase):

    async def test_calls_index_bulk_passing_received_message_objects(self):
        indexer_bulk_mock = mock.CoroutineMock()
        with mock.patch.object(routes.indexer, "bulk", indexer_bulk_mock):
            messages = [
                RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "value"}}, delivery_tag=10),
                RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "other-value"}}, delivery_tag=11),
            ]
            await routes.generic_app_log_indexer(messages)
            self.assertEqual(messages, list(indexer_bulk_mock.await_args_list[0][0][0]))

    async def test_app_uses_right_configs(self):

        with mock.patch.dict(os.environ,
                        LOGS_RABBITMQ_HOST="10.0.0.42",
                        LOGS_RABBITMQ_USER="myuser",
                        LOGS_RABBITMQ_PWD="secret",
                        LOGS_RABBITMQ_VHOST="myvhost",
                        LOGS_RABBITMQ_PREFETCH="1024",
                        LOGS_BULK_SIZE="64",
                        LOGS_QUEUE_NAMES="asgard/counts, asgard/counts/errors,  asgard/other   "):
            importlib.reload(conf)
            importlib.reload(routes)
            self.assertEqual("10.0.0.42", routes.app.host)
            self.assertEqual("myuser", routes.app.user)
            self.assertEqual("secret", routes.app.password)
            self.assertEqual(1024, routes.app.prefetch_count)
            self.assertEqual("myvhost", routes.app.routes_registry[routes.generic_app_log_indexer]['options']['vhost'])
            self.assertEqual(64, routes.app.routes_registry[routes.generic_app_log_indexer]['options']['bulk_size'])

    async def test_sets_logger_on_indexer(self):
        with mock.patch.object(routes.indexer, "bulk", mock.CoroutineMock()):
            logger_mock = mock.CoroutineMock()
            self.assertIsNone(routes.indexer.logger)
            messages = [mock.CoroutineMock()]
            conf.logger = logger_mock
            await routes.generic_app_log_indexer(messages)
            self.assertEqual(logger_mock, routes.indexer.logger)


