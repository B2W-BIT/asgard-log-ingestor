import os
import importlib

import asynctest
from asynctest import mock

from asyncworker.rabbitmq.message import RabbitMQMessage

from logingestor import conf
from logingestor import routes

class RoutesTest(asynctest.TestCase):

    async def test_calls_index_bulk_passing_received_message_objects(self):
        logger_function_mock = mock.CoroutineMock()
        indexer_bulk_mock = mock.CoroutineMock()
        with mock.patch.object(routes.indexer, "bulk", indexer_bulk_mock), \
                mock.patch.object(routes, "logger_function", logger_function_mock):
            messages = [
                RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "value"}}, delivery_tag=10),
                RabbitMQMessage(body={"timestamp": 1530132385, "key": "asgard.app.my.app", "payload": {"field": "other-value"}}, delivery_tag=11),
            ]
            await routes.generic_app_log_indexer(messages)
            self.assertEqual(messages, list(indexer_bulk_mock.await_args_list[0][0][0]))

            expected_logger_function_call = mock.call(conf.logger, 2, exc_tb=None, exc_type=None, exc_val=None, transactions={'bulk_index_time': mock.ANY})
            self.assertEqual([expected_logger_function_call], logger_function_mock.await_args_list)


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
            amqp_routes = routes.app.routes_registry.amqp_routes
            my_route = list(filter(lambda r: r["handler"] == routes.generic_app_log_indexer, amqp_routes))[0]
            self.assertEqual("10.0.0.42", routes.app.host)
            self.assertEqual("myuser", routes.app.user)
            self.assertEqual("secret", routes.app.password)
            self.assertEqual(1024, routes.app.prefetch_count)
            self.assertEqual("myvhost", my_route['options']['vhost'])
            self.assertEqual(64, my_route['options']['bulk_size'])

    async def test_logger_funcion_generates_correct_logline(self):
        logger_mock = mock.CoroutineMock(info=mock.CoroutineMock())
        await routes.logger_function(logger_mock, 42, transactions={"inner-transaction": 2, "outer-transaction": 4})
        self.assertEqual([mock.call({
            "event": "bulk_log_index",
            "messages": 42,
            "inner-transaction": 2,
            "outer-transaction": 4
        })], logger_mock.info.await_args_list)
