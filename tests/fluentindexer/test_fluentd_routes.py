import os
import importlib

from asynctest import mock
import asynctest

from asyncworker.rabbitmq.message import RabbitMQMessage

from fluentdindexer import routes
from logingestor import conf


class FluentdMonitoringIndexerRoutesTest(asynctest.TestCase):
    async def test_calls_index_bulk_passing_received_message_bodies(self):
        indexer_bulk_mock = mock.CoroutineMock()
        with mock.patch.object(routes.indexer, "bulk", indexer_bulk_mock):
            messages = [
                RabbitMQMessage(
                    body={
                        "timestamp": 1_530_132_385,
                        "key": "asgard.app.my.app",
                        "payload": {"field": "value"},
                    },
                    delivery_tag=10,
                ),
                RabbitMQMessage(
                    body={
                        "timestamp": 1_530_132_385,
                        "key": "asgard.app.my.app",
                        "payload": {"field": "other-value"},
                    },
                    delivery_tag=11,
                ),
            ]
            await routes.fluentd_monitoring_events_indexer(messages)
            self.assertEqual(
                messages, list(indexer_bulk_mock.await_args_list[0][0][0])
            )

    async def test_app_uses_right_configs(self):

        with mock.patch.dict(
            os.environ,
            FLUENTD_INDEXER_RABBITMQ_HOST="10.0.0.42",
            FLUENTD_INDEXER_RABBITMQ_USER="myuser",
            FLUENTD_INDEXER_RABBITMQ_PWD="secret",
            FLUENTD_INDEXER_RABBITMQ_VHOST="myvhost",
            FLUENTD_INDEXER_RABBITMQ_PREFETCH="1024",
            FLUENTD_INDEXER_BULK_SIZE="64",
            FLUENTD_INDEXER_QUEUE_NAMES="fluentd.internal.monitoring",
        ):
            importlib.reload(conf)
            importlib.reload(routes)
            self.assertEqual("10.0.0.42", routes.app.host)
            self.assertEqual("myuser", routes.app.user)
            self.assertEqual("secret", routes.app.password)
            self.assertEqual(1024, routes.app.prefetch_count)
            self.assertEqual(
                "myvhost",
                routes.app.routes_registry[
                    routes.fluentd_monitoring_events_indexer
                ]["options"]["vhost"],
            )
            self.assertEqual(
                64,
                routes.app.routes_registry[
                    routes.fluentd_monitoring_events_indexer
                ]["options"]["bulk_size"],
            )
            self.assertEqual(
                ["fluentd.internal.monitoring"],
                routes.app.routes_registry[
                    routes.fluentd_monitoring_events_indexer
                ]["route"],
            )
