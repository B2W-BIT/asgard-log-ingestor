import os
import importlib

import asynctest
from asynctest import mock

from logingestor import conf


class CommonConfTest(asynctest.TestCase):
    def test_load_bulk_timeout_as_integer(self):
        with mock.patch.dict(os.environ, INDEXER_BULK_INSERT_TIMEOUT="60"):
            importlib.reload(conf)
            self.assertEqual(60, conf.BULK_INSERT_TIMEOUT)


class StatsIndexerConfTest(asynctest.TestCase):
    def test_test_read_correct_configs(self):
        """
        Confirma que pegamos as configs da envs corretas.
        """
        with mock.patch.dict(
            os.environ,
            STATS_RABBITMQ_HOST="10.0.0.42",
            STATS_RABBITMQ_USER="myuser",
            STATS_RABBITMQ_PWD="secret",
            STATS_RABBITMQ_VHOST="myvhost",
            STATS_RABBITMQ_PREFETCH="1024",
            STATS_BULK_SIZE="64",
            STATS_QUEUE_NAMES="asgard/counts, asgard/counts/errors,  asgard/other   ",
            STATS_INDEX_PREFIX="asgard-app-stats-indexer",
        ):
            importlib.reload(conf)
            self.assertEqual("10.0.0.42", conf.STATS_RABBITMQ_HOST)
            self.assertEqual("myuser", conf.STATS_RABBITMQ_USER)
            self.assertEqual("secret", conf.STATS_RABBITMQ_PWD)
            self.assertEqual("myvhost", conf.STATS_RABBITMQ_VHOST)
            self.assertEqual(1024, conf.STATS_RABBITMQ_PREFETCH)
            self.assertEqual(64, conf.STATS_BULK_SIZE)
            self.assertEqual(
                ["asgard/counts", "asgard/counts/errors", "asgard/other"],
                conf.STATS_QUEUE_NAMES,
            )
            self.assertEqual(
                "asgard-app-stats-indexer", conf.STATS_INDEX_PREFIX
            )
